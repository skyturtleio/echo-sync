/**
 * The application the simulation runs: one `counters` table plus the `scope_grants` system
 * table, and intent-shaped mutations (increment/transfer are wrong if replayed over a server
 * result that already contains them).
 *
 * `runMutation` is the shared executor: the same handler runs optimistically on a client
 * replica and authoritatively on the reference server, with scope-role checks against the
 * grant rows of whichever store it runs on, and first-touch undo images for every write.
 * TEMPORARY stand-in for core's `Store.make` + `MutationRegistry.run`.
 */
import { Array as Arr, Effect, Option, Schema } from "effect"
import type { EncodedRow, MutationEnvelope, MutationRejection, UndoImage } from "../../client/sync/Wire.ts"
import type { MemoryStore } from "./MemoryStore.ts"

export const COUNTERS = "counters"
export const GRANTS = "scope_grants"
export const ANYONE = "*"
/** A table the server syncs but the clients' registry does not have (an older app version). */
export const NOTES = "notes"
/** The clients' registry. */
export const knownTable = (name: string) => name === COUNTERS || name === GRANTS

export type Role = "reader" | "writer" | "owner"

export const grantId = (scopeId: string, principal: string) => `${scopeId}|${principal}`

export const grantRow = (scopeId: string, principal: string, role: Role): EncodedRow => ({
  id: grantId(scopeId, principal),
  access_scope_id: scopeId,
  principal_id: principal,
  role
})

const Counter = Schema.Struct({
  id: Schema.String,
  access_scope_id: Schema.String,
  value: Schema.Number,
  updated_at: Schema.Number
})
type Counter = typeof Counter.Type

const GrantRole = Schema.Struct({ role: Schema.Literals(["reader", "writer", "owner"]) })

export const Args = {
  create: Schema.Struct({ id: Schema.String, scope: Schema.String }),
  increment: Schema.Struct({ id: Schema.String, by: Schema.Number }),
  transfer: Schema.Struct({ from: Schema.String, to: Schema.String, amount: Schema.Number }),
  move: Schema.Struct({ id: Schema.String, scope: Schema.String }),
  remove: Schema.Struct({ id: Schema.String })
}
export type MutationName = keyof typeof Args

class Rejected extends Schema.TaggedError<Rejected>()("Rejected", {
  reason: Schema.Literals(["ArgsDecodeFailed", "UnknownMutation", "RowNotFound", "ScopeForbidden", "Exists"]),
  detail: Schema.String
}) {}

export const roleOf = (store: MemoryStore, principal: string, scopeId: string) =>
  Effect.gen(function* () {
    let best: Option.Option<Role> = Option.none()
    for (const who of [principal, ANYONE]) {
      const row = yield* store.read(GRANTS, grantId(scopeId, who))
      const decoded = Option.flatMap(row, Schema.decodeUnknownOption(GrantRole))
      if (Option.isSome(decoded)) {
        const role = decoded.value.role
        if (Option.isNone(best) || rank[role] > rank[best.value]) best = Option.some(role)
      }
    }
    return best
  })

const rank: Record<Role, number> = { reader: 1, writer: 2, owner: 3 }

export interface RunResult {
  readonly undo: ReadonlyArray<UndoImage>
  readonly rejection: Option.Option<MutationRejection>
}

/**
 * Runs one mutation against `store` as `principal` with `ctx.now = now`. Writes go straight
 * to the store (the caller owns the transaction); on rejection the partial writes remain and
 * the caller restores them from `undo`. The server derives log entries from `undo` plus the
 * rows' state after commit.
 */
export const runMutation = (
  store: MemoryStore,
  principal: string,
  envelope: MutationEnvelope,
  now: number
): Effect.Effect<RunResult> => {
  const undo: Array<UndoImage> = []
  const touched = new Set<string>()

  const canWrite = (scopeId: string) =>
    Effect.gen(function* () {
      const role = yield* roleOf(store, principal, scopeId)
      if (Option.isNone(role) || role.value === "reader") {
        return yield* new Rejected({ reason: "ScopeForbidden", detail: scopeId })
      }
    })
  const get = (id: string) =>
    Effect.gen(function* () {
      const row = yield* store.read(COUNTERS, id)
      if (Option.isNone(row)) return yield* new Rejected({ reason: "RowNotFound", detail: id })
      const role = yield* roleOf(store, principal, row.value.access_scope_id)
      if (Option.isNone(role)) return yield* new Rejected({ reason: "ScopeForbidden", detail: id })
      return yield* Schema.decodeUnknownEffect(Counter)(row.value).pipe(Effect.orDie)
    })
  const write = (id: string, before: Option.Option<EncodedRow>, row: Option.Option<Counter>) =>
    Effect.gen(function* () {
      if (Option.isSome(before)) yield* canWrite(before.value.access_scope_id)
      if (Option.isSome(row)) yield* canWrite(row.value.access_scope_id)
      if (!touched.has(id)) {
        touched.add(id)
        undo.push({ table: COUNTERS, id, before })
      }
      yield* store.write(COUNTERS, id, row)
    })
  const put = (row: Counter) =>
    Effect.gen(function* () {
      const before = yield* store.read(COUNTERS, row.id)
      yield* write(row.id, before, Option.some({ ...row, updated_at: now }))
    })

  const argsError = Effect.mapError(() => new Rejected({ reason: "ArgsDecodeFailed", detail: envelope.name }))

  const handler = Effect.gen(function* () {
    switch (envelope.name) {
      case "create": {
        const { id, scope } = yield* Schema.decodeUnknownEffect(Args.create)(envelope.args).pipe(argsError)
        const existing = yield* store.read(COUNTERS, id)
        if (Option.isSome(existing)) return yield* new Rejected({ reason: "Exists", detail: id })
        return yield* put({ id, access_scope_id: scope, value: 0, updated_at: now })
      }
      case "increment": {
        const { id, by } = yield* Schema.decodeUnknownEffect(Args.increment)(envelope.args).pipe(argsError)
        const row = yield* get(id)
        return yield* put({ ...row, value: row.value + by })
      }
      case "transfer": {
        const { from, to, amount } = yield* Schema.decodeUnknownEffect(Args.transfer)(envelope.args).pipe(argsError)
        const a = yield* get(from)
        yield* put({ ...a, value: a.value - amount })
        const b = yield* get(to)
        return yield* put({ ...b, value: b.value + amount })
      }
      case "move": {
        const { id, scope } = yield* Schema.decodeUnknownEffect(Args.move)(envelope.args).pipe(argsError)
        const row = yield* get(id)
        return yield* put({ ...row, access_scope_id: scope })
      }
      case "remove": {
        const { id } = yield* Schema.decodeUnknownEffect(Args.remove)(envelope.args).pipe(argsError)
        const before = yield* store.read(COUNTERS, id)
        yield* get(id)
        return yield* write(id, before, Option.none())
      }
      default:
        return yield* new Rejected({ reason: "UnknownMutation", detail: envelope.name })
    }
  })

  return handler.pipe(
    Effect.map(() => ({ undo, rejection: Option.none<MutationRejection>() })),
    Effect.catchTag("Rejected", (error) =>
      Effect.succeed({
        undo,
        rejection: Option.some<MutationRejection>({ _tag: error.reason, detail: error.detail })
      })
    )
  )
}

/** Restores pre-images in reverse write order. */
export const restore = (store: MemoryStore, undo: ReadonlyArray<UndoImage>) =>
  Effect.forEach(Arr.reverse(undo), (image) => store.write(image.table, image.id, image.before), { discard: true })
