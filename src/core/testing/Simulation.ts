/**
 * Deterministic simulation of N clients against the reference server (DESIGN §14).
 *
 * A seeded PRNG drives a schedule of local mutations, pushes (lost requests, transient stops
 * at random positions, lost responses), paginated pulls (failing between pages), server
 * out-of-band writes, grant/revoke/move, client reload (memory lost, disk kept), going
 * offline, log GC (forces resnapshot), optional ledger GC, and clock skew. Time is the
 * ambient `Clock` (run under `TestClock`), skewed per client.
 *
 * Invariants checked after every client step:
 * - replica = replay(pending, server state at the installed cursor restricted to the client's
 *   readable scopes). This one equation rules out flicker (an optimistic effect vanishing
 *   before its authoritative effect is installed), double optimistic application (replaying
 *   over a row that already contains the mutation), torn page sets, and stale rows.
 * - the outbox never holds a mutation whose ledger position is ≤ the installed cursor;
 * - no mutation is ever executed twice on the server.
 * After quiescence: every locally committed mutation executed exactly once, outboxes empty,
 * replicas equal the server restricted to readable scopes, and each server rejection
 * surfaced as exactly one notice.
 */
import { Array as Arr, Clock, Effect, Option, Order, Schema } from "effect"
import { TestClock } from "effect/testing"
import * as Engine from "../../client/sync/Engine.ts"
import type { EncodedRow, MutationEnvelope, PullDone, PullPage, PullResponse } from "../../client/sync/Wire.ts"
import * as MemoryStore from "./MemoryStore.ts"
import * as RefServer from "./RefServer.ts"
import { ANYONE, COUNTERS, GRANTS, grantId, grantRow, restore, roleOf, runMutation } from "./SimApp.ts"
import type { Role } from "./SimApp.ts"

export class TransportFailed extends Schema.TaggedError<TransportFailed>()("TransportFailed", {
  during: Schema.String
}) {}

export class InvariantViolated extends Schema.TaggedError<InvariantViolated>()("InvariantViolated", {
  seed: Schema.Number,
  step: Schema.Number,
  invariant: Schema.String,
  detail: Schema.String,
  trace: Schema.Array(Schema.String)
}) {
  override get message() {
    return `seed=${this.seed} step=${this.step}: ${this.invariant}: ${this.detail}\n  last events:\n    ${this.trace.join("\n    ")}`
  }
}

export interface Options {
  readonly seed: number
  readonly steps: number
  readonly pageSize?: number
  /** Deliberately wrong client that applies `PullPage`s as they arrive (proof the check bites). */
  readonly pullMode?: "staged" | "incremental"
  /** GC the ledger together with the log ("ledger GC'd while client offline"). */
  readonly ledgerGc?: boolean
}

export interface Stats {
  readonly mutations: number
  readonly resnapshots: number
  readonly reloadsMidSet: number
  readonly notices: number
  readonly transientStops: number
  readonly lostResponses: number
  /** Pending mutations released by an expired ledger row rather than an ack. */
  readonly expired: number
}

/** mulberry32: small, fast, good enough for schedules. */
const prng = (seed: number) => {
  let a = seed >>> 0
  const next = () => {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
  return {
    chance: (p: number) => next() < p,
    int: (n: number) => Math.floor(next() * n),
    pick: <A>(xs: ReadonlyArray<A>): Option.Option<A> => Option.fromNullishOr(xs[Math.floor(next() * xs.length)])
  }
}

const canonical = (tables: MemoryStore.Tables): string => {
  const lines: Array<string> = []
  for (const [table, rows] of tables) {
    for (const row of rows.values()) {
      const sorted = Arr.sort(Object.keys(row), Order.String).map((k) => `${k}=${JSON.stringify(row[k])}`)
      lines.push(`${table}/${row.id}{${sorted.join(",")}}`)
    }
  }
  return Arr.sort(lines, Order.String).join("\n")
}

const diff = (actual: string, expected: string) => {
  const a = new Set(actual.split("\n"))
  const e = new Set(expected.split("\n"))
  const extra = [...a].filter((l) => l !== "" && !e.has(l))
  const missing = [...e].filter((l) => l !== "" && !a.has(l))
  return `replica has [${extra.join("; ")}] but should have [${missing.join("; ")}]`
}

/** Rows of `tables` in scopes `principal` can read, judged by the grant rows in `tables`. */
const view = (tables: MemoryStore.Tables, principal: string) =>
  Effect.gen(function* () {
    const source = MemoryStore.make(tables)
    const out: MemoryStore.Tables = new Map()
    for (const [table, rows] of tables) {
      const kept = new Map<string, EncodedRow>()
      for (const [id, row] of rows) {
        if (Option.isSome(yield* roleOf(source, principal, row.access_scope_id))) kept.set(id, row)
      }
      if (kept.size > 0) out.set(table, kept)
    }
    return out
  })

export interface Disk {
  readonly replica: MemoryStore.MemoryStore
  meta: Engine.SyncMeta
  outbox: Array<Engine.PendingMutation>
  staged: Array<PullPage>
  notices: Array<Engine.Notice>
}

export const makeStorage = (disk: Disk): Engine.SyncStorage<never> => ({
  transaction: (self) =>
    Effect.suspend(() => {
      const saved = { meta: disk.meta, outbox: disk.outbox, staged: disk.staged, notices: disk.notices }
      return disk.replica.transaction(self).pipe(
        Effect.onExit((exit) =>
          Effect.sync(() => {
            if (exit._tag === "Failure") Object.assign(disk, saved)
          })
        )
      )
    }),
  rows: { write: disk.replica.write, purgeScope: disk.replica.purgeScope, clearAll: disk.replica.clearAll },
  meta: Effect.sync(() => disk.meta),
  setMeta: (meta) =>
    Effect.sync(() => {
      disk.meta = meta
    }),
  pending: Effect.sync(() => disk.outbox),
  appendPending: (entry) =>
    Effect.sync(() => {
      disk.outbox = [...disk.outbox, entry]
    }),
  setUndo: (seq, undo) =>
    Effect.sync(() => {
      disk.outbox = disk.outbox.map((e) => (e.envelope.seq === seq ? { ...e, undo } : e))
    }),
  removePending: (ids) =>
    Effect.sync(() => {
      const gone = new Set(ids)
      disk.outbox = disk.outbox.filter((e) => !gone.has(e.envelope.seq))
    }),
  staged: Effect.sync(() => disk.staged),
  stage: (page) =>
    Effect.sync(() => {
      disk.staged = [...disk.staged, page]
    }),
  clearStaged: Effect.sync(() => {
    disk.staged = []
  }),
  addNotices: (notices) =>
    Effect.sync(() => {
      disk.notices = [...disk.notices, ...notices]
    })
})

const skewed = (base: Clock.Clock, ms: number): Clock.Clock => ({
  currentTimeMillisUnsafe: () => base.currentTimeMillisUnsafe() + ms,
  currentTimeMillis: Effect.map(base.currentTimeMillis, (t) => t + ms),
  currentTimeNanosUnsafe: () => base.currentTimeNanosUnsafe() + BigInt(ms) * 1_000_000n,
  currentTimeNanos: Effect.map(base.currentTimeNanos, (t) => t + BigInt(ms) * 1_000_000n),
  monotonicTimeNanosUnsafe: () => base.monotonicTimeNanosUnsafe(),
  monotonicTimeNanos: base.monotonicTimeNanos,
  sleep: (duration) => base.sleep(duration)
})

const SCOPES = ["s0", "s1", "s2", "s3"]
const PRINCIPALS = ["alice", "bob"]
const CLIENTS: ReadonlyArray<readonly [string, string]> = [
  ["c1", "alice"],
  ["c2", "alice"],
  ["c3", "bob"]
]
const ROLES: ReadonlyArray<Role> = ["reader", "writer", "owner"]

export const run = (options: Options) =>
  Effect.gen(function* () {
    const rng = prng(options.seed)
    const server = RefServer.make({ pageSize: options.pageSize ?? 3 })
    const trace: Array<string> = []
    let step = 0
    let faults = true
    const stats = {
      mutations: 0,
      resnapshots: 0,
      reloadsMidSet: 0,
      notices: 0,
      transientStops: 0,
      lostResponses: 0,
      expired: 0
    }
    const committed: Array<MutationEnvelope> = []
    const ids: Array<string> = []

    const note = (event: string) => {
      trace.push(`#${step} ${event}`)
      if (trace.length > 25) trace.shift()
    }
    const violated = (invariant: string, detail: string) =>
      new InvariantViolated({ seed: options.seed, step, invariant, detail, trace: [...trace] })

    // Initial grants: a global catalog scope, private scopes, and a shared one.
    yield* server.writeDirect([
      { table: GRANTS, id: grantId("s0", ANYONE), row: Option.some(grantRow("s0", ANYONE, "reader")) },
      { table: GRANTS, id: grantId("s0", "alice"), row: Option.some(grantRow("s0", "alice", "owner")) },
      { table: GRANTS, id: grantId("s1", "alice"), row: Option.some(grantRow("s1", "alice", "owner")) },
      { table: GRANTS, id: grantId("s2", "bob"), row: Option.some(grantRow("s2", "bob", "owner")) },
      { table: GRANTS, id: grantId("s3", "alice"), row: Option.some(grantRow("s3", "alice", "writer")) },
      { table: GRANTS, id: grantId("s3", "bob"), row: Option.some(grantRow("s3", "bob", "writer")) }
    ])

    const clients = yield* Effect.forEach(CLIENTS, ([clientId, principal]) =>
      Effect.gen(function* () {
        server.register(clientId, principal)
        const disk: Disk = {
          replica: MemoryStore.make(),
          meta: Engine.initialMeta,
          outbox: [],
          staged: [],
          notices: []
        }
        const storage = makeStorage(disk)
        const executor: Engine.Executor<never> = {
          run: (envelope, createdAt) => runMutation(disk.replica, principal, envelope, createdAt)
        }
        const transport: Engine.Transport<TransportFailed> = {
          push: (request) =>
            Effect.gen(function* () {
              if (faults && rng.chance(0.1)) return yield* new TransportFailed({ during: "push request" })
              const stopAt = faults && rng.chance(0.15) ? Option.some(rng.int(request.mutations.length)) : Option.none()
              const response = yield* server.push(request, stopAt)
              if (Option.isSome(response.stopped)) stats.transientStops++
              if (faults && rng.chance(0.15)) {
                stats.lostResponses++
                return yield* new TransportFailed({ during: "push response" })
              }
              return response
            }),
          pull: (request) =>
            Effect.gen(function* () {
              if (faults && rng.chance(0.15)) return yield* new TransportFailed({ during: "pull request" })
              const response = yield* server.pull(request).pipe(
                Effect.tapError(() =>
                  Effect.sync(() => {
                    stats.resnapshots++
                  })
                )
              )
              if (faults && rng.chance(0.1)) return yield* new TransportFailed({ during: "pull response" })
              return response
            })
        }
        const client = {
          clientId,
          principal,
          disk,
          storage,
          executor,
          transport,
          online: true,
          skew: 0,
          engine: yield* Engine.make({ clientId, storage, executor, transport, pushBatchSize: 4 })
        }
        return client
      })
    )
    type Client = (typeof clients)[number]

    /** The deliberately wrong client: applies each `PullPage` as it arrives. */
    const incrementalPull = (client: Client) =>
      Effect.gen(function* () {
        const { storage, executor, transport } = client
        while (true) {
          const meta = client.disk.meta
          const from = meta.resnapshot ? null : meta.cursor
          const continuation = Option.fromNullishOr(client.disk.staged.at(-1)?.continuation)
          const response = yield* transport.pull({ clientId: client.clientId, cursor: from, continuation }).pipe(
            Effect.map(Option.some<PullResponse>),
            Effect.catchTag("ResnapshotRequired", () =>
              storage
                .transaction(
                  Effect.andThen(storage.clearStaged, storage.setMeta({ ...client.disk.meta, resnapshot: true }))
                )
                .pipe(Effect.as(Option.none<PullResponse>()))
            )
          )
          if (Option.isNone(response)) continue
          const page = response.value
          if (page._tag === "PullDone" || from === null) {
            if (page._tag === "PullPage") {
              yield* storage.transaction(storage.stage(page))
              continue
            }
            const done: PullDone = page
            return yield* storage.transaction(Engine.applyPageSet(storage, executor, client.disk.staged, done))
          }
          const early: PullDone = {
            _tag: "PullDone",
            changes: page.changes,
            acks: page.acks,
            nextCursor: from,
            expiredThrough: 0
          }
          yield* storage.transaction(
            Effect.gen(function* () {
              yield* Engine.applyPageSet(storage, executor, [], early)
              yield* storage.stage({ ...page, changes: [], acks: [] })
            })
          )
        }
      })

    const expectedReplica = (client: Client) =>
      Effect.gen(function* () {
        const cursor = client.disk.meta.cursor
        const snapshot = cursor === null ? new Map() : server.history.get(cursor)
        if (snapshot === undefined) return yield* violated("cursor names a server commit", `${cursor}`)
        const store = MemoryStore.make(yield* view(snapshot, client.principal))
        for (const entry of client.disk.outbox) {
          const result = yield* runMutation(store, client.principal, entry.envelope, entry.createdAt)
          if (Option.isSome(result.rejection)) yield* restore(store, result.undo)
        }
        return canonical(store.tables())
      })

    const checkClient = (client: Client) =>
      Effect.gen(function* () {
        const expected = yield* expectedReplica(client)
        const actual = canonical(client.disk.replica.tables())
        if (actual !== expected) {
          return yield* violated(`${client.clientId} replica = replay(pending, server@cursor)`, diff(actual, expected))
        }
        const cursor = client.disk.meta.cursor
        for (const entry of client.disk.outbox) {
          const ledger = server.ledgerEntry(client.clientId, entry.envelope.mutationId)
          if (cursor !== null && Option.isSome(ledger) && ledger.value.position <= cursor) {
            return yield* violated(
              "pending set never holds an acked mutation",
              `${client.clientId}#${entry.envelope.mutationId}`
            )
          }
        }
        const seen = new Set<string>()
        for (const notice of client.disk.notices) {
          if (seen.has(notice.envelope.mutationId)) {
            return yield* violated(
              "a rejection becomes a notice once",
              `${client.clientId}#${notice.envelope.mutationId}`
            )
          }
          seen.add(notice.envelope.mutationId)
        }
        for (const [key, count] of server.executions) {
          if (count > 1) return yield* violated("no intent applied twice on the server", `${key} executed ${count}×`)
        }
      })

    /** Network failures and local rejections are normal outcomes of a step. */
    const ignoreExpected = <A, E, R>(self: Effect.Effect<A, E, R>) =>
      self.pipe(
        Effect.asVoid,
        Effect.catch((error) =>
          error instanceof TransportFailed || error instanceof Engine.LocallyRejected
            ? Effect.sync(() => note(`  ↳ ${error._tag}`))
            : Effect.fail(error)
        )
      )

    const pull = (client: Client) => (options.pullMode === "incremental" ? incrementalPull(client) : client.engine.pull)

    const withClock = <A, E, R>(client: Client, self: Effect.Effect<A, E, R>) =>
      Effect.clockWith((clock) => Effect.provideService(self, Clock.Clock, skewed(clock, client.skew)))

    const randomMutation = (client: Client) => {
      // Mostly rows this client can see (so most intents apply locally), sometimes any id.
      const visible = [...(client.disk.replica.tables().get(COUNTERS)?.keys() ?? [])]
      const pool = visible.length > 0 && rng.chance(0.85) ? visible : ids
      const id = rng.pick(pool)
      const other = rng.pick(pool)
      const scope = rng.pick(SCOPES)
      const kind = rng.int(10)
      if (kind < 2 || Option.isNone(id) || Option.isNone(other) || Option.isNone(scope)) {
        const fresh = `${client.clientId}-${ids.length}`
        ids.push(fresh)
        return { name: "create", args: { id: fresh, scope: Option.getOrElse(scope, () => "s1") } }
      }
      if (kind < 6) return { name: "increment", args: { id: id.value, by: 1 + rng.int(5) } }
      if (kind < 8) return { name: "transfer", args: { from: id.value, to: other.value, amount: 1 + rng.int(3) } }
      if (kind < 9) return { name: "move", args: { id: id.value, scope: scope.value } }
      return { name: "remove", args: { id: id.value } }
    }

    const serverWrite = Effect.gen(function* () {
      const scope = Option.getOrElse(rng.pick(SCOPES), () => "s0")
      const principal = Option.getOrElse(rng.pick([...PRINCIPALS, ANYONE]), () => ANYONE)
      const kind = rng.int(4)
      if (kind === 0) {
        const role = Option.getOrElse(rng.pick(ROLES), (): Role => "reader")
        note(`server: grant ${principal} ${role} on ${scope}`)
        return yield* server.writeDirect([
          { table: GRANTS, id: grantId(scope, principal), row: Option.some(grantRow(scope, principal, role)) }
        ])
      }
      if (kind === 1) {
        note(`server: revoke ${principal} on ${scope}`)
        return yield* server.writeDirect([{ table: GRANTS, id: grantId(scope, principal), row: Option.none() }])
      }
      const id = rng.pick(ids)
      if (Option.isNone(id)) return
      const row = yield* server.store.read(COUNTERS, id.value)
      if (Option.isNone(row)) return
      const value = Number(row.value.value)
      note(`server: out-of-band ${kind === 2 ? "bump" : "delete"} ${id.value}`)
      yield* server.writeDirect([
        {
          table: COUNTERS,
          id: id.value,
          row: kind === 2 ? Option.some({ ...row.value, value: value + 100 }) : Option.none()
        }
      ])
    })

    const clientStep = (client: Client) =>
      Effect.gen(function* () {
        const roll = rng.int(100)
        if (roll < 40) {
          const { name, args } = randomMutation(client)
          note(`${client.clientId}: mutate ${name} ${JSON.stringify(args)}`)
          // Caller-generated, unordered idempotency key (DESIGN §6); occasionally a retried
          // enqueue of a still-pending id, which must not create a second intent.
          const retry = client.disk.outbox.at(-1)
          if (retry !== undefined && rng.chance(0.05)) {
            note(`${client.clientId}: retry enqueue ${retry.envelope.mutationId}`)
            const again = yield* client.engine.mutate(retry.envelope.mutationId, "increment", { id: "ignored", by: 99 })
            if (again.seq !== retry.envelope.seq)
              return yield* violated("enqueue is idempotent on mutationId", again.mutationId)
            return
          }
          const mutationId = `m${rng.int(2 ** 30).toString(36)}-${client.clientId}-${client.disk.meta.lastSeq}`
          yield* ignoreExpected(withClock(client, client.engine.mutate(mutationId, name, args)))
        } else if (roll < 60) {
          if (!client.online) return
          note(`${client.clientId}: push`)
          yield* ignoreExpected(client.engine.push)
        } else if (roll < 85) {
          if (!client.online) return
          note(`${client.clientId}: pull from ${client.disk.meta.cursor} (${client.disk.staged.length} staged)`)
          yield* ignoreExpected(pull(client))
        } else if (roll < 92) {
          if (client.disk.staged.length > 0) stats.reloadsMidSet++
          note(`${client.clientId}: reload (${client.disk.staged.length} staged)`)
          client.engine = yield* Engine.make({
            clientId: client.clientId,
            storage: client.storage,
            executor: client.executor,
            transport: client.transport,
            pushBatchSize: 4
          })
        } else if (roll < 97) {
          client.online = !client.online
          note(`${client.clientId}: ${client.online ? "online" : "offline"}`)
        } else {
          client.skew = rng.int(2) === 0 ? 0 : (rng.int(2) === 0 ? -1 : 1) * rng.int(3_600_000)
          note(`${client.clientId}: clock skew ${client.skew}ms`)
        }
      })

    const recordCommitted = (client: Client, before: number) => {
      for (const entry of client.disk.outbox) {
        if (entry.envelope.seq > before) committed.push(entry.envelope)
      }
    }

    for (step = 1; step <= options.steps; step++) {
      const roll = rng.int(100)
      if (roll < 85) {
        const client = Option.getOrThrow(rng.pick(clients))
        const before = client.disk.meta.lastSeq
        yield* clientStep(client)
        recordCommitted(client, before)
        yield* checkClient(client)
      } else if (roll < 95) {
        yield* serverWrite
      } else {
        const horizon = server.head() - BigInt(5 + rng.int(20))
        if (horizon > 0n) {
          note(`server: GC log${options.ledgerGc === true ? " + ledger" : ""} ≤ ${horizon}`)
          server.gcLog(horizon)
          if (options.ledgerGc === true) server.gcLedger(horizon)
        }
      }
      yield* TestClock.adjust(`${1 + rng.int(1000)} millis`)
    }

    // Quiescence: no faults, everyone online, push and pull until stable.
    step = options.steps + 1
    faults = false
    note("quiescence")
    for (const client of clients) client.online = true
    for (let round = 0; round < 30; round++) {
      for (const client of clients) {
        yield* client.engine.push
        yield* pull(client)
        yield* checkClient(client)
      }
      const settled = clients.every((c) => c.disk.outbox.length === 0 && c.disk.meta.cursor === server.head())
      if (settled) break
    }

    for (const client of clients) {
      if (client.disk.outbox.length > 0) {
        return yield* violated(
          "no intent lost: outbox drains after quiescence",
          `${client.clientId} still pending ${client.disk.outbox.map((e) => e.envelope.mutationId).join(",")}`
        )
      }
      const expected = canonical(yield* view(server.store.tables(), client.principal))
      const actual = canonical(client.disk.replica.tables())
      if (actual !== expected)
        return yield* violated(`${client.clientId} replica = server ∩ readable scopes`, diff(actual, expected))
    }
    for (const envelope of committed) {
      const key = `${envelope.clientId}#${envelope.mutationId}`
      const count = server.executions.get(key) ?? 0
      if (count !== 1)
        return yield* violated("every committed intent executes exactly once", `${key} executed ${count}×`)
      const ledger = server.ledgerEntry(envelope.clientId, envelope.mutationId)
      const client = clients.find((c) => c.clientId === envelope.clientId)
      const notices = client?.disk.notices.filter((n) => n.envelope.mutationId === envelope.mutationId).length ?? 0
      const rejected = Option.isSome(ledger) && ledger.value.outcome._tag === "Rejected"
      const unknown = client?.disk.notices.some(
        (n) => n.envelope.mutationId === envelope.mutationId && n._tag === "OutcomeUnknown"
      )
      if (Option.isSome(ledger) && (notices !== (rejected ? 1 : 0) || unknown === true)) {
        return yield* violated(
          "rejections surface as exactly one notice",
          `${key}: ${notices} notices, rejected=${rejected}`
        )
      }
      if (rejected) stats.notices++
      if (Option.isNone(ledger) && notices > 1) {
        return yield* violated("an expired mutation surfaces as at most one notice", `${key}: ${notices}`)
      }
    }
    for (const client of clients) {
      stats.expired += client.disk.notices.filter((n) => n._tag === "OutcomeUnknown").length
    }
    stats.mutations = committed.length
    const result: Stats = stats
    return result
  })
