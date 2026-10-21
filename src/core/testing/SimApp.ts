/**
 * The application the simulation runs, declared with core's real APIs: a `counters` SyncTable,
 * intent-shaped mutations (increment/transfer are wrong if replayed over a server result that
 * already contains them), and a `notes` table that only the server's registry has (a table
 * added by a newer deploy, DESIGN §8). Grants are core's `scope_grants` system table.
 */
import { DateTime, Effect, Schema } from "effect"
import { AccessScope, Mutation, Registry, ScopeGrants, SyncTable } from "../index.ts"

export const CounterId = Schema.String.check(Schema.isNonEmpty()).pipe(Schema.brand("sim/CounterId"))
export const NoteId = Schema.String.check(Schema.isNonEmpty()).pipe(Schema.brand("sim/NoteId"))

export const Counters = SyncTable.make("counters", {
  id: CounterId,
  accessScopeId: AccessScope.AccessScopeId,
  value: Schema.Int,
  updatedAt: Schema.DateTimeUtc
})

export const Notes = SyncTable.make("notes", {
  id: NoteId,
  accessScopeId: AccessScope.AccessScopeId,
  body: Schema.String
})

/** The clients' registry (an older app version: no `notes`). */
export const clientRegistry = Registry.make([Counters])
/** The server's registry. */
export const serverRegistry = Registry.make([Counters, Notes])

const create = Mutation.make("create", {
  args: Schema.Struct({ id: CounterId, scope: AccessScope.AccessScopeId }),
  handler: (ctx, { id, scope }) =>
    ctx.store.insert(Counters, { id, accessScopeId: scope, value: 0, updatedAt: ctx.now })
})

const increment = Mutation.make("increment", {
  args: Schema.Struct({ id: CounterId, by: Schema.Int }),
  handler: (ctx, { id, by }) =>
    Effect.gen(function* () {
      const row = yield* ctx.store.get(Counters, id)
      yield* ctx.store.patch(Counters, id, { value: row.value + by, updatedAt: ctx.now })
    })
})

const transfer = Mutation.make("transfer", {
  args: Schema.Struct({ from: CounterId, to: CounterId, amount: Schema.Int }),
  handler: (ctx, { from, to, amount }) =>
    Effect.gen(function* () {
      const a = yield* ctx.store.get(Counters, from)
      yield* ctx.store.patch(Counters, from, { value: a.value - amount, updatedAt: ctx.now })
      const b = yield* ctx.store.get(Counters, to)
      yield* ctx.store.patch(Counters, to, { value: b.value + amount, updatedAt: ctx.now })
    })
})

const move = Mutation.make("move", {
  args: Schema.Struct({ id: CounterId, scope: AccessScope.AccessScopeId }),
  handler: (ctx, { id, scope }) => ctx.store.patch(Counters, id, { accessScopeId: scope, updatedAt: ctx.now })
})

const remove = Mutation.make("remove", {
  args: Schema.Struct({ id: CounterId }),
  handler: (ctx, { id }) => ctx.store.delete(Counters, id)
})

export const mutations = Mutation.makeRegistry([create, increment, transfer, move, remove])

export const ANYONE = AccessScope.Everyone
export const scope = (id: string) => AccessScope.AccessScopeId.make(id)
export const principal = (id: string) => AccessScope.PrincipalId.make(id)

/** A `scope_grants` row in wire form, with core's derived id (DESIGN §4). */
export const grantRow = (scopeId: string, who: string, role: AccessScope.Role): SyncTable.EncodedRow =>
  Schema.encodeSync(ScopeGrants.ScopeGrants.wire)({
    id: ScopeGrants.grantId(scope(scopeId), principal(who)),
    accessScopeId: scope(scopeId),
    principalId: principal(who),
    role,
    grantedAt: DateTime.makeUnsafe(0)
  })
