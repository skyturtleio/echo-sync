// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { expect, it } from "@effect/vitest"
import { DateTime, Deferred, Effect, Fiber, Layer, Option, Schema, Scope } from "effect"
import type { Scheduler } from "effect/Scheduler"
import * as SqlClient from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Protocol, Registry, ScopeGrants, SyncTable } from "echo-sync"
import * as Outbox from "../Outbox.ts"
import * as Engine from "./Engine.ts"
import * as SqliteSyncStorage from "./SqliteSyncStorage.ts"

const TId = Schema.String.pipe(Schema.brand("TId"))
const T = SyncTable.make("t", { id: TId, accessScopeId: AccessScope.AccessScopeId })
const registry = Registry.make([T])
const Db = Layer.effectDiscard(SqliteSyncStorage.migrate(registry)).pipe(
  Layer.provideMerge(SqliteClient.layerMemory({}))
)

const makeStorage = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  const rows = {
    ids: Effect.orDie(sql<{ id: string }>`SELECT id FROM t ORDER BY id`).pipe(Effect.map((r) => r.map((x) => x.id)))
  }
  return { storage: SqliteSyncStorage.make(sql, registry), rows }
})

const mid = Ids.MutationId.make
const seqOf = Ids.Seq.make
const pos = Ids.Position.make
const row = (id: string): SyncTable.EncodedRow => ({ id, access_scope_id: "s" })
const decodePage = Schema.decodeUnknownSync(Protocol.PullPage)
const decodeDone = Schema.decodeUnknownSync(Protocol.PullDone)

const entry = (seq: Ids.Seq, mutationId: string) => ({
  envelope: { clientId: Ids.ClientId.make("c"), mutationId: mid(mutationId), seq, name: "t", args: { mutationId } },
  createdAt: 0,
  undo: [{ table: T, id: mutationId, before: Option.none() }]
})

it.live("a transaction failing on one fiber rolls back only its own writes while another fiber commits", () =>
  Effect.gen(function* () {
    const { storage, rows } = yield* makeStorage
    const wrote = yield* Deferred.make<void>()
    const failing = yield* Effect.forkChild(
      storage
        .transaction(
          Effect.gen(function* () {
            yield* storage.rows.write(T, "lost", Option.some(row("lost")))
            yield* storage.appendPending(entry(yield* storage.nextSeq, "lost"))
            yield* storage.setMeta({ cursor: pos(9n), pushedThrough: 9, resnapshot: true })
            yield* Deferred.succeed(wrote, undefined)
            yield* Effect.sleep("20 millis")
            return yield* Effect.fail("crash")
          })
        )
        .pipe(Effect.flip)
    )
    yield* Deferred.await(wrote)
    // Starts while the failing transaction is open; must not run inside it.
    yield* storage.transaction(
      Effect.gen(function* () {
        yield* storage.rows.write(T, "kept", Option.some(row("kept")))
        yield* storage.appendPending(entry(yield* storage.nextSeq, "kept"))
      })
    )
    expect(yield* Fiber.join(failing)).toBe("crash")
    expect(yield* rows.ids).toEqual(["kept"])
    const pending = yield* storage.transaction(storage.pending)
    expect(pending.map((e) => [e.envelope.mutationId, e.envelope.seq])).toEqual([["kept", 1]])
    expect(pending[0]?.undo).toEqual([{ table: T, id: "kept", before: Option.none() }])
    expect(yield* storage.meta).toEqual({ cursor: null, pushedThrough: 0, resnapshot: false })
  }).pipe(Effect.provide(Db))
)

it.effect("nextSeq starts at 1 and never reuses a seq, even after its released row is pruned", () =>
  Effect.gen(function* () {
    const { storage } = yield* makeStorage
    const append = (id: string) =>
      storage.transaction(
        Effect.gen(function* () {
          const seq = yield* storage.nextSeq
          yield* storage.appendPending(entry(seq, id))
          return seq
        })
      )
    expect(yield* append("a")).toBe(1)
    expect(yield* append("b")).toBe(2)
    yield* storage.transaction(storage.release([seqOf(2)]))
    expect((yield* storage.lookup(mid("b"))).pipe(Option.map((e) => e.seq))).toEqual(Option.some(2))
    expect((yield* storage.pending).map((e) => e.envelope.seq)).toEqual([1])
    yield* Effect.provideService(Outbox.pruneReleased(-1), SqlClient.SqlClient, yield* SqlClient.SqlClient)
    expect(yield* storage.lookup(mid("b"))).toEqual(Option.none())
    expect(yield* append("c")).toBe(3)
  }).pipe(Effect.provide(Db))
)

it.effect("staged pages, meta and notices round-trip with bigint positions", () =>
  Effect.gen(function* () {
    const { storage } = yield* makeStorage
    const page = decodePage({
      _tag: "PullPage",
      changes: [
        { _tag: "upsert", table: "t", row: { id: "r", access_scope_id: "s", n: 1 } },
        { _tag: "scopePurged", scopeId: "s2" }
      ],
      acks: [
        {
          mutationId: "m",
          position: String(2n ** 60n),
          outcome: { _tag: "Rejected", error: { _tag: "UnknownMutation", name: "x" } }
        }
      ],
      continuation: "k"
    })
    yield* storage.transaction(
      Effect.gen(function* () {
        yield* storage.stage(page)
        yield* storage.setMeta({ cursor: pos(2n ** 60n + 1n), pushedThrough: 3, resnapshot: false })
      })
    )
    expect(yield* storage.staged).toEqual([page])
    expect((yield* storage.meta).cursor).toBe(2n ** 60n + 1n)
    yield* storage.clearStaged
    expect(yield* storage.staged).toEqual([])
  }).pipe(Effect.provide(Db))
)

it.live("an interrupted transaction rolls back rows, outbox, seq and meta together", () =>
  Effect.gen(function* () {
    const { storage, rows } = yield* makeStorage
    const wrote = yield* Deferred.make<void>()
    const fiber = yield* Effect.forkChild(
      storage.transaction(
        Effect.gen(function* () {
          yield* storage.rows.write(T, "x", Option.some(row("x")))
          yield* storage.appendPending(entry(yield* storage.nextSeq, "x"))
          yield* storage.setMeta({ cursor: pos(5n), pushedThrough: 1, resnapshot: false })
          yield* Deferred.succeed(wrote, undefined)
          return yield* Effect.never
        })
      )
    )
    yield* Deferred.await(wrote)
    yield* Fiber.interrupt(fiber)
    expect(yield* rows.ids).toEqual([])
    expect(yield* storage.pending).toEqual([])
    expect(yield* storage.meta).toEqual({ cursor: null, pushedThrough: 0, resnapshot: false })
    // The rolled-back seq is given back: the next allocation is still 1.
    expect(yield* storage.transaction(storage.nextSeq)).toBe(1)
  }).pipe(Effect.provide(Db))
)

// --- migrate (DESIGN §9, §11): the replica schema and the bookkeeping that describes it move together.

const reindexed = Registry.make([
  SyncTable.make(
    "t",
    { id: TId, accessScopeId: AccessScope.AccessScopeId },
    { indexes: { by_scope: ["accessScopeId"] } }
  )
])
const reshapedT = SyncTable.make("t", {
  id: TId,
  accessScopeId: AccessScope.AccessScopeId,
  n: Schema.NullOr(Schema.Int)
})
const reshaped = Registry.make([reshapedT, SyncTable.make("u", { id: TId, accessScopeId: AccessScope.AccessScopeId })])

const page = decodePage({
  _tag: "PullPage",
  changes: [{ _tag: "upsert", table: "t", row: { id: "b", access_scope_id: "s" } }],
  acks: [],
  continuation: "k"
})

/** An installed replica mid-sync: rows, a cursor, a staged page, a pending entry with undo, a released one. */
const seedOldReplica = Effect.gen(function* () {
  const { storage } = yield* makeStorage
  yield* storage.transaction(
    Effect.gen(function* () {
      yield* storage.rows.write(T, "a", Option.some(row("a")))
      yield* storage.appendPending(entry(yield* storage.nextSeq, "p"))
      yield* storage.appendPending(entry(yield* storage.nextSeq, "r"))
      yield* storage.release([seqOf(2)])
      yield* storage.stage(page)
      yield* storage.setMeta({ cursor: pos(7n), pushedThrough: 2, resnapshot: true })
    })
  )
})

/** Which replica the database describes: "old", "new", or "mixed" (the failure). */
const classify = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  const { storage } = yield* makeStorage
  const hash = yield* sql<{ value: string }>`SELECT value FROM echo_meta WHERE key = 'replica_data_hash'`
  const columns = yield* sql<{ name: string }>`SELECT name FROM pragma_table_info('t')`
  const hasU = (yield* sql`SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'u'`).length === 1
  const rowIds = (yield* sql<{ id: string }>`SELECT id FROM t`).map((r) => r.id)
  const meta = yield* storage.meta
  const staged = (yield* storage.staged).length
  const pending = yield* storage.pending
  const undo = pending.flatMap((e) => e.undo).length
  // Kept in every outcome: the outbox (pending and released) and pushedThrough.
  const kept =
    pending.map((e) => e.envelope.seq).join() === "1" &&
    Option.isSome(yield* storage.lookup(mid("r"))) &&
    meta.pushedThrough === 2
  const old = [
    hash[0]?.value === registry.dataHash,
    columns.length === 2,
    !hasU,
    rowIds.join() === "a",
    meta.cursor === 7n,
    meta.resnapshot,
    staged === 1,
    undo === 1
  ]
  const fresh = [
    hash[0]?.value === reshaped.dataHash,
    columns.length === 3,
    hasU,
    rowIds.length === 0,
    meta.cursor === null,
    !meta.resnapshot,
    staged === 0,
    undo === 0
  ]
  if (kept && old.every(Boolean)) return "old"
  if (kept && fresh.every(Boolean)) return "new"
  return `mixed ${JSON.stringify({ kept, old, fresh })}`
})

it.effect("an index-only change keeps rows and cursor; a data-shape change rebuilds and resets them together", () =>
  Effect.gen(function* () {
    yield* seedOldReplica
    expect(yield* SqliteSyncStorage.migrate(registry)).toEqual({ rebuilt: false })
    expect(yield* classify).toBe("old")

    expect(reindexed.dataHash).toBe(registry.dataHash)
    expect(yield* SqliteSyncStorage.migrate(reindexed)).toEqual({ rebuilt: false })
    const sql = yield* SqlClient.SqlClient
    const indexes = yield* sql<{
      name: string
    }>`SELECT name FROM sqlite_schema WHERE type = 'index' AND sql IS NOT NULL AND tbl_name = 't'`
    expect(indexes.length).toBe(1)
    expect(yield* SqliteSyncStorage.migrate(registry)).toEqual({ rebuilt: false })
    expect(yield* classify).toBe("old")

    // A unique constraint is CREATE TABLE text: only a rebuild can apply it.
    const uniqueScope = Registry.make([
      SyncTable.make("t", { id: TId, accessScopeId: AccessScope.AccessScopeId }, { unique: [["accessScopeId"]] })
    ])
    expect(uniqueScope.dataHash).toBe(registry.dataHash)
    expect(yield* SqliteSyncStorage.migrate(uniqueScope)).toEqual({ rebuilt: true })
    const unique = SqliteSyncStorage.make(sql, uniqueScope)
    const uniqueT = Option.getOrThrow(uniqueScope.get("t"))
    yield* unique.rows.write(uniqueT, "u1", Option.some(row("u1")))
    const duplicate = yield* Effect.exit(unique.rows.write(uniqueT, "u2", Option.some(row("u2"))))
    expect(duplicate._tag).toBe("Failure")
    yield* unique.rows.clearAll

    expect(yield* SqliteSyncStorage.migrate(reshaped)).toEqual({ rebuilt: true })
    expect(yield* SqliteSyncStorage.migrate(reshaped)).toEqual({ rebuilt: false })
  }).pipe(Effect.provide(Db))
)

it("a data-shape migration interrupted at any scheduler boundary leaves the old replica or the new one, never a mix", async () => {
  const outcomes = new Map<string, Array<number>>()
  for (let boundary = 1; ; boundary++) {
    const scope = await Effect.runPromise(Scope.make())
    const context = await Effect.runPromise(Layer.buildWithScope(Db, scope))
    const run = Effect.runPromiseExitWith(context)
    await run(seedOldReplica)

    const controller = new AbortController()
    let ticks = 0
    let yielded = false
    const tasks: Array<() => void> = []
    const scheduler: Scheduler = {
      executionMode: "async",
      shouldYield: () => !yielded && ++ticks === boundary && (yielded = true),
      makeDispatcher: () => ({
        scheduleTask: (task) => {
          tasks.push(task)
        },
        flush: () => {
          while (tasks.length > 0) tasks.shift()?.()
        }
      })
    }
    const state = { settled: false }
    const migrating = run(SqliteSyncStorage.migrate(reshaped), { scheduler, signal: controller.signal }).finally(
      () => (state.settled = true)
    )
    if (yielded) controller.abort()
    while (!state.settled) {
      while (tasks.length > 0) tasks.shift()?.()
      await new Promise((resolve) => setTimeout(resolve, 0))
    }
    await migrating
    const outcome = await run(classify)
    const label = outcome._tag === "Success" ? outcome.value : `classify failed ${String(outcome.cause)}`
    outcomes.set(label, [...(outcomes.get(label) ?? []), boundary])
    await Effect.runPromise(Scope.close(scope, outcome))
    if (!yielded) break // the migration ran to completion before reaching this boundary: the sweep is done
  }
  expect([...outcomes.keys()].toSorted()).toEqual(["new", "old"])
  expect((outcomes.get("old") ?? []).length).toBeGreaterThan(10) // the sweep spans the transaction
})

it.effect("after a rebuild, installing the null-cursor set replays pending on top of it and recaptures undo", () =>
  Effect.gen(function* () {
    yield* seedOldReplica
    expect(yield* SqliteSyncStorage.migrate(reshaped)).toEqual({ rebuilt: true })
    const sql = yield* SqlClient.SqlClient
    const storage = SqliteSyncStorage.make(sql, reshaped)
    const tTable = Option.getOrThrow(reshaped.get("t"))
    // The pending entries' handler: inserts row <mutationId> with n = 1 in scope "s".
    const insert = Mutation.make("t", {
      args: Schema.Struct({ mutationId: TId }),
      handler: (ctx, { mutationId }) =>
        ctx.store.insert(reshapedT, { id: mutationId, accessScopeId: AccessScope.AccessScopeId.make("s"), n: 1 })
    })
    const actor = AccessScope.PrincipalId.make("me")
    const grant = Schema.encodeSync(ScopeGrants.ScopeGrants.wire)({
      id: ScopeGrants.grantId(AccessScope.AccessScopeId.make("s"), actor),
      accessScopeId: AccessScope.AccessScopeId.make("s"),
      principalId: actor,
      role: "writer",
      grantedAt: DateTime.makeUnsafe(0)
    })
    // The null-cursor set: the actor's grant and one server row.
    const done = decodeDone({
      _tag: "PullDone",
      changes: [
        { _tag: "upsert", table: "scope_grants", row: grant },
        { _tag: "upsert", table: "t", row: { id: "srv", access_scope_id: "s", n: null } }
      ],
      acks: [],
      nextCursor: "9",
      expiredThrough: 0
    })
    const context = { storage, mutations: Mutation.makeRegistry([insert]), actor, registry: reshaped }
    yield* storage.transaction(Engine.applyPageSet(context, [], done))
    const rows = yield* storage.rows.scan(tTable)
    expect(rows.map((r) => [r["id"], r["n"]])).toEqual([
      ["p", 1],
      ["srv", null]
    ])
    const pending = yield* storage.pending
    expect(pending.map((e) => [e.envelope.mutationId, e.undo])).toEqual([
      ["p", [{ table: tTable, id: "p", before: Option.none() }]]
    ])
    expect((yield* storage.meta).cursor).toBe(9n)
  }).pipe(Effect.provide(Db))
)
