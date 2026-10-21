// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { expect, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Layer, Option, Schema, Scope } from "effect"
import type { Scheduler } from "effect/Scheduler"
import * as SqlClient from "effect/sql/SqlClient"
import { AccessScope, Registry, SyncTable } from "echo-sync"
import * as Outbox from "../Outbox.ts"
import * as SqliteRowStore from "../SqliteRowStore.ts"
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

const entry = (seq: number, mutationId: string) => ({
  envelope: { clientId: "c", mutationId, seq, name: "t", args: { mutationId } },
  createdAt: 0,
  undo: [{ table: "t", id: mutationId, before: Option.none() }]
})

it.live("a transaction failing on one fiber rolls back only its own writes while another fiber commits", () =>
  Effect.gen(function* () {
    const { storage, rows } = yield* makeStorage
    const wrote = yield* Deferred.make<void>()
    const failing = yield* Effect.forkChild(
      storage
        .transaction(
          Effect.gen(function* () {
            yield* storage.rows.write("t", "lost", Option.some({ id: "lost", access_scope_id: "s" }))
            yield* storage.appendPending(entry(yield* storage.nextSeq, "lost"))
            yield* storage.setMeta({ cursor: 9n, pushedThrough: 9, resnapshot: true })
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
        yield* storage.rows.write("t", "kept", Option.some({ id: "kept", access_scope_id: "s" }))
        yield* storage.appendPending(entry(yield* storage.nextSeq, "kept"))
      })
    )
    expect(yield* Fiber.join(failing)).toBe("crash")
    expect(yield* rows.ids).toEqual(["kept"])
    const pending = yield* storage.transaction(storage.pending)
    expect(pending.map((e) => [e.envelope.mutationId, e.envelope.seq])).toEqual([["kept", 1]])
    expect(pending[0]?.undo).toEqual([{ table: "t", id: "kept", before: Option.none() }])
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
    yield* storage.transaction(storage.release([2]))
    expect((yield* storage.lookup("b")).pipe(Option.map((e) => e.seq))).toEqual(Option.some(2))
    expect((yield* storage.pending).map((e) => e.envelope.seq)).toEqual([1])
    yield* Effect.provideService(Outbox.pruneReleased(-1), SqlClient.SqlClient, yield* SqlClient.SqlClient)
    expect(yield* storage.lookup("b")).toEqual(Option.none())
    expect(yield* append("c")).toBe(3)
  }).pipe(Effect.provide(Db))
)

it.effect("staged pages, meta and notices round-trip with bigint positions", () =>
  Effect.gen(function* () {
    const { storage } = yield* makeStorage
    const page = {
      _tag: "PullPage" as const,
      changes: [
        { _tag: "upsert" as const, table: "t", row: { id: "r", access_scope_id: "s", n: 1 } },
        { _tag: "scopePurged" as const, scopeId: "s2" }
      ],
      acks: [{ mutationId: "m", position: 2n ** 60n, outcome: { _tag: "Applied" as const } }],
      continuation: "k"
    }
    yield* storage.transaction(
      Effect.gen(function* () {
        yield* storage.stage(page)
        yield* storage.setMeta({ cursor: 2n ** 60n + 1n, pushedThrough: 3, resnapshot: false })
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
          yield* storage.rows.write("t", "x", Option.some({ id: "x", access_scope_id: "s" }))
          yield* storage.appendPending(entry(yield* storage.nextSeq, "x"))
          yield* storage.setMeta({ cursor: 5n, pushedThrough: 1, resnapshot: false })
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
const reshaped = Registry.make([
  SyncTable.make("t", { id: TId, accessScopeId: AccessScope.AccessScopeId, n: Schema.NullOr(Schema.Int) }),
  SyncTable.make("u", { id: TId, accessScopeId: AccessScope.AccessScopeId })
])

const page = {
  _tag: "PullPage" as const,
  changes: [{ _tag: "upsert" as const, table: "t", row: { id: "b", access_scope_id: "s" } }],
  acks: [],
  continuation: "k"
}

/** An installed replica mid-sync: rows, a cursor, a staged page, a pending entry with undo, a released one. */
const seedOldReplica = Effect.gen(function* () {
  const { storage } = yield* makeStorage
  yield* storage.transaction(
    Effect.gen(function* () {
      yield* storage.rows.write("t", "a", Option.some({ id: "a", access_scope_id: "s" }))
      yield* storage.appendPending(entry(yield* storage.nextSeq, "p"))
      yield* storage.appendPending(entry(yield* storage.nextSeq, "r"))
      yield* storage.release([2])
      yield* storage.stage(page)
      yield* storage.setMeta({ cursor: 7n, pushedThrough: 2, resnapshot: true })
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
    Option.isSome(yield* storage.lookup("r")) &&
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
    yield* unique.rows.write("t", "u1", Option.some({ id: "u1", access_scope_id: "s" }))
    const duplicate = yield* Effect.exit(unique.rows.write("t", "u2", Option.some({ id: "u2", access_scope_id: "s" })))
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
    const t = SqliteRowStore.make(sql, reshaped)
    const tTable = Option.getOrUndefined(reshaped.get("t"))
    if (tTable === undefined) return yield* Effect.die(new Error("no table t"))
    // Optimistic handler: upserts row <mutationId> with n = 1, reporting the row's pre-image.
    const executor: Engine.Executor<unknown> = {
      run: (envelope) =>
        Effect.gen(function* () {
          const before = yield* t.read(tTable, envelope.mutationId)
          const row = { id: envelope.mutationId, access_scope_id: "s", n: 1 }
          yield* storage.rows.write("t", envelope.mutationId, Option.some(row))
          return {
            undo: [
              {
                table: "t",
                id: envelope.mutationId,
                before: Option.map(before, (r) => ({ ...r, id: envelope.mutationId, access_scope_id: "s" }))
              }
            ],
            rejection: Option.none()
          }
        })
    }
    const done = {
      _tag: "PullDone" as const,
      changes: [{ _tag: "upsert" as const, table: "t", row: { id: "srv", access_scope_id: "s", n: null } }],
      acks: [],
      nextCursor: 9n,
      expiredThrough: 0
    }
    yield* storage.transaction(
      Engine.applyPageSet(storage, executor, (name) => Option.isSome(reshaped.get(name)), [], done)
    )
    const rows = yield* t.scan(tTable)
    expect(rows.map((r) => [r["id"], r["n"]])).toEqual([
      ["p", 1],
      ["srv", null]
    ])
    const pending = yield* storage.pending
    expect(pending.map((e) => [e.envelope.mutationId, e.undo])).toEqual([
      ["p", [{ table: "t", id: "p", before: Option.none() }]]
    ])
    expect((yield* storage.meta).cursor).toBe(9n)
  }).pipe(Effect.provide(Db))
)
