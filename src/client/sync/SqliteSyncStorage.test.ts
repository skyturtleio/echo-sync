// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { expect, it } from "@effect/vitest"
import {
  Array as Arr,
  Cause,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Order,
  Schema,
  Scope,
  Stream
} from "effect"
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
    ids: Effect.orDie(sql<{ id: string }>`SELECT id FROM t ORDER BY id`).pipe(
      Effect.map((r) => Arr.map(r, (x) => x.id))
    )
  }
  return { storage: yield* SqliteSyncStorage.make(sql, registry), rows }
})

const mid = Ids.MutationId.make
const seqOf = Ids.Seq.make
const pos = Ids.Position.make
const row = (id: string): SyncTable.EncodedRow => ({ id, access_scope_id: "s" })
const decodePage = Schema.decodeUnknownEffect(Protocol.PullPage)
const decodeDone = Schema.decodeUnknownEffect(Protocol.PullDone)

const entry = (seq: Ids.Seq, mutationId: string) => ({
  envelope: { clientId: Ids.ClientId.make("c"), mutationId: mid(mutationId), seq, name: "t", args: { mutationId } },
  createdAt: 0,
  undo: [{ table: T, id: mutationId, before: Option.none() }]
})

it.effect("queued transactions, reads and pending counts progress before a releasing caller can reenter", () =>
  Effect.gen(function* () {
    const { storage } = yield* makeStorage
    const before = yield* storage.admit(storage.revision)
    const order: Array<string> = []
    const fibers = yield* storage.admit(
      Effect.gen(function* () {
        const save = yield* Effect.forkChild(
          storage.localCommit(
            Effect.gen(function* () {
              yield* storage.appendPending(entry(yield* storage.nextSeq, "saved"))
              order.push("save")
            })
          ),
          { startImmediately: true }
        )
        const read = yield* Effect.forkChild(
          storage.transaction(
            Effect.map(storage.pending, (pending) => {
              expect(Arr.map(pending, (p) => p.envelope.mutationId)).toEqual(["saved"])
              order.push("read")
            })
          ),
          { startImmediately: true }
        )
        const count = yield* Effect.forkChild(Stream.runHead(storage.pendingCount), { startImmediately: true })
        const install = yield* Effect.forkChild(
          storage.transaction(
            Effect.andThen(
              storage.release([seqOf(1)]),
              Effect.sync(() => order.push("install"))
            )
          ),
          { startImmediately: true }
        )
        return { save, read, count, install }
      })
    )
    // No yield between releasing admission and asking again: the old semaphore barges here.
    yield* storage.admit(Effect.sync(() => order.push("late")))
    expect((yield* Fiber.join(fibers.save)).change).toEqual({
      before,
      after: { epoch: before.epoch, offset: before.offset + 1 }
    })
    yield* Fiber.join(fibers.read)
    expect(yield* Fiber.join(fibers.count)).toEqual(Option.some(1))
    yield* Fiber.join(fibers.install)
    expect(order).toEqual(["save", "read", "install", "late"])
    expect(yield* storage.admit(storage.revision)).toEqual({ epoch: before.epoch, offset: before.offset + 2 })
    expect(yield* Stream.runHead(storage.pendingCount)).toEqual(Option.some(0))
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root; the test owns the database lifetime.
  }).pipe(Effect.provide(Db))
)

it.effect("cancelling queued head, middle and tail work never runs it or releases the active transaction", () =>
  Effect.gen(function* () {
    const { storage, rows } = yield* makeStorage
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const owner = yield* Effect.forkChild(
      storage.transaction(
        Effect.gen(function* () {
          yield* storage.rows.write(T, "owner", Option.some(row("owner")))
          yield* Deferred.succeed(entered, undefined)
          yield* Deferred.await(release)
        })
      )
    )
    yield* Deferred.await(entered)
    const ran: Array<number> = []
    const waiters = yield* Effect.forEach([0, 1, 2, 3, 4], (i) =>
      Effect.forkChild(storage.transaction(Effect.sync(() => ran.push(i))), { startImmediately: true })
    )
    for (const i of [0, 2, 4]) {
      const waiter = waiters[i]
      if (waiter !== undefined) yield* Fiber.interrupt(waiter)
    }
    expect(ran).toEqual([])
    yield* Deferred.succeed(release, undefined)
    yield* Fiber.join(owner)
    for (const i of [1, 3]) {
      const waiter = waiters[i]
      if (waiter !== undefined) yield* Fiber.join(waiter)
    }
    expect(ran).toEqual([1, 3])
    expect(yield* storage.transaction(rows.ids)).toEqual(["owner"])
    // Defects and typed failures must also pass ownership on.
    yield* Effect.exit(storage.admit(Effect.die("defect")))
    yield* Effect.exit(storage.admit(Effect.fail("failure")))
    expect(yield* storage.transaction(rows.ids)).toEqual(["owner"])
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root; the test owns the database lifetime.
  }).pipe(Effect.provide(Db))
)

// oxlint-disable-next-line effecttsgo/async-function -- External scheduler driver must await separately-run runtimes without inheriting the scheduler being interrupted.
it("interruption at every admission handoff boundary leaves transactions atomic and admission usable", async () => {
  let interrupted = 0
  const outcomes = new Set<number>()
  for (let boundary = 1; ; boundary++) {
    const scope = await Effect.runPromise(Scope.make())
    const context = await Effect.runPromise(Layer.buildWithScope(Db, scope))
    const run = Effect.runPromiseExitWith(context)
    const { storage, rows } = await Effect.runPromiseWith(context)(makeStorage)
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
    const program = Effect.gen(function* () {
      const waiter = yield* storage.admit(
        Effect.forkChild(
          storage.localCommit(
            Effect.gen(function* () {
              yield* storage.rows.write(T, "saved", Option.some(row("saved")))
              yield* storage.appendPending(entry(yield* storage.nextSeq, "saved"))
            })
          ),
          { startImmediately: true }
        )
      )
      // Race a new arrival with release and with cancellation of the granted waiter.
      yield* storage.admit(Effect.void)
      yield* Fiber.join(waiter)
    })
    const state = { settled: false }
    const running = run(program, { scheduler, signal: controller.signal }).finally(() => (state.settled = true))
    while (!state.settled) {
      if (yielded) controller.abort()
      tasks.shift()?.()
      await Effect.runPromise(Effect.sleep(0))
    }
    const exit = await running
    expect(exit._tag === "Success" || Arr.every(exit.cause.reasons, (reason) => reason._tag === "Interrupt")).toBe(true)
    if (exit._tag === "Failure") {
      interrupted++
    }
    const verified = await run(
      storage.transaction(
        Effect.gen(function* () {
          const ids = yield* rows.ids
          const pending = yield* storage.pending
          expect(ids).toEqual(Arr.map(pending, (p) => p.envelope.mutationId))
          expect(ids.length).toBeLessThanOrEqual(1)
          expect((yield* storage.revision).offset).toBe(ids.length)
          outcomes.add(ids.length)
          expect(yielded || Arr.join(ids, ",") === "saved").toBe(true)
          yield* storage.rows.write(T, "next", Option.some(row("next")))
          expect(yield* rows.ids).toContain("next")
        })
      )
    )
    await Effect.runPromise(Scope.close(scope, verified))
    expect(verified._tag).toBe("Success")
    if (!yielded) break
  }
  expect(interrupted).toBeGreaterThan(10)
  expect(Arr.sort(outcomes, Order.Number)).toEqual([0, 1])
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
    expect(Arr.map(pending, (e) => [e.envelope.mutationId, e.envelope.seq])).toEqual([["kept", 1]])
    expect(pending[0]?.undo).toEqual([{ table: T, id: "kept", before: Option.none() }])
    expect(yield* storage.meta).toEqual({ cursor: null, pushedThrough: 0, resnapshot: false })
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root; the test owns the database lifetime.
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
    expect(Arr.map(yield* storage.pending, (e) => e.envelope.seq)).toEqual([1])
    yield* Effect.provideService(Outbox.pruneReleased(-1), SqlClient.SqlClient, yield* SqlClient.SqlClient)
    expect(yield* storage.lookup(mid("b"))).toEqual(Option.none())
    expect(yield* append("c")).toBe(3)
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root; the test owns the database lifetime.
  }).pipe(Effect.provide(Db))
)

it.effect("staged pages and meta round-trip with bigint positions", () =>
  Effect.gen(function* () {
    const { storage } = yield* makeStorage
    const page = yield* decodePage({
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
    yield* storage.transaction(Effect.andThen(storage.stage(page), Effect.fail("rollback"))).pipe(Effect.flip)
    yield* storage.transaction(storage.stage({ ...page, continuation: Protocol.PullContinuation.make("next") }))
    expect(yield* storage.transaction(storage.stagedProgress)).toEqual({ continuation: Option.some("next"), count: 2 })
    yield* storage.clearStaged
    expect(yield* storage.staged).toEqual([])
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root; the test owns the database lifetime.
  }).pipe(Effect.provide(Db))
)

it.effect("resuming corrupt staged JSON or a non-string continuation dies naming echo_staged", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const storage = yield* SqliteSyncStorage.make(sql, registry)
    for (const page of [
      "not json",
      '{"continuation":42}',
      '{"continuation":null}',
      '{"continuation":{}}',
      '{"continuation":[]}'
    ]) {
      yield* sql`DELETE FROM echo_staged`
      yield* sql`INSERT INTO echo_staged (page) VALUES (${page})`
      const exit = yield* Effect.exit(storage.transaction(storage.stagedProgress))
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        expect(Arr.map(Arr.filter(exit.cause.reasons, Cause.isDieReason), (reason) => reason.defect)).toEqual([
          expect.objectContaining({ _tag: "CorruptStorage", table: "echo_staged" })
        ])
      }
    }
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root; the test owns the database lifetime.
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
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root; the test owns the database lifetime.
  }).pipe(Effect.provide(Db))
)

// --- migrate (SPEC §9, SPEC §13): the replica schema and the bookkeeping that describes it move together.

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
      yield* storage.stage(yield* page)
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
  const rowIds = Arr.map(yield* sql<{ id: string }>`SELECT id FROM t`, (r) => r.id)
  const meta = yield* storage.meta
  const staged = (yield* storage.staged).length
  const pending = yield* storage.pending
  const undo = Arr.flatMap(pending, (e) => e.undo).length
  // Kept in every outcome: the outbox (pending and released) and pushedThrough.
  const kept =
    Arr.join(
      Arr.map(pending, (e) => String(e.envelope.seq)),
      ","
    ) === "1" &&
    Option.isSome(yield* storage.lookup(mid("r"))) &&
    meta.pushedThrough === 2
  const old = [
    hash[0]?.value === registry.dataHash,
    columns.length === 2,
    !hasU,
    Arr.join(rowIds, ",") === "a",
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
  if (kept && Arr.every(old, Boolean)) return "old"
  if (kept && Arr.every(fresh, Boolean)) return "new"
  return `mixed ${yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))({ kept, old, fresh })}`
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
    const unique = yield* SqliteSyncStorage.make(sql, uniqueScope)
    const uniqueT = Option.getOrThrow(uniqueScope.get("t"))
    yield* unique.rows.write(uniqueT, "u1", Option.some(row("u1")))
    const duplicate = yield* Effect.exit(unique.rows.write(uniqueT, "u2", Option.some(row("u2"))))
    expect(duplicate._tag).toBe("Failure")
    yield* unique.rows.clearAll

    expect(yield* SqliteSyncStorage.migrate(reshaped)).toEqual({ rebuilt: true })
    expect(yield* SqliteSyncStorage.migrate(reshaped)).toEqual({ rebuilt: false })
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root; the test owns the database lifetime.
  }).pipe(Effect.provide(Db))
)

// oxlint-disable-next-line effecttsgo/async-function -- Drives isolated runtimes with a deliberately controlled scheduler and abort signal; the driver must remain outside them.
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
      await Effect.runPromise(Effect.sleep(0))
    }
    await migrating
    const outcome = await run(classify)
    const label = outcome._tag === "Success" ? outcome.value : `classify failed ${String(outcome.cause)}`
    outcomes.set(label, [...(outcomes.get(label) ?? []), boundary])
    await Effect.runPromise(Scope.close(scope, outcome))
    if (!yielded) break // the migration ran to completion before reaching this boundary: the sweep is done
  }
  expect(Arr.sort(outcomes.keys(), Order.String)).toEqual(["new", "old"])
  expect((outcomes.get("old") ?? []).length).toBeGreaterThan(10) // the sweep spans the transaction
})

it.effect("after a rebuild, installing the null-cursor set replays pending on top of it and recaptures undo", () =>
  Effect.gen(function* () {
    yield* seedOldReplica
    expect(yield* SqliteSyncStorage.migrate(reshaped)).toEqual({ rebuilt: true })
    const sql = yield* SqlClient.SqlClient
    const storage = yield* SqliteSyncStorage.make(sql, reshaped)
    const tTable = Option.getOrThrow(reshaped.get("t"))
    // The pending entries' handler: inserts row <mutationId> with n = 1 in scope "s".
    const insert = Mutation.make("t", {
      args: Schema.Struct({ mutationId: TId }),
      handler: (ctx, { mutationId }) =>
        ctx.store.insert(reshapedT, { id: mutationId, accessScopeId: AccessScope.AccessScopeId.make("s"), n: 1 })
    })
    const actor = AccessScope.PrincipalId.make("me")
    const grant = yield* Schema.encodeEffect(ScopeGrants.ScopeGrants.wire)({
      id: ScopeGrants.grantId(AccessScope.AccessScopeId.make("s"), actor),
      accessScopeId: AccessScope.AccessScopeId.make("s"),
      principalId: actor,
      role: "writer",
      grantedAt: DateTime.makeUnsafe(0)
    })
    // The null-cursor set: the actor's grant and one server row.
    const done = yield* decodeDone({
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
    expect(Arr.map(rows, (r) => [r["id"], r["n"]])).toEqual([
      ["p", 1],
      ["srv", null]
    ])
    const pending = yield* storage.pending
    expect(Arr.map(pending, (e) => [e.envelope.mutationId, e.undo])).toEqual([
      ["p", [{ table: tTable, id: "p", before: Option.none() }]]
    ])
    expect((yield* storage.meta).cursor).toBe(9n)
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root; the test owns the database lifetime.
  }).pipe(Effect.provide(Db))
)

it.effect("a database created by main's first outbox DDL migrates forward: undo column added, pending rows kept", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    // Verbatim from main 9c89ed5 Outbox.migrate, plus two rows written by that build.
    yield* sql`CREATE TABLE echo_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT`
    yield* sql`CREATE TABLE echo_outbox (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id TEXT NOT NULL,
        mutation_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        args TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        released_at INTEGER
      )`
    yield* sql`INSERT INTO echo_meta (key, value) VALUES ('client_id', 'c')`
    yield* sql`INSERT INTO echo_outbox (client_id, mutation_id, name, args, created_at, released_at)
      VALUES ('c', 'old1', 't', '{"mutationId":"old1"}', 5, NULL), ('c', 'old2', 't', '{}', 6, 7)`

    expect(yield* SqliteSyncStorage.migrate(registry)).toEqual({ rebuilt: false })
    const { storage } = yield* makeStorage
    const pending = yield* storage.transaction(storage.pending)
    expect(Arr.map(pending, (e) => [e.envelope.mutationId, e.envelope.seq, e.createdAt, e.undo])).toEqual([
      ["old1", 1, 5, []]
    ])
    expect(Option.isSome(yield* storage.lookup(mid("old2")))).toBe(true)
    // The new column works and the next seq continues after the old rows.
    yield* storage.transaction(storage.setUndo(seqOf(1), [{ table: T, id: "x", before: Option.none() }]))
    expect((yield* storage.pending)[0]?.undo).toEqual([{ table: T, id: "x", before: Option.none() }])
    expect(yield* storage.transaction(storage.nextSeq)).toBe(3)
    // Idempotent: running it again changes nothing.
    expect(yield* SqliteSyncStorage.migrate(registry)).toEqual({ rebuilt: false })
    expect(Arr.map(yield* storage.pending, (e) => e.envelope.mutationId)).toEqual(["old1"])
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test legacy SQLite composition root; the test owns migration and connection lifetime.
  }).pipe(Effect.provide(SqliteClient.layerMemory({})))
)

it.live(
  "a transaction or admitted statement queued behind a long transaction can be interrupted while it waits; the rest commit, one at a time",
  () =>
    Effect.gen(function* () {
      const { storage } = yield* makeStorage
      const ran: Array<string> = []
      const holding = yield* Deferred.make<void>()
      const gate = yield* Deferred.make<void>()
      // The holder plays a long pull install: it owns the connection until `gate` opens.
      const holder = yield* Effect.forkChild(
        storage.transaction(
          Effect.gen(function* () {
            yield* Deferred.succeed(holding, undefined)
            yield* Deferred.await(gate)
            ran.push("holder")
          })
        )
      )
      yield* Deferred.await(holding)
      // Serialization: no two transaction bodies are ever inside at once (each yields to invite overlap).
      const inside = { now: 0, max: 0 }
      const queued = (name: string) =>
        Effect.forkChild(
          storage.transaction(
            Effect.gen(function* () {
              inside.now++
              inside.max = Math.max(inside.max, inside.now)
              ran.push(name)
              for (let i = 0; i < 5; i++) yield* Effect.yieldNow
              yield* storage.appendPending(entry(yield* storage.nextSeq, name))
              inside.now--
            })
          )
        )
      const a = yield* queued("a")
      const x = yield* queued("x")
      const b = yield* queued("b")
      // `admit` (the leader's exec/subscribe path) waits on the same permit.
      const y = yield* Effect.forkChild(storage.admit(Effect.sync(() => ran.push("y"))))
      for (let i = 0; i < 100; i++) yield* Effect.yieldNow
      const interrupting = yield* Effect.forkChild(Fiber.interruptAll([x, y]))
      for (let i = 0; i < 100; i++) yield* Effect.yieldNow
      // The interrupt completes while the holder still owns the connection.
      const done = interrupting.pollUnsafe() !== undefined
      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.join(holder)
      yield* Fiber.join(a)
      yield* Fiber.join(b)
      yield* Fiber.join(interrupting)
      expect(done).toBe(true)
      // The interrupted body never ran; the others ran after the holder, in some order (no fairness promised).
      expect(ran[0]).toBe("holder")
      expect(Arr.sort(ran, Order.String)).toEqual(["a", "b", "holder"])
      expect(inside.max).toBe(1)
      const pending = yield* storage.pending
      expect(
        Arr.sort(
          Arr.map(pending, (e) => e.envelope.mutationId),
          Order.String
        )
      ).toEqual(["a", "b"])
      expect(Arr.map(pending, (e) => e.envelope.seq)).toEqual([1, 2])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root; the test owns the database lifetime.
    }).pipe(Effect.provide(Db))
)

it.live("a mutate queued behind a long pull install can be interrupted while it waits; its handler never runs", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const storage = yield* SqliteSyncStorage.make(sql, registry)
    const installing = yield* Deferred.make<void>()
    const gate = yield* Deferred.make<void>()
    const state = { blockReplay: false, added: 0 }
    // "block" stalls when the install replays it, so the install holds the connection until `gate` opens.
    const block = Mutation.make("block", {
      args: Schema.Struct({}),
      handler: () =>
        state.blockReplay ? Effect.andThen(Deferred.succeed(installing, undefined), Deferred.await(gate)) : Effect.void
    })
    const add = Mutation.make("add", { args: Schema.Struct({}), handler: () => Effect.sync(() => void state.added++) })
    const transport: Engine.Transport = {
      push: () => Effect.die("unused"),
      pull: () =>
        Stream.fromEffect(
          Effect.orDie(decodeDone({ _tag: "PullDone", changes: [], acks: [], nextCursor: "5", expiredThrough: 0 }))
        )
    }
    const engine = yield* Engine.make({
      clientId: Ids.ClientId.make("c"),
      actor: AccessScope.PrincipalId.make("me"),
      storage,
      registry,
      mutations: Mutation.makeRegistry([block, add]),
      transport
    })
    yield* engine.mutate(mid("p"), "block", {})
    state.blockReplay = true
    const pull = yield* Effect.forkChild(engine.pull)
    yield* Deferred.await(installing)

    const queued = yield* Effect.forkChild(engine.mutate(mid("q"), "add", {}))
    for (let i = 0; i < 100; i++) yield* Effect.yieldNow
    const interrupting = yield* Effect.forkChild(Fiber.interrupt(queued))
    for (let i = 0; i < 100; i++) yield* Effect.yieldNow
    // No deadlock: the interrupt completes while the install still holds the connection.
    const doneWhileHeld = interrupting.pollUnsafe() !== undefined
    state.blockReplay = false
    yield* Deferred.succeed(gate, undefined)
    yield* Fiber.join(pull)
    yield* Fiber.join(interrupting)

    expect(doneWhileHeld).toBe(true)
    expect(state.added).toBe(0) // the interrupted mutate's handler never ran, before or after release
    expect(yield* storage.transaction(storage.lookup(mid("q")))).toEqual(Option.none())
    expect((yield* storage.meta).cursor).toBe(5n) // the install committed
    // The connection is free again: the next mutate runs and takes the next seq (nothing consumed by q).
    const r = yield* engine.mutate(mid("r"), "add", {})
    expect([r.seq, state.added]).toEqual([2, 1])
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root; the test owns the database lifetime.
  }).pipe(Effect.provide(Db))
)
