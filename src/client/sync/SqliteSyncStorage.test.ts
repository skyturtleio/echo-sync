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
import * as Reactivity from "effect/reactivity/Reactivity"
import * as SqlClient from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Protocol, Registry, ScopeGrants, SyncTable } from "echo-sync"
import * as Outbox from "../Outbox.ts"
import * as Engine from "./Engine.ts"
import * as Catalogs from "./Catalogs.ts"
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

const migrate = (next: Registry.SyncRegistry, mutations = Mutation.makeRegistry([])) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const storage = yield* SqliteSyncStorage.make(sql, next)
    return yield* SqliteSyncStorage.migrate(
      next,
      Engine.replayMigrated({
        storage,
        mutations,
        actor: AccessScope.PrincipalId.make("me")
      })
    )
  })

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
      yield* storage.rows.write(
        ScopeGrants.ScopeGrants,
        "1:s:me",
        Option.some({ id: "1:s:me", access_scope_id: "s", principal_id: "me", role: "writer", granted_at: 0 })
      )
      yield* storage.appendPending({
        ...entry(yield* storage.nextSeq, "p"),
        undo: [{ table: T, id: "a", before: Option.some(row("a")) }]
      })
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
    Arr.join(rowIds, ",") === "a",
    meta.cursor === 7n,
    meta.resnapshot,
    staged === 0,
    undo === 1,
    columns.length === 3 && (yield* sql<{ n: number }>`SELECT n FROM t WHERE id = 'a'`)[0]?.n === 23,
    pending[0]?.undo[0]?.before.pipe(
      Option.map((r) => r["n"]),
      Option.getOrUndefined
    ) === null
  ]
  if (kept && Arr.every(old, Boolean)) return "old"
  if (kept && Arr.every(fresh, Boolean)) return "new"
  return `mixed ${yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))({ kept, old, fresh })}`
})

it.effect("index changes preserve rows and compatible shape changes preserve the offline base", () =>
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
    expect(yield* migrate(uniqueScope)).toEqual({ rebuilt: true })
    const unique = yield* SqliteSyncStorage.make(sql, uniqueScope)
    const uniqueT = Option.getOrThrow(uniqueScope.get("t"))
    expect(yield* unique.rows.scan(uniqueT)).toEqual([row("a")])
    yield* unique.rows.clearAll
    yield* unique.rows.write(uniqueT, "u1", Option.some(row("u1")))
    const duplicate = yield* Effect.exit(unique.rows.write(uniqueT, "u2", Option.some(row("u2"))))
    expect(duplicate._tag).toBe("Failure")
    yield* unique.rows.clearAll

    expect(yield* migrate(reshaped)).toEqual({ rebuilt: true })
    expect(yield* SqliteSyncStorage.migrate(reshaped)).toEqual({ rebuilt: false })
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root; the test owns the database lifetime.
  }).pipe(Effect.provide(Db))
)

it.effect("a nullable addition keeps stripped old-client rows offline and a null-cursor refresh repairs them", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const { storage } = yield* makeStorage
    const actor = AccessScope.PrincipalId.make("me")
    const mutations = Mutation.makeRegistry([])
    const response = yield* decodeDone({
      _tag: "PullDone",
      changes: [{ _tag: "upsert", table: "t", row: { ...row("a"), n: 37 } }],
      acks: [],
      nextCursor: "7",
      expiredThrough: 0
    })
    const old = yield* Engine.make({
      storage,
      registry,
      actor,
      mutations,
      clientId: Ids.ClientId.make("c"),
      transport: {
        push: () => Effect.die("unexpected push"),
        pull: () => Stream.make(response)
      }
    })
    yield* old.pull
    expect(yield* storage.rows.scan(T)).toEqual([row("a")]) // canonicalization stripped the backfill
    yield* migrate(reshaped)
    const next = yield* SqliteSyncStorage.make(sql, reshaped)
    expect(yield* next.rows.scan(reshapedT)).toEqual([{ ...row("a"), n: null }])
    expect(yield* next.meta).toMatchObject({ cursor: 7n, resnapshot: true })
    const engine = yield* Engine.make({
      storage: next,
      registry: reshaped,
      actor,
      mutations,
      clientId: Ids.ClientId.make("c"),
      transport: {
        push: () => Effect.die("unexpected push"),
        pull: (request) => {
          expect(request.cursor).toBeNull()
          return Stream.make(response)
        }
      }
    })
    yield* engine.pull
    expect(yield* next.rows.scan(reshapedT)).toEqual([{ ...row("a"), n: 37 }])
    expect(yield* next.meta).toMatchObject({ cursor: 7n, resnapshot: false })
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root.
  }).pipe(Effect.provide(Db))
)

it.effect("migration rebases offline edits with new handlers and rejection restores a new-shape undo image", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const before = SyncTable.make("edited", { id: TId, accessScopeId: AccessScope.AccessScopeId, label: Schema.String })
    const after = SyncTable.make("edited", {
      id: TId,
      accessScopeId: AccessScope.AccessScopeId,
      label: Schema.String,
      n: Schema.NullOr(Schema.Int)
    })
    const airports = SyncTable.catalog("airports", { id: TId, name: Schema.String })
    const oldRegistry = Registry.make([before])
    const newRegistry = Registry.make([after, airports])
    yield* migrate(oldRegistry)
    const storage = yield* SqliteSyncStorage.make(sql, oldRegistry)
    const actor = AccessScope.PrincipalId.make("me")
    const scope = AccessScope.AccessScopeId.make("s")
    const grant = yield* Schema.encodeEffect(ScopeGrants.ScopeGrants.wire)({
      id: ScopeGrants.grantId(scope, actor),
      accessScopeId: scope,
      principalId: actor,
      role: "writer",
      grantedAt: DateTime.makeUnsafe(0)
    })
    yield* storage.rows.write(ScopeGrants.ScopeGrants, String(grant["id"]), Option.some(grant))
    yield* storage.rows.write(before, "a", Option.some({ ...row("a"), label: "base" }))
    yield* storage.setMeta({ cursor: pos(7n), pushedThrough: 0, resnapshot: false })
    const oldMutations = Mutation.makeRegistry([
      Mutation.make("edit", {
        args: Schema.String,
        handler: (ctx, label) => ctx.store.patch(before, TId.make("a"), { label })
      })
    ])
    const newMutations = Mutation.makeRegistry([
      Mutation.make("edit", {
        args: Schema.String,
        handler: (ctx, label) =>
          Effect.gen(function* () {
            // The same deploy introduces a catalog: startup must initialize it before replay.
            const reference = yield* ctx.store.scan(airports)
            yield* ctx.store.patch(after, TId.make("a"), { label: `${label}-new`, n: 41 + reference.length })
          })
      })
    ])
    let pushes = 0
    const transport: Engine.Transport = {
      pull: () => Stream.empty,
      push: (request) => {
        pushes++
        return Effect.succeed({
          results: Arr.map(request.mutations, (m) => ({
            mutationId: m.mutationId,
            seq: m.seq,
            position: pos(7n),
            outcome: { _tag: "Rejected", error: new Mutation.UnknownMutation({ mutationName: "edit" }) }
          }))
        })
      }
    }
    const oldEngine = yield* Engine.make({
      storage,
      registry: oldRegistry,
      actor,
      mutations: oldMutations,
      clientId: Ids.ClientId.make("c"),
      transport
    })
    yield* oldEngine.mutate(mid("edit"), "edit", "local")
    yield* migrate(newRegistry, newMutations)
    const next = yield* SqliteSyncStorage.make(sql, newRegistry)
    expect(yield* next.rows.scan(after)).toEqual([{ ...row("a"), label: "local-new", n: 41 }])
    const pending = yield* next.pending
    expect(pending[0]?.undo[0]?.before).toEqual(Option.some({ ...row("a"), label: "base", n: null }))
    expect(yield* SqliteSyncStorage.notices(sql)).toEqual([])
    const engine = yield* Engine.make({
      storage: next,
      registry: newRegistry,
      actor,
      mutations: newMutations,
      clientId: Ids.ClientId.make("c"),
      transport
    })
    expect(yield* engine.push).toEqual({ processed: 0, stopped: true })
    expect(pushes).toBe(0)
    // The real engine deliberately refuses push responses during resnapshot. Exercise its
    // covered-rejection path after a refresh, which must undo using recaptured new-shape images.
    yield* next.transaction(
      Engine.applyPageSet(
        { storage: next, registry: newRegistry, actor, mutations: newMutations },
        [],
        yield* decodeDone({
          _tag: "PullDone",
          changes: [
            { _tag: "upsert", table: "scope_grants", row: grant },
            { _tag: "upsert", table: "edited", row: { ...row("a"), label: "base", n: 53 } }
          ],
          acks: [],
          nextCursor: "7",
          expiredThrough: 0
        })
      )
    )
    yield* engine.push
    expect(yield* next.rows.scan(after)).toEqual([{ ...row("a"), label: "base", n: 53 }])
    expect(yield* next.pending).toEqual([])
    expect((yield* SqliteSyncStorage.notices(sql)).length).toBe(1)
    // The baseline fits the new unique set, but the queued edit does not. A partial
    // write before its conflict must also disappear, without preventing offline open.
    yield* next.rows.write(after, "b", Option.some({ ...row("b"), label: "base-new", n: 88 }))
    yield* engine.mutate(mid("conflict"), "edit", "base")
    const constrained = SyncTable.make(
      "edited",
      {
        id: TId,
        accessScopeId: AccessScope.AccessScopeId,
        label: Schema.String,
        n: Schema.NullOr(Schema.Int)
      },
      { unique: [["label"]] }
    )
    const conflicting = Mutation.makeRegistry([
      Mutation.make("edit", {
        args: Schema.String,
        handler: (ctx, label) =>
          Effect.gen(function* () {
            yield* ctx.store.patch(constrained, TId.make("b"), { n: 999 })
            yield* ctx.store.patch(constrained, TId.make("a"), { label: `${label}-new` })
          })
      })
    ])
    yield* migrate(Registry.make([constrained]), conflicting)
    expect(yield* next.rows.scan(after)).toEqual([
      { ...row("a"), label: "base", n: 53 },
      { ...row("b"), label: "base-new", n: 88 }
    ])
    expect(Arr.map(yield* next.pending, (p) => [p.envelope.mutationId, p.undo])).toEqual([["conflict", []]])
    expect((yield* SqliteSyncStorage.notices(sql)).length).toBe(1)
    // Conversely, optimistic rows can fit while their baseline violates the new unique
    // set. Restore under old constraints, then empty only the incompatible table.
    yield* migrate(newRegistry)
    yield* next.release(Arr.map(yield* next.pending, (p) => p.envelope.seq))
    yield* next.rows.write(after, "a", Option.some({ ...row("a"), label: "duplicate", n: 1 }))
    const duplicate = { ...row("b"), label: "duplicate", n: 2 }
    yield* next.rows.write(after, "b", Option.some(duplicate))
    yield* next.appendPending({
      ...entry(yield* next.nextSeq, "separate"),
      undo: [{ table: after, id: "b", before: Option.some(duplicate) }]
    })
    yield* next.rows.write(after, "b", Option.some({ ...duplicate, label: "distinct" }))
    yield* migrate(Registry.make([constrained]))
    expect(yield* next.rows.scan(after)).toEqual([])
    expect(yield* next.rows.scan(ScopeGrants.ScopeGrants)).toEqual([grant])
    expect((yield* next.pending)[0]?.envelope.mutationId).toBe("separate")
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root.
  }).pipe(Effect.provide(Db))
)

it.effect("incompatible tables empty independently while drops and nullable relaxations retain exact rows", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const fixed = SyncTable.make("fixed", { id: TId, accessScopeId: AccessScope.AccessScopeId, n: Schema.Int })
    yield* migrate(Registry.make([T, fixed]))
    yield* sql`INSERT INTO t VALUES ('kept', 's')`
    yield* sql`INSERT INTO fixed VALUES ('changed', 's', 19)`
    const relaxed = SyncTable.make("fixed", {
      id: TId,
      accessScopeId: AccessScope.AccessScopeId,
      n: Schema.NullOr(Schema.Int)
    })
    yield* migrate(Registry.make([T, relaxed]))
    expect(yield* sql`SELECT n FROM fixed`).toEqual([{ n: 19 }])
    const changed = SyncTable.make("fixed", {
      id: TId,
      accessScopeId: AccessScope.AccessScopeId,
      n: Schema.NullOr(Schema.String)
    })
    yield* migrate(Registry.make([Option.getOrThrow(reindexed.get("t")), changed]))
    expect(yield* sql`SELECT * FROM fixed`).toEqual([])
    expect(yield* sql`SELECT id FROM t`).toEqual([{ id: "kept" }])
    expect(
      (yield* sql`SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 't' AND sql IS NOT NULL`).length
    ).toBe(1)
    yield* sql`INSERT INTO fixed VALUES ('drop', 's', 'text')`
    const dropped = SyncTable.make("fixed", { id: TId, accessScopeId: AccessScope.AccessScopeId })
    yield* migrate(Registry.make([T, dropped]))
    expect(yield* sql`SELECT * FROM fixed`).toEqual([{ id: "drop", access_scope_id: "s" }])
    // A new unique constraint incompatible with the base empties only its table.
    yield* sql`INSERT INTO fixed VALUES ('duplicate', 's')`
    const unique = SyncTable.make(
      "fixed",
      { id: TId, accessScopeId: AccessScope.AccessScopeId },
      { unique: [["accessScopeId"]] }
    )
    yield* migrate(Registry.make([T, unique]))
    expect(yield* sql`SELECT * FROM fixed`).toEqual([])
    expect(yield* sql`SELECT id FROM t`).toEqual([{ id: "kept" }])
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root.
  }).pipe(Effect.provide(Db))
)

it.effect("a pending unique-value swap survives migration, refresh and authoritative rejection", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const fields = { id: TId, accessScopeId: AccessScope.AccessScopeId, label: Schema.String }
    const before = SyncTable.make("swaps", fields, { unique: [["label"]] })
    const after = SyncTable.make("swaps", { ...fields, n: Schema.NullOr(Schema.Int) }, { unique: [["label"]] })
    const swap = (table: typeof before | typeof after) =>
      Mutation.makeRegistry([
        Mutation.make("swap", {
          args: Schema.Null,
          handler: (ctx) =>
            Effect.gen(function* () {
              yield* ctx.store.patch(table, TId.make("a"), { label: "temporary" })
              yield* ctx.store.patch(table, TId.make("b"), { label: "x" })
              yield* ctx.store.patch(table, TId.make("a"), { label: "y" })
            })
        })
      ])
    const oldRegistry = Registry.make([before])
    const nextRegistry = Registry.make([after])
    yield* migrate(oldRegistry)
    const storage = yield* SqliteSyncStorage.make(sql, oldRegistry)
    const grant = { id: "1:s:me", access_scope_id: "s", principal_id: "me", role: "writer", granted_at: 0 }
    yield* storage.rows.write(ScopeGrants.ScopeGrants, grant.id, Option.some(grant))
    yield* storage.rows.write(before, "a", Option.some({ ...row("a"), label: "x" }))
    yield* storage.rows.write(before, "b", Option.some({ ...row("b"), label: "y" }))
    yield* storage.setMeta({ cursor: pos(7n), resnapshot: false, pushedThrough: 0 })
    const transport: Engine.Transport = {
      pull: () => Stream.empty,
      push: (request) =>
        Effect.succeed({
          results: Arr.map(request.mutations, (m) => ({
            mutationId: m.mutationId,
            position: pos(7n),
            outcome: { _tag: "Rejected", error: new Mutation.UnknownMutation({ mutationName: "swap" }) }
          }))
        })
    }
    const actor = AccessScope.PrincipalId.make("me")
    const old = yield* Engine.make({
      storage,
      registry: oldRegistry,
      actor,
      clientId: Ids.ClientId.make("c"),
      mutations: swap(before),
      transport
    })
    yield* old.mutate(mid("swap"), "swap", null)
    const envelope = (yield* storage.pending)[0]?.envelope
    const mutations = swap(after)
    yield* migrate(nextRegistry, mutations)
    const next = yield* SqliteSyncStorage.make(sql, nextRegistry)
    const optimistic = [
      { ...row("a"), label: "y", n: null },
      { ...row("b"), label: "x", n: null }
    ]
    expect(yield* next.rows.scan(after)).toEqual(optimistic)
    expect((yield* next.pending)[0]?.envelope).toEqual(envelope)
    expect((yield* next.pending)[0]?.undo.length).toBe(2)
    const base = [
      { ...row("a"), label: "x", n: 11 },
      { ...row("b"), label: "y", n: 29 }
    ]
    yield* next.transaction(
      Engine.applyPageSet(
        { storage: next, registry: nextRegistry, actor, mutations },
        [],
        yield* decodeDone({
          _tag: "PullDone",
          changes: [
            { _tag: "upsert", table: "scope_grants", row: grant },
            ...Arr.map(base, (value) => ({ _tag: "upsert", table: "swaps", row: value }))
          ],
          acks: [],
          nextCursor: "7",
          expiredThrough: 0
        })
      )
    )
    expect(yield* next.rows.scan(after)).toEqual([
      { ...row("a"), label: "y", n: 11 },
      { ...row("b"), label: "x", n: 29 }
    ])
    const engine = yield* Engine.make({
      storage: next,
      registry: nextRegistry,
      actor,
      clientId: Ids.ClientId.make("c"),
      mutations,
      transport
    })
    yield* engine.push
    expect(yield* next.rows.scan(after)).toEqual(base)
    expect(yield* next.pending).toEqual([])
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root.
  }).pipe(Effect.provide(Db))
)

it.effect("incompatible catalog reads defer only dependent migration replay and never swallow handler defects", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const reactivity = yield* Reactivity.make
    const before = SyncTable.make("edits", { id: TId, accessScopeId: AccessScope.AccessScopeId, label: Schema.String })
    const after = SyncTable.make("edits", { ...before.fields, n: Schema.NullOr(Schema.Int) })
    const airport = SyncTable.catalog("airports", { id: TId, name: Schema.String })
    const expanded = SyncTable.catalog(
      "airports",
      { ...airport.fields, code: Schema.String },
      { indexes: { by_code: ["code"] } }
    )
    const oldRegistry = Registry.make([before, airport])
    const nextRegistry = Registry.make([after, expanded])
    yield* migrate(oldRegistry)
    const storage = yield* SqliteSyncStorage.make(sql, oldRegistry)
    const grant = { id: "1:s:me", access_scope_id: "s", principal_id: "me", role: "writer", granted_at: 0 }
    yield* storage.rows.write(ScopeGrants.ScopeGrants, grant.id, Option.some(grant))
    const ids = ["read", "scan", "range", "project", "aggregate", "other"]
    for (const id of ids) yield* storage.rows.write(before, id, Option.some({ ...row(id), label: "base" }))
    yield* Catalogs.install(sql, storage, reactivity, airport, "v1", [{ id: "airport", name: "Old" }])
    const old = yield* Engine.make({
      storage,
      registry: oldRegistry,
      actor: AccessScope.PrincipalId.make("me"),
      clientId: Ids.ClientId.make("c"),
      mutations: Mutation.makeRegistry([
        Mutation.make("edit", {
          args: Schema.String,
          handler: (ctx, id) => ctx.store.patch(before, TId.make(id), { label: "old-optimistic" })
        })
      ]),
      transport: { pull: () => Stream.empty, push: () => Effect.die("unused") }
    })
    for (const id of ids) yield* old.mutate(mid(id), "edit", id)
    const envelopes = Arr.map(yield* storage.pending, (pending) => pending.envelope)
    const defect = new Error("unrelated handler defect")
    const failed = yield* Effect.exit(
      migrate(
        nextRegistry,
        Mutation.makeRegistry([Mutation.make("edit", { args: Schema.String, handler: () => Effect.die(defect) })])
      )
    )
    expect(Exit.isFailure(failed)).toBe(true)
    if (Exit.isFailure(failed))
      expect(Arr.map(Arr.filter(failed.cause.reasons, Cause.isDieReason), (reason) => reason.defect)).toContain(defect)
    expect(yield* storage.rows.scan(before)).toEqual(
      Arr.map(["aggregate", "other", "project", "range", "read", "scan"], (id) => ({
        ...row(id),
        label: "old-optimistic"
      }))
    )
    const mutations = Mutation.makeRegistry([
      Mutation.make("edit", {
        args: Schema.String,
        handler: (ctx, id) =>
          Effect.gen(function* () {
            yield* ctx.store.patch(after, TId.make(id), { label: "partial" })
            if (id === "read") yield* ctx.store.get(expanded, TId.make("airport"))
            if (id === "scan") yield* ctx.store.scan(expanded)
            if (id === "range") yield* ctx.store.query(expanded).withIndex("by_code").collect
            if (id === "project") yield* ctx.store.query(expanded).withIndex("by_code").select("name").collect
            if (id === "aggregate") yield* ctx.store.query(expanded).withIndex("by_code").aggregate({ sum: [] })
            yield* ctx.store.patch(after, TId.make(id), { label: "new", n: 99 })
          })
      })
    ])
    yield* migrate(nextRegistry, mutations)
    const next = yield* SqliteSyncStorage.make(sql, nextRegistry)
    expect(Arr.map(yield* next.pending, (pending) => pending.envelope)).toEqual(envelopes)
    expect(Arr.map(yield* next.pending, (pending) => pending.undo.length)).toEqual([0, 0, 0, 0, 0, 1])
    expect(yield* sql`SELECT id, label, n FROM edits ORDER BY id`).toEqual([
      { id: "aggregate", label: "base", n: null },
      { id: "other", label: "new", n: 99 },
      { id: "project", label: "base", n: null },
      { id: "range", label: "base", n: null },
      { id: "read", label: "base", n: null },
      { id: "scan", label: "base", n: null }
    ])
    expect(yield* sql`SELECT * FROM airports`).toEqual([{ id: "airport", name: "Old" }])
    expect(yield* sql`SELECT value FROM echo_meta WHERE key = 'catalog:airports'`).toEqual([{ value: "v1" }])
    expect(yield* sql`SELECT * FROM echo_notices`).toEqual([])
    yield* Catalogs.install(sql, next, reactivity, expanded, "v2", [{ id: "airport", name: "New", code: "NEW" }])
    yield* next.transaction(
      Engine.applyPageSet(
        { storage: next, registry: nextRegistry, actor: AccessScope.PrincipalId.make("me"), mutations },
        [],
        yield* decodeDone({
          _tag: "PullDone",
          changes: [
            { _tag: "upsert", table: "scope_grants", row: grant },
            ...Arr.map(ids, (id) => ({ _tag: "upsert", table: "edits", row: { ...row(id), label: "server", n: 12 } }))
          ],
          acks: [],
          nextCursor: "7",
          expiredThrough: 0
        })
      )
    )
    expect(yield* sql`SELECT count(*) AS n FROM edits WHERE label = 'new' AND n = 99`).toEqual([{ n: 6 }])
    expect(Arr.map(yield* next.pending, (pending) => pending.undo.length)).toEqual([1, 1, 1, 1, 1, 1])
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root.
  }).pipe(Effect.provide(Db))
)

it.effect("stored physical undo survives codec changes and legacy replicas adopt manifests or rebuild once", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const fields = {
      id: TId,
      accessScopeId: AccessScope.AccessScopeId,
      flag: Schema.Boolean,
      at: Schema.DateTimeUtc,
      object: Schema.Struct({ nested: Schema.Array(Schema.Json) }),
      list: Schema.Array(Schema.Int),
      note: Schema.NullOr(Schema.String)
    }
    const old = SyncTable.make("physical", fields)
    const next = SyncTable.make("physical", { ...fields, n: Schema.NullOr(Schema.Int) })
    const oldRegistry = Registry.make([old, T])
    yield* migrate(oldRegistry)
    const storage = yield* SqliteSyncStorage.make(sql, oldRegistry)
    const base = {
      ...row("a"),
      flag: true,
      at: -123456,
      object: { nested: [false, null, "café"] },
      list: [3, -7],
      note: null
    }
    yield* storage.rows.write(old, "a", Option.some(base))
    yield* storage.rows.write(T, "kept", Option.some(row("kept")))
    yield* storage.setMeta({ cursor: pos(7n), pushedThrough: 0, resnapshot: false })
    yield* sql`DELETE FROM echo_meta WHERE key = 'replica_manifests'`
    yield* migrate(oldRegistry) // unchanged legacy open adopts manifests without emptying data
    expect(yield* storage.rows.scan(old)).toEqual([base])
    expect((yield* sql`SELECT 1 FROM echo_meta WHERE key = 'replica_manifests'`).length).toBe(1)
    yield* storage.appendPending({
      ...entry(yield* storage.nextSeq, "physical"),
      undo: [{ table: old, id: "a", before: Option.some(base) }]
    })
    yield* storage.rows.write(
      old,
      "a",
      Option.some({ ...base, flag: false, at: 98765, object: { nested: [] }, list: [], note: "optimistic" })
    )
    const nextRegistry = Registry.make([next, T])
    yield* migrate(nextRegistry)
    const after = yield* SqliteSyncStorage.make(sql, nextRegistry)
    expect(yield* after.rows.scan(next)).toEqual([{ ...base, n: null }])
    expect((yield* after.pending)[0]?.undo).toEqual([]) // unknown new handler retains intent only
    // A pre-manifest replica encountering a changed shape still uses the documented old fallback.
    yield* sql`DELETE FROM echo_meta WHERE key = 'replica_manifests'`
    yield* migrate(oldRegistry)
    expect(yield* storage.rows.scan(old)).toEqual([])
    expect(yield* storage.rows.scan(T)).toEqual([])
    expect(yield* storage.meta).toMatchObject({ cursor: null, resnapshot: false })
    expect((yield* storage.pending).length).toBe(1)
    expect(yield* migrate(oldRegistry)).toEqual({ rebuilt: false })
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test SQLite composition root.
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
    const mutations = Mutation.makeRegistry([
      Mutation.make("t", {
        args: Schema.Json,
        handler: (ctx) => ctx.store.patch(reshapedT, TId.make("a"), { n: 23 })
      })
    ])
    const migrating = run(migrate(reshaped, mutations), { scheduler, signal: controller.signal }).finally(
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
    expect(yield* migrate(reshaped)).toEqual({ rebuilt: true })
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
