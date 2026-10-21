// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { expect, it } from "@effect/vitest"
import { Cause, DateTime, Deferred, Effect, Exit, Fiber, Layer, Option, Queue, Schema, Stream } from "effect"
import * as Reactivity from "effect/reactivity/Reactivity"
import { RpcTest } from "effect/rpc"
import * as SqlClient from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Protocol, Query, Registry, ScopeGrants, type Store, SyncTable } from "echo-sync"
import * as Engine from "../sync/Engine.ts"
import * as SqliteSyncStorage from "../sync/SqliteSyncStorage.ts"
import * as SyncLoop from "../sync/SyncLoop.ts"
import { enqueueAndWake, makeHandlers } from "./Leader.ts"
import { type LeaderCalls, tabApi } from "./LeaderClient.ts"
import { LeaderRpcs, LeaderUnavailable } from "./LeaderRpcs.ts"

const T = SyncTable.make("t", { id: Schema.String.pipe(Schema.brand("TId")), accessScopeId: AccessScope.AccessScopeId })
const registry = Registry.make([T])
const Db = Layer.effectDiscard(SqliteSyncStorage.migrate(registry)).pipe(
  Layer.provideMerge(SqliteClient.layerMemory({}))
)
const decodeDone = Schema.decodeUnknownSync(Protocol.PullDone)
const position = Schema.decodeUnknownSync(Ids.Position)("1")

it.live("an enqueue cancelled while its COMMIT runs still wakes the loop: the entry is pushed with no other wake", () =>
  Effect.gen(function* () {
    const real = SqliteSyncStorage.make(yield* SqlClient.SqlClient, registry)
    const committed = yield* Deferred.make<void>()
    const gate = yield* Deferred.make<void>()
    const state = { gateNext: false }
    // The interrupt lands after COMMIT but before the transaction returns, as it can while withTransaction
    // masks COMMIT: the entry is durable, and the caller's fiber is interrupted on the way out.
    const storage: Engine.SyncStorage = {
      ...real,
      transaction: <A, E, R>(self: Effect.Effect<A, E, R>) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.tap(restore(real.transaction(self)), () => {
            if (!state.gateNext) return Effect.void
            state.gateNext = false
            return Effect.andThen(Deferred.succeed(committed, undefined), Deferred.await(gate))
          })
        )
    }
    const pushed: Array<string> = []
    const pushObserved = yield* Deferred.make<void>()
    const transport: Engine.Transport<never> = {
      push: (request) =>
        Effect.andThen(
          Effect.sync(() => void pushed.push(...request.mutations.map((m) => m.mutationId))),
          Effect.as(Deferred.succeed(pushObserved, undefined), {
            results: request.mutations.map((m) => ({
              mutationId: m.mutationId,
              position,
              outcome: { _tag: "Applied" as const }
            }))
          })
        ),
      pull: () =>
        Effect.succeed(decodeDone({ _tag: "PullDone", changes: [], acks: [], nextCursor: "1", expiredThrough: 0 }))
    }
    const engine = yield* Engine.make({
      clientId: Ids.ClientId.make("c"),
      actor: AccessScope.PrincipalId.make("me"),
      storage,
      registry,
      mutations: Mutation.makeRegistry([Mutation.make("add", { args: Schema.Struct({}), handler: () => Effect.void })]),
      transport
    })
    // The loop's first cycle (nothing to push, one pull) must be over before the gate is armed, so the
    // gated transaction is the enqueue's and the loop is idle, waiting for a wake.
    const pulled = yield* Deferred.make<void>()
    const observed: Engine.Engine<never> = {
      ...engine,
      pull: Effect.ensuring(engine.pull, Deferred.succeed(pulled, undefined))
    }
    const loop = yield* SyncLoop.make({ engine: observed, isFatalPush: () => false })
    yield* Effect.forkChild(loop.run)
    yield* Deferred.await(pulled)

    state.gateNext = true
    const enqueue = yield* Effect.forkChild(enqueueAndWake(engine, loop.wake)(Ids.MutationId.make("m"), "add", {}))
    yield* Deferred.await(committed)
    // COMMIT is done and the permit released; only the wrapper's tap holds the enqueue's fiber.
    expect((yield* real.transaction(real.pending)).map((e) => e.envelope.mutationId)).toEqual(["m"])
    expect(pushed).toEqual([])
    const interrupting = yield* Effect.forkChild(Fiber.interrupt(enqueue))
    yield* Effect.yieldNow
    yield* Deferred.succeed(gate, undefined)
    yield* Fiber.join(interrupting)
    expect(Exit.hasInterrupts(yield* Fiber.await(enqueue))).toBe(true)

    // Nothing else wakes the loop (no pokes, no further enqueue): the finalizer's wake must be enough.
    const push = yield* Deferred.await(pushObserved).pipe(Effect.timeout("2 seconds"), Effect.option)
    expect(Option.isSome(push) ? "push observed" : "no push within 2 s").toBe("push observed")
    expect(pushed).toEqual(["m"])
    expect((yield* loop.status).lastError).toBeNull()
  }).pipe(Effect.provide(Db))
)

// ---------------------------------------------------------------------------------------------
// Registered queries, raw live queries and raw exec over the leader's RPC handlers (DESIGN §10.1)
// ---------------------------------------------------------------------------------------------

const RowId = Schema.String.pipe(Schema.brand("RowId"))
const rowFields = { id: RowId, accessScopeId: AccessScope.AccessScopeId, body: Schema.String }
const Flags = SyncTable.make("flags", { ...rowFields })
const B = SyncTable.make("b", { ...rowFields })
const C = SyncTable.make("c", { ...rowFields })
const L = SyncTable.make("l", { ...rowFields })
const R = SyncTable.make("r", { ...rowFields })
const Raw = SyncTable.make("raw", { ...rowFields })
const Docs = SyncTable.make("docs", { ...rowFields }, { indexes: { by_body: ["body"] } })
const queryRegistry = Registry.make([Flags, B, C, L, R, Raw, Docs])
const scope = AccessScope.AccessScopeId.make("s")
const me = AccessScope.PrincipalId.make("me")

/** Reads `flags`, then only the table the flag names: its dependency set changes with the data. */
const pick = Query.make("pick", {
  args: Schema.Struct({}),
  success: Schema.Struct({ target: Schema.String, n: Schema.Number }),
  handler: (db) =>
    Effect.gen(function* () {
      const flag = yield* db.get(Flags, RowId.make("f"))
      const rows = yield* db.scan(flag.body === "b" ? B : C)
      return { target: flag.body, n: rows.length }
    })
})
/** Two reads that a writer always changes together, in one transaction. */
const pair = Query.make("pair", {
  args: Schema.Struct({}),
  success: Schema.Struct({ l: Schema.Number, r: Schema.Number }),
  handler: (db) =>
    Effect.gen(function* () {
      const l = yield* db.scan(L)
      // A slow run: a write that could land between the two reads gets the time to do so.
      yield* Effect.sleep("3 millis")
      const r = yield* db.scan(R)
      return { l: l.length, r: r.length }
    })
})

const addRaw = Mutation.make("raw.add", {
  args: Schema.Struct({ id: Schema.String }),
  handler: (ctx, { id }) => ctx.store.insert(Raw, { id: RowId.make(id), accessScopeId: scope, body: "x" })
})
/** Store writes need no services; a test-only guard so the runtime-reached write can be run. */
const isStoreWrite = (u: unknown): u is Effect.Effect<void, unknown> => Effect.isEffect(u)

/**
 * A registered query that tries to write. `ReadStore` has no `patch`, so it looks the method up at runtime,
 * as a buggy handler could; if the leader handed it a full store, this would patch `flags`.
 */
const flip = Query.make("flip", {
  args: Schema.Struct({}),
  success: Schema.String,
  handler: (db) =>
    Effect.suspend(() => {
      const patch: unknown = Reflect.get(db, "patch")
      if (typeof patch !== "function") return Effect.die(new Error("no write method on the query store"))
      const written: unknown = Reflect.apply(patch, db, [Flags, RowId.make("f"), { body: "b" }])
      return isStoreWrite(written) ? Effect.as(Effect.orDie(written), "flipped") : Effect.succeed("flipped")
    })
})

/** Only `db.query`: readability comes from the plan's scope_grants semi-join, never from `roleFor`. */
const docs = Query.make("docs", {
  args: Schema.Struct({}),
  success: Schema.Number,
  handler: (db) => Effect.map(db.query(Docs).withIndex("by_body").collect, (rows) => rows.length)
})

const leaderRpc = (options: { readonly allowRawExec: boolean }) =>
  Effect.gen(function* () {
    const reactivity = yield* Reactivity.make
    const sql = yield* SqliteClient.makeMemory({ installReactivityHooks: true }).pipe(
      Effect.provideService(Reactivity.Reactivity, reactivity)
    )
    yield* SqliteSyncStorage.migrate(queryRegistry).pipe(Effect.provideService(SqlClient.SqlClient, sql))
    const storage = SqliteSyncStorage.make(sql, queryRegistry)
    const put = <T extends SyncTable.AnySyncTable>(table: T, row: Store.RowOf<T>) =>
      storage.rows.write(table, row.id, Option.some(Schema.encodeUnknownSync(table.wire)(row)))
    const write = <T extends SyncTable.AnySyncTable>(table: T, row: Store.RowOf<T>) =>
      storage.transaction(put(table, row))
    yield* write(ScopeGrants.ScopeGrants, {
      id: ScopeGrants.grantId(scope, me),
      accessScopeId: scope,
      principalId: me,
      role: "owner",
      grantedAt: DateTime.makeUnsafe(0)
    })
    const engine = yield* Engine.make({
      clientId: Ids.ClientId.make("c"),
      actor: me,
      storage,
      registry: queryRegistry,
      mutations: Mutation.makeRegistry([addRaw]),
      transport: {
        push: () => Effect.die(new Error("no transport")),
        pull: () =>
          Effect.succeed(decodeDone({ _tag: "PullDone", changes: [], acks: [], nextCursor: "2", expiredThrough: 0 }))
      }
    })
    const handlers = yield* makeHandlers({
      sql,
      reactivity,
      storage,
      engine,
      loop: undefined,
      actor: me,
      queries: Query.makeRegistry([pick, pair, flip, docs]),
      allowRawExec: options.allowRawExec,
      info: { leaderId: "l", clientId: "c", dbName: "test", syncing: false }
    })
    const client = yield* RpcTest.makeClient(LeaderRpcs).pipe(Effect.provide(handlers))
    const row = (id: string, body = "x") => ({ id: RowId.make(id), accessScopeId: scope, body })
    const count = (table: string) =>
      Effect.map(sql.unsafe<{ readonly n: number }>(`SELECT count(*) AS n FROM ${table}`), (rs) => rs[0]?.n)
    const flagBody = Effect.map(
      sql<{ readonly body: string }>`SELECT body FROM flags WHERE id = 'f'`,
      (rs) => rs[0]?.body
    )
    return { client, storage, engine, put, write, row, count, flagBody }
  })

/** Collect a stream's emissions into a queue in the background. */
const collect = <A, E>(stream: Stream.Stream<A, E>) =>
  Effect.gen(function* () {
    const emissions = yield* Queue.unbounded<A>()
    yield* Effect.forkScoped(Stream.runForEach(stream, (a) => Queue.offer(emissions, a)))
    const next = Queue.take(emissions).pipe(Effect.timeout("5 seconds"))
    return { emissions, next }
  })

it.live(
  "a query re-emits after a write to a table only its latest run read, and not after one it stopped reading",
  () =>
    Effect.gen(function* () {
      const { client, write, row } = yield* leaderRpc({ allowRawExec: false })
      yield* write(Flags, row("f", "c"))
      const { emissions, next } = yield* collect(client.subscribe({ name: "pick", args: {} }))
      expect(yield* next).toEqual({ target: "c", n: 0 })
      // `b` was not read by the first run, but `flags` was: the switch re-runs, and that run reads `b`.
      yield* write(Flags, row("f", "b"))
      expect(yield* next).toEqual({ target: "b", n: 0 })
      // Fails if `b` were not picked up from the latest run (dependencies fixed at the first run).
      yield* write(B, row("b1"))
      expect(yield* next).toEqual({ target: "b", n: 1 })
      // Fails if `c`, read only by the first run, were still watched: that re-run would emit {b, 1} here.
      yield* write(C, row("c1"))
      yield* Effect.sleep("300 millis")
      expect(yield* Queue.size(emissions)).toBe(0)
      yield* write(B, row("b2"))
      expect(yield* next).toEqual({ target: "b", n: 2 })
    }).pipe(Effect.scoped)
)

it.live("every emission of a query is one snapshot: two tables written together never disagree", () =>
  Effect.gen(function* () {
    const { client, storage, put, row } = yield* leaderRpc({ allowRawExec: false })
    const { emissions, next } = yield* collect(
      client
        .subscribe({ name: "pair", args: {} })
        .pipe(Stream.mapEffect((u) => Schema.decodeUnknownEffect(pair.success)(u)))
    )
    expect(yield* next).toEqual({ l: 0, r: 0 })
    const n = 40
    // One transaction writes both tables; a run that read them outside one transaction could see l = r + 1.
    yield* Effect.forkScoped(
      Effect.forEach(
        Array.from({ length: n }, (_, i) => i + 1),
        (i) =>
          storage
            .transaction(Effect.andThen(put(L, row(`l${i}`)), put(R, row(`r${i}`))))
            .pipe(Effect.andThen(Effect.sleep("1 millis")))
      )
    )
    const seen: Array<{ readonly l: number; readonly r: number }> = []
    let last = { l: 0, r: 0 }
    while (last.l < n || last.r < n) {
      last = yield* next
      seen.push(last)
    }
    seen.push(...(yield* Queue.clear(emissions)))
    expect(seen.filter((e) => e.l !== e.r)).toEqual([])
  }).pipe(Effect.scoped)
)

it.live("a raw live query whose statement writes fails and commits nothing", () =>
  Effect.gen(function* () {
    const { client, count } = yield* leaderRpc({ allowRawExec: true })
    const result = yield* Stream.runHead(
      client.subscribeRaw({
        query: {
          statements: [
            { sql: "SELECT count(*) AS n FROM raw", params: [] },
            { sql: "INSERT INTO raw (id, access_scope_id, body) VALUES ('w', 's', 'x')", params: [] }
          ],
          tables: ["raw"]
        }
      })
    ).pipe(Effect.exit)
    expect(Exit.isFailure(result) ? String(Cause.squash(result.cause)) : "emitted").toContain("SqlFailure")
    expect(yield* count("raw")).toBe(0)
    // `query_only` is off again after the raw query: an allowed write still works.
    yield* client.exec({ sql: "INSERT INTO raw (id, access_scope_id, body) VALUES ('e', 's', 'x')", params: [] })
    expect(yield* count("raw")).toBe(1)
  }).pipe(Effect.scoped)
)

it.live("raw exec is refused, and writes nothing, on a leader started without allowRawExec", () =>
  Effect.gen(function* () {
    const { client, count } = yield* leaderRpc({ allowRawExec: false })
    const result = yield* client
      .exec({ sql: "INSERT INTO raw (id, access_scope_id, body) VALUES ('e', 's', 'x')", params: [] })
      .pipe(Effect.flip)
    expect(result._tag).toBe("RawExecDisabled")
    expect(yield* count("raw")).toBe(0)
  }).pipe(Effect.scoped)
)

it.live("raw subscribe is refused before any SQL runs on a leader started without allowRawExec", () =>
  Effect.gen(function* () {
    // Caller SQL can switch query_only off itself: only the flag keeps this from committing.
    const query = {
      statements: [
        { sql: "PRAGMA query_only = OFF", params: [] },
        { sql: "INSERT INTO raw (id, access_scope_id, body) VALUES ('w', 's', 'x')", params: [] }
      ],
      tables: ["raw"]
    }
    const refused = yield* leaderRpc({ allowRawExec: false })
    const error = yield* Stream.runHead(refused.client.subscribeRaw({ query })).pipe(Effect.flip)
    expect(error._tag).toBe("RawExecDisabled")
    expect(yield* refused.count("raw")).toBe(0)
    // With the flag the same subscription is served (the harness relies on it).
    const allowed = yield* leaderRpc({ allowRawExec: true })
    expect(Option.isSome(yield* Stream.runHead(allowed.client.subscribeRaw({ query })))).toBe(true)
  }).pipe(Effect.scoped)
)

it.live("a registered query that writes fails, commits nothing, and leaves the next mutation writable", () =>
  Effect.gen(function* () {
    const { client, write, row, engine, count, flagBody } = yield* leaderRpc({ allowRawExec: false })
    yield* write(Flags, row("f", "c"))
    const result = yield* Stream.runHead(client.subscribe({ name: "flip", args: {} })).pipe(Effect.exit)
    expect(Exit.isFailure(result) ? "failed" : "emitted").toBe("failed")
    expect(yield* flagBody).toBe("c")
    yield* engine.mutate(Ids.MutationId.make("m1"), "raw.add", { id: "r1" })
    expect(yield* count("raw")).toBe(1)
  }).pipe(Effect.scoped)
)

it.live("revoking a grant re-emits a live query whose handler only used db.query", () =>
  Effect.gen(function* () {
    const { client, storage, write, row } = yield* leaderRpc({ allowRawExec: false })
    yield* write(Docs, row("d1"))
    const { next } = yield* collect(client.subscribe({ name: "docs", args: {} }))
    expect(yield* next).toEqual(1)
    // Fails if scope_grants were not recorded as read: nothing would invalidate, and `next` times out.
    yield* storage.transaction(
      storage.rows.write(ScopeGrants.ScopeGrants, ScopeGrants.grantId(scope, me), Option.none())
    )
    expect(yield* next).toEqual(0)
  }).pipe(Effect.scoped)
)

// ---------------------------------------------------------------------------------------------
// Tab-side API (DESIGN §9.1) over a real leader, with leader loss injected between tab and leader
// ---------------------------------------------------------------------------------------------

const leaderLost = new LeaderUnavailable({ reason: "LeaderLost", message: "injected" })

/** `LeaderCalls` straight onto the leader's handlers; tests override the calls they inject faults into. */
const passThrough = (client: Effect.Success<ReturnType<typeof leaderRpc>>["client"]): LeaderCalls => ({
  enqueue: (envelope) => client.enqueue({ envelope }),
  subscribe: (name, args) => client.subscribe({ name, args }),
  notices: client.notices(),
  dismissNotices: (ids) => client.dismissNotices({ ids })
})

it.live("mutate retried across LeaderLost after the lost leader committed leaves exactly one outbox row", () =>
  Effect.gen(function* () {
    const { client, count } = yield* leaderRpc({ allowRawExec: false })
    const presented: Array<string> = []
    // The first attempt commits on the leader, then the tab sees the leader go away before the reply.
    const enqueue: LeaderCalls["enqueue"] = (envelope) =>
      Effect.suspend(() => {
        presented.push(envelope.mutationId)
        const real = client.enqueue({ envelope })
        return presented.length === 1 ? Effect.andThen(real, Effect.fail(leaderLost)) : real
      })
    const api = tabApi({ ...passThrough(client), enqueue })
    const enqueued = yield* api.mutate(addRaw, { id: "r1" })
    expect(presented.length).toBe(2)
    // Fails if a retry minted a new id: the second attempt would append a second entry (and a second apply).
    expect(new Set(presented).size).toBe(1)
    expect(yield* count("echo_outbox")).toBe(1)
    expect(yield* count("raw")).toBe(1)
    expect(enqueued.seq).toBe(1)
  }).pipe(Effect.scoped)
)

it.live("a query subscription across handover keeps the last emission until the new leader's fresh one", () =>
  Effect.gen(function* () {
    const { client, write, row } = yield* leaderRpc({ allowRawExec: false })
    yield* write(Flags, row("f", "c"))
    const lost = yield* Deferred.make<void>()
    const elected = yield* Deferred.make<void>()
    let opens = 0
    const subscribe: LeaderCalls["subscribe"] = (name, args) =>
      Stream.suspend(() => {
        opens++
        const real = client.subscribe({ name, args })
        // First leader: ends with LeaderLost when `lost` fires. Next leader: answers once `elected` fires.
        return opens === 1
          ? real.pipe(Stream.interruptWhen(Effect.andThen(Deferred.await(lost), Effect.fail(leaderLost))))
          : Stream.unwrap(Effect.as(Deferred.await(elected), real))
      })
    const api = tabApi({ ...passThrough(client), subscribe })
    const ended = yield* Deferred.make<string>()
    const emissions = yield* Queue.unbounded<{ readonly target: string; readonly n: number }>()
    yield* Effect.forkScoped(
      Stream.runForEach(api.subscribe(pick, {}), (a) => Queue.offer(emissions, a)).pipe(
        Effect.exit,
        Effect.flatMap((exit) =>
          Deferred.succeed(ended, Exit.isFailure(exit) ? String(Cause.squash(exit.cause)) : "done")
        )
      )
    )
    const next = Queue.take(emissions).pipe(Effect.timeout("5 seconds"))
    expect(yield* next).toEqual({ target: "c", n: 0 })
    yield* Deferred.succeed(lost, undefined)
    yield* Effect.sleep("150 millis")
    // Written while no leader serves this tab: nothing may be emitted (no reset, no error) until a fresh run.
    yield* write(C, row("c1"))
    yield* Effect.sleep("150 millis")
    expect(yield* Queue.size(emissions)).toBe(0)
    expect(yield* Deferred.poll(ended)).toEqual(Option.none())
    yield* Deferred.succeed(elected, undefined)
    expect(yield* next).toEqual({ target: "c", n: 1 })
    expect(opens).toBe(2)
    expect(yield* Deferred.poll(ended)).toEqual(Option.none())
  }).pipe(Effect.scoped)
)

const noticeFor = (mutationId: string, seq: number) => ({
  _tag: "OutcomeUnknown" as const,
  envelope: {
    clientId: Ids.ClientId.make("c"),
    mutationId: Ids.MutationId.make(mutationId),
    seq: Ids.Seq.make(seq),
    name: "raw.add",
    args: {}
  }
})

const ids = (list: ReadonlyArray<{ readonly notice: { readonly envelope: { readonly mutationId: string } } }>) =>
  list.map((n) => n.notice.envelope.mutationId)

it.live("a dismissed notice is gone from the notices stream, and an undismissed one survives a pull", () =>
  Effect.gen(function* () {
    const { client, storage, engine } = yield* leaderRpc({ allowRawExec: false })
    yield* storage.transaction(storage.addNotices([noticeFor("m1", 1), noticeFor("m2", 2)]))
    const api = tabApi(passThrough(client))
    const { next } = yield* collect(api.notices)
    const first = yield* next
    expect(ids(first)).toEqual(["m1", "m2"])
    const m1 = first.find((n) => n.notice.envelope.mutationId === "m1")
    yield* api.dismissNotices(m1 === undefined ? [] : [m1.id])
    expect(ids(yield* next)).toEqual(["m2"])
    yield* engine.pull
    const afterPull = yield* Stream.runHead(client.notices())
    expect(Option.map(afterPull, ids)).toEqual(Option.some(["m2"]))
  }).pipe(Effect.scoped)
)
