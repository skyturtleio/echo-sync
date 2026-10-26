// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { expect, it } from "@effect/vitest"
import {
  Array as Arr,
  Cause,
  Clock,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Queue,
  References,
  Schema,
  Scope,
  Stream,
  String as Str
} from "effect"
import * as Reactivity from "effect/reactivity/Reactivity"
import { RpcClientError, RpcTest } from "effect/rpc"
import * as Schedule from "effect/Schedule"
import * as SqlClient from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Protocol, Query, Registry, ScopeGrants, type Store, SyncTable } from "echo-sync"
import * as Engine from "../sync/Engine.ts"
import * as SqliteSyncStorage from "../sync/SqliteSyncStorage.ts"
import * as SyncLoop from "../sync/SyncLoop.ts"
import * as Provenance from "../Provenance.ts"
import { enqueueAndWake, makeHandlers } from "./Leader.ts"
import { type LeaderCalls, tabApi } from "./LeaderClient.ts"
import { LeaderRpcs, LeaderUnavailable } from "./LeaderRpcs.ts"
import { paginate } from "../sync/testing/Pull.ts"

const T = SyncTable.make("t", { id: Schema.String.pipe(Schema.brand("TId")), accessScopeId: AccessScope.AccessScopeId })
const registry = Registry.make([T])
const Db = Layer.effectDiscard(SqliteSyncStorage.migrate(registry)).pipe(
  Layer.provideMerge(SqliteClient.layerMemory({}))
)
const decodeDone = (value: unknown) => Effect.orDie(Schema.decodeUnknownEffect(Protocol.PullDone)(value))
const position = Ids.Position.make(1n)

it.live("an enqueue cancelled while its COMMIT runs still wakes the loop: the entry is pushed with no other wake", () =>
  Effect.gen(function* () {
    const real = yield* SqliteSyncStorage.make(yield* SqlClient.SqlClient, registry)
    const committed = yield* Deferred.make<void>()
    const gate = yield* Deferred.make<void>()
    const state = { gateNext: false }
    // The interrupt lands after COMMIT but before the transaction returns, as it can while withTransaction
    // masks COMMIT: the entry is durable, and the caller's fiber is interrupted on the way out.
    const storage: Engine.SyncStorage = {
      ...real,
      localCommit: <A, E, R>(self: Effect.Effect<A, E, R>) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.tap(restore(real.localCommit(self)), () => {
            if (!state.gateNext) return Effect.void
            state.gateNext = false
            return Effect.andThen(Deferred.succeed(committed, undefined), Deferred.await(gate))
          })
        )
    }
    const pushed: Array<string> = []
    const pushObserved = yield* Deferred.make<void>()
    const transport: Engine.Transport = {
      push: (request) =>
        Effect.andThen(
          Effect.sync(() => void pushed.push(...Arr.map(request.mutations, (m) => m.mutationId))),
          Effect.as(Deferred.succeed(pushObserved, undefined), {
            results: Arr.map(request.mutations, (m) => ({
              mutationId: m.mutationId,
              position,
              outcome: { _tag: "Applied" as const }
            }))
          })
        ),
      pull: () =>
        Stream.fromEffect(decodeDone({ _tag: "PullDone", changes: [], acks: [], nextCursor: "1", expiredThrough: 0 }))
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
    const observed: Engine.Engine = {
      ...engine,
      pull: Effect.ensuring(engine.pull, Deferred.succeed(pulled, undefined))
    }
    const loop = yield* SyncLoop.make({
      engine: observed,
      pendingMutations: real.pendingCount
    })
    yield* Effect.forkChild(loop.run)
    yield* Deferred.await(pulled)

    state.gateNext = true
    const enqueue = yield* Effect.forkChild(enqueueAndWake(engine, loop.wake)(Ids.MutationId.make("m"), "add", {}))
    yield* Deferred.await(committed)
    // COMMIT is done and the permit released; only the wrapper's tap holds the enqueue's fiber.
    expect(Arr.map(yield* real.transaction(real.pending), (e) => e.envelope.mutationId)).toEqual(["m"])
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
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test leader composition root; the test owns the SQLite and sync-loop lifetime.
  }).pipe(Effect.provide(Db))
)

// ---------------------------------------------------------------------------------------------
// Registered queries, raw live queries and raw exec over the leader's RPC handlers (INV-BRW-6)
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
const rawRows = Query.make("raw.rows", {
  args: Schema.Struct({}),
  success: Schema.Array(Schema.String),
  handler: (db) => Effect.map(db.scan(Raw), (rows) => Arr.map(rows, (row) => row.id))
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
      // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- Reflection preserves unknown errors: Effect.isEffect cannot validate an error type; orDie contains this test-only write.
      return isStoreWrite(written) ? Effect.as(Effect.orDie(written), "flipped") : Effect.succeed("flipped")
    })
})

/** Only `db.query`: readability comes from the plan's scope_grants semi-join, never from `roleFor`. */
const docs = Query.make("docs", {
  args: Schema.Struct({}),
  success: Schema.Number,
  handler: (db) => Effect.map(db.query(Docs).withIndex("by_body").collect, (rows) => rows.length)
})

const searchDocs = Query.make("docs.search", {
  args: Schema.String,
  success: Schema.Array(Schema.String),
  handler: (db, needle) =>
    Effect.map(db.query(Docs).withIndex("by_body").contains("body", needle).take(5), (rows) =>
      Arr.map(rows, (r) => r.id)
    )
})

const pageDocs = Query.make("docs.page", {
  args: Schema.Struct({ body: Schema.String, id: RowId }),
  success: Schema.Array(Schema.String),
  handler: (db, cursor) =>
    Effect.map(db.query(Docs).withIndex("by_body").order("desc").after(cursor).take(2), (rows) =>
      Arr.map(rows, (r) => r.id)
    )
})

const leaderRpc = (options: { readonly allowRawExec: boolean; readonly transport?: Engine.Transport }) =>
  Effect.gen(function* () {
    const reactivity = yield* Reactivity.make
    const sql = yield* SqliteClient.makeMemory({ installReactivityHooks: true }).pipe(
      Effect.provideService(Reactivity.Reactivity, reactivity)
    )
    yield* SqliteSyncStorage.migrate(queryRegistry).pipe(Effect.provideService(SqlClient.SqlClient, sql))
    const storage = yield* SqliteSyncStorage.make(sql, queryRegistry)
    const put = <T extends SyncTable.AnySyncTable>(table: T, row: Store.RowOf<T>) =>
      Effect.flatMap(Effect.orDie(Schema.encodeUnknownEffect(table.wire)(row)), (encoded) =>
        storage.rows.write(table, row.id, Option.some(encoded))
      )
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
      transport: options.transport ?? {
        push: () => Effect.die(new Error("no transport")),
        pull: () =>
          Stream.fromEffect(decodeDone({ _tag: "PullDone", changes: [], acks: [], nextCursor: "2", expiredThrough: 0 }))
      }
    })
    const handlers = yield* makeHandlers({
      sql,
      reactivity,
      storage,
      engine,
      loop: undefined,
      presence: undefined,
      actor: me,
      queries: Query.makeRegistry([pick, pair, flip, docs, searchDocs, pageDocs, rawRows]),
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
        .subscribeSnapshot({ name: "pair", args: {} })
        .pipe(
          Stream.mapEffect(({ value, revision }) =>
            Schema.decodeUnknownEffect(pair.success)(value).pipe(
              Effect.map((decoded) => ({ value: decoded, revision }))
            )
          )
        )
    )
    const initial = yield* next
    expect(initial.value).toEqual({ l: 0, r: 0 })
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
    const seen: Array<typeof initial> = []
    let last = initial
    while (last.value.l < n || last.value.r < n) {
      last = yield* next
      seen.push(last)
    }
    seen.push(...(yield* Queue.clear(emissions)))
    expect(Arr.filter(seen, (e) => e.value.l !== e.value.r)).toEqual([])
    // Each writer commits both rows once. A revision read after releasing admission can get ahead
    // of the result; one based on statement count or mutation seq cannot satisfy this either.
    for (const snapshot of seen) {
      expect(snapshot.revision).toEqual({ ...initial.revision, offset: initial.revision.offset + snapshot.value.l })
    }
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

it.live("an empty cursor page re-emits on inserts, refills after deletes and observes key moves and revocation", () =>
  Effect.gen(function* () {
    const { client, storage, write, row } = yield* leaderRpc({ allowRawExec: false })
    const { next } = yield* collect(client.subscribe({ name: "docs.page", args: { body: "tie", id: "d4" } }))
    expect(yield* next).toEqual([])
    yield* write(Docs, row("d1", "tie"))
    expect(yield* next).toEqual(["d1"])
    yield* write(Docs, row("d2", "tie"))
    expect(yield* next).toEqual(["d2", "d1"])
    yield* write(Docs, row("d3", "tie"))
    expect(yield* next).toEqual(["d3", "d2"])
    yield* storage.transaction(storage.rows.write(Docs, "d3", Option.none()))
    expect(yield* next).toEqual(["d2", "d1"])
    yield* write(Docs, row("d2", "zzz"))
    expect(yield* next).toEqual(["d1"])
    yield* storage.transaction(
      storage.rows.write(ScopeGrants.ScopeGrants, ScopeGrants.grantId(scope, me), Option.none())
    )
    expect(yield* next).toEqual([])
  }).pipe(Effect.scoped)
)

it.live("a registered search re-emits after edits, deletes and grant revocation", () =>
  Effect.gen(function* () {
    const { client, storage, write, row } = yield* leaderRpc({ allowRawExec: false })
    yield* write(Docs, row("d1", "crew alice"))
    yield* write(Docs, row("d2", "crew bob"))
    const { next } = yield* collect(client.subscribe({ name: "docs.search", args: "alice" }))
    expect(yield* next).toEqual(["d1"])
    yield* write(Docs, row("d2", "alice training"))
    expect(yield* next).toEqual(["d2", "d1"])
    yield* storage.transaction(storage.rows.write(Docs, "d1", Option.none()))
    expect(yield* next).toEqual(["d2"])
    yield* storage.transaction(
      storage.rows.write(ScopeGrants.ScopeGrants, ScopeGrants.grantId(scope, me), Option.none())
    )
    expect(yield* next).toEqual([])
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
// Tab-side API (SPEC §12.3) over a real leader, with leader loss injected between tab and leader
// ---------------------------------------------------------------------------------------------

const leaderLost = new LeaderUnavailable({ reason: "LeaderLost", message: "injected" })

/** `LeaderCalls` straight onto the leader's handlers; tests override the calls they inject faults into. */
const passThrough = (client: Effect.Success<ReturnType<typeof leaderRpc>>["client"]): LeaderCalls => ({
  enqueue: (envelope) => client.enqueue({ envelope }),
  subscribe: (name, args) => client.subscribe({ name, args }),
  subscribeSnapshot: (name, args) => client.subscribeSnapshot({ name, args }),
  notices: client.notices(),
  dismissNotices: (ids) => client.dismissNotices({ ids }),
  syncStatus: client.syncStatus(),
  requestSync: client.requestSync(),
  presence: (room, state, revision) => client.presence({ room, tabId: Protocol.TabId.make("tab"), state, revision }),
  track: (room, state, revision) => client.track({ room, tabId: Protocol.TabId.make("tab"), state, revision })
})

it.live(
  "snapshot provenance requires every incorporated local receipt and never certifies a rollback or an unknown gap",
  () =>
    Effect.gen(function* () {
      const { client, storage, write, row } = yield* leaderRpc({ allowRawExec: true })
      const api = tabApi(passThrough(client))
      const { next } = yield* collect(api.subscribeSnapshot(rawRows, {}))
      const initial = yield* next
      expect(initial.value).toEqual([])
      expect(Provenance.classify(undefined, initial.revision, [])).toBe("refresh-required")
      const first = yield* api.mutate(addRaw, { id: "a" })
      const a = yield* next
      expect(a.value).toEqual(["a"])
      expect(first.change).toEqual({ before: initial.revision, after: a.revision })
      expect(first.seq).not.toBe(a.revision.offset)
      const second = yield* api.mutate(addRaw, { id: "b" })
      const b = yield* next
      expect(b.value).toEqual(["a", "b"])
      // The consumer may coalesce a and b; both intervals, not just the newest receipt, are required.
      expect(Provenance.classify(initial.revision, b.revision, [second])).toBe("refresh-required")
      expect(Provenance.classify(initial.revision, b.revision, [second, first, first])).toBe("local-only")
      // Sparse receipt lists have the same meaning as their present entries; holes are not receipts.
      const sparse: Array<typeof first> = []
      sparse[2] = first
      sparse[4] = second
      expect(Provenance.classify(initial.revision, b.revision, sparse)).toBe("local-only")
      expect(Provenance.classify(a.revision, b.revision, [second])).toBe("local-only")
      expect(Provenance.classify(b.revision, a.revision, [first, second])).toBe("refresh-required")

      const duplicate = yield* api.mutate(addRaw, { id: "a" }, { mutationId: Ids.MutationId.make(first.mutationId) })
      expect(duplicate.change).toBeNull()
      expect(duplicate.seq).toBe(first.seq)
      const rolledBack = yield* Effect.exit(
        storage.transaction(
          Effect.andThen(
            storage.rows.write(Raw, "rollback", Option.some({ id: "rollback", access_scope_id: "s", body: "x" })),
            Effect.fail("rollback")
          )
        )
      )
      expect(Exit.isFailure(rolledBack)).toBe(true)
      const afterRollback = yield* Stream.runHead(api.subscribeSnapshot(rawRows, {})).pipe(
        Effect.map(Option.getOrThrow)
      )
      expect(afterRollback).toEqual(b)
      expect(Provenance.classify(b.revision, afterRollback.revision, [])).toBe("unchanged")
      expect(Exit.isFailure(yield* Effect.exit(api.mutate(addRaw, { id: "a" })))).toBe(true)
      expect(yield* Stream.runHead(api.subscribeSnapshot(rawRows, {})).pipe(Effect.map(Option.getOrThrow))).toEqual(b)

      yield* write(Raw, row("external"))
      const external = yield* next
      expect(external.value).toEqual(["a", "b", "external"])
      const third = yield* api.mutate(addRaw, { id: "c" })
      const c = yield* next
      expect(Provenance.classify(b.revision, c.revision, [third])).toBe("refresh-required")
      expect(Provenance.classify(external.revision, c.revision, [third])).toBe("local-only")
      yield* client.exec({ sql: "UPDATE raw SET body = 'remote' WHERE id = 'a'", params: [] })
      const raw = yield* next
      expect(Provenance.classify(c.revision, raw.revision, [third])).toBe("refresh-required")
    }).pipe(Effect.scoped)
)

it.live(
  "reconnecting pull and rejection remain refresh-required between local commits even when their notifications coalesce",
  () =>
    Effect.gen(function* () {
      let rejectedId = ""
      let online = false
      let cursor = "2"
      const { client, engine, storage } = yield* leaderRpc({
        allowRawExec: false,
        transport: {
          push: () => Effect.die("unused"),
          pull: () =>
            online
              ? Stream.fromEffect(
                  decodeDone({
                    _tag: "PullDone",
                    changes:
                      cursor === "2"
                        ? [
                            {
                              _tag: "upsert",
                              table: "raw",
                              row: { id: "server", access_scope_id: "s", body: "remote" }
                            }
                          ]
                        : [],
                    acks: [
                      {
                        mutationId: rejectedId,
                        position: cursor,
                        outcome: { _tag: "Rejected", error: { _tag: "UnknownMutation", name: "raw.add" } }
                      }
                    ],
                    nextCursor: cursor,
                    expiredThrough: 0
                  })
                )
              : Stream.fail(pullOffline())
        }
      })
      // A previously installed replica; a reconnect is incremental, not a new scope snapshot.
      yield* storage.transaction(storage.setMeta({ cursor: position, pushedThrough: 0, resnapshot: false }))
      const api = tabApi(passThrough(client))
      const { next } = yield* collect(api.subscribeSnapshot(rawRows, {}))
      const initial = yield* next
      const rejected = yield* api.mutate(addRaw, { id: "rejected" })
      rejectedId = rejected.mutationId
      const optimistic = yield* next
      expect(optimistic.value).toEqual(["rejected"])
      expect(Provenance.classify(initial.revision, optimistic.revision, [rejected])).toBe("local-only")
      expect(Exit.isFailure(yield* Effect.exit(engine.pull))).toBe(true)
      expect(yield* Stream.runHead(api.subscribeSnapshot(rawRows, {})).pipe(Effect.map(Option.getOrThrow))).toEqual(
        optimistic
      )
      online = true
      yield* engine.pull
      const reconciled = yield* next
      expect(reconciled.value).toEqual(["server"])
      expect(Provenance.classify(optimistic.revision, reconciled.revision, [rejected])).toBe("refresh-required")
      const own = yield* api.mutate(addRaw, { id: "later" })
      const latest = yield* next
      expect(latest.value).toEqual(["later", "server"])
      expect(Provenance.classify(initial.revision, latest.revision, [rejected, own])).toBe("refresh-required")
      expect(Provenance.classify(reconciled.revision, latest.revision, [own])).toBe("local-only")
      const notices = yield* Stream.runHead(api.notices).pipe(Effect.map(Option.getOrThrow))
      expect(Arr.map(notices, ({ notice }) => notice._tag)).toEqual(["Rejected"])
      // An own rejection with NO external rows must still undo the local effect and require refresh.
      rejectedId = own.mutationId
      cursor = "3"
      yield* engine.pull
      const onlyRejection = yield* next
      expect(onlyRejection.value).toEqual(["server"])
      expect(Provenance.classify(latest.revision, onlyRejection.revision, [own])).toBe("refresh-required")
    }).pipe(Effect.scoped)
)

it.live("snapshot resubscription after leader loss cannot reuse the old lifetime's local receipts", () =>
  Effect.gen(function* () {
    const first = yield* leaderRpc({ allowRawExec: false })
    let active = first.client
    const lost = yield* Deferred.make<void>()
    let opens = 0
    const calls = passThrough(first.client)
    const api = tabApi({
      ...calls,
      subscribeSnapshot: (name, args) =>
        Stream.suspend(() => {
          opens++
          return opens === 1
            ? calls
                .subscribeSnapshot(name, args)
                .pipe(Stream.interruptWhen(Effect.andThen(Deferred.await(lost), Effect.fail(leaderLost))))
            : active.subscribeSnapshot({ name, args })
        })
    })
    const { next } = yield* collect(api.subscribeSnapshot(rawRows, {}))
    const initial = yield* next
    const receipt = yield* api.mutate(addRaw, { id: "old" })
    const own = yield* next
    expect(Provenance.classify(initial.revision, own.revision, [receipt])).toBe("local-only")
    active = (yield* leaderRpc({ allowRawExec: false })).client
    yield* Deferred.succeed(lost, undefined)
    const fresh = yield* next
    expect(fresh.value).toEqual([])
    expect(fresh.revision.epoch).not.toBe(own.revision.epoch)
    expect(Provenance.classify(initial.revision, fresh.revision, [receipt])).toBe("refresh-required")
  }).pipe(Effect.scoped)
)

it.live("mutate rejects excess args before enqueue rather than silently saving a stripped intent", () =>
  Effect.gen(function* () {
    const { client, count } = yield* leaderRpc({ allowRawExec: false })
    const api = tabApi(passThrough(client))
    const input = { id: "r1", typo: "lost data" }
    const nested = Mutation.make("nested", {
      args: Schema.Struct({ customTimes: Schema.Record(Schema.String, Schema.Struct({ minutes: Schema.Int })) }),
      handler: () => Effect.void
    })
    const nestedInput = { customTimes: { night: { minutes: 12, typo: 3 } } }
    for (const attempt of [api.mutate(addRaw, input), api.mutate(nested, nestedInput)]) {
      const error = yield* Effect.flip(attempt)
      expect(error._tag).toBe("LocallyRejected")
      if (error._tag !== "LocallyRejected") throw error
      expect(error.error._tag).toBe("ArgsDecodeFailed")
      if (error.error._tag !== "ArgsDecodeFailed") throw error.error
      expect(error.error.message).toContain("typo")
    }
    expect(yield* count("echo_outbox")).toBe(0)
    expect(yield* count("raw")).toBe(0)
    const loose = Mutation.make(addRaw.name, { ...addRaw, onExcessProperty: "ignore" })
    yield* api.mutate(loose, input)
    expect(yield* count("echo_outbox")).toBe(1)
    expect(yield* count("raw")).toBe(1)
    yield* api.mutate(addRaw, { id: "r2" })
    expect(yield* count("echo_outbox")).toBe(2)
    expect(yield* count("raw")).toBe(2)
  }).pipe(Effect.scoped)
)

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
    const initial = yield* Stream.runHead(api.subscribeSnapshot(rawRows, {})).pipe(Effect.map(Option.getOrThrow))
    const enqueued = yield* api.mutate(addRaw, { id: "r1" })
    expect(presented.length).toBe(2)
    // Fails if a retry minted a new id: the second attempt would append a second entry (and a second apply).
    expect(new Set(presented).size).toBe(1)
    expect(yield* count("echo_outbox")).toBe(1)
    expect(yield* count("raw")).toBe(1)
    expect(enqueued.seq).toBe(1)
    expect(enqueued.change).toBeNull()
    const fresh = yield* Stream.runHead(api.subscribeSnapshot(rawRows, {})).pipe(Effect.map(Option.getOrThrow))
    expect(fresh.value).toEqual(["r1"])
    expect(Provenance.classify(initial.revision, fresh.revision, [enqueued])).toBe("refresh-required")
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
  Arr.map(list, (n) => n.notice.envelope.mutationId)

it.live("a dismissed notice is gone from the notices stream, and an undismissed one survives a pull", () =>
  Effect.gen(function* () {
    const { client, storage, engine } = yield* leaderRpc({ allowRawExec: false })
    yield* storage.transaction(storage.addNotices([noticeFor("m1", 1), noticeFor("m2", 2)]))
    const api = tabApi(passThrough(client))
    const { next } = yield* collect(api.notices)
    const first = yield* next
    expect(ids(first)).toEqual(["m1", "m2"])
    const m1 = Option.getOrUndefined(Arr.findFirst(first, (n) => n.notice.envelope.mutationId === "m1"))
    yield* api.dismissNotices(m1 === undefined ? [] : [m1.id])
    expect(ids(yield* next)).toEqual(["m2"])
    yield* engine.pull
    const afterPull = yield* Stream.runHead(client.notices())
    expect(Option.map(afterPull, ids)).toEqual(Option.some(["m2"]))
  }).pipe(Effect.scoped)
)

// ---------------------------------------------------------------------------------------------
// syncStatus and requestSync (SPEC §10) over leaders wired as `program` wires them
// ---------------------------------------------------------------------------------------------

/** A push integrity failure (the server refused the batch): halts push. */
const integrity = () =>
  new Protocol.MalformedEnvelope({
    clientId: Ids.ClientId.make("c"),
    mutationId: Ids.MutationId.make("m"),
    message: "bad envelope"
  })
/** No usable RPC result: transient. */
const unreachable = () =>
  new RpcClientError.RpcClientError({ reason: new RpcClientError.RpcClientDefect({ message: "offline", cause: null }) })
const pushOffline = unreachable
const pullOffline = unreachable
const decodePush = (value: unknown) => Effect.orDie(Schema.decodeUnknownEffect(Protocol.PushResponse)(value))

/** One replica database, shared by the successive leaders of a test (a handover keeps the file). */
const replicaDb = Effect.gen(function* () {
  const reactivity = yield* Reactivity.make
  const sql = yield* SqliteClient.makeMemory({ installReactivityHooks: true }).pipe(
    Effect.provideService(Reactivity.Reactivity, reactivity)
  )
  yield* SqliteSyncStorage.migrate(queryRegistry).pipe(Effect.provideService(SqlClient.SqlClient, sql))
  const storage = yield* SqliteSyncStorage.make(sql, queryRegistry)
  const grant = {
    id: ScopeGrants.grantId(scope, me),
    accessScopeId: scope,
    principalId: me,
    role: "owner" as const,
    grantedAt: DateTime.makeUnsafe(0)
  }
  const encodedGrant = yield* Schema.encodeEffect(ScopeGrants.ScopeGrants.wire)(grant)
  // A replica that already installed a snapshot (cursor 1), so a pull is incremental and keeps the grant.
  yield* storage.transaction(
    Effect.andThen(
      storage.rows.write(ScopeGrants.ScopeGrants, grant.id, Option.some(encodedGrant)),
      storage.setMeta({ cursor: position, pushedThrough: 0, resnapshot: false })
    )
  )
  return { sql, reactivity }
})

/** A leader over `db` with a running sync loop and the reactive pending count, as `program` builds it. */
const syncLeader = (
  db: Effect.Success<typeof replicaDb>,
  transport: Engine.Transport,
  retryDelay: "1 second" | "1 hour" = "1 second",
  /** Holds back the first durable count (a slow or busy store) until it completes. */
  countGate: Effect.Effect<void> = Effect.void
) =>
  Effect.gen(function* () {
    const storage = yield* SqliteSyncStorage.make(db.sql, queryRegistry)
    const engine = yield* Engine.make({
      clientId: Ids.ClientId.make("c"),
      actor: me,
      storage,
      registry: queryRegistry,
      mutations: Mutation.makeRegistry([addRaw]),
      transport
    })
    const loop = yield* SyncLoop.make({
      engine,
      pendingMutations: Stream.unwrap(Effect.as(countGate, storage.pendingCount)),
      retryDelay,
      maxRetryDelay: "1 hour"
    })
    yield* Effect.forkScoped(loop.run)
    const handlers = yield* makeHandlers({
      sql: db.sql,
      reactivity: db.reactivity,
      storage,
      engine,
      loop,
      presence: undefined,
      actor: me,
      queries: Query.makeRegistry([]),
      allowRawExec: false,
      info: { leaderId: "l", clientId: "c", dbName: "test", syncing: true }
    })
    return yield* RpcTest.makeClient(LeaderRpcs).pipe(Effect.provide(handlers))
  })

/** Take emissions until one satisfies `p` (5 s budget); returns it. */
const awaitStatus = (emissions: Queue.Queue<SyncLoop.SyncStatus>, p: (s: SyncLoop.SyncStatus) => boolean) =>
  Queue.take(emissions).pipe(Effect.repeat({ until: p }), Effect.timeout("5 seconds"))

it.live(
  "pendingMutations follows every append and release (not only cycle ends), and a new leader starts from the durable outbox",
  () =>
    Effect.gen(function* () {
      const db = yield* replicaDb
      // Pull answers only when the test offers it the acks to deliver; push applies at fresh positions.
      const pulls = yield* Queue.unbounded<ReadonlyArray<string>>()
      const positions = new Map<string, number>()
      let next = 100
      let cursor = 1
      const transport: Engine.Transport = {
        push: (request) =>
          Effect.suspend(() =>
            decodePush({
              results: Arr.map(request.mutations, (m) => {
                positions.set(m.mutationId, ++next)
                return { mutationId: m.mutationId, position: String(next), outcome: { _tag: "Applied" } }
              })
            })
          ),
        pull: paginate(() =>
          Effect.flatMap(Queue.take(pulls), (acked) => {
            const acks = Arr.map(acked, (id) => ({
              mutationId: id,
              position: String(positions.get(id) ?? 0),
              outcome: { _tag: "Applied" }
            }))
            cursor = Math.max(cursor, ...Arr.map(acked, (id) => positions.get(id) ?? 0))
            return decodeDone({ _tag: "PullDone", changes: [], acks, nextCursor: String(cursor), expiredThrough: 0 })
          })
        )
      }
      const first = yield* Scope.make()
      const leader1 = yield* syncLeader(db, transport).pipe(Scope.provide(first))
      const lost = yield* Deferred.make<void>()
      let opens = 0
      const leader2 = yield* Deferred.make<typeof leader1>()
      const api = tabApi({
        ...passThrough(leader1),
        syncStatus: Stream.suspend(() =>
          ++opens === 1
            ? leader1
                .syncStatus()
                .pipe(Stream.interruptWhen(Effect.andThen(Deferred.await(lost), Effect.fail(leaderLost))))
            : Stream.unwrap(Effect.map(Deferred.await(leader2), (c) => c.syncStatus()))
        )
      })
      const emissions = yield* Queue.unbounded<SyncLoop.SyncStatus>()
      const ended = yield* Deferred.make<void>()
      yield* Effect.forkScoped(
        Stream.runForEach(api.syncStatus, (s) => Queue.offer(emissions, s)).pipe(
          Effect.ensuring(Deferred.succeed(ended, undefined))
        )
      )
      const enqueue = (id: string) =>
        leader1.enqueue({
          envelope: { mutationId: Ids.MutationId.make(id), name: "raw.add", args: { id } }
        })

      // The first cycle is parked in pull. An enqueue is counted at once, while that cycle still runs.
      yield* awaitStatus(emissions, (s) => s.phase === "pulling")
      yield* enqueue("m1")
      const appended = yield* awaitStatus(emissions, (s) => s.pendingMutations === 1)
      expect([appended.phase, appended.lastSyncedAt]).toEqual(["pulling", null])

      // Cycle 1 ends; cycle 2 pushes m1 (Applied above the cursor: pushed, not installed) and parks in pull.
      yield* Queue.offer(pulls, [])
      const pushed = yield* awaitStatus(emissions, (s) => s.phase === "pulling" && positions.has("m1"))
      // The first install found unsent intent, so it scheduled delivery without claiming sync.
      expect(pushed.lastSyncedAt).toBeNull()
      expect(pushed.pendingMutations).toBe(1)
      expect(positions.has("m1")).toBe(true)

      // The install that carries m1's ack releases it.
      yield* Queue.offer(pulls, ["m1"])
      yield* awaitStatus(emissions, (s) => s.pendingMutations === 0)

      // m2 is appended; then the leader goes away and a new one opens the same database.
      yield* enqueue("m2")
      yield* awaitStatus(emissions, (s) => s.pendingMutations === 1)
      // The tab sees the loss first (as LeaderClient's epoch watch reports it), then the leader's scope ends.
      yield* Deferred.succeed(lost, undefined)
      yield* Scope.close(first, Exit.void)
      yield* Effect.sleep("50 millis")
      yield* Queue.clear(emissions)
      // The successor's first durable count is slow. Until it lands, the tab must keep the old leader's
      // last emission: a status from the new leader now could only claim 0 pending writes.
      const counted = yield* Deferred.make<void>()
      yield* Deferred.succeed(leader2, yield* syncLeader(db, transport, "1 second", Deferred.await(counted)))
      yield* Effect.sleep("300 millis")
      expect(yield* Queue.clear(emissions)).toEqual([])
      expect(yield* Deferred.isDone(ended)).toBe(false)
      yield* Deferred.succeed(counted, undefined)
      // The new leader's first status (lastSyncedAt null: nothing synced in this leader) counts m2.
      const fresh = yield* Queue.take(emissions).pipe(Effect.timeout("5 seconds"))
      expect([fresh.pendingMutations, fresh.lastSyncedAt]).toEqual([1, null])
      expect(opens).toBe(2)
      expect(yield* Deferred.isDone(ended)).toBe(false)
    }).pipe(Effect.scoped)
)

it.live("requestSync during backoff runs the next cycle at once and leaves a halted push halted", () =>
  Effect.gen(function* () {
    const db = yield* replicaDb
    const calls = { push: 0, pull: 0 }
    const transport: Engine.Transport = {
      push: () => Effect.suspend(() => (calls.push++, Effect.fail(integrity()))),
      pull: paginate(() => Effect.suspend(() => (calls.pull++, Effect.fail(pullOffline()))))
    }
    // An hour of backoff: only a wake can bring the next cycle within the test's budget.
    const client = yield* syncLeader(db, transport, "1 hour")
    const api = tabApi(passThrough(client))
    const emissions = yield* Queue.unbounded<SyncLoop.SyncStatus>()
    yield* Effect.forkScoped(Stream.runForEach(api.syncStatus, (s) => Queue.offer(emissions, s)))
    yield* client.enqueue({
      envelope: { mutationId: Ids.MutationId.make("m1"), name: "raw.add", args: { id: "m1" } }
    })
    const halted = yield* awaitStatus(emissions, (s) => s.push._tag === "Halted" && s.phase === "backoff")
    const pullsBefore = calls.pull
    expect(halted.nextRetryAt ?? 0).toBeGreaterThan((yield* Clock.currentTimeMillis) + 30 * 60_000)
    expect(halted.lastSyncedAt).toBeNull()

    yield* api.requestSync
    const woken = yield* Effect.repeat(
      Effect.sync(() => calls.pull),
      { until: (n) => n > pullsBefore, schedule: Schedule.spaced("10 millis") }
    ).pipe(Effect.timeoutOption("3 seconds"))
    expect(Option.isSome(woken) ? "next cycle ran" : "still waiting out the backoff").toBe("next cycle ran")
    const after = yield* awaitStatus(emissions, (s) => s.phase === "backoff" && calls.pull > pullsBefore)
    expect(after.push._tag).toBe("Halted")
    expect(calls.push).toBe(1)
  }).pipe(Effect.scoped)
)

it.live(
  "a push whose response is lost keeps the entry pending until the pull installs its ack, and that cycle sets no lastSyncedAt",
  () =>
    Effect.gen(function* () {
      const db = yield* replicaDb
      const pulls = yield* Queue.unbounded<ReadonlyArray<string>>()
      // The server: records each mutation once (ledger), answers a resubmission with the recorded position.
      const recorded = new Map<string, number>()
      let pushes = 0
      let next = 100
      let cursor = 1
      const transport: Engine.Transport = {
        push: (request) =>
          Effect.suspend(() => {
            pushes++
            const results = Arr.map(request.mutations, (m) => {
              const at = recorded.get(m.mutationId) ?? ++next
              recorded.set(m.mutationId, at)
              return { mutationId: m.mutationId, position: String(at), outcome: { _tag: "Applied" } }
            })
            // The first push that carries a mutation commits on the server, then its response is lost.
            return pushes === 1 ? Effect.fail(pushOffline()) : decodePush({ results })
          }),
        pull: paginate(() =>
          Effect.flatMap(Queue.take(pulls), (acked) => {
            const acks = Arr.map(acked, (id) => ({
              mutationId: id,
              position: String(recorded.get(id) ?? 0),
              outcome: { _tag: "Applied" }
            }))
            cursor = Math.max(cursor, ...Arr.map(acked, (id) => recorded.get(id) ?? 0))
            return decodeDone({ _tag: "PullDone", changes: [], acks, nextCursor: String(cursor), expiredThrough: 0 })
          })
        )
      }
      const client = yield* syncLeader(db, transport)
      const api = tabApi(passThrough(client))
      const emissions = yield* Queue.unbounded<SyncLoop.SyncStatus>()
      const log: Array<SyncLoop.SyncStatus> = []
      yield* Effect.forkScoped(
        Stream.runForEach(api.syncStatus, (s) =>
          Effect.andThen(
            Effect.sync(() => log.push(s)),
            Queue.offer(emissions, s)
          )
        )
      )
      // Cycle 1 (empty outbox) syncs cleanly: lastSyncedAt T1.
      yield* awaitStatus(emissions, (s) => s.phase === "pulling")
      yield* Queue.offer(pulls, [])
      const t1 = (yield* awaitStatus(emissions, (s) => s.lastSyncedAt !== null)).lastSyncedAt
      yield* client.enqueue({
        envelope: { mutationId: Ids.MutationId.make("m1"), name: "raw.add", args: { id: "m1" } }
      })
      // Cycle 2: the push reaches the server but its response is lost; the pull installs nothing for m1.
      yield* awaitStatus(emissions, (s) => s.phase === "pulling" && s.pendingMutations === 1)
      const from = Option.getOrElse(
        Arr.findFirstIndex(log, (s) => s.pendingMutations === 1),
        () => -1
      )
      yield* Queue.offer(pulls, [])
      const failed = yield* awaitStatus(emissions, (s) => s.phase === "backoff")
      expect([failed.pendingMutations, failed.lastSyncedAt, failed.lastError === null]).toEqual([1, t1, false])
      // Cycle 3 (after the backoff): the resubmission is answered from the ledger, still not installed.
      yield* awaitStatus(emissions, (s) => s.phase === "pulling" && pushes === 2)
      expect(recorded.size).toBe(1)
      const beforeAck = log.length
      yield* Queue.offer(pulls, ["m1"])
      // The release and the cycle's timestamp may publish in either order: wait for both.
      const released = yield* awaitStatus(
        emissions,
        (s) => s.pendingMutations === 0 && s.lastSyncedAt !== null && s.lastSyncedAt !== t1
      )
      // Every status between the enqueue and the authoritative ack's install counted m1.
      expect(Arr.map(log.slice(from, beforeAck), (s) => s.pendingMutations)).toEqual(
        Arr.map(log.slice(from, beforeAck), () => 1)
      )
      expect(Arr.every(log.slice(from, beforeAck), (s) => s.lastSyncedAt === t1)).toBe(true)
      expect(released.lastSyncedAt ?? 0).toBeGreaterThan(t1 ?? Number.POSITIVE_INFINITY)
    }).pipe(Effect.scoped)
)

it.live(
  "presence across handover re-joins the next leader with the latest tracked state and keeps the stream open",
  () =>
    Effect.gen(function* () {
      const room = AccessScope.AccessScopeId.make("room-1")
      const snap = (state: Schema.Json): Protocol.RoomSnapshot => ({
        _tag: "snapshot",
        room,
        entries: [{ principalId: me, clientId: Ids.ClientId.make("c"), tabId: Protocol.TabId.make("tab"), state }]
      })
      const joins: Array<Schema.Json> = []
      const tracks: Array<Schema.Json> = []
      // Every (state, revision) pair a leader was asked to join or track with, in order.
      const pairs: Array<string> = []
      // This tab's entry state as each leader's server would hold it (a join sets it, a track replaces it).
      const held: Array<Schema.Json | undefined> = [undefined, undefined, undefined]
      const lost = yield* Deferred.make<void>()
      const elected = yield* Deferred.make<void>()
      let leader = 1
      const calls: LeaderCalls = {
        ...passThrough((yield* leaderRpc({ allowRawExec: false })).client),
        // Leader 1 answers once, then is lost; leader 2 answers once elected. Each join echoes its state.
        presence: (_room, state, revision) =>
          Stream.suspend(() => {
            joins.push(state)
            pairs.push(`join ${JSON.stringify(state)}@${revision}`)
            held[leader] = state
            return leader === 1
              ? Stream.concat(Stream.make(snap(state)), Stream.never).pipe(
                  Stream.interruptWhen(Effect.andThen(Deferred.await(lost), Effect.fail(leaderLost)))
                )
              : Stream.unwrap(Effect.as(Deferred.await(elected), Stream.concat(Stream.make(snap(state)), Stream.never)))
          }),
        // While no leader serves this tab, a track is lost with it.
        track: (_room, state, revision) =>
          Effect.suspend(() =>
            Deferred.isDoneUnsafe(lost) && !Deferred.isDoneUnsafe(elected)
              ? Effect.fail(leaderLost)
              : Effect.gen(function* () {
                  const json = yield* Effect.orDie(Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(state))
                  tracks.push(state)
                  pairs.push(`track ${json}@${revision}`)
                  held[leader] = state
                })
          )
      }
      const api = tabApi(calls)
      const emissions = yield* Queue.unbounded<Protocol.RoomSnapshot>()
      const ended = yield* Deferred.make<void>()
      yield* Effect.forkScoped(
        Stream.runForEach(api.presence(room, "s0"), (s) => Queue.offer(emissions, s)).pipe(
          Effect.ensuring(Deferred.succeed(ended, undefined))
        )
      )
      const next = Queue.take(emissions).pipe(Effect.timeout("5 seconds"))
      expect((yield* next).entries[0]?.state).toBe("s0")
      yield* api.track(room, "s1")
      yield* Deferred.succeed(lost, undefined)
      leader = 2
      yield* Effect.sleep("150 millis")
      // Tracked while no leader serves the tab: the track is lost with the leader, and the stream stays open.
      yield* api.track(room, "s2")
      expect(yield* Deferred.isDone(ended)).toBe(false)
      yield* Deferred.succeed(elected, undefined)
      yield* next
      // The rejoin was read before s2 (whose track was lost with the leader): it carries the latest state at
      // that point, never the opening s0, and the catch-up after its first snapshot brings the new leader to s2.
      expect(joins).toEqual(["s0", "s1"])
      // Revisions are the tab's (SPEC §10.3): the handover rejoin takes a fresh one (2), above
      // anything the old leader was ever given, so a late old-leader track is fenced; s2 then takes 3.
      expect(pairs).toEqual(['join "s0"@0', 'track "s1"@1', 'join "s1"@2', 'track "s2"@3'])
      expect(held[2]).toBe("s2")
      expect(yield* Deferred.isDone(ended)).toBe(false)
      // track on a room this tab has not joined is a no-op.
      const before = tracks.length
      yield* api.track(AccessScope.AccessScopeId.make("other"), "x")
      expect(tracks.length).toBe(before)
    }).pipe(Effect.scoped)
)

it.live(
  "presence entry is dropped when the tab stream closes: a later track is a no-op and the room can be re-joined",
  () =>
    Effect.gen(function* () {
      const room = AccessScope.AccessScopeId.make("room-1")
      const open = new Set<string>()
      const tracks: Array<Schema.Json> = []
      const calls: LeaderCalls = {
        ...passThrough((yield* leaderRpc({ allowRawExec: false })).client),
        presence: (r) => {
          const empty: Protocol.RoomSnapshot = { _tag: "snapshot", room: r, entries: [] }
          return Stream.concat(Stream.make(empty), Stream.never).pipe(
            Stream.onStart(Effect.sync(() => open.add(r))),
            Stream.ensuring(Effect.sync(() => open.delete(r)))
          )
        },
        track: (_r, state) => Effect.sync(() => void tracks.push(state))
      }
      const api = tabApi(calls)
      const first = yield* Stream.runHead(api.presence(room, "a"))
      expect(Option.isSome(first)).toBe(true)
      // runHead closed the stream: the leader-side stream ended with it, and track is a no-op.
      expect(open.size).toBe(0)
      yield* api.track(room, "b")
      expect(tracks).toEqual([])
      // A second open of the same room while one is open is refused; after it closes, joining again works.
      const holder = yield* Effect.forkScoped(Stream.runDrain(api.presence(room, "c")))
      yield* Effect.sleep("50 millis")
      const second = yield* Effect.flip(Stream.runDrain(api.presence(room, "d")))
      expect(second._tag).toBe("PresenceAlreadyOpen")
      yield* Fiber.interrupt(holder)
      expect(Option.isSome(yield* Stream.runHead(api.presence(room, "e")))).toBe(true)
    }).pipe(Effect.scoped)
)

/** Tab-side presence calls over a fake leader: counts opens, records tracks, `hangTracks` never answers one. */
/**
 * Tab-side presence calls over a fake leader: counts opens, records every track attempt, and answers a
 * track of `state` with `answer(state)` (default: at once).
 */
const presenceCalls = (
  client: Effect.Success<ReturnType<typeof leaderRpc>>["client"],
  answer: (state: Schema.Json) => Effect.Effect<void, LeaderUnavailable> = () => Effect.void
) => {
  const seen = { opens: 0, open: 0, tracks: new Array<Schema.Json>() }
  const calls: LeaderCalls = {
    ...passThrough(client),
    presence: (r, state) => {
      const snap: Protocol.RoomSnapshot = {
        _tag: "snapshot",
        room: r,
        entries: [{ principalId: me, clientId: Ids.ClientId.make("c"), tabId: Protocol.TabId.make("tab"), state }]
      }
      // `ensuring` also runs for a stream interrupted before it started: count only a started one.
      return Stream.suspend(() => {
        let started = false
        return Stream.concat(Stream.make(snap), Stream.never).pipe(
          Stream.onStart(Effect.sync(() => ((started = true), seen.opens++, seen.open++))),
          Stream.ensuring(Effect.sync(() => void (started && seen.open--)))
        )
      })
    },
    track: (_r, state) =>
      Effect.andThen(
        Effect.sync(() => void seen.tracks.push(state)),
        answer(state)
      )
  }
  return { calls, seen }
}

// The races below need preemption inside each opening fiber: a tiny scheduler budget on the fiber
// itself, swept against how long the parent lets it run before acting.
const preempted = <A, E>(self: Effect.Effect<A, E>, ops: number) =>
  Effect.provideService(self, References.MaxOpsBeforeYield, ops)

it.live(
  "two presence opens of one room at the same time: exactly one reaches the leader, the other is PresenceAlreadyOpen",
  () =>
    Effect.gen(function* () {
      const room = AccessScope.AccessScopeId.make("room-1")
      const { client } = yield* leaderRpc({ allowRawExec: false })
      for (let ops = 3; ops < 30; ops++) {
        const { calls, seen } = presenceCalls(client)
        const api = tabApi(calls)
        const a = yield* Effect.forkChild(preempted(Effect.exit(Stream.runDrain(api.presence(room, "a"))), ops))
        const b = yield* Effect.forkChild(preempted(Effect.exit(Stream.runDrain(api.presence(room, "b"))), ops))
        yield* Effect.repeat(Effect.yieldNow, { times: 100 })
        const ended = Arr.filter([a.pollUnsafe(), b.pollUnsafe()], (x) => x !== undefined)
        const refused = ended[0]?._tag === "Success" ? ended[0].value : undefined
        expect({ ops, opens: seen.opens, ended: ended.length }).toEqual({ ops, opens: 1, ended: 1 })
        expect(
          refused !== undefined && Exit.isFailure(refused) && Str.includes("PresenceAlreadyOpen")(String(refused.cause))
        ).toBe(true)
        yield* Fiber.interrupt(a)
        yield* Fiber.interrupt(b)
      }
    })
)

it.live(
  "a presence stream cancelled at any point frees its room: reopening succeeds and a later track is dropped",
  () =>
    Effect.gen(function* () {
      const room = AccessScope.AccessScopeId.make("room-1")
      const { client } = yield* leaderRpc({ allowRawExec: false })
      for (let ops = 3; ops < 35; ops++) {
        for (let turns = 0; turns < 20; turns++) {
          const { calls, seen } = presenceCalls(client)
          const api = tabApi(calls)
          const fiber = yield* Effect.forkChild(preempted(Stream.runDrain(api.presence(room, "a")), ops))
          yield* Effect.repeat(Effect.yieldNow, { times: turns })
          yield* Fiber.interrupt(fiber)
          yield* api.track(room, "late")
          const reopened = yield* Effect.exit(Stream.runHead(api.presence(room, "b")))
          expect({ ops, turns, ok: Exit.isSuccess(reopened), open: seen.open, tracks: seen.tracks }).toEqual({
            ops,
            turns,
            ok: true,
            open: 0,
            tracks: []
          })
        }
      }
    })
)

it.live("a tab's presence open carries a genuine (state, revision) pair even when a track races its start", () =>
  Effect.gen(function* () {
    const room = AccessScope.AccessScopeId.make("room-1")
    const { client } = yield* leaderRpc({ allowRawExec: false })
    // States are named after their revision, so a torn read shows up as a mismatched pair.
    for (let ops = 3; ops < 35; ops++) {
      for (let turns = 0; turns < 20; turns++) {
        const { calls } = presenceCalls(client)
        const pairs: Array<string> = []
        const api = tabApi({
          ...calls,
          presence: (r, state, revision) =>
            Stream.suspend(() => {
              pairs.push(`${String(state)}@${revision}`)
              return calls.presence(r, state, revision)
            })
        })
        const fiber = yield* Effect.forkChild(preempted(Stream.runDrain(api.presence(room, "s0")), ops))
        yield* Effect.repeat(Effect.yieldNow, { times: turns })
        yield* api.track(room, "s1")
        yield* Effect.repeat(Effect.yieldNow, { times: 50 })
        const torn = Arr.filter(pairs, (p) => p !== "s0@0" && p !== "s1@1")
        expect({ ops, turns, torn }).toEqual({ ops, turns, torn: [] })
        yield* Fiber.interrupt(fiber)
      }
    }
  })
)

it.live("closing and reopening a room never reuses a revision: the reopened join is above every earlier one", () =>
  Effect.gen(function* () {
    const room = AccessScope.AccessScopeId.make("room-1")
    const { client } = yield* leaderRpc({ allowRawExec: false })
    const { calls } = presenceCalls(client)
    const opened: Array<number> = []
    const api = tabApi({
      ...calls,
      presence: (r, state, revision) =>
        Stream.suspend(() => {
          opened.push(revision)
          return calls.presence(r, state, revision)
        })
    })
    // First lifetime: open (0), two tracks (1, 2) that may still be in flight when the stream closes.
    const first = yield* Effect.forkChild(Stream.runDrain(api.presence(room, "a")))
    yield* Effect.repeat(Effect.yieldNow, { times: 50 })
    yield* api.track(room, "b")
    yield* api.track(room, "c")
    yield* Fiber.interrupt(first)
    // A late old-lifetime track@2 must not outrank the reopened entry, so the reopen takes 3.
    expect(Option.isSome(yield* Stream.runHead(api.presence(room, "d")))).toBe(true)
    expect(opened).toEqual([0, 3])
  })
)

// Short deadlines for the tab's sender, so these run in live time.
const quickSender = { presenceSendTimeout: "100 millis", presenceRetryDelay: "20 millis" } as const

it.live(
  "a tab send that never completes: track returns at once, snapshots flow, and the latest state is retried without a rejoin",
  () =>
    Effect.gen(function* () {
      const room = AccessScope.AccessScopeId.make("room-1")
      const { client } = yield* leaderRpc({ allowRawExec: false })
      const { calls, seen } = presenceCalls(client, (state) => (state === "c" ? Effect.never : Effect.void))
      const api = tabApi(calls, quickSender)
      const emissions = yield* Queue.unbounded<Protocol.RoomSnapshot>()
      const holder = yield* Effect.forkScoped(
        Stream.runForEach(api.presence(room, "b"), (s) => Queue.offer(emissions, s))
      )
      yield* Queue.take(emissions).pipe(Effect.timeout("5 seconds"))
      // Two tracks while the first send hangs: both return immediately.
      yield* api.track(room, "c").pipe(Effect.timeout("100 millis"))
      yield* Effect.sleep("20 millis")
      yield* api.track(room, "d").pipe(Effect.timeout("100 millis"))
      // c's deadline expires and the retry carries d, on the same join: the presence stream never ended.
      yield* Effect.sleep("400 millis")
      expect([seen.tracks, seen.opens, seen.open]).toEqual([["c", "d"], 1, 1])
      yield* Fiber.interrupt(holder)
      // The closed stream ended its sender; a later track is dropped.
      expect(seen.open).toBe(0)
      yield* api.track(room, "e")
      yield* Effect.sleep("50 millis")
      expect(seen.tracks).toEqual(["c", "d"])
    }).pipe(Effect.scoped)
)

it.live("a tab send lost with LeaderLost while the presence stream stays up is retried with the latest state", () =>
  Effect.gen(function* () {
    const room = AccessScope.AccessScopeId.make("room-1")
    const { client } = yield* leaderRpc({ allowRawExec: false })
    // Only the track call sees the leader go away (a unary RPC error); the room stream itself is healthy.
    const { calls, seen } = presenceCalls(client, (state) => (state === "s1" ? Effect.fail(leaderLost) : Effect.void))
    const api = tabApi(calls, quickSender)
    const emissions = yield* Queue.unbounded<Protocol.RoomSnapshot>()
    yield* Effect.forkScoped(Stream.runForEach(api.presence(room, "s0"), (s) => Queue.offer(emissions, s)))
    yield* Queue.take(emissions).pipe(Effect.timeout("5 seconds"))
    yield* api.track(room, "s1")
    yield* Effect.sleep("5 millis")
    yield* api.track(room, "s2")
    yield* Effect.sleep("300 millis")
    expect([seen.tracks, seen.opens, seen.open]).toEqual([["s1", "s2"], 1, 1])
  }).pipe(Effect.scoped)
)
