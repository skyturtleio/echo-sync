import { assert, describe, it } from "@effect/vitest"
import * as PgClient from "@effect/sql-pg/PgClient"
import {
  Array as Arr,
  Cause,
  Context,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Queue,
  Ref,
  Schedule,
  Schema,
  Stream
} from "effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as HttpRouter from "effect/http/HttpRouter"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { SqlClient } from "effect/sql/SqlClient"
import { SqlError, UnknownError } from "effect/sql/SqlError"
import { AccessScope, Ids, Mutation, Protocol } from "echo-sync"
import { layerHttp } from "./rpc.ts"
import { grant, Items, pgAvailable, registry, RowId, TestDatabase } from "./test-support.ts"

const addItem = Mutation.make("items.add", {
  args: Schema.Struct({ id: Schema.String, body: Schema.String }),
  handler: (ctx, args) =>
    ctx.store.insert(Items, {
      id: RowId.make(args.id),
      accessScopeId: AccessScope.AccessScopeId.make("s1"),
      body: args.body
    })
})
const mutations = Mutation.makeRegistry([addItem])

/**
 * A real RpcClient (HTTP POST, NDJSON) against the mounted router, as the browser transport
 * speaks it (DESIGN §6). The handler shares the test's database clients.
 */
const connectWith = (wrap: (sql: SqlClient) => SqlClient) =>
  Effect.gen(function* () {
    const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
    const sql = Context.get(db, SqlClient)
    return yield* connectTo(Context.add(db, SqlClient, wrap(sql)))
  })

const connectTo = (db: Context.Context<SqlClient | PgClient.PgClient>) =>
  Effect.gen(function* () {
    const app = layerHttp({
      registry,
      mutations,
      principalFor: () => Effect.succeed(AccessScope.PrincipalId.make("p")),
      ledger: { conflictRetry: undefined }
    }).pipe(Layer.provide(Layer.succeedContext(db)))
    const web = HttpRouter.toWebHandler(app, { disableLogger: true })
    yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
    const fetch: typeof globalThis.fetch = Object.assign(
      (input: RequestInfo | URL, init?: RequestInit) => web.handler(new Request(input, init)),
      { preconnect: globalThis.fetch.preconnect }
    )
    return yield* RpcClient.make(Protocol.SyncRpcs).pipe(
      Effect.provide(
        RpcClient.layerProtocolHttp({ url: "http://echo.test/sync" }).pipe(
          Layer.provide([
            RpcSerialization.layerNdjson,
            FetchHttpClient.layer.pipe(Layer.provide(Layer.succeed(FetchHttpClient.Fetch)(fetch)))
          ])
        )
      )
    )
  })

const connect = connectWith((sql) => sql)

/**
 * A SqlClient whose transactions run `inner` replaced by `around(inner)`: deterministic fault
 * injection at the transaction boundary (pull's REPEATABLE READ snapshot is its only one).
 */
const aroundTransactions =
  (
    around: (
      inner: Effect.Effect<unknown, unknown, unknown>,
      sql: SqlClient
    ) => Effect.Effect<unknown, unknown, unknown>
  ) =>
  (sql: SqlClient): SqlClient =>
    new Proxy(sql, {
      get: (target, key, receiver) =>
        key === "withTransaction"
          ? (inner: Effect.Effect<unknown, unknown, unknown>) => target.withTransaction(around(inner, target))
          : Reflect.get(target, key, receiver)
    })

const push = (ids: Arr.NonEmptyReadonlyArray<readonly [string, string]>) =>
  Protocol.PushRequest.make({
    clientId: Ids.ClientId.make("d"),
    mutations: Arr.map(ids, ([id, body], i) => ({
      mutationId: Ids.MutationId.make(id),
      seq: Ids.Seq.make(i + 1),
      name: "items.add",
      args: { id, body }
    }))
  })

const writer = Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* grant("p", "s1")
  yield* sql`UPDATE echo_sync.scope_grants SET role = 'writer' WHERE principal_id = 'p'`
})

describe.skipIf(!pgAvailable)("sync RPC over HTTP (§6, §8)", () => {
  it.live("a pushed mutation's result position and outcome reach the client and come back as its ack", () =>
    Effect.gen(function* () {
      yield* writer
      const client = yield* connect
      const pushed = yield* client.push(push([["m1", "hello"]]))
      const [result] = pushed.results
      if (result === undefined) return assert.fail("expected one result")
      assert.strictEqual(result.outcome._tag, "Applied")
      const pulled = yield* client.pull({ clientId: Ids.ClientId.make("d"), cursor: null, continuation: Option.none() })
      if (pulled._tag !== "PullDone") return assert.fail("expected PullDone")
      assert.deepStrictEqual(pulled.acks, [
        { mutationId: result.mutationId, position: result.position, outcome: result.outcome }
      ])
      assert.isTrue(pulled.changes.some((c) => c._tag === "upsert" && c.table === "items" && c.row["body"] === "hello"))
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live("a connection lost mid-batch is a short results list, never a defect the client would halt on", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* writer
      // The second mutation's write kills its own backend: a real connection failure mid-transaction.
      yield* sql`
        CREATE FUNCTION kill_self() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.body = 'kill' THEN PERFORM pg_terminate_backend(pg_backend_pid()); END IF;
          RETURN NEW;
        END $$`
      yield* sql`CREATE TRIGGER kill_self BEFORE INSERT ON items FOR EACH ROW EXECUTE FUNCTION kill_self()`
      const client = yield* connect
      const exit = yield* Effect.exit(
        client.push(
          push([
            ["m1", "ok"],
            ["m2", "kill"],
            ["m3", "ok"]
          ])
        )
      )
      if (!Exit.isSuccess(exit)) return assert.fail(`push failed: ${String(exit.cause)}`)
      assert.deepStrictEqual(
        exit.value.results.map((r) => r.mutationId),
        ["m1"]
      )
      const ledgered = yield* sql<{ mutation_id: string }>`SELECT mutation_id FROM echo_sync.mutations`
      assert.deepStrictEqual(
        ledgered.map((r) => r.mutation_id),
        ["m1"]
      )
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live("a database failure under pull reaches the client as a typed ServerUnavailable, not a defect", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* grant("p", "s1")
      const client = yield* connect
      // Injected SqlError: every pull reads engine_state first.
      yield* sql`ALTER TABLE echo_sync.engine_state RENAME TO engine_state_gone`
      const exit = yield* Effect.exit(
        client.pull({ clientId: Ids.ClientId.make("d"), cursor: null, continuation: Option.none() })
      )
      assert.deepStrictEqual(failureTag(exit), "ServerUnavailable")
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live("a connection lost before ROLLBACK of a failed pull is ServerUnavailable, not the rollback's defect", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      // Pull fails typed (unknownCursor) inside its snapshot; the backend then dies, so the
      // pinned client's ROLLBACK raises a defect carrying a SqlError.
      const client = yield* connectWith(
        aroundTransactions((inner, sql) =>
          Effect.tapError(inner, () => Effect.ignore(sql`SELECT pg_terminate_backend(pg_backend_pid())`))
        )
      )
      const exit = yield* Effect.exit(
        client.pull({ clientId: Ids.ClientId.make("d"), cursor: Ids.Position.make(1n), continuation: Option.none() })
      )
      assert.deepStrictEqual(failureTag(exit), "ServerUnavailable")
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live("a SQL failure together with a programmer defect under pull still reaches the client as a defect", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      const sqlError = new SqlError({ reason: new UnknownError({ cause: null, message: "connection lost" }) })
      const client = yield* connectWith(
        aroundTransactions(() =>
          Effect.failCause(Cause.fromReasons([Cause.makeFailReason(sqlError), Cause.makeDieReason(new Error("bug"))]))
        )
      )
      const exit = yield* Effect.exit(
        client.pull({ clientId: Ids.ClientId.make("d"), cursor: null, continuation: Option.none() })
      )
      assert.isTrue(Exit.hasDies(exit), failureTag(exit))
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live("a pokes subscriber gets a poke as soon as LISTEN is in place, so a reconnect has no catch-up gap", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      const client = yield* connect
      const received = yield* Queue.unbounded<unknown>()
      yield* Effect.forkChild(
        Stream.runForEach(client.pokes({ clientId: Ids.ClientId.make("d") }), (poke) => Queue.offer(received, poke))
      )
      // No write has happened: the first poke is the server's subscribe-then-poke, not a notification.
      const first = yield* Effect.timeoutOption(Queue.take(received), "3 seconds")
      assert.isTrue(Option.isSome(first), "no initial poke after subscribing")
      yield* sql`INSERT INTO items (id, access_scope_id) VALUES ('a', 's1')`
      const second = yield* Effect.timeoutOption(Queue.take(received), "3 seconds")
      assert.isTrue(Option.isSome(second), "no poke after a committed captured write")
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live("a pokes subscription whose LISTEN fails emits nothing: the initial poke never precedes LISTEN", () =>
    Effect.gen(function* () {
      const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
      const pg = Context.get(db, PgClient.PgClient)
      const sqlError = new SqlError({ reason: new UnknownError({ cause: null, message: "listen refused" }) })
      const failingListen: PgClient.PgClient = new Proxy(pg, {
        get: (target, key, receiver) =>
          key === "listen" ? () => Effect.fail(sqlError) : Reflect.get(target, key, receiver)
      })
      const client = yield* connectTo(Context.add(db, PgClient.PgClient, failingListen))
      const emitted = yield* Ref.make(0)
      const exit = yield* Effect.exit(
        Stream.runForEach(client.pokes({ clientId: Ids.ClientId.make("d") }), () => Ref.update(emitted, (n) => n + 1))
      )
      assert.deepStrictEqual(failureTag(exit), "ServerUnavailable")
      assert.strictEqual(yield* Ref.get(emitted), 0)
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live("a lost LISTEN connection ends the poke stream with ServerUnavailable, not a defect", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      const client = yield* connect
      const stream = yield* Effect.forkChild(
        Effect.exit(Stream.runDrain(client.pokes({ clientId: Ids.ClientId.make("d") })))
      )
      // Wait for the listener's backend, then kill it: a real connection loss.
      const killed = sql<{ killed: boolean }>`
        SELECT pg_terminate_backend(pid) AS killed FROM pg_stat_activity
        WHERE datname = current_database() AND query ILIKE 'listen%'`.pipe(
        Effect.flatMap((rows) => (rows.some((r) => r.killed) ? Effect.void : Effect.fail("no listener yet"))),
        Effect.retry(Schedule.spaced("20 millis").pipe(Schedule.upTo({ times: 100 })))
      )
      yield* killed
      const exit = yield* Fiber.join(stream)
      assert.deepStrictEqual(failureTag(exit), "ServerUnavailable")
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )
})

/** The tag of the exit's sole typed failure, or a description of why there is none. */
const failureTag = (exit: Exit.Exit<unknown, { readonly _tag: string }>) => {
  if (Exit.isSuccess(exit)) return "success"
  if (exit.cause.reasons.some((r) => !Cause.isFailReason(r))) return `not a typed failure: ${String(exit.cause)}`
  return exit.cause.reasons.map((r) => (Cause.isFailReason(r) ? r.error._tag : "")).join(",")
}
