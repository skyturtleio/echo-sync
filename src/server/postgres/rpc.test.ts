import { assert, describe, it } from "@effect/vitest"
import * as PgClient from "@effect/sql-pg/PgClient"
import {
  Array as Arr,
  Cause,
  Context,
  Duration,
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
import * as TestClock from "effect/testing/TestClock"
import { SyncTransportError } from "../../client/sync/SyncFailure.ts"
import * as SyncLoop from "../../client/sync/SyncLoop.ts"
import type { ClientPolicy } from "./pull.ts"
import { layerHttp, sharedPokes } from "./rpc.ts"
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

const connectTo = (
  db: Context.Context<SqlClient | PgClient.PgClient>,
  heartbeatInterval?: Duration.Input,
  principalFor: () => Effect.Effect<AccessScope.PrincipalId, Protocol.Unauthorized> = () =>
    Effect.succeed(AccessScope.PrincipalId.make("p")),
  clientPolicy?: ClientPolicy
) =>
  Effect.gen(function* () {
    const app = layerHttp({
      registry,
      mutations,
      principalFor,
      clientPolicy,
      ledger: { conflictRetry: undefined },
      heartbeatInterval
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

  it.live("an infrastructure defect in principalFor is a transient stop (empty results), not a push failure", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* writer
      const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
      // A session lookup in Postgres whose connection is lost: the pinned client's orDie raises it as a defect.
      const lost = new SqlError({ reason: new UnknownError({ cause: "session store unreachable" }) })
      const client = yield* connectTo(db, undefined, () => Effect.die(lost))
      const exit = yield* Effect.exit(client.push(push([["m1", "ok"]])))
      if (!Exit.isSuccess(exit)) return assert.fail(`push failed: ${String(exit.cause)}`)
      assert.deepStrictEqual(exit.value.results, [])
      const ledgered = yield* sql<{ mutation_id: string }>`SELECT mutation_id FROM echo_sync.mutations`
      assert.deepStrictEqual(ledgered, [])
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
      const received = yield* Queue.unbounded<typeof Protocol.PokesElement.Type>()
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

const pullFor = (clientId: string, principal?: AccessScope.PrincipalId) => ({
  clientId: Ids.ClientId.make(clientId),
  ...(principal === undefined ? {} : { principal }),
  cursor: null,
  continuation: Option.none()
})
const pushFor = (clientId: string, principal?: AccessScope.PrincipalId) => ({
  ...push([["m1", "written"]]),
  clientId: Ids.ClientId.make(clientId),
  ...(principal === undefined ? {} : { principal })
})
const refusal = (exit: Exit.Exit<unknown, { readonly _tag: string }>) =>
  Exit.isFailure(exit) ? Option.getOrUndefined(Exit.findErrorOption(exit)) : undefined

describe.skipIf(!pgAvailable)("the replica's principal against the authenticated session (§16.21)", () => {
  const alice = AccessScope.PrincipalId.make("alice-replica-principal")
  const bob = AccessScope.PrincipalId.make("bob-session-principal")
  /**
   * The server with bob's session. Bob may write s1 and holds a row there, so any request that ran
   * as bob would bind, ledger or deliver something. `statements` counts every statement the
   * handlers issue and `policyCalls` every `clientPolicy` run: a request refused before any sync
   * work issues neither.
   */
  const asBob = Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* grant(bob, "s1")
    yield* sql`UPDATE echo_sync.scope_grants SET role = 'writer' WHERE principal_id = ${bob}`
    yield* sql`INSERT INTO items (id, access_scope_id, body) VALUES ('bobs', 's1', 'bob only')`
    const counts = { statements: 0, policyCalls: 0 }
    const counting = new Proxy(sql, {
      apply: (target, self, args) => {
        counts.statements++
        return Reflect.apply(target, self, args)
      },
      get: (target, key, receiver) => {
        const value = Reflect.get(target, key, receiver)
        return (key === "unsafe" || key === "withTransaction") && typeof value === "function"
          ? (...args: ReadonlyArray<unknown>) => {
              counts.statements++
              return Reflect.apply(value, target, args)
            }
          : value
      }
    })
    const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
    const client = yield* connectTo(
      Context.add(db, SqlClient, counting),
      undefined,
      () => Effect.succeed(bob),
      () => Effect.sync(() => void counts.policyCalls++)
    )
    return { client, counts }
  })
  /** Every row a request could bind, ledger, record or write, in full. */
  const state = Effect.gen(function* () {
    const sql = yield* SqlClient
    const [row] = yield* sql<{ state: string }>`SELECT jsonb_build_object(
      'clients', (SELECT jsonb_agg(to_jsonb(t) ORDER BY client_id) FROM echo_sync.clients t),
      'ledger', (SELECT jsonb_agg(to_jsonb(t) ORDER BY position) FROM echo_sync.mutations t),
      'cursors', (SELECT jsonb_agg(to_jsonb(t) ORDER BY client_id, position) FROM echo_sync.client_scopes t),
      'items', (SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM items t)
    )::text AS state`
    return row?.state
  })
  const clientRows = Effect.gen(function* () {
    const sql = yield* SqlClient
    return yield* sql<{ client_id: string; principal_id: string }>`
      SELECT client_id, principal_id FROM echo_sync.clients ORDER BY client_id`
  })

  it.live(
    "push, pull and pokes asserting another principal than the session's run no statement or policy and change no row, even for a bound client",
    () =>
      Effect.gen(function* () {
        const { client, counts } = yield* asBob
        const refuse = Effect.gen(function* () {
          const before = { ...counts, state: yield* state }
          const exits = [
            yield* Effect.exit(client.push(pushFor("fresh", alice))),
            yield* Effect.exit(client.pull(pullFor("fresh", alice))),
            yield* Effect.exit(
              Stream.runHead(client.pokes({ clientId: Ids.ClientId.make("fresh"), principal: alice }))
            ),
            yield* Effect.exit(client.push(pushFor("bound", alice))),
            yield* Effect.exit(client.pull(pullFor("bound", alice)))
          ]
          const messages = new Set<string>()
          for (const exit of exits) {
            const error = refusal(exit)
            assert.strictEqual(error?._tag, "Unauthorized", String(exit))
            messages.add(error !== undefined && "message" in error ? String(error.message) : "")
          }
          // One fixed message, naming neither principal.
          assert.strictEqual(messages.size, 1)
          assert.isFalse(
            [...messages].some((m) => m.includes(alice) || m.includes(bob)),
            [...messages].join()
          )
          assert.deepStrictEqual({ ...counts, state: yield* state }, before)
        })
        assert.deepStrictEqual(yield* clientRows, [])
        yield* refuse

        // Control: the same requests asserting the session's own principal run as bob.
        assert.strictEqual((yield* client.push(pushFor("bound", bob))).results.length, 1)
        assert.strictEqual((yield* client.pull(pullFor("bound", bob)))._tag, "PullDone")
        assert.isAbove(counts.statements, 0)
        assert.isAbove(counts.policyCalls, 0)
        assert.deepStrictEqual(yield* clientRows, [{ client_id: "bound", principal_id: bob }])

        // Every request is checked, not only the first: bob's bound client asserting alice is refused alike.
        yield* refuse
      }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live("push, pull and pokes from an old client without the field still bind an unknown client on first sight", () =>
    Effect.gen(function* () {
      const { client } = yield* asBob
      assert.strictEqual((yield* client.push(pushFor("old-push"))).results.length, 1)
      assert.strictEqual((yield* client.pull(pullFor("old-pull")))._tag, "PullDone")
      assert.isTrue(Option.isSome(yield* Stream.runHead(client.pokes({ clientId: Ids.ClientId.make("old-pokes") }))))
      assert.deepStrictEqual(yield* clientRows, [
        { client_id: "old-pokes", principal_id: bob },
        { client_id: "old-pull", principal_id: bob },
        { client_id: "old-push", principal_id: bob }
      ])
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )
})

/** The number of backends in this database sitting in `LISTEN`. */
const listeners = Effect.gen(function* () {
  const sql = yield* SqlClient
  const [row] = yield* sql<{ n: number }>`
    SELECT count(*)::int AS n FROM pg_stat_activity
    WHERE datname = current_database() AND query ILIKE 'listen%'`
  return row?.n ?? 0
})

/** Opens a pokes stream whose pokes land in a queue; the stream's exit lands in `ended`. */
const openPokes = (client: Effect.Success<typeof connect>, ended: Queue.Queue<string>) =>
  Effect.gen(function* () {
    const received = yield* Queue.unbounded<typeof Protocol.PokesElement.Type>()
    yield* Effect.forkChild(
      Effect.exit(
        Stream.runForEach(client.pokes({ clientId: Ids.ClientId.make("d") }), (poke) => Queue.offer(received, poke))
      ).pipe(Effect.flatMap((exit) => Queue.offer(ended, failureTag(exit))))
    )
    return received
  })

/** The next `poke` within 5 s in total; heartbeats in between are skipped, they announce nothing. */
const nextPoke = (received: Queue.Queue<typeof Protocol.PokesElement.Type>) =>
  Effect.flatMap(
    Effect.timeoutOption(
      Effect.repeat(Queue.take(received), { until: (element) => element._tag === "poke" }),
      "5 seconds"
    ),
    (poke) => (Option.isSome(poke) ? Effect.void : Effect.fail("no poke within 5 s"))
  )

describe.skipIf(!pgAvailable)("one LISTEN per server process (§6)", () => {
  it.live(
    "more open pokes streams than pool connections do not block push or pull, and the server holds one LISTEN",
    () =>
      Effect.gen(function* () {
        yield* writer
        const pg = yield* PgClient.PgClient
        const url = pg.config.url
        if (url === undefined) return assert.fail("test database has no url")
        // A pool of 3; per-stream LISTEN would pin all 3 with the 6 streams below.
        const small = yield* Layer.build(PgClient.layer({ url, maxConnections: 3 }))
        const client = yield* connectTo(small)
        const ended = yield* Queue.unbounded<string>()
        const streams = yield* Effect.forEach(Arr.range(1, 6), () => openPokes(client, ended))
        for (const received of streams) yield* nextPoke(received) // every initial poke

        const pushed = yield* client.push(push([["m1", "hello"]])).pipe(Effect.timeout("10 seconds"))
        assert.strictEqual(pushed.results[0]?.outcome._tag, "Applied")
        const pulled = yield* client
          .pull({ clientId: Ids.ClientId.make("d"), cursor: null, continuation: Option.none() })
          .pipe(Effect.timeout("10 seconds"))
        assert.strictEqual(pulled._tag, "PullDone")
        // The push's commit notifies every open stream through the one LISTEN.
        for (const received of streams) yield* nextPoke(received)
        assert.strictEqual(yield* listeners, 1)
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    60_000
  )

  it.live(
    "subscribe-then-poke: a commit before the stream opened is covered by the initial poke's pull, a commit after it notifies",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        yield* grant("p", "s1")
        const client = yield* connect
        yield* sql`INSERT INTO items (id, access_scope_id) VALUES ('before', 's1')`
        const received = yield* openPokes(client, yield* Queue.unbounded<string>())
        yield* nextPoke(received)
        const pulled = yield* client.pull({
          clientId: Ids.ClientId.make("d"),
          cursor: null,
          continuation: Option.none()
        })
        if (pulled._tag !== "PullDone") return assert.fail("expected PullDone")
        assert.isTrue(
          pulled.changes.some((c) => c._tag === "upsert" && c.table === "items" && c.row["id"] === "before")
        )
        yield* sql`INSERT INTO items (id, access_scope_id) VALUES ('after', 's1')`
        yield* nextPoke(received)
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    60_000
  )

  it.live(
    "a lost shared LISTEN ends every open pokes stream with ServerUnavailable; after it is re-established a new stream gets its initial poke and notifications",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        const client = yield* connect
        const ended = yield* Queue.unbounded<string>()
        const streams = yield* Effect.forEach(Arr.range(1, 3), () => openPokes(client, ended))
        for (const received of streams) yield* nextPoke(received)
        assert.strictEqual(yield* listeners, 1)

        yield* sql`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
          WHERE datname = current_database() AND query ILIKE 'listen%'`
        const endings = yield* Effect.forEach(Arr.range(1, 3), () =>
          Effect.timeoutOption(Queue.take(ended), "5 seconds")
        )
        assert.deepStrictEqual(
          endings.map((e) => Option.getOrElse(e, () => "still open")),
          ["ServerUnavailable", "ServerUnavailable", "ServerUnavailable"]
        )

        // A client reconnecting with backoff: open streams until one gets its initial poke.
        const reconnected = yield* Effect.gen(function* () {
          const failed = yield* Queue.unbounded<string>()
          const received = yield* openPokes(client, failed)
          const first = yield* Effect.raceFirst(Effect.as(Queue.take(received), "poke"), Queue.take(failed))
          return first === "poke" ? received : yield* Effect.fail(first)
        }).pipe(Effect.retry(Schedule.spaced("50 millis").pipe(Schedule.upTo({ times: 100 }))))
        yield* sql`INSERT INTO items (id, access_scope_id) VALUES ('a', 's1')`
        yield* nextPoke(reconnected)
        assert.strictEqual(yield* listeners, 1)
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    60_000
  )
})

describe.skipIf(!pgAvailable)("pokes liveness (§6)", () => {
  it.live(
    "an idle pokes stream carries a heartbeat at least every heartbeatInterval, and no heartbeat makes the client pull",
    () =>
      Effect.gen(function* () {
        const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
        const client = yield* connectTo(db, "200 millis")
        const seen: Array<{ readonly tag: string; readonly at: number }> = []
        const pulls = { n: 0 }
        // The leader's loop: followPokes over the real RPC stream, `wake` is what runs a pull.
        const stream = client.pokes({ clientId: Ids.ClientId.make("d") }).pipe(
          Stream.tap((element) => Effect.sync(() => void seen.push({ tag: element._tag, at: Date.now() }))),
          Stream.mapError((reason) => new SyncTransportError({ reason }))
        )
        yield* Effect.forkChild(
          SyncLoop.followPokes({ connect: Effect.succeed(stream), wake: Effect.sync(() => void pulls.n++) })
        )
        yield* eventually(
          Effect.sync(() => seen.length >= 5),
          "an initial poke and four heartbeats"
        )
        // No commit happened: the initial poke, then heartbeats only, never more than one interval
        // (plus scheduling slack) apart.
        assert.deepStrictEqual(
          seen.map((e) => e.tag),
          ["poke", ...Arr.makeBy(seen.length - 1, () => "heartbeat")]
        )
        const gaps = seen.slice(1).map((e, i) => e.at - (seen[i]?.at ?? e.at))
        assert.isTrue(
          gaps.every((gap) => gap < 450),
          `gaps ${gaps.join(", ")} ms`
        )
        assert.strictEqual(pulls.n, 1) // the initial poke's pull only
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    60_000
  )
})

/** A channel name over 63 bytes: `PgClient.listen` reserves its pool connection, then rejects it. */
const tooLong = "x".repeat(64)

/**
 * The test database's own pool of 3, whose `listen` the test can steer: while `failing()` the
 * attempt fails after reserving its connection (a post-reservation failure on the real driver).
 */
const steeredPool = (failing: () => boolean) =>
  Effect.gen(function* () {
    const url = (yield* PgClient.PgClient).config.url
    if (url === undefined) return yield* Effect.die("test database has no url")
    const small = yield* Layer.build(PgClient.layer({ url, maxConnections: 3 }))
    const pg = Context.get(small, PgClient.PgClient)
    const calls = { attempts: 0 }
    const steered: PgClient.PgClient = new Proxy(pg, {
      get: (target, key, receiver) =>
        key === "listen"
          ? (channel: string) => {
              calls.attempts++
              return target.listen(failing() ? tooLong : channel)
            }
          : Reflect.get(target, key, receiver)
    })
    return { context: Context.add(small, PgClient.PgClient, steered), pg: steered, calls }
  })

/** Real time, independent of the TestClock. */
const realSleep = (ms: number) => Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, ms)))
const eventually = <E, R>(check: Effect.Effect<boolean, E, R>, what: string) =>
  Effect.gen(function* () {
    for (let i = 0; i < 250; i++) {
      if (yield* check) return
      yield* realSleep(20)
    }
    return yield* Effect.die(`timed out waiting for ${what}`)
  })
const killListener = Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
    WHERE datname = current_database() AND query ILIKE 'listen%'`
})

describe.skipIf(!pgAvailable)("re-establishing the shared LISTEN (§6)", () => {
  it.live(
    "LISTEN attempts that fail after reserving a connection release it: recovery succeeds after more failures than the pool has connections",
    () =>
      Effect.gen(function* () {
        yield* writer
        let failures = 0
        const { context, calls } = yield* steeredPool(() => failures++ < 4)
        const client = yield* connectTo(context)
        // A reconnecting client: open streams until one gets its initial poke.
        const received = yield* Effect.gen(function* () {
          const failed = yield* Queue.unbounded<string>()
          const pokes = yield* openPokes(client, failed)
          const first = yield* Effect.raceFirst(Effect.as(Queue.take(pokes), "poke"), Queue.take(failed))
          return first === "poke" ? pokes : yield* Effect.fail(first)
        }).pipe(Effect.retry(Schedule.spaced("50 millis").pipe(Schedule.upTo({ times: 200 }))))
        assert.strictEqual(calls.attempts, 5)
        const pushed = yield* client.push(push([["m1", "hello"]])).pipe(Effect.timeout("10 seconds"))
        assert.strictEqual(pushed.results[0]?.outcome._tag, "Applied")
        yield* nextPoke(received)
        assert.strictEqual(yield* listeners, 1)
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    60_000
  )

  it.effect(
    "every LISTEN retry waits its backoff, after an established connection is lost as after a failed attempt, and the backoff resets once LISTEN is confirmed",
    () =>
      Effect.gen(function* () {
        let failing = false
        const { pg, calls } = yield* steeredPool(() => failing)
        // Each backoff reports its delay right before its sleep starts: the clock is advanced only
        // once the sleep it should release is about to be registered, never on an attempt count
        // (which moves before the attempt's asynchronous reservation has failed).
        const delays: Array<number> = []
        yield* sharedPokes(pg, {
          heartbeatInterval: "1 hour",
          onBackoff: (d) => Effect.sync(() => void delays.push(Duration.toMillis(d)))
        })
        const backoffs = (n: number) =>
          eventually(
            Effect.sync(() => delays.length === n),
            `backoff ${n}`
          )
        const attemptsAre = (n: number) =>
          eventually(
            Effect.sync(() => calls.attempts === n),
            `attempt ${n}`
          )
        const stillAt = (n: number) =>
          Effect.andThen(
            realSleep(300),
            Effect.sync(() => assert.strictEqual(calls.attempts, n))
          )
        const listening = eventually(
          Effect.map(listeners, (n) => n === 1),
          "one LISTEN"
        )
        yield* listening
        assert.strictEqual(calls.attempts, 1)

        // An established LISTEN lost: no reconnect until the first backoff (50 ms) has passed.
        yield* killListener
        yield* backoffs(1)
        yield* stillAt(1)
        yield* TestClock.adjust("49 millis")
        yield* stillAt(1)
        yield* TestClock.adjust("1 millis")
        yield* attemptsAre(2)
        yield* listening

        // Lost again, then failed attempts: 50 ms (reset), then 100 ms, then 200 ms.
        failing = true
        yield* killListener
        yield* backoffs(2)
        yield* stillAt(2)
        yield* TestClock.adjust("50 millis")
        yield* backoffs(3)
        assert.strictEqual(calls.attempts, 3)
        yield* TestClock.adjust("99 millis")
        yield* stillAt(3)
        yield* TestClock.adjust("1 millis")
        yield* backoffs(4)
        assert.strictEqual(calls.attempts, 4)
        failing = false
        yield* TestClock.adjust("199 millis")
        yield* stillAt(4)
        yield* TestClock.adjust("1 millis")
        yield* attemptsAre(5)
        yield* listening
        assert.deepStrictEqual(delays, [50, 50, 100, 200])
      }).pipe(Effect.provide(TestDatabase)),
    60_000
  )
})

/** The tag of the exit's sole typed failure, or a description of why there is none. */
const failureTag = (exit: Exit.Exit<unknown, { readonly _tag: string }>) => {
  if (Exit.isSuccess(exit)) return "success"
  if (exit.cause.reasons.some((r) => !Cause.isFailReason(r))) return `not a typed failure: ${String(exit.cause)}`
  return exit.cause.reasons.map((r) => (Cause.isFailReason(r) ? r.error._tag : "")).join(",")
}
