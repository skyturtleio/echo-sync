import { assert, describe, it } from "@effect/vitest"
import type * as PgClient from "@effect/sql-pg/PgClient"
import { Array as Arr, Effect, Exit, Layer, Option, Schema } from "effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as HttpRouter from "effect/http/HttpRouter"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { SqlClient } from "effect/sql/SqlClient"
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
const connect = Effect.gen(function* () {
  const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
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
})
