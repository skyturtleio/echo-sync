import { assert, describe, it } from "@effect/vitest"
import type * as PgClient from "@effect/sql-pg/PgClient"
import { Effect, Exit, Layer, Option } from "effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as HttpRouter from "effect/http/HttpRouter"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Protocol, Registry, SyncTable } from "echo-sync"
import { grant, Items, Notes, pgAvailable, registry, RowId, TestDatabase } from "./postgres/test-support.ts"
import * as Startup from "./startup.ts"

const options: Startup.StartupOptions = {
  registry,
  mutations: Mutation.makeRegistry([]),
  principalFor: () => Effect.succeed(AccessScope.PrincipalId.make("p"))
}

/** The sync server as an app mounts it, over the test database. */
const server = Effect.map(Effect.context<SqlClient | PgClient.PgClient>(), (db) =>
  Startup.layer(options).pipe(Layer.provide(Layer.succeedContext(db)))
)

/** One pull through a real HTTP/NDJSON RpcClient against a freshly started server. */
const pullOnce = Effect.gen(function* () {
  const web = HttpRouter.toWebHandler(yield* server, { disableLogger: true })
  yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
  const fetch: typeof globalThis.fetch = Object.assign(
    (input: RequestInfo | URL, init?: RequestInit) => web.handler(new Request(input, init)),
    { preconnect: globalThis.fetch.preconnect }
  )
  const client = yield* RpcClient.make(Protocol.SyncRpcs).pipe(
    Effect.provide(
      RpcClient.layerProtocolHttp({ url: "http://echo.test/sync" }).pipe(
        Layer.provide([
          RpcSerialization.layerNdjson,
          FetchHttpClient.layer.pipe(Layer.provide(Layer.succeed(FetchHttpClient.Fetch)(fetch)))
        ])
      )
    )
  )
  return yield* Effect.exit(
    client.pull({ clientId: Ids.ClientId.make("d"), cursor: null, continuation: Option.none() })
  )
}).pipe(Effect.scoped)

describe.skipIf(!pgAvailable)("server startup (§11)", () => {
  it.live("a server whose database drifted from the registry never serves; restored, it serves", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* grant("p", "s1")
      yield* sql`INSERT INTO items (id, access_scope_id, body) VALUES ('a', 's1', 'x')`
      // Two declared columns gone, in two tables: the Drift must list both, not the first only.
      yield* sql`ALTER TABLE items DROP COLUMN body`
      yield* sql`ALTER TABLE notes DROP COLUMN body`

      const built = yield* Effect.exit(
        Effect.scoped(Layer.build((yield* server).pipe(Layer.provide(HttpRouter.layer))))
      )
      if (Exit.isSuccess(built)) return assert.fail("the server started against a drifted database")
      const [reason] = built.cause.reasons
      const drift = reason !== undefined && reason._tag === "Fail" ? reason.error : undefined
      if (drift === undefined || !("_tag" in drift) || drift._tag !== "Drift") {
        return assert.fail(`expected Drift, got ${String(built.cause)}`)
      }
      const tables = drift.mismatches.filter((m) => m.problem.includes("body")).map((m) => m.table)
      assert.deepStrictEqual(tables.toSorted(), ["items", "notes"])
      // Through HTTP: no pull is ever answered (the RPC layer is never built).
      assert.isTrue(Exit.isFailure(yield* pullOnce), "a drifted server answered a pull")

      yield* sql`ALTER TABLE items ADD COLUMN body text COLLATE "C" NOT NULL DEFAULT ''`
      yield* sql`ALTER TABLE notes ADD COLUMN body text COLLATE "C" NOT NULL DEFAULT ''`
      const served = yield* pullOnce
      if (!Exit.isSuccess(served)) return assert.fail(`restored server did not serve: ${String(served.cause)}`)
      assert.strictEqual(served.value._tag, "PullDone")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "a registry table the app migrations never created fails startup as a Drift naming it, not a bare SqlError",
    () =>
      Effect.gen(function* () {
        const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
        const Missing = SyncTable.make("missing_table", { id: RowId, accessScopeId: AccessScope.AccessScopeId })
        const layer = Startup.layer({ ...options, registry: Registry.make([Items, Notes, Missing]) }).pipe(
          Layer.provide(Layer.succeedContext(db))
        )
        const built = yield* Effect.exit(Effect.scoped(Layer.build(layer.pipe(Layer.provide(HttpRouter.layer)))))
        if (Exit.isSuccess(built)) return assert.fail("the server started without one of its tables")
        const [reason] = built.cause.reasons
        const error = reason !== undefined && reason._tag === "Fail" ? reason.error : undefined
        if (error === undefined || !("_tag" in error) || error._tag !== "Drift") {
          return assert.fail(`expected Drift, got ${String(built.cause)}`)
        }
        assert.deepStrictEqual(
          error.mismatches.filter((m) => m.table === "missing_table").map((m) => m.problem),
          ["not attached (no echo_sync capture trigger); run attach at startup", "table does not exist"]
        )
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live("prepare run concurrently by several instances succeeds for all of them", () =>
    Effect.gen(function* () {
      const results = yield* Effect.all(
        Array.from({ length: 4 }, () => Effect.exit(Startup.prepare(options))),
        { concurrency: "unbounded" }
      )
      assert.deepStrictEqual(
        results.map((r) => r._tag),
        ["Success", "Success", "Success", "Success"]
      )
    }).pipe(Effect.provide(TestDatabase))
  )
})
