import { assert, describe, it } from "@effect/vitest"
import * as PgClient from "@effect/sql-pg/PgClient"
import { Effect, Exit, Layer, Option, Redacted, Schema } from "effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as HttpRouter from "effect/http/HttpRouter"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Protocol, Registry, SyncTable } from "echo-sync"
import {
  EmptyDatabase,
  grant,
  Items,
  Notes,
  pgAvailable,
  registry,
  RowId,
  TestDatabase
} from "./postgres/test-support.ts"
import * as Startup from "./startup.ts"
import { recorder } from "../../test/telemetry-recorder.ts"

const options: Startup.StartupOptions = {
  registry,
  mutations: Mutation.makeRegistry([]),
  principalFor: () => Effect.succeed(AccessScope.PrincipalId.make("p"))
}

/** The sync server as an app mounts it, over the test database. */
const serverFor = (startupOptions: Startup.StartupOptions) =>
  Effect.map(Effect.context<SqlClient | PgClient.PgClient>(), (db) =>
    Startup.layer(startupOptions).pipe(Layer.provide(Layer.succeedContext(db)))
  )

const server = serverFor(options)

/** One pull through a real HTTP/NDJSON RpcClient against a freshly started server. */
const pullOnce = (startupOptions: Startup.StartupOptions = options) =>
  Effect.gen(function* () {
    const web = HttpRouter.toWebHandler(yield* serverFor(startupOptions), { disableLogger: true })
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

describe.skipIf(!pgAvailable)("server startup (SPEC §13)", () => {
  it.live(
    "startup's internal steps (verify, its per-table checks) must never hand the host a span other than echo.server.startup",
    () =>
      Effect.gen(function* () {
        const host = recorder()
        yield* Startup.prepare({ registry }).pipe(Effect.provideContext(host.context))
        assert.deepStrictEqual(
          host.spans.map((span) => span.name),
          ["echo.server.startup"]
        )
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live("body-supplied cookie and origin headers cannot change a push's HTTP principal or origin policy", () =>
    Effect.gen(function* () {
      const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
      const web = HttpRouter.toWebHandler(
        Startup.layer({
          ...options,
          mutations: Mutation.makeRegistry([
            Mutation.make("noop", { args: Schema.Struct({}), handler: () => Effect.void })
          ]),
          principalFor: (headers) =>
            headers["origin"] === "https://app.test" && headers["cookie"] === "session=http-secret"
              ? Effect.succeed(AccessScope.PrincipalId.make("http-principal"))
              : Effect.fail(
                  new Protocol.Unauthorized({
                    reason: "authenticationRefused",
                    message: "HTTP credential or origin refused"
                  })
                )
        }).pipe(Layer.provide(Layer.succeedContext(db))),
        { disableLogger: true }
      )
      yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
      const push = (cookie: string, origin: string, bodyCookie: string, bodyOrigin: string) =>
        Effect.promise(async () => {
          const response = await web.handler(
            new Request("http://echo.test/sync", {
              method: "POST",
              headers: { cookie, origin, "content-type": "text/plain" },
              body: `${JSON.stringify({
                _tag: "Request",
                id: "1",
                tag: "push",
                payload: {
                  clientId: "http-headers",
                  mutations: [{ mutationId: "http-mutation", seq: 1, name: "noop", args: {} }]
                },
                headers: [
                  ["cookie", bodyCookie],
                  ["origin", bodyOrigin]
                ]
              })}\n`
            })
          )
          return response.text()
        })
      // Real HTTP credentials/policy admit the push despite the conflicting body headers.
      const accepted = yield* push("session=http-secret", "https://app.test", "session=body", "https://evil.test")
      assert.include(accepted, '"_tag":"Success"')
      assert.deepStrictEqual(
        yield* (yield* SqlClient)`SELECT principal_id FROM echo_sync.clients WHERE client_id = 'http-headers'`,
        [{ principal_id: "http-principal" }]
      )
      // Neither a forged origin nor a forged credential can rescue a refused HTTP request.
      for (const { cookie, origin } of [
        { cookie: "session=http-secret", origin: "https://evil.test" },
        { cookie: "session=wrong", origin: "https://app.test" }
      ]) {
        const refused = yield* push(cookie, origin, "session=http-secret", "https://app.test")
        assert.include(refused, '"_tag":"Unauthorized"')
      }
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live(
    "an old server must serve push and pull on an expanded database, while a new server must refuse the unmigrated schema",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        const old = {
          ...options,
          mutations: Mutation.makeRegistry([
            Mutation.make("noop", { args: Schema.Struct({}), handler: () => Effect.void })
          ])
        }
        const expandedItems = SyncTable.make("items", {
          id: RowId,
          accessScopeId: AccessScope.AccessScopeId,
          body: Schema.String,
          extraNote: Schema.NullOr(Schema.String)
        })
        const next = { ...options, registry: Registry.make([expandedItems, Notes]) }

        yield* sql`ALTER TABLE items ADD COLUMN extra_note text COLLATE "C"`
        const web = HttpRouter.toWebHandler(yield* serverFor(old), { disableLogger: true })
        yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
        const pushed = yield* Effect.promise(async () => {
          const response = await web.handler(
            new Request("http://echo.test/sync", {
              method: "POST",
              headers: { "content-type": "text/plain" },
              body: `${JSON.stringify({
                _tag: "Request",
                id: "1",
                tag: "push",
                payload: {
                  clientId: "old-server-client",
                  mutations: [{ mutationId: "old-server-push", seq: 1, name: "noop", args: {} }]
                },
                headers: []
              })}\n`
            })
          )
          return response.text()
        })
        assert.include(pushed, '"_tag":"Success"')
        const pulled = yield* pullOnce(old)
        if (!Exit.isSuccess(pulled))
          return assert.fail(`old server did not serve after the database expanded: ${String(pulled.cause)}`)
        assert.strictEqual(pulled.value._tag, "PullDone")

        yield* sql`ALTER TABLE items DROP COLUMN extra_note`
        const started = yield* Effect.exit(Startup.prepare(next))
        if (Exit.isSuccess(started)) return assert.fail("new server started before its added column was migrated")
        const [reason] = started.cause.reasons
        const drift = reason !== undefined && reason._tag === "Fail" ? reason.error : undefined
        if (drift === undefined || !("_tag" in drift) || drift._tag !== "Drift") {
          return assert.fail(`expected Drift, got ${String(started.cause)}`)
        }
        assert.isTrue(drift.mismatches.some((m) => m.table === "items" && m.problem.includes("extra_note")))
      }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

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
      assert.isTrue(Exit.isFailure(yield* pullOnce()), "a drifted server answered a pull")

      yield* sql`ALTER TABLE items ADD COLUMN body text COLLATE "C" NOT NULL DEFAULT ''`
      yield* sql`ALTER TABLE notes ADD COLUMN body text COLLATE "C" NOT NULL DEFAULT ''`
      const served = yield* pullOnce()
      if (!Exit.isSuccess(served)) return assert.fail(`restored server did not serve: ${String(served.cause)}`)
      assert.strictEqual(served.value._tag, "PullDone")
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
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

  it.live("concurrent first starts on an empty database all succeed and run the app migration exactly once", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      const appMigrations = {
        "001_init": Effect.gen(function* () {
          const db = yield* SqlClient
          for (const table of [Items, Notes]) {
            yield* db.unsafe(table.postgres.createTable)
            for (const ddl of table.postgres.createIndexes) yield* db.unsafe(ddl)
          }
          yield* db`CREATE TABLE app_migration_runs (n int)`
          yield* db`INSERT INTO app_migration_runs VALUES (1)`
        })
      }
      const results = yield* Effect.all(
        Array.from({ length: 20 }, () => Effect.exit(Startup.prepare({ registry, appMigrations }))),
        { concurrency: "unbounded" }
      )
      const failed = results.filter(Exit.isFailure).map((r) => String(r.cause))
      assert.deepStrictEqual(failed, [])
      assert.deepStrictEqual(yield* sql`SELECT n FROM app_migration_runs`, [{ n: 1 }])
    }).pipe(Effect.provide(EmptyDatabase))
  )

  it.live("a table captured in two schemas fails startup with one Drift naming both, with every other mismatch", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      // public.items is attached; zz.items is captured behind attach's back (a hand-written trigger).
      yield* sql`CREATE SCHEMA zz`
      yield* sql`CREATE TABLE zz.items (LIKE public.items INCLUDING ALL)`
      yield* sql`CREATE TRIGGER echo_sync_capture AFTER INSERT OR UPDATE OR DELETE ON zz.items
                 FOR EACH ROW EXECUTE FUNCTION echo_sync.echo_sync_capture_changes()`
      yield* sql`CREATE TRIGGER echo_sync_reject_truncate BEFORE TRUNCATE ON zz.items
                 FOR EACH STATEMENT EXECUTE FUNCTION echo_sync.reject_truncate()`
      // An unrelated mismatch that must arrive in the same Drift.
      yield* sql`ALTER TABLE notes DROP COLUMN body`
      const exit = yield* Effect.exit(Startup.prepare(options))
      if (Exit.isSuccess(exit)) return assert.fail("startup accepted a table captured in two schemas")
      const [reason] = exit.cause.reasons
      const error = reason !== undefined && reason._tag === "Fail" ? reason.error : undefined
      if (error === undefined || !("_tag" in error) || error._tag !== "Drift") {
        return assert.fail(`expected Drift, got ${String(exit.cause)}`)
      }
      assert.isTrue(
        error.mismatches.some((m) => m.table === "items" && m.problem.includes("captured in 2 schemas (public, zz)")),
        JSON.stringify(error.mismatches)
      )
      assert.isTrue(
        error.mismatches.some((m) => m.table === "notes" && m.problem.includes("body")),
        JSON.stringify(error.mismatches)
      )
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a search_path that resolves the table name to an uncaptured relation fails startup with a Drift", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      // public.items is attached; zz.items is an uncaptured twin that search_path now prefers.
      yield* sql`CREATE SCHEMA zz`
      yield* sql`CREATE TABLE zz.items (LIKE public.items INCLUDING ALL)`
      const [row] = yield* sql<{ db: string }>`SELECT current_database() AS db`
      if (row === undefined) return assert.fail("no database name")
      yield* sql`ALTER DATABASE ${sql(row.db)} SET search_path = zz, public`
      // Pooled connections keep their session settings, so start a client the setting applies to.
      const url = new URL(process.env["ECHO_PG_URL"] ?? "")
      url.pathname = `/${row.db}`
      const exit = yield* Effect.exit(Startup.prepare(options)).pipe(
        Effect.provide(PgClient.layer({ url: Redacted.make(url.toString()), maxConnections: 4 }))
      )
      if (Exit.isSuccess(exit)) return assert.fail("startup served while app SQL resolves to an uncaptured relation")
      const [reason] = exit.cause.reasons
      const error = reason !== undefined && reason._tag === "Fail" ? reason.error : undefined
      if (error === undefined || !("_tag" in error) || error._tag !== "Drift") {
        return assert.fail(`expected Drift, got ${String(exit.cause)}`)
      }
      assert.isTrue(
        error.mismatches.some(
          (m) =>
            m.table === "items" &&
            m.problem.includes("zz.items") &&
            m.problem.includes("captured relation is in public")
        ),
        JSON.stringify(error.mismatches)
      )
    }).pipe(Effect.provide(TestDatabase))
  )
})
