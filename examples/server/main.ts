/**
 * Vertical-slice sync server (dev only): the landed Postgres server (`echo-sync/server`) serving the
 * harness app (examples/browser-spike/app.ts) at `/sync`, HTTP POST + NDJSON, on Bun. See
 * docs/vertical-slice.md.
 *
 *   ECHO_PG_URL=postgres://postgres:echo@localhost:5432/postgres bun examples/server/main.ts
 *
 * At startup it creates the database `ECHO_SLICE_DB` (default `echo_slice`) if missing, runs the engine
 * migrations, creates the app tables from their SyncTable DDL and attaches them. Not production code:
 * the principal is read from a header the client sets (no authentication), and `/admin/*` lets the
 * scenarios grant, revoke and inspect the ledger.
 */
import * as PgClient from "@effect/sql-pg/PgClient"
import { Effect, Layer, ManagedRuntime, Redacted } from "effect"
import * as HttpRouter from "effect/http/HttpRouter"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Protocol, ScopeGrants } from "../../src/core/index.ts"
import { Postgres } from "../../src/server/index.ts"
import { principalHeader, registry, serverHooks, serverMutations, Todos } from "../browser-spike/app.ts"

const port = Number(process.env["ECHO_SLICE_PORT"] ?? "5200")
const adminUrl = new URL(process.env["ECHO_PG_URL"] ?? "postgres://postgres:echo@localhost:5432/postgres")
const dbName = process.env["ECHO_SLICE_DB"] ?? "echo_slice"
const dbUrl = new URL(adminUrl)
dbUrl.pathname = `/${dbName}`

const ensureDatabase = Effect.gen(function* () {
  const sql = yield* SqlClient
  const found = yield* sql`SELECT 1 FROM pg_database WHERE datname = ${dbName}`
  if (found.length === 0) yield* sql.unsafe(`CREATE DATABASE "${dbName}"`)
}).pipe(Effect.provide(PgClient.layer({ url: Redacted.make(adminUrl.toString()), maxConnections: 1 })))

const Db = PgClient.layer({ url: Redacted.make(dbUrl.toString()), maxConnections: 40 })

/** Engine schema, then the app's tables as its own migration would create them, then capture. */
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* Postgres.migrate
  for (const table of registry.tables) {
    if (table === ScopeGrants.ScopeGrants) continue
    const [{ exists } = { exists: false }] = yield* sql<{ exists: boolean }>`
      SELECT to_regclass(${table.name}) IS NOT NULL AS exists`
    if (!exists) {
      yield* sql.unsafe(table.postgres.createTable)
      for (const ddl of table.postgres.createIndexes) yield* sql.unsafe(ddl)
    }
    yield* Postgres.attach(table)
  }
})

const sync = Postgres.layerHttp({
  registry,
  mutations: serverMutations,
  principalFor: (headers) => {
    const principal = headers[principalHeader]
    return principal === undefined || principal === ""
      ? Effect.fail(new Protocol.Unauthorized({ message: `missing ${principalHeader}` }))
      : Effect.succeed(AccessScope.PrincipalId.make(principal))
  }
}).pipe(Layer.provide(Db))

// ── /admin: what the scenarios need to drive and observe the server ─────────────────────────────────
const admin = ManagedRuntime.make(Db)

/** Per client id, how many requests of each RPC tag reached this process (read from the NDJSON body). */
const rpcCounts = new Map<string, Record<string, number>>()
const countRpcs = (body: string) => {
  for (const line of body.split("\n")) {
    if (line.trim() === "") continue
    const message: unknown = JSON.parse(line)
    if (typeof message !== "object" || message === null) continue
    const tag = "tag" in message && typeof message.tag === "string" ? message.tag : undefined
    const payload = "payload" in message ? message.payload : undefined
    const clientId =
      typeof payload === "object" && payload !== null && "clientId" in payload && typeof payload.clientId === "string"
        ? payload.clientId
        : undefined
    if (tag === undefined || clientId === undefined) continue
    const counts = rpcCounts.get(clientId) ?? {}
    counts[tag] = (counts[tag] ?? 0) + 1
    rpcCounts.set(clientId, counts)
  }
}

const json = (value: unknown) =>
  new Response(
    JSON.stringify(value, (_, v: unknown) => (typeof v === "bigint" ? String(v) : v)),
    {
      headers: { "content-type": "application/json" }
    }
  )

const adminRoute = (url: URL): Effect.Effect<Response, unknown, SqlClient> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    const q = (name: string) => url.searchParams.get(name) ?? ""
    switch (url.pathname) {
      case "/admin/grant": {
        const scope = AccessScope.AccessScopeId.make(q("scope"))
        const principal = AccessScope.PrincipalId.make(q("principal"))
        yield* sql`INSERT INTO echo_sync.access_scopes (id) VALUES (${scope}) ON CONFLICT DO NOTHING`
        yield* sql`INSERT INTO echo_sync.scope_grants (id, access_scope_id, principal_id, role, granted_at)
          VALUES (${ScopeGrants.grantId(scope, principal)}, ${scope}, ${principal}, ${q("role") || "writer"}, now())
          ON CONFLICT DO NOTHING`
        return json({ ok: true })
      }
      case "/admin/revoke":
        yield* sql`DELETE FROM echo_sync.scope_grants WHERE access_scope_id = ${q("scope")} AND principal_id = ${q("principal")}`
        return json({ ok: true })
      case "/admin/ledger":
        // Every ledger row for a mutation id, across clients: exactly-once means exactly one row.
        return json(
          yield* sql`SELECT client_id, mutation_id, seq, position, outcome FROM echo_sync.mutations
            WHERE mutation_id = ${q("mutationId")} ORDER BY position`
        )
      case "/admin/todos":
        return json(
          yield* sql.unsafe(
            `SELECT id, access_scope_id, title FROM ${Todos.name} WHERE access_scope_id = $1 ORDER BY id`,
            [q("scope")]
          )
        )
      case "/admin/stats":
        return json({
          rpcs: rpcCounts.get(q("clientId")) ?? {},
          started: serverHooks.started.get(q("mutationId")) ?? 0
        })
      default:
        return new Response("not found", { status: 404 })
    }
  })

await Effect.runPromise(ensureDatabase)
await admin.runPromise(setup)
const web = HttpRouter.toWebHandler(sync, { disableLogger: true })

const server = Bun.serve({
  port,
  hostname: "127.0.0.1",
  // The pokes stream is a long-lived response that may stay quiet for minutes.
  idleTimeout: 0,
  fetch: async (request) => {
    const url = new URL(request.url)
    if (url.pathname.startsWith("/admin/")) {
      return admin.runPromise(adminRoute(url)).catch((error: unknown) => new Response(String(error), { status: 500 }))
    }
    if (url.pathname === "/sync" || url.pathname === "/sync/") countRpcs(await request.clone().text())
    return web.handler(request)
  }
})
console.log(`echo-sync slice server on http://127.0.0.1:${server.port}/sync (database ${dbName})`)

const shutdown = () => {
  void server.stop(true)
  void web.dispose().finally(() => admin.dispose().finally(() => process.exit(0)))
}
process.on("SIGTERM", shutdown)
process.on("SIGINT", shutdown)
