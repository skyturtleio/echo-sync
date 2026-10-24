// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../src/client/testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { assert, describe, it } from "@effect/vitest"
import type * as PgClient from "@effect/sql-pg/PgClient"
import { Context, Effect, Layer, Schema } from "effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as HttpRouter from "effect/http/HttpRouter"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation } from "echo-sync"
import * as Engine from "../src/client/sync/Engine.ts"
import * as SqliteSyncStorage from "../src/client/sync/SqliteSyncStorage.ts"
import * as Transport from "../src/client/sync/Transport.ts"
import { layerHttp } from "../src/server/postgres/rpc.ts"
import { grant, Items, pgAvailable, registry, RowId, TestDatabase } from "../src/server/postgres/test-support.ts"

// Scope creation (SPEC §4) end to end: the real Postgres server (RPC adapter over HTTP + NDJSON),
// the real Transport, and the real Engine over SQLite. The client's claim is optimistic (no grant
// for the scope in its replica); the server's is authoritative. Runs only with ECHO_PG_URL.

const createScope = Mutation.make("scopes.create", {
  args: Schema.Struct({ scope: Schema.String }),
  handler: (ctx, args) => ctx.store.createScope(AccessScope.AccessScopeId.make(args.scope))
})
const addItem = Mutation.make("items.addIn", {
  args: Schema.Struct({ id: Schema.String, scope: Schema.String }),
  handler: (ctx, args) =>
    ctx.store.insert(Items, {
      id: RowId.make(args.id),
      accessScopeId: AccessScope.AccessScopeId.make(args.scope),
      body: "optimistic"
    })
})
const mutations = Mutation.makeRegistry([createScope, addItem])
const mid = Ids.MutationId.make

/** The real server (principal b) over HTTP, and b's real Engine over an in-memory SQLite replica. */
const setup = Effect.gen(function* () {
  const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
  const web = HttpRouter.toWebHandler(
    layerHttp({
      registry,
      mutations,
      principalFor: () => Effect.succeed(AccessScope.PrincipalId.make("b"))
    }).pipe(Layer.provide(Layer.succeedContext(db))),
    { disableLogger: true }
  )
  yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
  const fetch: typeof globalThis.fetch = Object.assign(
    (input: RequestInfo | URL, init?: RequestInit) => web.handler(new Request(input, init)),
    { preconnect: globalThis.fetch.preconnect }
  )
  const sqlite = Context.get(yield* Layer.build(SqliteClient.layerMemory({})), SqlClient)
  yield* SqliteSyncStorage.migrate(registry).pipe(Effect.provideService(SqlClient, sqlite))
  const storage = SqliteSyncStorage.make(sqlite, registry)
  const transport = yield* Transport.make.pipe(Effect.provide(Transport.layerProtocolHttp("http://echo.test/sync")))
  const engine = yield* Engine.make({
    clientId: Ids.ClientId.make("b-device"),
    actor: AccessScope.PrincipalId.make("b"),
    storage,
    registry,
    mutations,
    transport
  })
  const replica = Effect.orDie(
    Effect.all({
      grants: sqlite<{ access_scope_id: string; principal_id: string; role: string }>`
        SELECT access_scope_id, principal_id, role FROM scope_grants ORDER BY access_scope_id`,
      items: sqlite<{ id: string }>`SELECT id FROM items ORDER BY id`
    })
  ).pipe(
    Effect.map(({ grants, items }) => ({
      grants: grants.map((g) => `${g.access_scope_id}:${g.principal_id}:${g.role}`),
      items: items.map((i) => i.id)
    }))
  )
  const notices = Effect.map(SqliteSyncStorage.notices(sqlite), (all) =>
    all.map(({ notice }): ReadonlyArray<string | null> => [
      notice.envelope.mutationId,
      notice._tag === "Rejected" ? notice.error._tag : notice._tag,
      notice._tag === "Rejected" && notice.error._tag === "ScopeForbidden" ? notice.error.required : null
    ])
  )
  const pending = Effect.map(storage.transaction(storage.pending), (entries) =>
    entries.map((e) => ({ mutationId: e.envelope.mutationId, undo: e.undo.length }))
  )
  return { pg: Context.get(db, SqlClient), engine, replica, notices, pending, fetch }
})

describe.skipIf(!pgAvailable)("scope creation (SPEC §4) against the real server", () => {
  it.live(
    "an optimistic createScope of an id the server already has is rejected ScopeForbidden, and the pull undoes the grant and the write that depended on it; a fresh id is applied and stays",
    () =>
      Effect.gen(function* () {
        // "taken" exists on the server (another principal's scope), invisible to this client.
        yield* grant("someone-else", "taken")
        const { pg, engine, replica, notices, pending, fetch } = yield* setup

        yield* Effect.gen(function* () {
          yield* engine.pull
          // Optimistic: the claim passes (no grant for "taken" here), the owner grant appears, and
          // the next write in "taken" is authorized by it.
          yield* engine.mutate(mid("create-taken"), "scopes.create", { scope: "taken" })
          yield* engine.mutate(mid("write-taken"), "items.addIn", { id: "t1", scope: "taken" })
          yield* engine.mutate(mid("create-fresh"), "scopes.create", { scope: "fresh" })
          yield* engine.mutate(mid("write-fresh"), "items.addIn", { id: "f1", scope: "fresh" })
          assert.deepStrictEqual(yield* replica, {
            grants: ["fresh:b:owner", "taken:b:owner"],
            items: ["f1", "t1"]
          })

          assert.strictEqual((yield* engine.push).processed, 4)
          yield* engine.pull

          // The server's verdicts: "taken" is ScopeForbidden{owner}, so the dependent write is
          // ScopeForbidden too; "fresh" and its write are applied.
          assert.deepStrictEqual(yield* notices, [
            ["create-taken", "ScopeForbidden", "owner"],
            ["write-taken", "ScopeForbidden", "writer"]
          ])
          assert.deepStrictEqual(yield* pending, [])
          // The pull undid the optimistic grant and the write that depended on it.
          assert.deepStrictEqual(yield* replica, { grants: ["fresh:b:owner"], items: ["f1"] })
        }).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))

        // The server agrees: b owns "fresh" only; "taken" still has only its original grant.
        assert.deepStrictEqual(
          yield* pg<{ access_scope_id: string; principal_id: string; role: string }>`
            SELECT access_scope_id, principal_id, role FROM echo_sync.scope_grants ORDER BY access_scope_id, principal_id`,
          [
            { access_scope_id: "fresh", principal_id: "b", role: "owner" },
            { access_scope_id: "taken", principal_id: "someone-else", role: "reader" }
          ]
        )
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    60_000
  )

  it.live(
    "when only the creation is processed and rejected, a dependent mutation stays pending with no optimistic effect and gets no notice until the server's own verdict (INV-CLI-3)",
    () =>
      Effect.gen(function* () {
        yield* grant("someone-else", "taken")
        const { engine, replica, notices, pending, fetch } = yield* setup

        yield* Effect.gen(function* () {
          yield* engine.pull
          // Only the creation reaches the server; it is rejected there.
          yield* engine.mutate(mid("create-taken"), "scopes.create", { scope: "taken" })
          assert.strictEqual((yield* engine.push).processed, 1)
          // Before the verdict is pulled, the optimistic grant still authorizes a dependent write.
          yield* engine.mutate(mid("write-taken"), "items.addIn", { id: "t1", scope: "taken" })
          assert.deepStrictEqual(yield* replica, { grants: ["taken:b:owner"], items: ["t1"] })

          // The pull settles the creation only. The rebase replays the dependent: it no longer
          // applies locally, so its optimistic row is gone (empty undo), but it stays queued and
          // gets no notice, because only the server's verdict settles it.
          yield* engine.pull
          assert.deepStrictEqual(yield* notices, [["create-taken", "ScopeForbidden", "owner"]])
          assert.deepStrictEqual(yield* pending, [{ mutationId: "write-taken", undo: 0 }])
          assert.deepStrictEqual(yield* replica, { grants: [], items: [] })

          // Pushed, the server answers it ScopeForbidden{writer}; that verdict is the notice.
          assert.strictEqual((yield* engine.push).processed, 1)
          yield* engine.pull
          assert.deepStrictEqual(yield* notices, [
            ["create-taken", "ScopeForbidden", "owner"],
            ["write-taken", "ScopeForbidden", "writer"]
          ])
          assert.deepStrictEqual(yield* pending, [])
          assert.deepStrictEqual(yield* replica, { grants: [], items: [] })
        }).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    60_000
  )
})
