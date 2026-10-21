// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../src/client/testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { assert, describe, it } from "@effect/vitest"
import type * as PgClient from "@effect/sql-pg/PgClient"
import { Context, Effect, Layer, Option, Schema } from "effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as HttpRouter from "effect/http/HttpRouter"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation } from "echo-sync"
import * as Engine from "../src/client/sync/Engine.ts"
import * as SqliteSyncStorage from "../src/client/sync/SqliteSyncStorage.ts"
import * as Transport from "../src/client/sync/Transport.ts"
import { layerHttp } from "../src/server/postgres/rpc.ts"
import { grant, Items, pgAvailable, registry, RowId, TestDatabase } from "../src/server/postgres/test-support.ts"

// The §16.5 expiry floor end to end: the real Postgres server (RPC adapter over HTTP + NDJSON), the
// real Transport, and the real Engine over SQLite. Runs only with ECHO_PG_URL (bun run test:pg).

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
const clientId = Ids.ClientId.make("expiry-client")
const mid = Ids.MutationId.make

describe.skipIf(!pgAvailable)("expiry floor (DESIGN §16.5) against the real server", () => {
  it.live(
    "a pending, never-acked mutation at or below the server's raised floor becomes OutcomeUnknown on the next pull: released, not re-pushed, no ledger row",
    () =>
      Effect.gen(function* () {
        const pg = yield* SqlClient
        yield* grant("p", "s1")
        yield* pg`UPDATE echo_sync.scope_grants SET role = 'writer' WHERE principal_id = 'p'`

        // Server: the landed RPC adapter, reached through an in-process fetch that counts push requests.
        const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
        const web = HttpRouter.toWebHandler(
          layerHttp({
            registry,
            mutations,
            principalFor: () => Effect.succeed(AccessScope.PrincipalId.make("p"))
          }).pipe(Layer.provide(Layer.succeedContext(db))),
          { disableLogger: true }
        )
        yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
        const sent = { push: 0 }
        const fetch: typeof globalThis.fetch = Object.assign(
          async (input: RequestInfo | URL, init?: RequestInit) => {
            const request = new Request(input, init)
            if ((await request.clone().text()).includes('"tag":"push"')) sent.push++
            return web.handler(request)
          },
          { preconnect: globalThis.fetch.preconnect }
        )

        // Client: SQLite replica, the leader's Transport and Engine.
        const sqlite = Context.get(yield* Layer.build(SqliteClient.layerMemory({})), SqlClient)
        yield* SqliteSyncStorage.migrate(registry).pipe(Effect.provideService(SqlClient, sqlite))
        const storage = SqliteSyncStorage.make(sqlite, registry)
        const transport = yield* Transport.make.pipe(
          Effect.provide(Transport.layerProtocolHttp("http://echo.test/sync"))
        )
        const engine = yield* Engine.make({
          clientId,
          actor: AccessScope.PrincipalId.make("p"),
          storage,
          registry,
          mutations,
          transport
        })
        const items = Effect.orDie(sqlite<{ id: string }>`SELECT id FROM items ORDER BY id`).pipe(
          Effect.map((rows) => rows.map((r) => r.id))
        )
        const ledger = (mutationId: string) =>
          pg<{
            seq: bigint
          }>`SELECT seq FROM echo_sync.mutations WHERE client_id = ${clientId} AND mutation_id = ${mutationId}`

        // `FetchHttpClient` reads `Fetch` from the fiber that sends each request.
        yield* Effect.gen(function* () {
          yield* engine.pull
          // m1 (seq 1) is pushed and acked normally; m2 (seq 2) stays pending and is never pushed.
          yield* engine.mutate(mid("m1"), "items.add", { id: "i1", body: "pushed" })
          assert.strictEqual((yield* engine.push).processed, 1)
          yield* engine.pull
          const m2 = yield* engine.mutate(mid("m2"), "items.add", { id: "i2", body: "expired" })
          assert.strictEqual(m2.seq, 2)
          assert.deepStrictEqual(yield* items, ["i1", "i2"]) // i2 is optimistic

          // The operator raises the floor past m2, as expiry would once m2's ledger row is gone.
          yield* pg`UPDATE echo_sync.clients SET expired_through = 2,
            expired_position = (SELECT position FROM echo_sync.mutations WHERE client_id = ${clientId} AND mutation_id = 'm1')
            WHERE client_id = ${clientId}`
          yield* engine.pull

          // The pull alone settled m2: OutcomeUnknown, released, its optimistic effect gone with the rebase.
          assert.deepStrictEqual(yield* storage.transaction(storage.pending), [])
          const notices = yield* SqliteSyncStorage.notices(sqlite)
          assert.deepStrictEqual(
            notices.map(({ notice }) => [notice._tag, notice.envelope.mutationId, notice.envelope.seq]),
            [["OutcomeUnknown", "m2", 2]]
          )
          const outbox = yield* Effect.orDie(
            sqlite<{ released_at: number | null }>`SELECT released_at FROM echo_outbox WHERE mutation_id = 'm2'`
          )
          assert.isTrue(outbox.length === 1 && outbox[0]?.released_at != null, "m2's outbox row is released")
          assert.deepStrictEqual(yield* items, ["i1"])

          // Nothing is left to push, and the server never saw m2.
          const before = sent.push
          assert.strictEqual((yield* engine.push).processed, 0)
          assert.strictEqual(sent.push, before)
          assert.deepStrictEqual(yield* ledger("m2"), [])
          assert.strictEqual((yield* ledger("m1")).length, 1)
          assert.isTrue(Option.isSome(yield* storage.transaction(storage.lookup(mid("m2")))))
        }).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    60_000
  )
})
