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
import { expireLedger } from "../src/server/postgres/gc.ts"
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

/** The landed RPC adapter over the test database, reached through an in-process fetch that counts push requests. */
const serve = Effect.gen(function* () {
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
  return { sent, fetch }
})

/** A client device: SQLite replica (optionally restored from `backup`), the leader's Transport and Engine. */
const device = Effect.gen(function* () {
  const sqlite = Context.get(yield* Layer.build(SqliteClient.layerMemory({})), SqlClient)
  yield* SqliteSyncStorage.migrate(registry).pipe(Effect.provideService(SqlClient, sqlite))
  const storage = SqliteSyncStorage.make(sqlite, registry)
  const transport = yield* Transport.make.pipe(Effect.provide(Transport.layerProtocolHttp("http://echo.test/sync")))
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
  return { sqlite, storage, engine, items }
})

/** Every row of every table of `from`, as a restorable backup. */
const backup = (from: SqlClient) =>
  Effect.orDie(
    Effect.gen(function* () {
      const tables = yield* from<{ name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
      const copy: Array<{ table: string; rows: ReadonlyArray<Record<string, unknown>> }> = []
      for (const { name } of tables)
        copy.push({ table: name, rows: yield* from.unsafe<Record<string, unknown>>(`SELECT * FROM "${name}"`) })
      return copy
    })
  )

/** Replaces every table's rows of `into` (same schema) with the backup's. */
const restore = (into: SqlClient, copy: Effect.Success<ReturnType<typeof backup>>) =>
  Effect.orDie(
    into.withTransaction(
      Effect.gen(function* () {
        for (const { table, rows } of copy) {
          yield* into.unsafe(`DELETE FROM "${table}"`)
          for (const row of rows) yield* into`INSERT INTO ${into(table)} ${into.insert(row)}`
        }
      })
    )
  )

describe.skipIf(!pgAvailable)("expiry floor (DESIGN §16.5) against the real server", () => {
  it.live(
    "a pending, never-acked mutation at or below the server's raised floor becomes OutcomeUnknown on the next pull (released, not re-pushed, no ledger row); one above the floor stays pending and pushes",
    () =>
      Effect.gen(function* () {
        const pg = yield* SqlClient
        yield* grant("p", "s1")
        yield* pg`UPDATE echo_sync.scope_grants SET role = 'writer' WHERE principal_id = 'p'`

        const { sent, fetch } = yield* serve
        const { sqlite, storage, engine, items } = yield* device
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
          // m3 (seq 3) is above the floor: it must survive the expiry and push afterwards.
          yield* engine.mutate(mid("m3"), "items.add", { id: "i3", body: "above the floor" })
          assert.deepStrictEqual(yield* items, ["i1", "i2", "i3"]) // i2 and i3 are optimistic

          // Synthetic fixture standing in for expiry: the floor is raised past m2 by hand. A compliant
          // retention floor (DESIGN §16.5: never above the client's last presented cursor, only after the
          // released-row window) could not cover a never-pushed m2, so this proves the protocol and the
          // client's settlement of an expired seq, not a legal retention lifecycle.
          yield* pg`UPDATE echo_sync.clients SET expired_through = 2,
            expired_position = (SELECT position FROM echo_sync.mutations WHERE client_id = ${clientId} AND mutation_id = 'm1')
            WHERE client_id = ${clientId}`
          yield* engine.pull

          // The pull alone settled m2: OutcomeUnknown, released, its optimistic effect gone with the rebase.
          // m3, above the floor, is untouched: still pending, still applied optimistically.
          assert.deepStrictEqual(
            (yield* storage.transaction(storage.pending)).map((e) => e.envelope.mutationId),
            ["m3"]
          )
          const notices = yield* SqliteSyncStorage.notices(sqlite)
          assert.deepStrictEqual(
            notices.map(({ notice }) => [notice._tag, notice.envelope.mutationId, notice.envelope.seq]),
            [["OutcomeUnknown", "m2", 2]]
          )
          const outbox = yield* Effect.orDie(
            sqlite<{ released_at: number | null }>`SELECT released_at FROM echo_outbox WHERE mutation_id = 'm2'`
          )
          assert.isTrue(outbox.length === 1 && outbox[0]?.released_at != null, "m2's outbox row is released")
          assert.deepStrictEqual(yield* items, ["i1", "i3"])

          // The next push sends m3 alone (the server never sees m2), and the server applies it.
          const before = sent.push
          assert.strictEqual((yield* engine.push).processed, 1)
          assert.strictEqual(sent.push, before + 1)
          yield* engine.pull
          assert.deepStrictEqual(yield* storage.transaction(storage.pending), [])
          assert.deepStrictEqual(yield* items, ["i1", "i3"])
          assert.deepStrictEqual(yield* ledger("m2"), [])
          assert.deepStrictEqual((yield* ledger("m1")).length, 1)
          assert.deepStrictEqual(
            (yield* ledger("m3")).map((r) => Number(r.seq)),
            [3]
          )
          assert.strictEqual((yield* SqliteSyncStorage.notices(sqlite)).length, 1)
          assert.isTrue(Option.isSome(yield* storage.transaction(storage.lookup(mid("m2")))))
        }).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    60_000
  )

  it.live(
    "a device that lost the ack (restored from a backup taken while the mutation was pending) and pulls after its ledger row expired gets OutcomeUnknown and releases it, never executing it again",
    () =>
      Effect.gen(function* () {
        const pg = yield* SqlClient
        yield* grant("p", "s1")
        yield* pg`UPDATE echo_sync.scope_grants SET role = 'writer' WHERE principal_id = 'p'`
        const { sent, fetch } = yield* serve
        const original = yield* device
        const executions = pg<{ n: number }>`SELECT count(*)::int AS n FROM items WHERE id = 'i1'`

        yield* Effect.gen(function* () {
          yield* original.engine.pull
          yield* original.engine.mutate(mid("m1"), "items.add", { id: "i1", body: "once" })
          // The backup holds m1 pending, before its push.
          const copy = yield* backup(original.sqlite)
          assert.strictEqual((yield* original.engine.push).processed, 1)
          yield* original.engine.pull // installs m1's ack
          yield* original.engine.pull // presents the cursor past it: the server stamps acked_at
          assert.deepStrictEqual(yield* original.storage.transaction(original.storage.pending), [])

          // Ten days later the ledger row has been acked longer than the default 7-day retention.
          yield* pg`UPDATE echo_sync.mutations SET acked_at = acked_at - interval '10 days'`
          assert.deepStrictEqual(yield* expireLedger(), { clients: 1, deleted: 1 })

          // The device's storage is restored from the backup: m1 pending, the old cursor, no ack.
          const restored = yield* device
          yield* restore(restored.sqlite, copy)
          assert.deepStrictEqual(
            (yield* restored.storage.transaction(restored.storage.pending)).map((e) => e.envelope.mutationId),
            ["m1"]
          )
          yield* restored.engine.pull
          const notices = yield* SqliteSyncStorage.notices(restored.sqlite)
          assert.deepStrictEqual(
            notices.map(({ notice }) => [notice._tag, notice.envelope.mutationId]),
            [["OutcomeUnknown", "m1"]]
          )
          assert.deepStrictEqual(yield* restored.storage.transaction(restored.storage.pending), [])
          // Its effect is in the installed replica (the server applied it once); nothing is re-pushed.
          assert.deepStrictEqual(yield* restored.items, ["i1"])
          const before = sent.push
          assert.strictEqual((yield* restored.engine.push).processed, 0)
          assert.strictEqual(sent.push, before)
          assert.deepStrictEqual(yield* executions, [{ n: 1 }])
        }).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    60_000
  )
})
