// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../src/client/testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { assert, describe, it } from "@effect/vitest"
import type * as PgClient from "@effect/sql-pg/PgClient"
import { Context, Effect, Fiber, Layer, Option, Schema, Stream } from "effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as HttpRouter from "effect/http/HttpRouter"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Protocol, ScopeGrants } from "echo-sync"
import * as Engine from "../src/client/sync/Engine.ts"
import * as SqliteSyncStorage from "../src/client/sync/SqliteSyncStorage.ts"
import { PullTransportError, PushTransportError } from "../src/client/sync/SyncFailure.ts"
import * as SyncLoop from "../src/client/sync/SyncLoop.ts"
import * as Transport from "../src/client/sync/Transport.ts"
import { layerHttp } from "../src/server/postgres/rpc.ts"
import { grant, Items, pgAvailable, registry, RowId, TestDatabase } from "../src/server/postgres/test-support.ts"

// First-registration account binding (DESIGN §16.21) end to end: a replica created for alice whose
// browser session is bob's. The real Postgres server over HTTP + NDJSON, the real Transport, and the
// real Engine and SyncLoop over SQLite. Runs only with ECHO_PG_URL.

const alice = AccessScope.PrincipalId.make("alice")
const bob = AccessScope.PrincipalId.make("bob")
const addItem = Mutation.make("items.add", {
  args: Schema.Struct({ id: Schema.String }),
  handler: (ctx, args) =>
    ctx.store.insert(Items, {
      id: RowId.make(args.id),
      accessScopeId: AccessScope.AccessScopeId.make("alice-scope"),
      body: "alice's write"
    })
})
/** Allowed optimistically with no grant in the replica, so a replica that never synced can enqueue it. */
const createScope = Mutation.make("scopes.create", {
  args: Schema.Struct({ scope: Schema.String }),
  handler: (ctx, args) => ctx.store.createScope(AccessScope.AccessScopeId.make(args.scope))
})
const mutations = Mutation.makeRegistry([addItem, createScope])

/** The transport's classified reason, or the failure itself when it is not a transport failure. */
const reasonOf = (failure: { readonly _tag: string }) =>
  failure instanceof PushTransportError || failure instanceof PullTransportError ? failure.reason : failure

/** Alice may write her scope; bob reads his own, which holds a row only he may see. */
const seed = Effect.gen(function* () {
  const pg = yield* SqlClient
  yield* grant(alice, "alice-scope")
  yield* grant(bob, "bob-scope")
  yield* pg`UPDATE echo_sync.scope_grants SET role = 'writer' WHERE principal_id = ${alice}`
  yield* pg`INSERT INTO items (id, access_scope_id, body) VALUES ('alice-row', 'alice-scope', 'a'), ('bob-row', 'bob-scope', 'b')`
})

/** The server authenticates whoever `session.current` is; the replica always acts as alice. */
const setup = Effect.gen(function* () {
  const session = { current: bob }
  const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
  const web = HttpRouter.toWebHandler(
    layerHttp({ registry, mutations, principalFor: () => Effect.succeed(session.current) }).pipe(
      Layer.provide(Layer.succeedContext(db))
    ),
    { disableLogger: true }
  )
  yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
  const fetch: typeof globalThis.fetch = Object.assign(
    (input: RequestInfo | URL, init?: RequestInit) => web.handler(new Request(input, init)),
    { preconnect: globalThis.fetch.preconnect }
  )
  const sqlite = Context.get(yield* Layer.build(SqliteClient.layerMemory({ installReactivityHooks: true })), SqlClient)
  yield* SqliteSyncStorage.migrate(registry).pipe(Effect.provideService(SqlClient, sqlite))
  const storage = SqliteSyncStorage.make(sqlite, registry)
  const transport = yield* Transport.make.pipe(Effect.provide(Transport.layerProtocolHttp("http://echo.test/sync")))
  const clientId = Ids.ClientId.make("alice-device")
  const engine = yield* Engine.make({ clientId, actor: alice, storage, registry, mutations, transport })
  const replica = Effect.orDie(
    Effect.all({
      grants: sqlite<{ g: string }>`SELECT access_scope_id || ':' || principal_id AS g FROM scope_grants ORDER BY g`,
      items: sqlite<{ id: string }>`SELECT id FROM items ORDER BY id`
    })
  ).pipe(Effect.map(({ grants, items }) => ({ grants: grants.map((r) => r.g), items: items.map((r) => r.id) })))
  /** Every server row a request could bind, ledger, record or write, in full. */
  const server = Effect.gen(function* () {
    const pg = yield* SqlClient
    const [row] = yield* pg<{ state: string }>`SELECT jsonb_build_object(
      'clients', (SELECT jsonb_agg(to_jsonb(t) ORDER BY client_id) FROM echo_sync.clients t),
      'ledger', (SELECT jsonb_agg(to_jsonb(t) ORDER BY position) FROM echo_sync.mutations t),
      'cursors', (SELECT jsonb_agg(to_jsonb(t) ORDER BY client_id, position) FROM echo_sync.client_scopes t),
      'scopes', (SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM echo_sync.access_scopes t),
      'grants', (SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM echo_sync.scope_grants t),
      'items', (SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM items t)
    )::text AS state`
    return row?.state
  })
  const serverRows = Effect.gen(function* () {
    const pg = yield* SqlClient
    return {
      clients: yield* pg<{ client_id: string; principal_id: string }>`
        SELECT client_id, principal_id FROM echo_sync.clients ORDER BY client_id`,
      ledger: (yield* pg<{ mutation_id: string }>`SELECT mutation_id FROM echo_sync.mutations ORDER BY position`).map(
        (r) => r.mutation_id
      )
    }
  })
  /** The whole local replica: rows, outbox, staged pages and sync metadata. */
  const local = Effect.all({
    items: storage.rows.scan(Items),
    grants: storage.rows.scan(ScopeGrants.ScopeGrants),
    pending: storage.pending,
    staged: storage.staged,
    meta: storage.meta
  })
  const provide = <A, E, R>(self: Effect.Effect<A, E, R>) => Effect.provideService(self, FetchHttpClient.Fetch, fetch)
  return { session, clientId, storage, transport, engine, replica, local, server, serverRows, provide }
})

const waitFor = (loop: SyncLoop.SyncLoop, predicate: (s: SyncLoop.SyncStatus) => boolean) =>
  Stream.runHead(Stream.filter(loop.changes, predicate)).pipe(Effect.timeout("10 seconds"))

describe.skipIf(!pgAvailable)("replica principal vs. session (DESIGN §16.21) against the real server", () => {
  it.live(
    "a fresh replica first synced under another principal's session binds no client, ledgers nothing and installs none of its data",
    () =>
      Effect.gen(function* () {
        yield* seed
        const { session, clientId, storage, transport, engine, replica, local, server, serverRows, provide } =
          yield* setup
        // Enqueued before the first sync, so the first push carries it.
        const write = yield* engine.mutate(Ids.MutationId.make("offline-scope"), "scopes.create", {
          scope: "alice-new"
        })
        const before = { server: yield* server, local: yield* local }
        assert.deepStrictEqual((yield* serverRows).clients, [])
        assert.deepStrictEqual(
          before.local.pending.map((p) => p.envelope),
          [write]
        )
        const loop = yield* SyncLoop.make({
          engine,
          pendingMutations: storage.pendingCount
        })
        const running = yield* Effect.forkScoped(provide(loop.run))
        // The engine's credential path: `unauthorized`, retried, the replica kept as is.
        const first = yield* waitFor(loop, (s) => s.lastError !== null || s.lastSyncedAt !== null)
        assert.strictEqual(Option.getOrUndefined(first)?.lastError?.kind, "unauthorized")
        assert.instanceOf(reasonOf(yield* Effect.flip(provide(engine.push))), Protocol.Unauthorized)
        assert.instanceOf(reasonOf(yield* Effect.flip(provide(engine.pull))), Protocol.Unauthorized)
        const pokes = yield* Effect.flip(provide(Stream.runDrain(Stream.unwrap(transport.pokes(clientId, alice)))))
        assert.instanceOf(reasonOf(pokes), Protocol.Unauthorized)
        const join = {
          room: AccessScope.AccessScopeId.make("bob-scope"),
          tabId: Protocol.TabId.make("tab"),
          state: null,
          revision: 0
        }
        const presence = transport.presence(clientId, alice)
        const joined = yield* Effect.flip(provide(Stream.runDrain(Stream.unwrap(presence.room(join)))))
        assert.instanceOf(joined, Protocol.Unauthorized)
        assert.instanceOf(yield* Effect.flip(provide(presence.track(join))), Protocol.Unauthorized)
        assert.strictEqual((yield* loop.status).lastSyncedAt, null)
        assert.deepStrictEqual({ server: yield* server, local: yield* local }, before)

        // The session becomes alice's: the same replica binds to alice, delivers its write as alice
        // and gets only her data.
        session.current = alice
        yield* loop.wake
        yield* waitFor(loop, (s) => s.lastError === null && s.pendingMutations === 0 && s.lastSyncedAt !== null)
        assert.deepStrictEqual(yield* serverRows, {
          clients: [{ client_id: clientId, principal_id: alice }],
          ledger: ["offline-scope"]
        })
        assert.deepStrictEqual(yield* replica, {
          grants: ["alice-new:alice", "alice-scope:alice"],
          items: ["alice-row"]
        })
        yield* Fiber.interrupt(running)
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    30_000
  )

  it.live(
    "a registered replica with pending writes keeps them when the session switches: push refused, nothing of the other principal installed",
    () =>
      Effect.gen(function* () {
        yield* seed
        const { session, storage, engine, replica, local, server, serverRows, provide } = yield* setup
        session.current = alice
        yield* provide(engine.pull)
        const write = yield* engine.mutate(Ids.MutationId.make("pending-write"), "items.add", { id: "alice-new" })
        assert.deepStrictEqual((yield* replica).items, ["alice-new", "alice-row"])

        session.current = bob
        const before = { server: yield* server, local: yield* local }
        assert.instanceOf(reasonOf(yield* Effect.flip(provide(engine.push))), Protocol.Unauthorized)
        assert.instanceOf(reasonOf(yield* Effect.flip(provide(engine.pull))), Protocol.Unauthorized)
        assert.deepStrictEqual({ server: yield* server, local: yield* local }, before)
        assert.deepStrictEqual((yield* serverRows).ledger, [])
        assert.deepStrictEqual(
          before.local.pending.map((p) => p.envelope),
          [write]
        )

        // Back on alice's session the preserved write is delivered once.
        session.current = alice
        assert.strictEqual((yield* provide(engine.push)).processed, 1)
        yield* provide(engine.pull)
        assert.deepStrictEqual(yield* storage.pending, [])
        assert.deepStrictEqual((yield* serverRows).ledger, ["pending-write"])
        assert.deepStrictEqual((yield* replica).items, ["alice-new", "alice-row"])
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    30_000
  )
})
