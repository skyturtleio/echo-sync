// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../src/client/testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import type * as PgClient from "@effect/sql-pg/PgClient"
import { describe, expect, it } from "@effect/vitest"
import { Clock, Context, Effect, Exit, Fiber, Layer, Option, Schema, Stream } from "effect"
import { FetchHttpClient, HttpRouter } from "effect/http"
import { TestClock } from "effect/testing"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Ids, Migrate, Mutation, Protocol, Registry, SyncTable } from "echo-sync"
import * as Engine from "../src/client/sync/Engine.ts"
import * as SqliteSyncStorage from "../src/client/sync/SqliteSyncStorage.ts"
import * as SyncLoop from "../src/client/sync/SyncLoop.ts"
import * as Transport from "../src/client/sync/Transport.ts"
import { type ClientPolicy } from "../src/server/postgres/pull.ts"
import { layerHttp } from "../src/server/postgres/rpc.ts"
import { grant, Items, Notes, pgAvailable, registry, RowId, TestDatabase } from "../src/server/postgres/test-support.ts"

const actor = AccessScope.PrincipalId.make("p")
const clientId = Ids.ClientId.make("version-client")
const mid = Ids.MutationId.make
const oldRegistry = Registry.make([Items])
const extendedItems = SyncTable.make("items", {
  id: RowId,
  accessScopeId: AccessScope.AccessScopeId,
  body: Schema.String,
  extraNote: Schema.NullOr(Schema.String)
})
const newRegistry = Registry.make([extendedItems, Notes])
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

/** The real HTTP/NDJSON stack, including payload/error codecs, principal and policy. */
const serve = Effect.fnUntraced(function* (tables: Registry.SyncRegistry, clientPolicy?: ClientPolicy) {
  const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
  const web = HttpRouter.toWebHandler(
    layerHttp({
      registry: tables,
      mutations,
      clientPolicy,
      principalFor: () => Effect.succeed(actor),
      pageLimit: 1
    }).pipe(Layer.provide(Layer.succeedContext(db))),
    { disableLogger: true }
  )
  yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
  const sent: Array<{ tag: string; payload: Protocol.PushRequest | Protocol.PullRequest }> = []
  const Request = Schema.Struct({ tag: Schema.String, payload: Schema.Unknown })
  const fetch: typeof globalThis.fetch = Object.assign(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new globalThis.Request(input, init)
      for (const line of (await request.clone().text()).trim().split("\n")) {
        const message = Schema.decodeUnknownSync(Schema.fromJsonString(Request))(line)
        if (message.tag === "push")
          sent.push({ tag: "push", payload: Schema.decodeUnknownSync(Protocol.PushRequest)(message.payload) })
        if (message.tag === "pull")
          sent.push({ tag: "pull", payload: Schema.decodeUnknownSync(Protocol.PullRequest)(message.payload) })
      }
      return web.handler(request)
    },
    { preconnect: globalThis.fetch.preconnect }
  )
  const rpc = yield* Transport.make.pipe(Effect.provide(Transport.layerProtocolHttp("http://echo.test/sync")))
  const transport: Engine.Transport<Transport.TransportError> = {
    push: (request) => rpc.push(request).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch)),
    pull: (request) => rpc.pull(request).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
  }
  return { transport, sent }
})

const device = Effect.fnUntraced(function* (
  tables: Registry.SyncRegistry,
  transport: Engine.Transport<Transport.TransportError>,
  database?: SqliteClient.SqliteClient
) {
  const sqlite =
    database ??
    Context.get(
      yield* Layer.build(SqliteClient.layerMemory({ installReactivityHooks: true })),
      SqliteClient.SqliteClient
    )
  yield* SqliteSyncStorage.migrate(tables).pipe(Effect.provideService(SqlClient, sqlite))
  const storage = SqliteSyncStorage.make(sqlite, tables)
  const engine = yield* Engine.make({ clientId, actor, registry: tables, storage, mutations, transport })
  return { sqlite, storage, engine }
})

const writableScope = Effect.gen(function* () {
  yield* grant("p", "s1")
  const pg = yield* SqlClient
  yield* pg`UPDATE echo_sync.scope_grants SET role = 'writer' WHERE principal_id = 'p'`
})
const settle = Effect.repeat(Effect.yieldNow, { times: 50 })
const waitFor = (loop: SyncLoop.SyncLoop, predicate: (s: SyncLoop.SyncStatus) => boolean) =>
  Stream.runHead(Stream.filter(loop.changes, predicate)).pipe(Effect.timeout("5 seconds"))

const done = (row: SyncTable.EncodedRow): Protocol.PullResponse => ({
  _tag: "PullDone",
  changes: [{ _tag: "upsert", table: "items", row }],
  acks: [],
  nextCursor: Ids.Position.make(999n),
  expiredThrough: Ids.SeqFloor.make(0)
})

describe.skipIf(!pgAvailable)("two-direction version gate (DESIGN §16.7), real Postgres and SQLite over HTTP", () => {
  it.live(
    "compatibility control: an old client still syncs additive columns and unknown tables with default serve",
    () =>
      Effect.gen(function* () {
        const pg = yield* SqlClient
        yield* writableScope
        yield* pg`ALTER TABLE items ADD COLUMN extra_note text COLLATE "C"`
        yield* pg`INSERT INTO items (id, access_scope_id, body, extra_note) VALUES ('server', 's1', 'visible', 'new')`
        yield* pg`INSERT INTO notes (id, access_scope_id, body) VALUES ('unknown', 's1', 'new table')`
        const { transport, sent } = yield* serve(newRegistry)
        const { engine, storage } = yield* device(oldRegistry, transport)
        yield* engine.pull
        expect(yield* storage.rows.scan(Items)).toEqual([{ id: "server", access_scope_id: "s1", body: "visible" }])
        yield* engine.mutate(mid("old-intent"), "items.add", { id: "old-write", body: "works" })
        expect((yield* engine.push).processed).toBe(1)
        yield* engine.pull
        expect(yield* storage.pending).toEqual([])
        expect(yield* pg`SELECT id, extra_note FROM items WHERE id = 'old-write'`).toEqual([
          { id: "old-write", extra_note: null }
        ])
        expect(oldRegistry.dataHash).not.toBe(newRegistry.dataHash)
        expect(sent.length).toBeGreaterThan(4) // metadata is carried on continuations too
        // Absence is accepted as legacy, not decoded as a malformed request.
        expect(
          (yield* transport.pull({ clientId: Ids.ClientId.make("legacy"), cursor: null, continuation: Option.none() }))
            ._tag
        ).toBe("PullPage")
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    20_000
  )

  it.live(
    "a client below the generated floor stops retrying, can mutate offline, and delivers the original intents after update",
    () =>
      Effect.gen(function* () {
        const pg = yield* SqlClient
        yield* writableScope
        const first = Migrate.generate({
          previous: Migrate.emptySnapshot,
          registry: Registry.make([Items]),
          existing: [],
          name: "old"
        })
        const second = Migrate.generate({
          previous: Migrate.decodeSnapshot(first.snapshot),
          registry: Registry.make([Notes, Items]),
          existing: [first.id],
          name: "current"
        })
        const history = Migrate.decodeSnapshot(second.snapshot).history ?? []
        const allowed = new Set(history.filter((h) => Number(h.migration.split("_")[0]) >= 2).map((h) => h.dataHash))
        let enforce = false
        const policy: ClientPolicy = (version) =>
          !enforce ||
          (version !== undefined && allowed.has(version.dataHash) && version.wireVersion === Protocol.wireVersion)
            ? Effect.void
            : Effect.fail(new Protocol.ClientOutdated({ message: "private diagnostic, never public status" }))
        const { transport, sent } = yield* serve(registry, policy)
        const { engine, storage, sqlite } = yield* device(oldRegistry, transport)
        yield* engine.pull
        const original = yield* engine.mutate(mid("before-refusal"), "items.add", { id: "a", body: "before" })
        const beforeRows = yield* storage.rows.scan(Items)
        const beforeMeta = yield* storage.meta
        const serverState = pg`SELECT jsonb_build_object(
          'items', (SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM items t),
          'ledger', (SELECT jsonb_agg(to_jsonb(t) ORDER BY position) FROM echo_sync.mutations t),
          'clients', (SELECT jsonb_agg(to_jsonb(t) ORDER BY client_id) FROM echo_sync.clients t),
          'cursors', (SELECT jsonb_agg(to_jsonb(t) ORDER BY client_id, position) FROM echo_sync.client_scopes t),
          'log', (SELECT jsonb_agg(to_jsonb(t) ORDER BY position) FROM echo_sync.log t)
        )::text AS state`
        const beforeServer = yield* serverState
        enforce = true
        // Both directions fail typed, before registration, replay/ledger lookup or cursor validation.
        const refused = yield* Effect.exit(
          transport.pull({
            clientId: Ids.ClientId.make("never-registered"),
            clientVersion: { dataHash: oldRegistry.dataHash, wireVersion: 99 },
            cursor: Ids.Position.make(99999n),
            continuation: Option.none()
          })
        )
        expect(refused._tag).toBe("Failure")
        expect(Option.getOrNull(Exit.findErrorOption(refused))?._tag).toBe("ClientOutdated")
        expect(yield* Effect.flip(engine.push)).toBeInstanceOf(Protocol.ClientOutdated)
        expect(history.map((h) => h.migration)).toEqual(["001_old", "002_current"])
        expect(allowed.size).toBe(1)
        const loop = yield* SyncLoop.make({
          engine,
          isFatalPush: Transport.isFatalPush,
          pendingMutations: storage.pendingCount
        })
        const clock = yield* TestClock.make()
        const running = yield* Effect.forkScoped(loop.run.pipe(Effect.provideService(Clock.Clock, clock)))
        yield* waitFor(loop, (s) => s.lastError?.kind === "outdated")
        expect((yield* loop.status).nextRetryAt).toBeNull()
        expect((yield* loop.status).phase).toBe("idle")
        expect((yield* loop.status).lastError?.message).not.toContain("private diagnostic")
        const stoppedAt = sent.length
        const offline = yield* engine.mutate(mid("while-refused"), "items.add", { id: "b", body: "offline" })
        yield* waitFor(loop, (s) => s.pendingMutations === 2)
        yield* loop.wake
        yield* clock.adjust("5 minutes")
        yield* loop.wake
        yield* settle
        expect(sent.length).toBe(stoppedAt)
        expect(yield* serverState).toEqual(beforeServer)
        expect(yield* storage.meta).toEqual(beforeMeta)
        expect((yield* storage.rows.scan(Items)).slice(0, 1)).toEqual(beforeRows)
        expect((yield* storage.pending).map((p) => p.envelope)).toEqual([original, offline])
        yield* Fiber.interrupt(running)
        // Reopen the same database with fresh storage/engine handles and the normal registry migration.
        const updated = yield* device(Registry.make([Items, Notes]), transport, sqlite)
        expect((yield* updated.storage.pending).map((p) => p.envelope)).toEqual([original, offline])
        const resumed = yield* SyncLoop.make({
          engine: updated.engine,
          isFatalPush: Transport.isFatalPush,
          pendingMutations: updated.storage.pendingCount
        })
        const resuming = yield* Effect.forkScoped(resumed.run)
        yield* waitFor(resumed, (s) => s.lastSyncedAt !== null && s.pendingMutations === 0)
        expect((yield* resumed.status).lastError).toBeNull()
        expect((yield* resumed.status).pendingMutations).toBe(0)
        expect(sent.filter((r) => r.tag === "push").map((r) => r.payload.clientVersion)).toEqual([
          { dataHash: oldRegistry.dataHash, wireVersion: Protocol.wireVersion },
          { dataHash: oldRegistry.dataHash, wireVersion: Protocol.wireVersion },
          { dataHash: registry.dataHash, wireVersion: Protocol.wireVersion }
        ])
        expect(yield* pg`SELECT mutation_id, seq FROM echo_sync.mutations ORDER BY seq`).toEqual([
          { mutation_id: original.mutationId, seq: BigInt(original.seq) },
          { mutation_id: offline.mutationId, seq: BigInt(offline.seq) }
        ])
        expect(yield* pg`SELECT id, body FROM items ORDER BY id`).toEqual([
          { id: "a", body: "before" },
          { id: "b", body: "offline" }
        ])
        yield* Fiber.interrupt(resuming)
        // No-pending clients discover refusal on pull; subsequent wakes cannot resume that direction either.
        const empty = yield* device(oldRegistry, transport)
        const pullLoop = yield* SyncLoop.make({
          engine: empty.engine,
          isFatalPush: Transport.isFatalPush,
          pendingMutations: empty.storage.pendingCount
        })
        const pulling = yield* Effect.forkScoped(pullLoop.run)
        yield* waitFor(pullLoop, (s) => s.lastError?.kind === "outdated")
        const refusedPull = sent.length
        yield* pullLoop.wake
        yield* clock.adjust("1 minute")
        expect(sent.length).toBe(refusedPull)
        yield* Fiber.interrupt(pulling)
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    20_000
  )

  it.live(
    "a newer client reports server-outdated with backoff and never rebuilds or installs a partial set",
    () =>
      Effect.gen(function* () {
        const pg = yield* SqlClient
        yield* writableScope
        yield* pg`INSERT INTO items (id, access_scope_id, body) VALUES ('server', 's1', 'older payload')`
        const { transport, sent } = yield* serve(registry)
        // Bootstrap at the old schema, then emulate a newer client with a previously valid installed replica.
        const old = yield* device(registry, transport)
        yield* old.engine.pull
        const meta = yield* old.storage.meta
        const grants = yield* old.storage.rows.scan(Option.getOrThrow(registry.get("scope_grants")))
        const { engine, storage } = yield* device(newRegistry, transport)
        yield* storage.transaction(
          Effect.gen(function* () {
            for (const row of grants)
              yield* storage.rows.write(
                Option.getOrThrow(newRegistry.get("scope_grants")),
                String(row["id"]),
                Option.some(row)
              )
            yield* storage.rows.write(
              extendedItems,
              "kept",
              Option.some({ id: "kept", access_scope_id: "s1", body: "installed", extra_note: null })
            )
            yield* storage.setMeta(meta)
          })
        )
        yield* engine.mutate(mid("pending"), "items.add", { id: "pending", body: "optimistic" })
        // Mark it pushed to isolate pull incompatibility without changing the server's row shape.
        yield* storage.setMeta({ ...meta, pushedThrough: 1 })
        yield* pg`INSERT INTO notes (id, access_scope_id, body) VALUES ('staged', 's1', 'never partially installed')`
        yield* pg`UPDATE items SET body = 'changed since cursor' WHERE id = 'server'`
        const savedRows = yield* storage.rows.scan(extendedItems)
        const savedPending = yield* storage.pending
        const savedMeta = yield* storage.meta
        const loop = yield* SyncLoop.make({
          engine,
          isFatalPush: Transport.isFatalPush,
          pendingMutations: storage.pendingCount
        })
        const clock = yield* TestClock.make()
        const running = yield* Effect.forkScoped(loop.run.pipe(Effect.provideService(Clock.Clock, clock)))
        yield* waitFor(loop, (s) => s.phase === "backoff")
        expect((yield* loop.status).lastError?.kind).toBe("server-outdated")
        expect((yield* loop.status).nextRetryAt).not.toBeNull()
        expect(yield* storage.rows.scan(extendedItems)).toEqual(savedRows)
        expect(yield* storage.pending).toEqual(savedPending)
        expect(yield* storage.meta).toEqual(savedMeta)
        expect(yield* storage.rows.scan(Notes)).toEqual([])
        const staged = yield* storage.staged
        expect(staged.flatMap((p) => p.changes).map((c) => (c._tag === "upsert" ? c.table : c._tag))).toEqual(["notes"])
        const attempts = sent.length
        yield* clock.adjust("1 second")
        yield* waitFor(loop, (s) => s.phase === "backoff" && s.nextRetryAt !== null && s.nextRetryAt > 1000)
        expect(sent.length).toBeGreaterThan(attempts)
        expect(
          sent
            .slice(attempts)
            .every((r) => r.tag === "pull" && "cursor" in r.payload && r.payload.cursor === meta.cursor)
        ).toBe(true)
        expect(yield* storage.rows.scan(extendedItems)).toEqual(savedRows)
        expect(yield* storage.meta).toEqual(savedMeta)
        expect(yield* storage.staged).toEqual(staged)
        expect(
          sent
            .slice(attempts)
            .every(
              (r) =>
                r.payload.clientVersion?.dataHash === newRegistry.dataHash &&
                r.payload.clientVersion.wireVersion === Protocol.wireVersion
            )
        ).toBe(true)
        yield* Fiber.interrupt(running)
        // Invalid present fields are ordinary malformed data, not a missing-key/server-behind signal.
        const probe = yield* Engine.make({
          clientId,
          actor,
          registry: newRegistry,
          storage,
          mutations,
          transport: {
            push: transport.push,
            pull: () =>
              Effect.succeed(done({ id: "null-is-present", access_scope_id: "s1", body: 42, extra_note: null }))
          }
        })
        const malformed = yield* Effect.exit(probe.pull)
        expect(Exit.isFailure(malformed) && Option.getOrNull(Exit.findErrorOption(malformed))?._tag).toBe(
          "MalformedChange"
        )
        const valid = yield* Engine.make({
          clientId,
          actor,
          registry: newRegistry,
          storage,
          mutations,
          transport: {
            push: transport.push,
            pull: () =>
              Effect.succeed(
                done({
                  id: "null-is-present",
                  access_scope_id: "s1",
                  body: "valid",
                  extra_note: null,
                  unknown_column: "drop me"
                })
              )
          }
        })
        yield* valid.pull
        expect(yield* storage.rows.read(extendedItems, "null-is-present")).toEqual(
          Option.some({ id: "null-is-present", access_scope_id: "s1", body: "valid", extra_note: null })
        )
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    20_000
  )
})
