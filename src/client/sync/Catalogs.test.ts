/* oxlint-disable effecttsgo/strict-effect-provide -- Each test owns its disposable SQLite layer and scope. */
// oxlint-disable-next-line import/no-unassigned-import -- Node fetch shim for wa-sqlite.
import "../testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { expect, it } from "@effect/vitest"
import { Array as Arr, DateTime, Deferred, Effect, Fiber, Option, Queue, Schema, Stream } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { HttpClient, HttpClientResponse } from "effect/http"
import * as Reactivity from "effect/reactivity/Reactivity"
import { AccessScope, Registry, Store, SyncTable } from "echo-sync"
import * as Catalogs from "./Catalogs.ts"
import * as Storage from "./SqliteSyncStorage.ts"
import { liveRuns } from "../worker/Leader.ts"

const Airport = SyncTable.catalog(
  "airports",
  { id: Schema.String.pipe(Schema.brand("AirportId")), name: Schema.String },
  { indexes: { by_name: ["name"] } }
)
const registry = Registry.make([Airport])
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* Storage.migrate(registry)
  yield* Catalogs.open(sql, registry)
  const storage = yield* Storage.make(sql, registry)
  const reactivity = yield* Reactivity.make
  const instance = Store.make(storage.rows, {
    actor: AccessScope.PrincipalId.make("p"),
    roleFor: () => Effect.succeed(Option.none()),
    claimScope: () => Effect.succeed(false),
    now: DateTime.makeUnsafe(0)
  })
  return { sql, storage, reactivity, store: instance.store }
})

it.effect(
  "catalog discovery upgrades a previously stripped shape at the same version despite another catalog failing",
  () =>
    Effect.gen(function* () {
      const { sql, storage, reactivity } = yield* setup
      const expanded = SyncTable.catalog("airports", { ...Airport.fields, code: Schema.String })
      const broken = SyncTable.catalog("broken", Airport.fields)
      const nextRegistry = Registry.make([broken, expanded])
      const payload = [{ id: "a", name: "Alpha", code: "AAA" }]
      yield* Catalogs.install(sql, storage, reactivity, Airport, "same", payload)
      yield* Catalogs.open(sql, nextRegistry)
      yield* Catalogs.install(sql, storage, reactivity, broken, "old", [{ id: "b", name: "Before" }])
      const installed = yield* Deferred.make<void>()
      const observed = {
        ...storage,
        invalidateRevision: Effect.andThen(storage.invalidateRevision, Deferred.succeed(installed, undefined))
      }
      const discover = yield* Catalogs.make(
        sql,
        observed,
        reactivity,
        nextRegistry,
        "http://catalog.test/sync",
        Effect.succeed({})
      ).pipe(
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((request, url) =>
            Effect.sync(() =>
              HttpClientResponse.fromWeb(
                request,
                new Response(
                  url.pathname === "/sync/catalog/broken/bad"
                    ? '[{"id":"b","name":123}]'
                    : '[{"id":"a","name":"Alpha","code":"AAA"}]'
                )
              )
            )
          )
        )
      )
      yield* discover({ broken: "bad", airports: "same" })
      yield* Deferred.await(installed)
      expect(yield* storage.admit(sql`SELECT id, name, code FROM airports`)).toEqual([
        { id: "a", name: "Alpha", code: "AAA" }
      ])
      expect(yield* storage.admit(sql`SELECT name FROM broken`)).toEqual([{ name: "Before" }])
      expect(yield* storage.admit(sql`SELECT value FROM echo_meta WHERE key = 'catalog:broken'`)).toEqual([
        { value: "old" }
      ])
    }).pipe(Effect.scoped, Effect.provide(SqliteClient.layerMemory({})))
)

it.effect("catalog swaps reemit global queries without grants and survive scoped replica rebuilds and purges", () =>
  Effect.gen(function* () {
    const { sql, storage, reactivity, store } = yield* setup
    const values = yield* Queue.unbounded<ReadonlyArray<string>>()
    yield* Effect.forkScoped(
      liveRuns(reactivity, (watch) =>
        storage.transaction(
          Effect.gen(function* () {
            const rows = yield* store.query(Airport).withIndex("by_name").collect
            yield* watch([Airport.name])
            return Arr.map(rows, (row) => row.name)
          })
        )
      ).pipe(Stream.runForEach((rows) => Queue.offer(values, rows)))
    )
    expect(yield* Queue.take(values)).toEqual([])
    yield* Catalogs.install(sql, storage, reactivity, Airport, "v1", [{ id: "a", name: "Alpha" }])
    expect(yield* Queue.take(values)).toEqual(["Alpha"])
    yield* Catalogs.install(sql, storage, reactivity, Airport, "v2", [
      { id: "b", name: "Bravo" },
      { id: "c", name: "Charlie" }
    ])
    expect(yield* Queue.take(values)).toEqual(["Bravo", "Charlie"])
    yield* storage.transaction(
      Effect.andThen(storage.rows.clearAll, storage.rows.purgeScope(AccessScope.AccessScopeId.make("s")))
    )
    const T = SyncTable.make("extra", {
      id: Schema.String.pipe(Schema.brand("ExtraId")),
      accessScopeId: AccessScope.AccessScopeId
    })
    yield* Storage.migrate(Registry.make([Airport, T]))
    expect(Arr.map(yield* store.scan(Airport), (row) => row.name)).toEqual(["Bravo", "Charlie"])
    const indexed = SyncTable.catalog("airports", Airport.fields, { indexes: { renamed: ["name"] } })
    yield* Catalogs.open(sql, Registry.make([indexed]))
    expect(Arr.map(yield* store.query(indexed).withIndex("renamed").collect, (row) => row.name)).toEqual([
      "Bravo",
      "Charlie"
    ])
    expect(yield* sql`SELECT value FROM echo_meta WHERE key = 'catalog:airports'`).toEqual([{ value: "v2" }])
    expect(yield* sql`SELECT name FROM sqlite_schema WHERE type = 'index' AND name = 'airports__renamed'`).toHaveLength(
      1
    )
    // @ts-expect-error Deliberately bypass the type boundary: JavaScript callers must also fail closed.
    expect((yield* Effect.exit(store.delete(Airport, "b")))._tag).toBe("Failure")
  }).pipe(Effect.scoped, Effect.provide(SqliteClient.layerMemory({})))
)

it.effect("an interrupted or undecodable catalog fill keeps the published table and version across reopen", () =>
  Effect.gen(function* () {
    const { sql, storage, reactivity, store } = yield* setup
    yield* Catalogs.install(sql, storage, reactivity, Airport, "old", [{ id: "old", name: "Old" }])
    const filling = yield* Deferred.make<void>()
    const blocked: Storage.SqliteSyncStorage = {
      ...storage,
      rows: {
        ...storage.rows,
        writeMany: (table, rows) =>
          Effect.andThen(
            storage.rows.writeMany(table, rows),
            Effect.andThen(Deferred.succeed(filling, undefined), Effect.never)
          )
      }
    }
    const fiber = yield* Effect.forkChild(
      Catalogs.install(sql, blocked, reactivity, Airport, "new", [{ id: "new", name: "New" }])
    )
    yield* Deferred.await(filling)
    yield* Fiber.interrupt(fiber)
    yield* Catalogs.open(sql, registry)
    expect(Arr.map(yield* store.scan(Airport), (row) => row.name)).toEqual(["Old"])
    expect(yield* sql`SELECT value FROM echo_meta WHERE key = 'catalog:airports'`).toEqual([{ value: "old" }])
    const invalid = [...Arr.makeBy(500, (i) => ({ id: String(i), name: "Valid" })), { id: "bad", name: 3 }]
    expect((yield* Effect.exit(Catalogs.install(sql, storage, reactivity, Airport, "bad", invalid)))._tag).toBe(
      "Failure"
    )
    yield* Catalogs.open(sql, registry)
    expect(Arr.map(yield* store.scan(Airport), (row) => row.name)).toEqual(["Old"])
    expect(yield* sql`SELECT name FROM sqlite_schema WHERE name = 'airports__next'`).toEqual([])
  }).pipe(Effect.scoped, Effect.provide(SqliteClient.layerMemory({})))
)
