/** Real OPFS catalog installer with timing wrappers; no alternate installation algorithm. */
/* oxlint-disable effecttsgo/strict-effect-provide -- Standalone benchmark worker entry point. */
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { Cause, Effect, Fiber, Schema } from "effect"
import * as Reactivity from "effect/reactivity/Reactivity"
import { HttpClient, FetchHttpClient } from "effect/http"
import { SqlClient } from "effect/sql/SqlClient"
import { SyncTable } from "../../src/core/index.ts"
import * as OpfsSqlite from "../../src/client/worker/OpfsSqlite.ts"
import * as Storage from "../../src/client/sync/SqliteSyncStorage.ts"
import * as Catalogs from "../../src/client/sync/Catalogs.ts"
import { Airports, registry, principalHeader } from "./app.ts"

const flags = new URLSearchParams(self.location.search)
const run = Effect.gen(function* () {
  const serve = yield* OpfsSqlite.open({
    poolDirectory: `catalog-bench-${flags.get("id") ?? "test"}`,
    dbName: "catalog.db"
  })
  const channel = new MessageChannel()
  yield* Effect.forkScoped(serve(channel.port1))
  const reactivity = yield* Reactivity.make
  const real = yield* SqliteClient.make({ worker: Effect.succeed(channel.port2), installReactivityHooks: true }).pipe(
    Effect.provideService(Reactivity.Reactivity, reactivity)
  )
  let largestLockMs = 0
  const transaction = real.withTransaction
  const measuredTransaction: SqlClient["withTransaction"] = (work) =>
    Effect.suspend(() => {
      const start = performance.now()
      return transaction(work).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            largestLockMs = Math.max(largestLockMs, performance.now() - start)
          })
        )
      )
    })
  const sql = Object.assign(real, { withTransaction: measuredTransaction })
  yield* Storage.migrate(registry).pipe(Effect.provideService(SqlClient, sql))
  const abandoned = yield* sql`SELECT name FROM sqlite_schema WHERE name = 'airport_reference__next'`
  yield* Catalogs.open(sql, registry)
  if (flags.get("mode") === "recover") {
    const [count] = yield* sql<{ n: number }>`SELECT count(*) AS n FROM airport_reference`
    const [version] = yield* sql<{ value: string }>`SELECT value FROM echo_meta WHERE key = 'catalog:airport_reference'`
    const shadows = yield* sql`SELECT name FROM sqlite_schema WHERE name = 'airport_reference__next'`
    return { rows: count?.n, version: version?.value, abandoned: abandoned.length, shadows: shadows.length }
  }
  const realStorage = yield* Storage.make(sql, registry)
  let swapMs = 0
  let largestChunkMs = 0
  const storage: Storage.SqliteSyncStorage = {
    ...realStorage,
    rows: {
      ...realStorage.rows,
      writeMany: (table, rows) =>
        Effect.suspend(() => {
          const start = performance.now()
          return realStorage.rows.writeMany(table, rows).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                largestChunkMs = Math.max(largestChunkMs, performance.now() - start)
              })
            )
          )
        })
    },
    admit: (work) =>
      realStorage.admit(
        Effect.suspend(() => {
          const start = performance.now()
          return work.pipe(
            Effect.ensuring(
              Effect.sync(() => {
                swapMs = performance.now() - start
              })
            )
          )
        })
      )
  }
  const http = HttpClient.filterStatusOk(yield* HttpClient.HttpClient)
  const url = `${self.location.origin}/sync/catalog/airport_reference/${flags.get("version") ?? ""}`
  const downloadStart = performance.now()
  const response = yield* http.get(url, { headers: { [principalHeader]: "benchmark" } })
  const text = yield* response.text
  const downloadMs = performance.now() - downloadStart
  const rows = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Array(SyncTable.EncodedRow)))(text)
  if (flags.get("mode") === "crash") {
    yield* Catalogs.install(sql, realStorage, reactivity, Airports, "previous", rows.slice(0, 3))
    const paused: Storage.SqliteSyncStorage = {
      ...realStorage,
      transaction: (work) =>
        realStorage.transaction(work).pipe(
          Effect.tap(() =>
            Effect.gen(function* () {
              const [count] = yield* sql<{ n: number }>`SELECT count(*) AS n FROM airport_reference__next`.pipe(
                Effect.orDie
              )
              if ((count?.n ?? 0) >= 500) {
                self.postMessage({ ok: true, paused: count?.n }, { transfer: [] })
                return yield* Effect.never
              }
            })
          )
        )
    }
    yield* Catalogs.install(sql, paused, reactivity, Airports, "interrupted", rows)
    return yield* Effect.die("crash probe did not pause")
  }
  // Measure queries against a populated published catalog, not an empty first-install table.
  yield* Catalogs.install(sql, realStorage, reactivity, Airports, "previous", rows)
  let queries = 0
  let maxQueryMs = 0
  let queryError: string | undefined
  let filling = true
  const query = yield* Effect.forkChild(
    Effect.gen(function* () {
      // oxlint-disable-next-line no-unmodified-loop-condition -- The installing fiber clears this after publication.
      while (filling) {
        const start = performance.now()
        const results = yield* realStorage.transaction(
          sql`SELECT id FROM airport_reference WHERE ident >= 'A042000' ORDER BY ident, id LIMIT 20`
        )
        if (results.length !== 20) return yield* Effect.die("published catalog query lost rows during fill")
        maxQueryMs = Math.max(maxQueryMs, performance.now() - start)
        queries++
      }
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.sync(() => {
          if (!Cause.hasInterrupts(cause)) queryError = Cause.pretty(cause)
        })
      )
    ),
    { startImmediately: true }
  )
  largestLockMs = 0
  const start = performance.now()
  yield* Catalogs.install(sql, storage, reactivity, Airports, flags.get("version") ?? "", rows)
  const fillMs = performance.now() - start
  filling = false
  yield* Fiber.join(query)
  const [count] = yield* sql<{ n: number }>`SELECT count(*) AS n FROM airport_reference`
  const resources = performance.getEntriesByName(url)
  const entry = resources[0]
  const encodedBytes =
    Number(response.headers["content-length"]) ||
    (entry instanceof PerformanceResourceTiming ? entry.encodedBodySize : null)
  return {
    rows: count?.n,
    encodedBytes,
    decodedBytes: new TextEncoder().encode(text).byteLength,
    downloadMs,
    fillMs,
    largestLockMs,
    largestChunkMs,
    swapMs,
    queries,
    queryError,
    maxQueryMs
  }
}).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer))

Effect.runFork(
  run.pipe(
    Effect.tap((result) => Effect.sync(() => self.postMessage({ ok: true, result }, { transfer: [] }))),
    Effect.catchCause((cause) =>
      Effect.sync(() => self.postMessage({ ok: false, error: Cause.pretty(cause) }, { transfer: [] }))
    )
  )
)
