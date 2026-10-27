import { Cause, Effect, Queue, Schema, String as Str } from "effect"
import { HttpClient } from "effect/http"
import type * as Reactivity from "effect/reactivity/Reactivity"
import type * as SqlClient from "effect/sql/SqlClient"
import { type Registry, SyncTable } from "echo-sync"
import type { SqliteSyncStorage } from "./SqliteSyncStorage.ts"
import * as Telemetry from "../../internal/Telemetry.ts"

const payload = Schema.fromJsonString(Schema.Array(SyncTable.EncodedRow))
const key = (table: SyncTable.AnySyncTable) => `catalog:${table.name}`

/** A previous leader may have died during a fill. The published table is never touched here. */
export const open = (sql: SqlClient.SqlClient, registry: Registry.SyncRegistry) =>
  sql.withTransaction(
    Effect.gen(function* () {
      for (const table of registry.catalogs) {
        yield* sql.unsafe(`DROP TABLE IF EXISTS "${table.name}__next"`)
        const [exists] = yield* sql`SELECT name FROM sqlite_schema WHERE type = 'table' AND name = ${table.name}`
        if (exists === undefined) {
          yield* sql.unsafe(table.sqlite.createTable)
          for (const ddl of table.sqlite.createIndexes) yield* sql.unsafe(ddl)
        } else {
          const [shape] = yield* sql<{
            value: string
          }>`SELECT value FROM echo_meta WHERE key = ${`${key(table)}:shape`}`
          const [indexes] = yield* sql<{
            value: string
          }>`SELECT value FROM echo_meta WHERE key = ${`${key(table)}:indexes`}`
          if (shape?.value === table.manifest.dataHash && indexes?.value !== table.manifest.indexHash) {
            const current = yield* sql<{ name: string }>`SELECT name FROM sqlite_schema
              WHERE type = 'index' AND sql IS NOT NULL AND tbl_name = ${table.name}`
            for (const index of current) yield* sql.unsafe(`DROP INDEX "${Str.replaceAll('"', '""')(index.name)}"`)
            for (const ddl of table.sqlite.createIndexes) yield* sql.unsafe(ddl)
            yield* sql`INSERT INTO echo_meta (key, value) VALUES (${`${key(table)}:indexes`}, ${table.manifest.indexHash})
              ON CONFLICT (key) DO UPDATE SET value = excluded.value`
          }
        }
      }
    })
  )

/** FIFO-admitted bounded fills, then a single atomic publication with explicit invalidation. */
export const install = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  storage: SqliteSyncStorage,
  reactivity: Reactivity.Reactivity,
  table: SyncTable.AnySyncTable,
  version: string,
  rows: ReadonlyArray<SyncTable.EncodedRow>
) {
  const next = `${table.name}__next`
  yield* storage.transaction(
    Effect.gen(function* () {
      yield* sql.unsafe(`DROP TABLE IF EXISTS "${next}"`)
      yield* sql.unsafe(Str.replace(`"${table.name}"`, `"${next}"`)(table.sqlite.createTable))
    })
  )
  const shadow = { ...table, name: next }
  for (let offset = 0; offset < rows.length; offset += 500) {
    // Validate the app's schema, not just the storage column kinds. Failed decode leaves the old catalog.
    const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(table.wire))(rows.slice(offset, offset + 500))
    const encoded = yield* Schema.encodeEffect(Schema.Array(table.wire))(decoded)
    yield* storage.transaction(storage.rows.writeMany(shadow, encoded))
  }
  yield* storage.admit(
    Effect.uninterruptible(
      Effect.gen(function* () {
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe(`DROP TABLE "${table.name}"`)
            yield* sql.unsafe(`ALTER TABLE "${next}" RENAME TO "${table.name}"`)
            for (const ddl of table.sqlite.createIndexes) yield* sql.unsafe(ddl)
            yield* sql`INSERT INTO echo_meta (key, value) VALUES (${key(table)}, ${version})
        ON CONFLICT (key) DO UPDATE SET value = excluded.value`
            for (const [suffix, value] of [
              ["shape", table.manifest.dataHash],
              ["indexes", table.manifest.indexHash]
            ]) {
              yield* sql`INSERT INTO echo_meta (key, value) VALUES (${`${key(table)}:${suffix}`}, ${value})
                ON CONFLICT (key) DO UPDATE SET value = excluded.value`
            }
          })
        )
        yield* storage.invalidateRevision
        yield* reactivity.invalidate([table.name])
      })
    )
  )
})

export const make = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  storage: SqliteSyncStorage,
  reactivity: Reactivity.Reactivity,
  registry: Registry.SyncRegistry,
  syncUrl: string,
  headers: Effect.Effect<Readonly<Record<string, string>>>
) {
  const updates = yield* Queue.sliding<Readonly<Record<string, string>>>(1)
  const http = HttpClient.filterStatusOk(yield* HttpClient.HttpClient)
  const sync = Effect.fnUntraced(function* (table: SyncTable.AnySyncTable, version: string) {
    const [stored] = yield* storage.admit(sql<{ value: string }>`SELECT value FROM echo_meta WHERE key = ${key(table)}`)
    const [shape] = yield* storage.admit(
      sql<{ value: string }>`SELECT value FROM echo_meta WHERE key = ${`${key(table)}:shape`}`
    )
    if (stored?.value === version && shape?.value === table.manifest.dataHash) return
    const requestHeaders = yield* headers
    const text = yield* http
      .get(`${Str.replace(/\/$/, "")(syncUrl)}/catalog/${table.name}/${encodeURIComponent(version)}`, {
        headers: requestHeaders
      })
      .pipe(
        Effect.flatMap((response) => response.text),
        Effect.timeout("45 seconds")
      )
    const rows = yield* Schema.decodeUnknownEffect(payload)(text)
    yield* install(sql, storage, reactivity, table, version, rows)
  })
  yield* Effect.forkScoped(
    Effect.forever(
      Effect.gen(function* () {
        const versions = yield* Queue.take(updates)
        let failed = false
        for (const table of registry.catalogs) {
          const version = versions[table.name]
          if (version === undefined) continue
          yield* sync(table, version).pipe(
            Effect.catchCause((cause) =>
              Effect.gen(function* () {
                if (Cause.hasInterrupts(cause)) return yield* Effect.failCause(cause)
                failed = true
                yield* Telemetry.log("Debug", "echo-sync: pull failed; retrying", cause)
              })
            )
          )
        }
        if (failed) {
          yield* Effect.sleep("5 seconds")
          // Never replace a newer discovery with the failed version.
          if ((yield* Queue.size(updates)) === 0) yield* Queue.offer(updates, versions)
        }
      })
    )
  )
  return (versions: Readonly<Record<string, string>>) => Queue.offer(updates, versions)
})
