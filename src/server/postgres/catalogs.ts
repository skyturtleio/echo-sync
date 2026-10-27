import { Array as Arr, Context, Effect, Layer, Option, Semaphore, String as Str } from "effect"
import { gzipSync } from "node:zlib"
import { SqlClient } from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import { type Registry, SyncTable } from "echo-sync"
import { decodeRow, selectList, q } from "./rowStore.ts"
import { syncedTables } from "./schema.ts"
import * as Telemetry from "../../internal/Telemetry.ts"

export interface Snapshot {
  readonly version: string
  readonly bytes: Uint8Array
}

export class Catalogs extends Context.Service<
  Catalogs,
  {
    readonly get: (table: SyncTable.AnySyncTable) => Effect.Effect<Snapshot, SqlError>
    readonly versions: Effect.Effect<Record<string, string>, SqlError>
  }
>()("echo-sync/server/Catalogs") {}

/** One current compressed snapshot per table, shared by discovery and downloads. */
export const layer = (registry: Registry.SyncRegistry) =>
  Layer.effect(Catalogs)(
    Effect.gen(function* () {
      const sql = yield* SqlClient
      const cache = new Map<string, { readonly generation: string; readonly snapshot: Snapshot }>()
      const locks = new Map(Arr.map(registry.catalogs, (table) => [table.name, Semaphore.makeUnsafe(1)] as const))
      const get = (table: SyncTable.AnySyncTable): Effect.Effect<Snapshot, SqlError> => {
        const lock = locks.get(table.name)
        if (lock === undefined) return Effect.die("unregistered catalog")
        return lock
          .withPermit(
            sql.withTransaction(
              Effect.gen(function* () {
                yield* sql`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`
                const [state] = yield* sql<{ generation: string }>`SELECT generation::text AS generation
        FROM echo_sync.catalog_generations WHERE table_name = ${table.name}`
                if (state === undefined) return yield* Effect.die("catalog has no generation")
                const cached = cache.get(table.name)
                if (cached?.generation === state.generation) return cached.snapshot
                const relation = Option.getOrUndefined(
                  Arr.findFirst(
                    yield* syncedTables.pipe(Effect.provideService(SqlClient, sql)),
                    (entry) => entry.name === table.name
                  )
                )
                if (relation === undefined) return yield* Effect.die("catalog is not attached")
                const rows = yield* sql.unsafe(
                  `SELECT ${selectList(table)} FROM ${q(relation.schema)}.${q(table.name)} ORDER BY id`
                )
                const encoded = yield* Effect.forEach(rows, (row) => decodeRow(table, row))
                const json = SyncTable.canonicalJson(encoded)
                const digest = yield* Effect.promise(() =>
                  crypto.subtle.digest("SHA-256", new TextEncoder().encode(table.manifest.dataHash + json))
                )
                const version = Arr.join(
                  Array.from(new Uint8Array(digest), (byte) => Str.padStart(2, "0")(byte.toString(16))),
                  ""
                )
                const snapshot = { version, bytes: gzipSync(json) }
                cache.set(table.name, { generation: state.generation, snapshot })
                return snapshot
              })
            )
          )
          .pipe(Telemetry.boundary)
      }
      return {
        get,
        versions: Effect.gen(function* () {
          const versions: Record<string, string> = {}
          for (const table of registry.catalogs) versions[table.name] = (yield* get(table)).version
          return versions
        })
      }
    })
  )
