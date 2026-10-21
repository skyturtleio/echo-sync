import { Effect, Layer } from "effect"
import type * as HttpRouter from "effect/http/HttpRouter"
import { SqlClient } from "effect/sql/SqlClient"
import { ScopeGrants } from "echo-sync"
import { migrateApp, verify } from "./migrate/index.ts"
import { layerHttp, type SyncServerOptions } from "./postgres/rpc.ts"
import { attach, migrate } from "./postgres/schema.ts"

export interface StartupOptions extends SyncServerOptions {
  /** The app's generated migrations (`NNN_name` → default export), run after the engine's. */
  readonly appMigrations?: Record<string, Effect.Effect<void, unknown, SqlClient>> | undefined
  readonly path?: HttpRouter.PathInput | undefined
}

/**
 * Startup order (DESIGN §11): engine `migrate` → app migrations → `attach` every app table →
 * `Migrate.verify` (fails with one `Drift` listing every mismatch). `scope_grants` is attached by
 * the engine migration.
 *
 * Safe to run concurrently from several instances: `migrate` and `migrateApp` lock their history
 * tables, `attach` is idempotent and serialized on its own advisory lock, `verify` only reads.
 */
export const prepare = (options: Pick<StartupOptions, "registry" | "appMigrations">) =>
  Effect.gen(function* () {
    yield* migrate
    yield* migrateApp(options.appMigrations ?? {})
    const sql = yield* SqlClient
    for (const table of options.registry.tables) {
      if (table === ScopeGrants.ScopeGrants) continue
      // A table the app migrations did not create is not attached here: attach would fail with a
      // bare SqlError. verify then reports it as "table does not exist" in the same Drift as
      // every other mismatch. The guard resolves the relation exactly as `attach(table)` without a
      // schema does (schema.ts: `quote_ident(name)::regclass`) and as verify's unattached
      // fallback does (`to_regclass(quote_ident(name))`): unqualified, through `search_path`,
      // same quoting. `to_regclass` only returns NULL where `::regclass` raises.
      const [found] = yield* sql<{
        exists: boolean
      }>`SELECT to_regclass(quote_ident(${table.name})) IS NOT NULL AS exists`
      if (found?.exists === true) yield* attach(table)
    }
    yield* verify(options.registry)
  })

/**
 * The sync server: `prepare`, then mount `Protocol.SyncRpcs`. The RPC layer is built only after
 * `prepare` succeeds, so on `Drift` (or any startup failure) nothing is served and the layer
 * fails with the full mismatch list.
 */
export const layer = (options: StartupOptions) => Layer.unwrap(Effect.as(prepare(options), layerHttp(options)))
