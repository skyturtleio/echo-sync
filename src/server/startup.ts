import { Effect, Layer } from "effect"
import type * as HttpRouter from "effect/http/HttpRouter"
import type { SqlClient } from "effect/sql/SqlClient"
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
 * the engine migration. Idempotent, so every server instance runs it.
 */
export const prepare = (options: Pick<StartupOptions, "registry" | "appMigrations">) =>
  Effect.gen(function* () {
    yield* migrate
    yield* migrateApp(options.appMigrations ?? {})
    for (const table of options.registry.tables) {
      if (table !== ScopeGrants.ScopeGrants) yield* attach(table)
    }
    yield* verify(options.registry)
  })

/**
 * The sync server: `prepare`, then mount `Protocol.SyncRpcs`. The RPC layer is built only after
 * `prepare` succeeds, so on `Drift` (or any startup failure) nothing is served and the layer
 * fails with the full mismatch list.
 */
export const layer = (options: StartupOptions) => Layer.unwrap(Effect.as(prepare(options), layerHttp(options)))
