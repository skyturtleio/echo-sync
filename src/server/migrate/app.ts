import type { Effect } from "effect"
import * as PgMigrator from "@effect/sql-pg/PgMigrator"
import type { SqlClient } from "effect/sql/SqlClient"

/** The app migrations' own history table (DESIGN §11); never the engine's `echo_sync.migrations`. */
export const appMigrationsTable = "echo_sync.app_migrations"

/**
 * Runs the app's generated migrations (`NNN_name` → module default export), numbered from 001,
 * in their own history table. Run after the engine's `migrate` (which creates the `echo_sync`
 * schema), then `attach` each table, then `verify`.
 */
export const migrateApp = (
  migrations: Record<string, Effect.Effect<void, unknown, SqlClient>>,
  options?: { readonly table?: string }
) =>
  PgMigrator.make({})({
    loader: PgMigrator.fromRecord(migrations),
    table: options?.table ?? appMigrationsTable
  })
