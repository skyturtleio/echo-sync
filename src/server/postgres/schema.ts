import { Effect, Schema } from "effect"
import * as PgMigrator from "@effect/sql-pg/PgMigrator"
import { SqlClient } from "effect/sql/SqlClient"
import engine from "./migrations/001_engine.ts"

/** Migrator history table (DESIGN.md §4). */
export const migrationsTable = "echo_sync.migrations"

/**
 * Brings the `echo_sync` schema up to date. Safe to run concurrently from several server
 * instances: the migrator locks its history table.
 *
 * Uses `PgMigrator.make` without a schema dumper so the only requirement is `SqlClient`
 * (`PgMigrator.run` also demands process/filesystem services for `pg_dump`).
 */
export const migrate = Effect.gen(function* () {
  const sql = yield* SqlClient
  // The history table lives in `echo_sync`, so the schema must exist before the migrator runs.
  yield* sql`CREATE SCHEMA IF NOT EXISTS echo_sync`
  return yield* PgMigrator.make({})({
    loader: PgMigrator.fromRecord({ "001_engine": engine }),
    table: migrationsTable
  })
})

/**
 * `echo_sync.log.table_name` is the unqualified table name (the SyncTable name on the wire), so
 * two synced tables with one name in different schemas would be indistinguishable.
 */
export class TableNameConflict extends Schema.TaggedError<TableNameConflict>()("TableNameConflict", {
  table: Schema.String,
  existingSchema: Schema.String
}) {}

/**
 * Starts change capture on an app table. The table must have `id` (primary key, text-castable)
 * and `access_scope_id text not null`. Idempotent.
 *
 * Atomic: the name check and both triggers commit together (a savepoint when called inside a
 * migration's transaction), so a table is never captured but truncatable. Attaches serialize on
 * an engine advisory lock, so two concurrent attaches of one name from different schemas cannot
 * both pass the check.
 *
 * Every writer participates: mutators, imports, scripts, and cascades all go through the row
 * trigger, which takes the engine lock before allocating a log position (§7).
 */
export const attach = (tableName: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* sql.withTransaction(
      Effect.gen(function* () {
        // Engine key + 1 ("echosync" + 1): distinct from the position lock, so attaching does
        // not stall writers.
        yield* sql`SELECT pg_advisory_xact_lock(7305797848413072996)`
        // A fresh statement snapshot after the lock: a concurrent attach that held it is visible.
        const target = yield* sql<{ schema: string; name: string }>`
          SELECT n.nspname AS schema, c.relname AS name FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.oid = ${tableName}::regclass`
        const clash = (yield* syncedTables).find((t) => t.name === target[0]?.name && t.schema !== target[0]?.schema)
        if (clash !== undefined) {
          return yield* new TableNameConflict({ table: clash.name, existingSchema: clash.schema })
        }
        yield* sql`
          CREATE OR REPLACE TRIGGER echo_sync_capture
          AFTER INSERT OR UPDATE OR DELETE ON ${sql(tableName)}
          FOR EACH ROW EXECUTE FUNCTION echo_sync.echo_sync_capture_changes()`
        yield* sql`
          CREATE OR REPLACE TRIGGER echo_sync_reject_truncate
          BEFORE TRUNCATE ON ${sql(tableName)}
          FOR EACH STATEMENT EXECUTE FUNCTION echo_sync.reject_truncate()`
      })
    )
  })

/** A table with change capture attached, as recorded in `echo_sync.log.table_name`. */
export interface SyncedTable {
  readonly schema: string
  readonly name: string
}

/** Synced tables, discovered from the catalog so there is no second registry to drift. */
export const syncedTables = Effect.gen(function* () {
  const sql = yield* SqlClient
  return yield* sql<SyncedTable>`
    SELECT n.nspname AS schema, c.relname AS name
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE t.tgfoid = 'echo_sync.echo_sync_capture_changes'::regproc
    ORDER BY c.relname`
})
