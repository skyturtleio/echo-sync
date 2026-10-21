import { Effect, Option, Schema } from "effect"
import * as PgMigrator from "@effect/sql-pg/PgMigrator"
import { SqlClient } from "effect/sql/SqlClient"
import type { Registry, SyncTable } from "echo-sync"
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
 * Starts change capture on a SyncTable's relation: `table.name` in `options.schema`, or resolved
 * through `search_path` when no schema is given. The relation is created from the SyncTable's DDL
 * (`table.postgres.createTable`) by the app's migrations. Idempotent.
 *
 * Owns its transaction, at READ COMMITTED: the name check and both triggers commit together, so
 * a table is never captured but truncatable. Attaches serialize on an engine advisory lock, and
 * READ COMMITTED gives the check a fresh snapshot after the lock, so two concurrent attaches of
 * one name from different schemas cannot both pass. Inside a caller's transaction (e.g. a
 * REPEATABLE READ snapshot taken before a concurrent attach committed) the check could miss that
 * attach, so calling it there is a defect (DESIGN §4).
 *
 * Every writer participates: mutators, imports, scripts, and cascades all go through the row
 * trigger, which takes the engine lock before allocating a log position (§7).
 */
export const attach = (table: SyncTable.AnySyncTable, options?: { readonly schema?: string | undefined }) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    const relation = options?.schema === undefined ? sql(table.name) : sql`${sql(options.schema)}.${sql(table.name)}`
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService))) {
      return yield* Effect.die("attach must own its transaction; do not call it inside withTransaction")
    }
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`SET TRANSACTION ISOLATION LEVEL READ COMMITTED`
        // Engine key + 1 ("echosync" + 1): distinct from the position lock, so attaching does
        // not stall writers.
        yield* sql`SELECT pg_advisory_xact_lock(7305797848413072996)`
        // READ COMMITTED: a fresh statement snapshot after the lock sees any attach that held it.
        const target = yield* sql<{ schema: string; name: string }>`
          SELECT n.nspname AS schema, c.relname AS name FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.oid = ${options?.schema === undefined ? table.name : `${options.schema}.${table.name}`}::regclass`
        const clash = (yield* syncedTables).find((t) => t.name === target[0]?.name && t.schema !== target[0]?.schema)
        if (clash !== undefined) {
          return yield* new TableNameConflict({ table: clash.name, existingSchema: clash.schema })
        }
        yield* sql`
          CREATE OR REPLACE TRIGGER echo_sync_capture
          AFTER INSERT OR UPDATE OR DELETE ON ${relation}
          FOR EACH ROW EXECUTE FUNCTION echo_sync.echo_sync_capture_changes()`
        yield* sql`
          CREATE OR REPLACE TRIGGER echo_sync_reject_truncate
          BEFORE TRUNCATE ON ${relation}
          FOR EACH STATEMENT EXECUTE FUNCTION echo_sync.reject_truncate()`
      })
    )
  })

/** A table with change capture attached, as recorded in `echo_sync.log.table_name`. */
export interface SyncedTable {
  readonly schema: string
  readonly name: string
}

/** Relations with change capture attached, from the catalog. */
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

/** A registry table and the schema of its captured relation. */
export interface Resolved {
  readonly table: SyncTable.AnySyncTable
  readonly schema: string
}

/**
 * Registry tables that are attached, sorted by name (the walk order). A registry table without
 * capture is not synced: walking it would deliver rows whose later changes are never logged.
 * A captured relation outside the registry is unknown to clients and skipped (DESIGN §8).
 */
export const resolveTables = (registry: Registry.SyncRegistry) =>
  Effect.map(syncedTables, (captured) => {
    const schemaOf = new Map(captured.map((t) => [t.name, t.schema]))
    return registry.tables
      .flatMap((table): ReadonlyArray<Resolved> => {
        const schema = schemaOf.get(table.name)
        return schema === undefined ? [] : [{ table, schema }]
      })
      .toSorted((a, b) => (a.table.name < b.table.name ? -1 : a.table.name > b.table.name ? 1 : 0))
  })
