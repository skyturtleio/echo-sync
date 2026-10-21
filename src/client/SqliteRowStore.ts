/**
 * The SQLite `ReplicaRowStore` (DESIGN §4, §8, §10) over the leader's single connection.
 *
 * Contract (core `Store.RowStore`): every method runs inside a transaction the caller owns and never
 * begins or commits one; no authorization; the only typed failure is the transient `StoreUnavailable`.
 * A SQL error the driver marks retryable maps to `StoreUnavailable`; any other SQL error (a constraint
 * violation, a syntax error) or a stored row that does not decode is a bug or corruption, so a defect.
 *
 * Values cross into SQL only through the derived codecs: rows through `table.sqlite.row`, index plan
 * values (wire values) through their key column's `column.sqlite`, so booleans bind as 0/1 and
 * timestamps as epoch ms. Range order is written out (`ASC NULLS FIRST` / `DESC NULLS LAST`) to match
 * core's single comparator; TEXT compares BINARY, which is code-point order for UTF-8.
 */
import { Effect, Option, Schema } from "effect"
import * as SqlClient from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import { Errors, type Registry, type Store, type SyncTable } from "../core/index.ts"

type Table = SyncTable.AnySyncTable
type Value = SyncTable.SqliteValue

const q = (name: string) => `"${name.replaceAll('"', '""')}"`

const unavailable = <A, R>(effect: Effect.Effect<A, SqlError, R>): Effect.Effect<A, Errors.StoreUnavailable, R> =>
  Effect.catch(effect, (error) =>
    error.isRetryable ? Effect.fail(new Errors.StoreUnavailable({ message: error.message })) : Effect.die(error)
  )

const decodeRows = (table: Table, rows: ReadonlyArray<unknown>) =>
  Effect.forEach(rows, (row) => Effect.orDie(Schema.decodeUnknownEffect(table.sqlite.row)(row)))

const bind = (column: SyncTable.Column, value: Schema.Json): Effect.Effect<Value> =>
  Effect.orDie(Schema.encodeEffect(column.sqlite)(value))

export const make = (sql: SqlClient.SqlClient, registry: Registry.SyncRegistry): Store.ReplicaRowStore => {
  const select = (table: Table, where: string, params: ReadonlyArray<Value>, orderBy: string) =>
    unavailable(sql.unsafe(`SELECT * FROM ${q(table.name)}${where} ORDER BY ${orderBy}`, params)).pipe(
      Effect.flatMap((rows) => decodeRows(table, rows))
    )

  return {
    read: (table, id) =>
      select(table, ` WHERE "id" = ?`, [id], `"id"`).pipe(Effect.map((rows) => Option.fromNullishOr(rows[0]))),

    write: (table, id, row) =>
      Option.isNone(row)
        ? Effect.asVoid(unavailable(sql.unsafe(`DELETE FROM ${q(table.name)} WHERE "id" = ?`, [id])))
        : Effect.gen(function* () {
            const stored = yield* Effect.orDie(Schema.encodeEffect(table.sqlite.row)({ ...row.value, id }))
            const names = table.columns.map((c) => c.name)
            const updates = names.filter((n) => n !== "id").map((n) => `${q(n)} = excluded.${q(n)}`)
            yield* unavailable(
              sql.unsafe(
                `INSERT INTO ${q(table.name)} (${names.map(q).join(", ")}) VALUES (${names.map(() => "?").join(", ")})
                 ON CONFLICT ("id") DO ${updates.length === 0 ? "NOTHING" : `UPDATE SET ${updates.join(", ")}`}`,
                names.map((n) => stored[n] ?? null)
              )
            )
          }),

    range: (table, plan) =>
      Effect.gen(function* () {
        const where: Array<string> = []
        const params: Array<Value> = []
        for (const [i, value] of plan.eq.entries()) {
          const column = plan.index.key[i]
          if (column === undefined)
            return yield* Effect.die(new Error(`range: eq longer than index ${plan.index.name}`))
          if (value === null) where.push(`${q(column.name)} IS NULL`)
          else {
            params.push(yield* bind(column, value))
            where.push(`${q(column.name)} = ?`)
          }
        }
        const ranged = plan.index.key[plan.eq.length]
        if (ranged !== undefined) {
          for (const [bound, strict, loose] of [
            [plan.lower, ">", ">="],
            [plan.upper, "<", "<="]
          ] as const) {
            if (Option.isNone(bound)) continue
            params.push(yield* bind(ranged, bound.value.value))
            // A comparison against a bound never matches NULL, as `planMatches` requires.
            where.push(`${q(ranged.name)} ${bound.value.inclusive ? loose : strict} ?`)
          }
        }
        const dir = plan.order === "asc" ? "ASC NULLS FIRST" : "DESC NULLS LAST"
        return yield* select(
          table,
          where.length === 0 ? "" : ` WHERE ${where.join(" AND ")}`,
          params,
          plan.index.key.map((c) => `${q(c.name)} ${dir}`).join(", ")
        )
      }),

    scan: (table) => select(table, "", [], `"id"`),

    purgeScope: (scopeId) =>
      Effect.forEach(
        registry.tables,
        (table) => unavailable(sql.unsafe(`DELETE FROM ${q(table.name)} WHERE "access_scope_id" = ?`, [scopeId])),
        { discard: true }
      ),

    // Only the registry's replicated tables: echo_outbox, echo_staged and echo_meta are untouched.
    clearAll: Effect.forEach(registry.tables, (table) => unavailable(sql.unsafe(`DELETE FROM ${q(table.name)}`)), {
      discard: true
    })
  }
}

const StoredTables = Schema.fromJsonString(Schema.Array(Schema.String))

/**
 * Bring the replica tables in line with `registry` (DESIGN §9, §11), inside one transaction.
 * - First run: create every table and index.
 * - `dataHash` changed: drop the previous replica tables and recreate them empty (rebuild the replica,
 *   keep the outbox). Returns `rebuilt: true`; the caller must reset the sync cursor to `null` in the
 *   same transaction it uses next, and clear pending undo images (they describe dropped rows).
 * - Only `indexHash` changed: drop and recreate the declared indexes; rows stay (no re-download).
 */
export const migrate = (
  registry: Registry.SyncRegistry
): Effect.Effect<{ readonly rebuilt: boolean }, SqlError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`CREATE TABLE IF NOT EXISTS echo_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT`
        const meta = new Map(
          (yield* sql<{ key: string; value: string }>`SELECT key, value FROM echo_meta WHERE key LIKE 'replica_%'`).map(
            (r) => [r.key, r.value]
          )
        )
        const put = (key: string, value: string) =>
          sql`INSERT INTO echo_meta (key, value) VALUES (${key}, ${value})
            ON CONFLICT (key) DO UPDATE SET value = excluded.value`
        const createIndexes = Effect.forEach(
          registry.tables.flatMap((t) => t.sqlite.createIndexes),
          (ddl) => sql.unsafe(ddl)
        )
        const previous = meta.get("replica_tables")
        const fresh = previous === undefined
        const rebuilt = !fresh && meta.get("replica_data_hash") !== registry.dataHash

        if (fresh || rebuilt) {
          if (rebuilt) {
            const names = yield* Effect.orDie(Schema.decodeEffect(StoredTables)(previous))
            for (const name of names) yield* sql.unsafe(`DROP TABLE IF EXISTS ${q(name)}`)
          }
          for (const table of registry.tables) yield* sql.unsafe(table.sqlite.createTable)
          yield* createIndexes
        } else if (meta.get("replica_index_hash") !== registry.indexHash) {
          const placeholders = registry.tables.map(() => "?").join(", ")
          const indexes = yield* sql.unsafe<{ name: string }>(
            `SELECT name FROM sqlite_schema WHERE type = 'index' AND sql IS NOT NULL AND tbl_name IN (${placeholders})`,
            registry.tables.map((t) => t.name)
          )
          for (const { name } of indexes) yield* sql.unsafe(`DROP INDEX ${q(name)}`)
          yield* createIndexes
        }
        yield* put(
          "replica_tables",
          yield* Effect.orDie(Schema.encodeEffect(StoredTables)(registry.tables.map((t) => t.name)))
        )
        yield* put("replica_data_hash", registry.dataHash)
        yield* put("replica_index_hash", registry.indexHash)
        return { rebuilt }
      })
    )
  })
