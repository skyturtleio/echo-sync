import * as PgTypes from "@effect/sql-pg/PgTypes"
import { Effect, Option, Schema } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import { Errors, type Store, type SyncTable } from "echo-sync"

/**
 * The transient store failure for a SQL error. The reason tag leads the message so the ledger
 * can tell a conflict (retried) from other transients without a second error channel.
 */
export const unavailable = (error: SqlError) =>
  new Errors.StoreUnavailable({ message: `${error.reason._tag}: ${error.message}` })

export const isConflictUnavailable = (error: Errors.StoreUnavailable) =>
  error.message.startsWith("SerializationError:") || error.message.startsWith("DeadlockError:")

/** Quoted SQL identifier. */
export const q = (name: string) => `"${name.replaceAll('"', '""')}"`

/**
 * Declared columns only, so a column the app added to the relation but not to the SyncTable
 * never leaves the database (DESIGN §11). Names are validated identifiers (core rejects others).
 */
export const selectList = (table: SyncTable.AnySyncTable, alias?: string) =>
  table.columns.map((c) => (alias === undefined ? q(c.name) : `${alias}.${q(c.name)}`)).join(", ")

/** Postgres row → wire row through the table's own codec. Stored rows are trusted: a failure is a defect. */
export const decodeRow = (table: SyncTable.AnySyncTable, row: unknown) =>
  Effect.orDie(Schema.decodeUnknownEffect(table.postgres.row)(row))

/** Wire value → bind parameter for one column (JSON columns are bound explicitly as jsonb). */
const bindColumn = (column: SyncTable.Column, value: Schema.Json) => {
  const pg = Schema.encodeSync(column.postgres)(value)
  return column.kind === "json" && pg !== null ? PgTypes.jsonb(pg) : pg
}

/**
 * Postgres `RowStore` (DESIGN §4) over the relations of `schemaOf` (SyncTable name → schema).
 * Runs inside the caller's transaction; maps every SQL failure to `StoreUnavailable`.
 */
const decodeAll = (table: SyncTable.AnySyncTable, rows: ReadonlyArray<unknown>) =>
  Effect.forEach(rows, (row) => decodeRow(table, row))

export const make = (schemaOf: ReadonlyMap<string, string>): Effect.Effect<Store.RowStore, never, SqlClient> => {
  // A table without a mapping is not attached (or not in the registry): writing it would commit
  // rows no client ever hears about, so it is a defect, never a guess at a schema.
  const relation = (table: SyncTable.AnySyncTable) => {
    const schema = schemaOf.get(table.name)
    return schema === undefined ? undefined : `${q(schema)}.${q(table.name)}`
  }
  return Effect.gen(function* () {
    const sql = yield* SqlClient
    const run = (statement: string, params: ReadonlyArray<unknown>) =>
      sql.unsafe<Record<string, unknown>>(statement, params).pipe(Effect.mapError(unavailable))
    /** `statement` receives the table's quoted relation; an unattached table dies before any SQL. */
    const query = (
      table: SyncTable.AnySyncTable,
      statement: (relation: string) => string,
      params: ReadonlyArray<unknown>
    ) => {
      const rel = relation(table)
      return rel === undefined
        ? Effect.die(`RowStore: table ${table.name} is not an attached registry table`)
        : run(statement(rel), params)
    }

    const rowStore: Store.RowStore = {
      read: (table, id) =>
        Effect.flatMap(
          query(table, (rel) => `SELECT ${selectList(table)} FROM ${rel} WHERE "id" = $1`, [id]),
          (rows) =>
            rows[0] === undefined ? Effect.succeed(Option.none()) : Effect.map(decodeRow(table, rows[0]), Option.some)
        ),
      write: (table, id, row) => {
        if (Option.isNone(row)) return Effect.asVoid(query(table, (rel) => `DELETE FROM ${rel} WHERE "id" = $1`, [id]))
        const params = table.columns.map((c) => bindColumn(c, row.value[c.name] ?? null))
        const names = table.columns.map((c) => q(c.name))
        return Effect.asVoid(
          query(
            table,
            (rel) => `INSERT INTO ${rel} (${names.join(", ")}) VALUES (${names.map((_, i) => `$${i + 1}`).join(", ")})
             ON CONFLICT ("id") DO UPDATE SET ${names.map((n) => `${n} = EXCLUDED.${n}`).join(", ")}`,
            params
          )
        )
      },
      range: (table, plan) => {
        const where: Array<string> = []
        const params: Array<unknown> = []
        plan.eq.forEach((value, i) => {
          const c = plan.index.key[i]
          if (c === undefined) return
          if (value === null) where.push(`${q(c.name)} IS NULL`)
          else {
            params.push(bindColumn(c, value))
            where.push(`${q(c.name)} = $${params.length}`)
          }
        })
        const ranged = plan.index.key[plan.eq.length]
        for (const [bound, strict, loose] of [
          [plan.lower, ">", ">="],
          [plan.upper, "<", "<="]
        ] as const) {
          if (Option.isSome(bound) && ranged !== undefined) {
            params.push(bindColumn(ranged, bound.value.value))
            where.push(`${q(ranged.name)} ${bound.value.inclusive ? loose : strict} $${params.length}`)
          }
        }
        const dir = plan.order === "asc" ? "ASC NULLS FIRST" : "DESC NULLS LAST"
        const order = plan.index.key.map((c) => `${q(c.name)} ${dir}`).join(", ")
        return Effect.flatMap(
          query(
            table,
            (rel) =>
              `SELECT ${selectList(table)} FROM ${rel}${where.length > 0 ? ` WHERE ${where.join(" AND ")}` : ""} ORDER BY ${order}`,
            params
          ),
          (rows) => decodeAll(table, rows)
        )
      },
      scan: (table) =>
        Effect.flatMap(
          query(table, (rel) => `SELECT ${selectList(table)} FROM ${rel} ORDER BY "id"`, []),
          (rows) => decodeAll(table, rows)
        )
    }
    return rowStore
  })
}
