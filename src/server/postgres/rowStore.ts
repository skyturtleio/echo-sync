import * as PgTypes from "@effect/sql-pg/PgTypes"
import { Array as Arr, Effect, Option, Schema, String as Str } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import { Errors, ScopeGrants, Store, type SyncTable } from "echo-sync"
import * as Keyset from "../../internal/Keyset.ts"
import * as Telemetry from "../../internal/Telemetry.ts"

/** The adapter, not diagnostic text, decides whether an unavailable store is a conflict. */
export const unavailable = (error: SqlError) => {
  const failure = new Errors.StoreUnavailable({
    message: `${error.reason._tag}: ${error.message}`,
    reason:
      error.reason._tag === "SerializationError"
        ? "SerializationConflict"
        : error.reason._tag === "DeadlockError"
          ? "Deadlock"
          : "Unavailable"
  })
  // Diagnostic only: not a schema field, so driver details cannot cross a wire codec.
  Object.defineProperty(failure, "cause", { value: error })
  return failure
}

export const isConflictUnavailable = (error: Errors.StoreUnavailable) =>
  error.reason === "SerializationConflict" || error.reason === "Deadlock"

/** Quoted SQL identifier. */
export const q = (name: string) => `"${Str.replaceAll('"', '""')(name)}"`

/**
 * Declared columns only, so a column the app added to the relation but not to the SyncTable
 * never leaves the database (SPEC §3). Names are validated identifiers (core rejects others).
 */
export const selectList = (table: SyncTable.AnySyncTable, alias?: string) =>
  Arr.join(
    Arr.map(table.columns, (c) => (alias === undefined ? q(c.name) : `${alias}.${q(c.name)}`)),
    ", "
  )

/** Postgres row → wire row through the table's own codec. Stored rows are trusted: a failure is a defect. */
export const decodeRow = (table: SyncTable.AnySyncTable, row: unknown) =>
  Effect.suspend(() => Store.decodeStored(table.postgres.row, table.name, row))

/** Driver wrappers apply after encoding, once per column, without running another codec. */
const parameter = (column: SyncTable.Column, value: SyncTable.PgValue) =>
  column.kind === "json" && value !== null ? PgTypes.jsonb(value) : value

const bindColumn = Effect.fnUntraced(function* (
  table: SyncTable.AnySyncTable,
  column: SyncTable.Column,
  value: Schema.Json
) {
  const stored = yield* Schema.encodeEffect(column.postgres)(value).pipe(
    Effect.catch((cause) =>
      Effect.die(new Error(`RowStore.range: cannot encode "${table.name}" column "${column.name}"`, { cause }))
    )
  )
  return parameter(column, stored)
})

/**
 * Postgres `RowStore` (SPEC §4) over the relations of `schemaOf` (SyncTable name → schema).
 * Runs inside the caller's transaction. Writes preserve constraint failures; reads and all
 * other SQL failures retain the transient `StoreUnavailable` mapping.
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
    /** `statement` receives the table's quoted relation; an unattached table dies before any SQL. */
    const query = (
      table: SyncTable.AnySyncTable,
      statement: (relation: string) => string,
      params: ReadonlyArray<unknown>
    ) => {
      const rel = relation(table)
      return rel === undefined
        ? Effect.die(`RowStore: table ${table.name} is not an attached registry table`)
        : sql.unsafe<Record<string, unknown>>(statement(rel), params).pipe(Telemetry.boundary)
    }

    const rowStore: Store.RowStore = {
      read: (table, id) =>
        Effect.flatMap(
          query(table, (rel) => `SELECT ${selectList(table)} FROM ${rel} WHERE "id" = $1`, [id]).pipe(
            Effect.mapError(unavailable)
          ),
          (rows) =>
            rows[0] === undefined ? Effect.succeed(Option.none()) : Effect.map(decodeRow(table, rows[0]), Option.some)
        ),
      write: (table, id, row) =>
        Effect.gen(function* () {
          if (Option.isNone(row))
            return yield* Effect.asVoid(query(table, (rel) => `DELETE FROM ${rel} WHERE "id" = $1`, [id]))
          const stored = yield* Schema.encodeEffect(table.postgres.row)(row.value).pipe(
            Effect.catch((cause) =>
              Effect.die(new Error(`RowStore.write: cannot encode "${table.name}" row "${id}"`, { cause }))
            )
          )
          const params = Arr.map(table.columns, (c) => parameter(c, stored[c.name] ?? null))
          const names = Arr.map(table.columns, (c) => q(c.name))
          yield* query(
            table,
            (rel) => `INSERT INTO ${rel} (${Arr.join(names, ", ")}) VALUES (${Arr.join(
              Arr.map(names, (_, i) => `$${i + 1}`),
              ", "
            )})
             ON CONFLICT ("id") DO UPDATE SET ${Arr.join(
               Arr.map(names, (n) => `${n} = EXCLUDED.${n}`),
               ", "
             )}`,
            params
          )
        }).pipe(
          Effect.mapError((error) =>
            error.reason._tag === "UniqueViolation" || error.reason._tag === "ConstraintError"
              ? new Errors.ConstraintViolated({
                  kind: error.reason._tag === "UniqueViolation" ? "unique" : "other",
                  constraint: error.reason._tag === "UniqueViolation" ? error.reason.constraint : null,
                  message: error.message
                })
              : unavailable(error)
          )
        ),
      range: Effect.fnUntraced(function* (table, plan) {
        yield* Store.checkLimit(plan.limit)
        const where: Array<string> = []
        const params: Array<unknown> = []
        for (const [i, value] of plan.eq.entries()) {
          const c = plan.index.key[i]
          if (c === undefined) return yield* Effect.die(new Error(`range: eq longer than index ${plan.index.name}`))
          if (value === null) where.push(`${q(c.name)} IS NULL`)
          else {
            params.push(yield* bindColumn(table, c, value))
            where.push(`${q(c.name)} = $${params.length}`)
          }
        }
        const ranged = plan.index.key[plan.eq.length]
        for (const [bound, strict, loose] of [
          [plan.lower, ">", ">="],
          [plan.upper, "<", "<="]
        ] as const) {
          if (Option.isSome(bound) && ranged !== undefined) {
            params.push(yield* bindColumn(table, ranged, bound.value.value))
            where.push(`${q(ranged.name)} ${bound.value.inclusive ? loose : strict} $${params.length}`)
          }
        }
        const bindings: Array<{ readonly column: SyncTable.Column; readonly value: Store.KeyValue }> = []
        where.push(
          ...Keyset.predicates(plan, (column, value) => {
            bindings.push({ column, value })
            return `$${params.length + bindings.length}`
          })
        )
        for (const { column, value } of bindings) params.push(yield* bindColumn(table, column, value))
        if (plan.text !== undefined) {
          params.push(plan.text.needle)
          where.push(`strpos(t.${q(plan.text.column.name)}, $${params.length}) > 0`)
        }
        if (Option.isSome(plan.readableBy)) {
          // Readable: a grant for one of the principals on the row's scope (any role satisfies
          // reader), through scope_grants' unique (access_scope_id, principal_id), in this snapshot.
          const grants = relation(ScopeGrants.ScopeGrants)
          if (grants === undefined) return yield* Effect.die("RowStore: scope_grants is not mapped to a schema")
          params.push([...plan.readableBy.value])
          where.push(
            `EXISTS (SELECT 1 FROM ${grants} AS g WHERE g."access_scope_id" = t."access_scope_id" AND g."principal_id" = ANY($${params.length}::text[]))`
          )
        }
        let limit = ""
        if (Option.isSome(plan.limit)) {
          const n = plan.limit.value
          params.push(n)
          limit = ` LIMIT $${params.length}`
        }
        const dir = plan.order === "asc" ? "ASC NULLS FIRST" : "DESC NULLS LAST"
        const order = Arr.join(
          Arr.map(plan.index.key, (c) => `t.${q(c.name)} ${dir}`),
          ", "
        )
        const rows = yield* query(
          table,
          (rel) =>
            `SELECT ${selectList(table, "t")} FROM ${rel} AS t${where.length > 0 ? ` WHERE ${Arr.join(where, " AND ")}` : ""} ORDER BY ${order}${limit}`,
          params
        ).pipe(Effect.mapError(unavailable))
        return yield* decodeAll(table, rows)
      }),
      scan: (table) =>
        Effect.flatMap(
          query(table, (rel) => `SELECT ${selectList(table)} FROM ${rel} ORDER BY "id"`, []).pipe(
            Effect.mapError(unavailable)
          ),
          (rows) => decodeAll(table, rows)
        )
    }
    return rowStore
  })
}
