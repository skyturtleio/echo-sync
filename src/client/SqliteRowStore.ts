/**
 * The SQLite `ReplicaRowStore` (SPEC §4, §9, §3) over the leader's single connection.
 *
 * Contract (core `Store.RowStore`): every method runs inside a transaction the caller owns and never
 * begins or commits one; no authorization. Retryable SQL errors map to `StoreUnavailable`;
 * write constraints map to `ConstraintViolated` for local rejection/undo. Other SQL errors and
 * stored rows that do not decode are bugs or corruption, so defects.
 *
 * Values cross into SQL only through the derived codecs: rows through `table.sqlite.row`, index plan
 * values (wire values) through their key column's `column.sqlite`, so booleans bind as 0/1 and
 * timestamps as epoch ms. Range order is written out (`ASC NULLS FIRST` / `DESC NULLS LAST`) to match
 * core's single comparator; TEXT compares BINARY, which is code-point order for UTF-8.
 */
import { Array as Arr, Effect, Option, Schema, String as Str } from "effect"
import type * as SqlClient from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import { Errors, ScopeGrants, type Registry, Store, type SyncTable } from "echo-sync"
import * as Keyset from "../internal/Keyset.ts"

type Table = SyncTable.AnySyncTable
type Value = SyncTable.SqliteValue

const q = (name: string) => `"${Str.replaceAll('"', '""')(name)}"`

/** Retryable SQL errors become the transient `StoreUnavailable`; any other SQL error is a defect. */
export const unavailable = <A, R>(
  effect: Effect.Effect<A, SqlError, R>
): Effect.Effect<A, Errors.StoreUnavailable, R> =>
  Effect.catch(effect, (error) =>
    error.isRetryable ? Effect.fail(new Errors.StoreUnavailable({ message: error.message })) : Effect.die(error)
  )

// A declared constraint may reject optimistic replay after a schema deploy. Classify at the
// write boundary so Mutation.runHandler restores partial writes without swallowing app defects.
const writeStatement = <A, R>(effect: Effect.Effect<A, SqlError, R>) =>
  Effect.catch(effect, (error): Effect.Effect<never, Errors.ConstraintViolated | Errors.StoreUnavailable> =>
    error.reason._tag === "UniqueViolation" || error.reason._tag === "ConstraintError"
      ? Effect.fail(
          new Errors.ConstraintViolated({
            kind: error.reason._tag === "UniqueViolation" ? "unique" : "other",
            constraint: error.reason._tag === "UniqueViolation" ? error.reason.constraint : null,
            message: error.message
          })
        )
      : unavailable(Effect.fail(error))
  )

const decodeRows = (table: Table, rows: ReadonlyArray<unknown>) =>
  Effect.forEach(rows, (row) => Store.decodeStored(table.sqlite.row, table.name, row))

const bind = Effect.fnUntraced(function* (table: Table, column: SyncTable.Column, value: Schema.Json) {
  return yield* Schema.encodeEffect(column.sqlite)(value).pipe(
    Effect.catch((cause) =>
      Effect.die(new Error(`SqliteRowStore.range: cannot encode "${table.name}" column "${column.name}"`, { cause }))
    )
  )
})

// MAX_VARIABLE_NUMBER in the pinned wa-sqlite build (PRAGMA compile_options), also used by OPFS.
const maxParameters = 32766

const upsert = (table: Table) => {
  const names = Arr.map(table.columns, (c) => c.name)
  const updates = Arr.map(
    Arr.filter(names, (n) => n !== "id"),
    (n) => `${q(n)} = excluded.${q(n)}`
  )
  const prefix = `INSERT INTO ${q(table.name)} (${Arr.join(Arr.map(names, q), ", ")}) VALUES `
  const tuple = `(${Arr.join(
    Arr.map(names, () => "?"),
    ", "
  )})`
  const suffix = ` ON CONFLICT ("id") DO ${updates.length === 0 ? "NOTHING" : `UPDATE SET ${Arr.join(updates, ", ")}`}`
  const single = prefix + tuple + suffix
  // One bounded cache entry for the latest batch size, in addition to the single-row statement.
  let lastSize = 0
  let lastSql = ""
  return {
    names,
    batchSize: Math.floor(maxParameters / names.length),
    encode: Schema.encodeEffect(table.sqlite.row),
    encodeMany: Schema.encodeEffect(Schema.Array(table.sqlite.row)),
    statement: (size: number) => {
      if (size === 1) return single
      if (size !== lastSize) {
        lastSql =
          prefix +
          Arr.join(
            Arr.makeBy(size, () => tuple),
            ", "
          ) +
          suffix
        lastSize = size
      }
      return lastSql
    }
  }
}

export const make = (sql: SqlClient.SqlClient, registry: Registry.SyncRegistry): Store.ReplicaRowStore => {
  const upserts = new WeakMap<Table, ReturnType<typeof upsert>>()
  const upsertFor = (table: Table) => {
    const existing = upserts.get(table)
    if (existing !== undefined) return existing
    const created = upsert(table)
    upserts.set(table, created)
    return created
  }
  // The table is aliased `t`, so a readable-scope semi-join on scope_grants can name it.
  const select = (table: Table, where: string, params: ReadonlyArray<Value>, orderBy: string, limit = "") =>
    unavailable(sql.unsafe(`SELECT t.* FROM ${q(table.name)} AS t${where} ORDER BY ${orderBy}${limit}`, params)).pipe(
      Effect.flatMap((rows) => decodeRows(table, rows))
    )

  return {
    read: (table, id) =>
      select(table, ` WHERE "id" = ?`, [id], `"id"`).pipe(Effect.map((rows) => Option.fromNullishOr(rows[0]))),

    write: (table, id, row) =>
      Option.isNone(row)
        ? Effect.asVoid(unavailable(sql.unsafe(`DELETE FROM ${q(table.name)} WHERE "id" = ?`, [id])))
        : Effect.gen(function* () {
            const plan = upsertFor(table)
            const stored = yield* plan
              .encode({ ...row.value, id })
              .pipe(
                Effect.catch((cause) =>
                  Effect.die(new Error(`SqliteRowStore.write: cannot encode "${table.name}" row "${id}"`, { cause }))
                )
              )
            yield* writeStatement(
              sql.unsafe(
                plan.statement(1),
                Arr.map(plan.names, (n) => stored[n] ?? null)
              )
            )
          }),

    writeMany: Effect.fnUntraced(function* (table, rows) {
      const plan = upsertFor(table)
      for (let offset = 0; offset < rows.length; offset += plan.batchSize) {
        const stored = yield* plan.encodeMany(rows.slice(offset, offset + plan.batchSize)).pipe(
          Effect.catch((cause) =>
            Effect.die(
              new Error(`SqliteRowStore.writeMany: cannot encode "${table.name}" batch starting at row ${offset}`, {
                cause
              })
            )
          )
        )
        yield* writeStatement(
          sql.unsafe(
            plan.statement(stored.length),
            Arr.flatMap(stored, (row) => Arr.map(plan.names, (n) => row[n] ?? null))
          )
        )
      }
    }),

    range: (table, plan) =>
      Effect.gen(function* () {
        // Validated first: the limit is then a safe integer, written into the SQL text.
        yield* Store.checkLimit(plan.limit)
        const where: Array<string> = []
        const params: Array<Value> = []
        for (const [i, value] of plan.eq.entries()) {
          const column = plan.index.key[i]
          if (column === undefined)
            return yield* Effect.die(new Error(`range: eq longer than index ${plan.index.name}`))
          if (value === null) where.push(`${q(column.name)} IS NULL`)
          else {
            params.push(yield* bind(table, column, value))
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
            params.push(yield* bind(table, ranged, bound.value.value))
            // A comparison against a bound never matches NULL, as `planMatches` requires.
            where.push(`${q(ranged.name)} ${bound.value.inclusive ? loose : strict} ?`)
          }
        }
        const bindings: Array<{ readonly column: SyncTable.Column; readonly value: Store.KeyValue }> = []
        where.push(
          ...Keyset.predicates(plan, (column, value) => {
            bindings.push({ column, value })
            return `?${params.length + bindings.length}`
          })
        )
        for (const { column, value } of bindings) params.push(yield* bind(table, column, value))
        if (plan.text !== undefined) {
          params.push(plan.text.needle)
          where.push(`instr(t.${q(plan.text.column.name)}, ?) > 0`)
        }
        if (Option.isSome(plan.readableBy)) {
          // Readable: a grant for one of the principals on the row's scope (any role satisfies
          // reader), found through scope_grants' unique (access_scope_id, principal_id).
          params.push(...plan.readableBy.value)
          where.push(
            `EXISTS (SELECT 1 FROM ${q(ScopeGrants.ScopeGrants.name)} AS g WHERE g."access_scope_id" = t."access_scope_id" AND g."principal_id" IN (${Arr.join(
              Arr.map(plan.readableBy.value, () => "?"),
              ", "
            )}))`
          )
        }
        const dir = plan.order === "asc" ? "ASC NULLS FIRST" : "DESC NULLS LAST"
        return yield* select(
          table,
          where.length === 0 ? "" : ` WHERE ${Arr.join(where, " AND ")}`,
          params,
          Arr.join(
            Arr.map(plan.index.key, (c) => `t.${q(c.name)} ${dir}`),
            ", "
          ),
          Option.match(plan.limit, { onNone: () => "", onSome: (n) => ` LIMIT ${n}` })
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
    // `WHERE true` defeats SQLite's truncate optimization, which deletes without firing the update hook:
    // live queries (INV-BRW-6) would never see the rows go when nothing is installed in their place.
    clearAll: Effect.forEach(
      registry.tables,
      (table) => unavailable(sql.unsafe(`DELETE FROM ${q(table.name)} WHERE true`)),
      { discard: true }
    )
  }
}
