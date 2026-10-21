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
import type * as SqlClient from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import { Errors, ScopeGrants, type Registry, Store, type SyncTable } from "echo-sync"

type Table = SyncTable.AnySyncTable
type Value = SyncTable.SqliteValue

const q = (name: string) => `"${name.replaceAll('"', '""')}"`

/** Retryable SQL errors become the transient `StoreUnavailable`; any other SQL error is a defect. */
export const unavailable = <A, R>(
  effect: Effect.Effect<A, SqlError, R>
): Effect.Effect<A, Errors.StoreUnavailable, R> =>
  Effect.catch(effect, (error) =>
    error.isRetryable ? Effect.fail(new Errors.StoreUnavailable({ message: error.message })) : Effect.die(error)
  )

const decodeRows = (table: Table, rows: ReadonlyArray<unknown>) =>
  Effect.forEach(rows, (row) => Effect.orDie(Schema.decodeUnknownEffect(table.sqlite.row)(row)))

const bind = (column: SyncTable.Column, value: Schema.Json): Effect.Effect<Value> =>
  Effect.orDie(Schema.encodeEffect(column.sqlite)(value))

export const make = (sql: SqlClient.SqlClient, registry: Registry.SyncRegistry): Store.ReplicaRowStore => {
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
        if (Option.isSome(plan.readableBy)) {
          // Readable: a grant for one of the principals on the row's scope (any role satisfies
          // reader), found through scope_grants' unique (access_scope_id, principal_id).
          params.push(...plan.readableBy.value)
          where.push(
            `EXISTS (SELECT 1 FROM ${q(ScopeGrants.ScopeGrants.name)} AS g WHERE g."access_scope_id" = t."access_scope_id" AND g."principal_id" IN (${plan.readableBy.value.map(() => "?").join(", ")}))`
          )
        }
        const dir = plan.order === "asc" ? "ASC NULLS FIRST" : "DESC NULLS LAST"
        return yield* select(
          table,
          where.length === 0 ? "" : ` WHERE ${where.join(" AND ")}`,
          params,
          plan.index.key.map((c) => `t.${q(c.name)} ${dir}`).join(", "),
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
    // live queries (DESIGN §10) would never see the rows go when nothing is installed in their place.
    clearAll: Effect.forEach(
      registry.tables,
      (table) => unavailable(sql.unsafe(`DELETE FROM ${q(table.name)} WHERE true`)),
      { discard: true }
    )
  }
}
