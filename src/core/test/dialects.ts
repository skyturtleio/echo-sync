/**
 * Test-only harness: real SQLite (node:sqlite) and real Postgres (`ECHO_PG_URL`, skipped when
 * unset) holding tables created from `SyncTable` DDL. Rows go through the table's own dialect
 * codecs, so a round-trip exercises the declared column types, not an in-memory copy.
 */
import * as PgClient from "@effect/sql-pg/PgClient"
import * as PgTypes from "@effect/sql-pg/PgTypes"
import {
  Array as Arr,
  Clock,
  Config,
  Effect,
  Option,
  Random,
  Record as Rec,
  Redacted,
  Schema,
  String as Str
} from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { DatabaseSync, type SQLInputValue } from "node:sqlite"
import type { IndexRangePlan } from "../Store.ts"
import type { AnySyncTable, Column, EncodedRow, PgValue } from "../SyncTable.ts"

export const pgUrl = Effect.runSync(Config.String("ECHO_PG_URL").pipe(Config.withDefault("")))
export const pgAvailable = pgUrl !== ""

export interface Dialect {
  readonly name: "sqlite" | "postgres"
  readonly create: (table: AnySyncTable) => Promise<void>
  readonly insert: (table: AnySyncTable, row: EncodedRow) => Promise<void>
  readonly readAll: (table: AnySyncTable) => Promise<ReadonlyArray<EncodedRow>>
  /** Executes `plan` as SQL following the SPEC §3 rules (NULLS FIRST asc / LAST desc). */
  readonly range: (table: AnySyncTable, plan: IndexRangePlan) => Promise<ReadonlyArray<string>>
  readonly close: () => Promise<void>
}

const q = (name: string) => `"${name}"`

const planSql = <V>(
  plan: IndexRangePlan,
  table: AnySyncTable,
  param: (i: number) => string,
  bind: (column: Column, value: string | number | boolean) => V
) => {
  const where: Array<string> = []
  const params: Array<V> = []
  Arr.forEach(plan.eq, (value, i) => {
    const c = plan.index.key[i]
    if (c === undefined) throw new Error("eq longer than key")
    if (value === null) where.push(`${q(c.name)} IS NULL`)
    else {
      params.push(bind(c, value))
      where.push(`${q(c.name)} = ${param(params.length)}`)
    }
  })
  const ranged = plan.index.key[plan.eq.length]
  for (const [bound, strict, loose] of [
    [plan.lower, ">", ">="],
    [plan.upper, "<", "<="]
  ] as const) {
    if (bound._tag === "Some" && ranged !== undefined) {
      params.push(bind(ranged, bound.value.value))
      where.push(`${q(ranged.name)} ${bound.value.inclusive ? loose : strict} ${param(params.length)}`)
    }
  }
  const dir = plan.order === "asc" ? "ASC NULLS FIRST" : "DESC NULLS LAST"
  const sql = `SELECT ${q("id")} FROM ${q(table.name)}${where.length ? ` WHERE ${Arr.join(where, " AND ")}` : ""} ORDER BY ${Arr.join(
    Arr.map(plan.index.key, (c) => `${q(c.name)} ${dir}`),
    ", "
  )}`
  return { sql, params }
}

export const sqlite = (): Dialect => {
  const db = new DatabaseSync(":memory:")
  return {
    name: "sqlite",
    create: (table) =>
      Effect.runPromise(
        Effect.sync(() => {
          db.exec(table.sqlite.createTable)
          for (const ddl of table.sqlite.createIndexes) db.exec(ddl)
        })
      ),
    insert: (table, row) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const stored = yield* Schema.encodeEffect(table.sqlite.row)(row)
          const cols = Rec.keys(stored)
          const values: Array<SQLInputValue> = Arr.map(cols, (c) => stored[c] ?? null)
          db.prepare(
            `INSERT INTO ${q(table.name)} (${Arr.join(Arr.map(cols, q), ", ")}) VALUES (${Arr.join(
              Arr.map(cols, () => "?"),
              ", "
            )})`
          ).run(...values)
        })
      ),
    readAll: (table) =>
      Effect.runPromise(
        Effect.suspend(() =>
          Effect.forEach(db.prepare(`SELECT * FROM ${q(table.name)} ORDER BY ${q("id")}`).all(), (row) =>
            Schema.decodeUnknownEffect(table.sqlite.row)(row)
          )
        )
      ),
    range: (table, plan) =>
      Effect.runPromise(
        Effect.sync(() => {
          const { sql, params } = planSql(
            plan,
            table,
            () => "?",
            // oxlint-disable-next-line effecttsgo/schema-sync -- The synchronous SQL builder binds already validated scalar keys, preserving codec defects.
            (c, v): SQLInputValue => Schema.encodeSync(c.sqlite)(v)
          )
          return Arr.map(db.prepare(sql).all(...params), (r) => String(r["id"]))
        })
      ),
    close: () => Effect.runPromise(Effect.sync(() => db.close()))
  }
}

/** Binds a dialect value for sql-pg: JSON columns must be bound explicitly as jsonb. */
const pgParam = (table: AnySyncTable, column: string, value: PgValue): unknown =>
  Option.getOrUndefined(Arr.findFirst(table.columns, (c) => c.name === column))?.kind === "json" && value !== null
    ? PgTypes.jsonb(value)
    : value

const run = <A, E>(f: (sql: SqlClient) => Effect.Effect<A, E>) =>
  Effect.gen(function* () {
    return yield* f(yield* SqlClient)
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Promise fixture runner owns a fresh SQL pool for this operation and closes it on completion.
  }).pipe(Effect.provide(PgClient.layer({ url: Redacted.make(pgUrl), maxConnections: 1 })), Effect.orDie)

export const postgres = (): Promise<Dialect> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const runPromise = Effect.runPromiseWith(yield* Effect.context<never>())
      const time = yield* Clock.currentTimeMillis
      const random = yield* Random.next
      const schema = `echo_core_test_${time.toString(36)}_${Str.slice(2, 8)(random.toString(36))}`
      yield* run((sql) => sql.unsafe(`CREATE SCHEMA ${schema}`))
      const query = (statement: string, params: ReadonlyArray<unknown> = []) =>
        run((sql) =>
          sql.withTransaction(
            Effect.andThen(
              sql.unsafe(`SET LOCAL search_path TO ${schema}`),
              sql.unsafe<Record<string, unknown>>(statement, params)
            )
          )
        )
      return {
        name: "postgres",
        create: (table) =>
          runPromise(
            Effect.gen(function* () {
              yield* query(table.postgres.createTable)
              for (const ddl of table.postgres.createIndexes) yield* query(ddl)
            })
          ),
        insert: (table, row) =>
          runPromise(
            Effect.gen(function* () {
              const stored: { readonly [column: string]: PgValue } = yield* Schema.encodeEffect(table.postgres.row)(row)
              const cols = Rec.keys(stored)
              yield* query(
                `INSERT INTO ${q(table.name)} (${Arr.join(Arr.map(cols, q), ", ")}) VALUES (${Arr.join(
                  Arr.map(cols, (_, i) => `$${i + 1}`),
                  ", "
                )})`,
                Arr.map(cols, (c) => pgParam(table, c, stored[c] ?? null))
              )
            })
          ),
        readAll: (table) =>
          runPromise(
            Effect.flatMap(query(`SELECT * FROM ${q(table.name)} ORDER BY ${q("id")}`), (rows) =>
              Effect.forEach(rows, (row) => Schema.decodeUnknownEffect(table.postgres.row)(row))
            )
          ),
        range: (table, plan) =>
          runPromise(
            Effect.gen(function* () {
              const { sql, params } = planSql(
                plan,
                table,
                (i) => `$${i}`,
                // oxlint-disable-next-line effecttsgo/schema-sync -- Synchronous SQL builder callback binds validated scalar keys.
                (c, v) => Schema.encodeSync(c.postgres)(v)
              )
              return Arr.map(yield* query(sql, params), (r) => String(r["id"]))
            })
          ),
        close: () => runPromise(Effect.asVoid(run((sql) => sql.unsafe(`DROP SCHEMA ${schema} CASCADE`))))
      }
    })
  )
