/**
 * Test-only harness: real SQLite (node:sqlite) and real Postgres (`ECHO_PG_URL`, skipped when
 * unset) holding tables created from `SyncTable` DDL. Rows go through the table's own dialect
 * codecs, so a round-trip exercises the declared column types, not an in-memory copy.
 */
import * as PgClient from "@effect/sql-pg/PgClient"
import * as PgTypes from "@effect/sql-pg/PgTypes"
import { Effect, Redacted, Schema } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { DatabaseSync, type SQLInputValue } from "node:sqlite"
import type { IndexRangePlan } from "../Store.ts"
import type { AnySyncTable, Column, EncodedRow, PgValue } from "../SyncTable.ts"

export const pgUrl = process.env["ECHO_PG_URL"] ?? ""
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
  plan.eq.forEach((value, i) => {
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
  const sql = `SELECT ${q("id")} FROM ${q(table.name)}${where.length ? ` WHERE ${where.join(" AND ")}` : ""} ORDER BY ${plan.index.key
    .map((c) => `${q(c.name)} ${dir}`)
    .join(", ")}`
  return { sql, params }
}

export const sqlite = (): Dialect => {
  const db = new DatabaseSync(":memory:")
  return {
    name: "sqlite",
    create: async (table) => {
      db.exec(table.sqlite.createTable)
      for (const ddl of table.sqlite.createIndexes) db.exec(ddl)
    },
    insert: async (table, row) => {
      const stored = Schema.encodeSync(table.sqlite.row)(row)
      const cols = Object.keys(stored)
      const values: Array<SQLInputValue> = cols.map((c) => stored[c] ?? null)
      db.prepare(
        `INSERT INTO ${q(table.name)} (${cols.map(q).join(", ")}) VALUES (${cols.map(() => "?").join(", ")})`
      ).run(...values)
    },
    readAll: async (table) =>
      db
        .prepare(`SELECT * FROM ${q(table.name)} ORDER BY ${q("id")}`)
        .all()
        .map((r) => Schema.decodeUnknownSync(table.sqlite.row)(r)),
    range: async (table, plan) => {
      const { sql, params } = planSql(
        plan,
        table,
        () => "?",
        (c, v): SQLInputValue => Schema.encodeSync(c.sqlite)(v)
      )
      return db
        .prepare(sql)
        .all(...params)
        .map((r) => String(r["id"]))
    },
    close: async () => db.close()
  }
}

/** Binds a dialect value for sql-pg: JSON columns must be bound explicitly as jsonb. */
const pgParam = (table: AnySyncTable, column: string, value: PgValue): unknown =>
  table.columns.find((c) => c.name === column)?.kind === "json" && value !== null ? PgTypes.jsonb(value) : value

const run = <A>(f: (sql: SqlClient) => Effect.Effect<A, unknown>) =>
  Effect.runPromise(
    Effect.gen(function* () {
      return yield* f(yield* SqlClient)
    }).pipe(Effect.provide(PgClient.layer({ url: Redacted.make(pgUrl), maxConnections: 1 })), Effect.orDie)
  )

export const postgres = async (): Promise<Dialect> => {
  const schema = `echo_core_test_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
  await run((sql) => sql.unsafe(`CREATE SCHEMA ${schema}`))
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
    create: async (table) => {
      await query(table.postgres.createTable)
      for (const ddl of table.postgres.createIndexes) await query(ddl)
    },
    insert: async (table, row) => {
      const stored: { readonly [column: string]: PgValue } = Schema.encodeSync(table.postgres.row)(row)
      const cols = Object.keys(stored)
      await query(
        `INSERT INTO ${q(table.name)} (${cols.map(q).join(", ")}) VALUES (${cols.map((_, i) => `$${i + 1}`).join(", ")})`,
        cols.map((c) => pgParam(table, c, stored[c] ?? null))
      )
    },
    readAll: async (table) =>
      (await query(`SELECT * FROM ${q(table.name)} ORDER BY ${q("id")}`)).map((r) =>
        Schema.decodeUnknownSync(table.postgres.row)(r)
      ),
    range: async (table, plan) => {
      const { sql, params } = planSql(
        plan,
        table,
        (i) => `$${i}`,
        (c, v) => Schema.encodeSync(c.postgres)(v)
      )
      return (await query(sql, params)).map((r) => String(r["id"]))
    },
    close: async () => {
      await run((sql) => sql.unsafe(`DROP SCHEMA ${schema} CASCADE`))
    }
  }
}
