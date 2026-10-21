/**
 * Startup verifier (DESIGN §11, "Generator and verifier contract"): compares the live Postgres
 * catalog with the registry's manifests and fails with one `Drift` listing every mismatch. Run it
 * after `migrate` and the app migrations, before serving; on `Drift` the server must not serve.
 *
 * Reads the structured catalog (`pg_attribute`, `pg_index.indkey` / `indoption` /
 * `indcollation`, `pg_trigger`), never DDL text. Checked for every registry table, `scope_grants`
 * included: attached (both `echo_sync` triggers); every declared column present with its type,
 * text collation `"C"`, and nullability (§11 key presence); primary key exactly `(id)`; each
 * declared index `table__index` a plain btree on the key columns in order, `ASC NULLS FIRST` on
 * declared fields and `ASC` on the trailing `id`, text keys collated `"C"` (§10); each declared
 * unique set backed by a unique index on exactly those columns. Extra live columns and indexes
 * are allowed. The engine's own tables are not checked.
 */
import { Effect, Schema } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import type { Registry, SyncTable } from "echo-sync"

export const Mismatch = Schema.Struct({ table: Schema.String, problem: Schema.String })
export type Mismatch = typeof Mismatch.Type

export class Drift extends Schema.TaggedError<Drift>()("Drift", { mismatches: Schema.Array(Mismatch) }) {
  override get message() {
    return `schema drift (refusing to serve):\n${this.mismatches.map((m) => `  ${m.table}: ${m.problem}`).join("\n")}`
  }
}

/** `format_type` of each column kind (text additionally needs collation "C"). */
const expectedType: Record<SyncTable.ColumnKind, string> = {
  text: "text",
  integer: "bigint",
  double: "double precision",
  boolean: "boolean",
  timestamp: "timestamp with time zone",
  json: "jsonb"
}

/** `pg_index.indoption` bits (src/include/catalog/pg_index.h). */
const INDOPTION_DESC = 1
const INDOPTION_NULLS_FIRST = 2

interface Relation {
  readonly oid: number
  readonly schema: string
}
interface ColumnRow {
  readonly name: string
  readonly type: string
  readonly notNull: boolean
  readonly collation: string | null
}
interface IndexColumnRow {
  readonly index: string
  readonly ord: number
  readonly column: string | null
  readonly option: number
  readonly collation: string | null
  readonly unique: boolean
  readonly primary: boolean
  readonly plain: boolean
}

const sameList = (a: ReadonlyArray<unknown>, b: ReadonlyArray<unknown>) =>
  a.length === b.length && a.every((x, i) => x === b[i])

const describeKey = (rows: ReadonlyArray<IndexColumnRow>) =>
  rows
    .map((r) => {
      const dir = r.option & INDOPTION_DESC ? "DESC" : "ASC"
      const nulls = r.option & INDOPTION_NULLS_FIRST ? "NULLS FIRST" : "NULLS LAST"
      return `${r.column ?? "<expr>"} ${dir} ${nulls}${r.collation === null ? "" : ` COLLATE "${r.collation}"`}`
    })
    .join(", ")

export const verify = (registry: Registry.SyncRegistry) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    const mismatches: Array<Mismatch> = []
    for (const table of registry.tables) {
      const drift = (problem: string) => mismatches.push({ table: table.name, problem })

      // Resolve through the capture trigger (attach's mapping); fall back to search_path so an
      // unattached table is still checked column by column.
      const attached = yield* sql<Relation>`
        SELECT DISTINCT c.oid::int AS oid, n.nspname AS schema FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE t.tgfoid = 'echo_sync.echo_sync_capture_changes'::regproc AND c.relname = ${table.name}`
      let relation = attached[0]
      if (relation === undefined) {
        drift("not attached (no echo_sync capture trigger); run attach at startup")
        const found = yield* sql<Relation>`
          SELECT c.oid::int AS oid, n.nspname AS schema FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.oid = to_regclass(quote_ident(${table.name}))`
        relation = found[0]
        if (relation === undefined) {
          drift("table does not exist")
          continue
        }
      } else {
        const truncate = yield* sql<{ n: number }>`
          SELECT count(*)::int AS n FROM pg_trigger
          WHERE tgrelid = ${relation.oid}::oid AND tgfoid = 'echo_sync.reject_truncate'::regproc`
        if ((truncate[0]?.n ?? 0) === 0) drift("missing the echo_sync truncate guard trigger")
      }

      const columns = yield* sql<ColumnRow>`
        SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type,
               a.attnotnull AS "notNull", co.collname AS collation
        FROM pg_attribute a LEFT JOIN pg_collation co ON co.oid = a.attcollation
        WHERE a.attrelid = ${relation.oid}::oid AND a.attnum > 0 AND NOT a.attisdropped`
      const live = new Map(columns.map((c) => [c.name, c]))
      for (const column of table.columns) {
        const c = live.get(column.name)
        if (c === undefined) {
          drift(`declared column ${column.name} is missing (every wire row must carry it, §11)`)
          continue
        }
        if (c.type !== expectedType[column.kind])
          drift(`column ${column.name} is ${c.type}, declared ${expectedType[column.kind]}`)
        if (column.kind === "text" && c.collation !== "C") {
          drift(`column ${column.name} has collation ${c.collation ?? "none"}, declared "C" (§10)`)
        }
        if (c.notNull === column.nullable) {
          drift(
            `column ${column.name} is ${c.notNull ? "NOT NULL" : "nullable"}, declared ${column.nullable ? "nullable" : "NOT NULL"}`
          )
        }
      }

      const indexColumns = yield* sql<IndexColumnRow>`
        SELECT ic.relname AS index, k.ord::int AS ord, a.attname AS column,
               i.indoption[k.ord - 1]::int AS option, co.collname AS collation,
               i.indisunique AS unique, i.indisprimary AS primary,
               (am.amname = 'btree' AND i.indpred IS NULL AND i.indexprs IS NULL) AS plain
        FROM pg_index i
        JOIN pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_am am ON am.oid = ic.relam
        CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
        LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
        LEFT JOIN pg_collation co ON co.oid = i.indcollation[k.ord - 1]
        WHERE i.indrelid = ${relation.oid}::oid
        ORDER BY ic.relname, k.ord`
      const byIndex = new Map<string, Array<IndexColumnRow>>()
      for (const row of indexColumns) {
        const list = byIndex.get(row.index) ?? []
        list.push(row)
        byIndex.set(row.index, list)
      }
      const indexes = [...byIndex.values()]

      const pk = indexes.find((rows) => rows[0]?.primary === true)
      if (
        pk === undefined ||
        !sameList(
          pk.map((r) => r.column),
          ["id"]
        )
      ) {
        drift(
          `primary key is ${pk === undefined ? "missing" : `(${pk.map((r) => r.column).join(", ")})`}, declared (id)`
        )
      }

      for (const index of Object.values(table.indexes)) {
        const name = `${table.name}__${index.name}`
        const rows = byIndex.get(name)
        const want = index.key.map((c, i) => ({
          column: c.name,
          option: i === index.key.length - 1 ? 0 : INDOPTION_NULLS_FIRST,
          collation: c.kind === "text" ? "C" : null
        }))
        const wanted = want
          .map((w) => `${w.column} ASC ${w.option ? "NULLS FIRST" : "NULLS LAST"}${w.collation ? ' COLLATE "C"' : ""}`)
          .join(", ")
        if (rows === undefined) {
          drift(`declared index ${name} (${wanted}) is missing`)
          continue
        }
        const matches =
          rows.every((r) => r.plain && !r.unique) &&
          sameList(
            rows.map((r) => `${r.column}|${r.option}|${r.collation}`),
            want.map((w) => `${w.column}|${w.option}|${w.collation}`)
          )
        if (!matches)
          drift(`index ${name} is (${describeKey(rows)})${rows[0]?.plain ? "" : " non-plain"}, declared (${wanted})`)
      }

      for (const set of table.manifest.unique) {
        const backed = indexes.some(
          (rows) =>
            rows.every((r) => r.unique && !r.primary && r.plain) &&
            sameList(
              rows.map((r) => r.column),
              set
            )
        )
        if (!backed) drift(`declared unique set (${set.join(", ")}) has no unique index on exactly those columns`)
      }
    }
    if (mismatches.length > 0) return yield* new Drift({ mismatches })
  })
