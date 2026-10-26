/**
 * Startup verifier (SPEC §13, "Generator and verifier contract"): compares the live Postgres
 * catalog with the registry's manifests and fails with one `Drift` listing every mismatch. Run it
 * after `migrate` and the app migrations, before serving; on `Drift` the server must not serve.
 *
 * Reads the structured catalog (`pg_attribute`, `pg_index.indkey` / `indoption` /
 * `indcollation`, `pg_trigger`), never DDL text. Checked for every registry table, `scope_grants`
 * included, on every relation the capture trigger maps the name to: attached (capture `AFTER ROW
 * INSERT|UPDATE|DELETE` and truncate guard `BEFORE STATEMENT TRUNCATE`, both enabled, no `WHEN`,
 * no column list); every declared column present with its type,
 * text collation `"C"`, and nullability (SPEC §13 key presence); primary key exactly `(id)`; each
 * declared index `table__index` a plain btree on the key columns in order, `ASC NULLS FIRST` on
 * declared fields and `ASC` on the trailing `id`, text keys collated `"C"` (SPEC §3); each declared
 * unique set backed by a unique index whose key is exactly those columns, `ASC NULLS LAST`,
 * `NULLS DISTINCT`. Indexes count only if valid, ready and live, and only their key columns
 * (`indnkeyatts`) are compared, never `INCLUDE` payload. Extra live columns and indexes
 * are allowed. The engine's own tables are not checked.
 */
import { Effect, Schema } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import type { Registry, SyncTable } from "echo-sync"
import * as Telemetry from "../../internal/Telemetry.ts"

export const Mismatch = Schema.Struct({ table: Schema.String, problem: Schema.String })
export type Mismatch = typeof Mismatch.Type

// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
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

/** `pg_trigger.tgtype` bits (src/include/catalog/pg_trigger.h). */
const TRIGGER_ROW = 1
const TRIGGER_BEFORE = 2
const TRIGGER_INSERT = 4
const TRIGGER_DELETE = 8
const TRIGGER_UPDATE = 16
const TRIGGER_TRUNCATE = 32
/** What attach creates: AFTER INSERT OR UPDATE OR DELETE … FOR EACH ROW. */
const CAPTURE_TYPE = TRIGGER_ROW | TRIGGER_INSERT | TRIGGER_UPDATE | TRIGGER_DELETE
/** What attach creates: BEFORE TRUNCATE … FOR EACH STATEMENT. */
const GUARD_TYPE = TRIGGER_BEFORE | TRIGGER_TRUNCATE
/** `tgenabled` values that fire in an ordinary (`session_replication_role = origin`) session. */
const FIRES = new Set(["O", "A"])

interface Relation {
  readonly oid: number
  readonly schema: string
}
interface TriggerRow {
  readonly name: string
  readonly capture: boolean
  readonly enabled: string
  readonly type: number
  readonly when: boolean
  readonly columnList: boolean
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
  /** Within `indnkeyatts`; `INCLUDE` payload columns are not key. */
  readonly key: boolean
  readonly column: string | null
  readonly option: number | null
  readonly collation: string | null
  readonly unique: boolean
  readonly primary: boolean
  readonly plain: boolean
  /** `indisvalid AND indisready AND indislive`: usable for lookups and maintained by writes. */
  readonly usable: boolean
  readonly nullsNotDistinct: boolean
}

/** One key column of an index, as the catalog describes it. */
interface KeyColumn {
  readonly column: string | null
  readonly option: number | null
  readonly collation: string | null
}

const sameList = (a: ReadonlyArray<unknown>, b: ReadonlyArray<unknown>) =>
  a.length === b.length && a.every((x, i) => x === b[i])
const keyText = (k: KeyColumn) => `${k.column}|${k.option}|${k.collation}`
const sameKey = (a: ReadonlyArray<KeyColumn>, b: ReadonlyArray<KeyColumn>) => sameList(a.map(keyText), b.map(keyText))

const describeKey = (rows: ReadonlyArray<KeyColumn>) =>
  rows
    .map((r) => {
      const option = r.option ?? 0
      const dir = option & INDOPTION_DESC ? "DESC" : "ASC"
      const nulls = option & INDOPTION_NULLS_FIRST ? "NULLS FIRST" : "NULLS LAST"
      return `${r.column ?? "<expr>"} ${dir} ${nulls}${r.collation === null ? "" : ` COLLATE "${r.collation}"`}`
    })
    .join(", ")

const describeTrigger = (t: TriggerRow) =>
  `${t.name} (tgtype ${t.type}, tgenabled ${t.enabled}${t.when ? ", WHEN clause" : ""}${t.columnList ? ", column list" : ""})`

/** The key of an index (its first `indnkeyatts` columns). */
const keyOf = (rows: ReadonlyArray<IndexColumnRow>): ReadonlyArray<KeyColumn> =>
  rows.filter((r) => r.key).map((r) => ({ column: r.column, option: r.option, collation: r.collation }))

/** Index key collation of a column kind (text columns are declared `COLLATE "C"`). */
const collationOf = (kind: SyncTable.ColumnKind) => (kind === "text" ? "C" : null)

const checkRelation = Effect.fn("verify.checkRelation")(function* (
  table: SyncTable.AnySyncTable,
  relation: Relation,
  drift: (problem: string) => void
) {
  const sql = yield* SqlClient

  // Both echo_sync triggers must exist, fire for ordinary writes, and have attach's shape.
  // Every trigger carrying either function is checked: a second, partial capture trigger
  // would double-log, and a disabled one beside a working one is still drift.
  const triggers = yield* sql<TriggerRow>`
      SELECT tgname AS name, tgfoid = 'echo_sync.echo_sync_capture_changes'::regproc AS capture,
             tgenabled::text AS enabled, tgtype::int AS type, tgqual IS NOT NULL AS "when",
             cardinality(tgattr::int2[]) > 0 AS "columnList"
      FROM pg_trigger
      WHERE tgrelid = ${relation.oid}::oid AND NOT tgisinternal
        AND tgfoid IN ('echo_sync.echo_sync_capture_changes'::regproc, 'echo_sync.reject_truncate'::regproc)
      ORDER BY tgname`
  for (const [capture, label, type, shape] of [
    [true, "capture", CAPTURE_TYPE, "AFTER INSERT OR UPDATE OR DELETE FOR EACH ROW"],
    [false, "truncate guard", GUARD_TYPE, "BEFORE TRUNCATE FOR EACH STATEMENT"]
  ] as const) {
    const found = triggers.filter((t) => t.capture === capture)
    if (found.length === 0) drift(`missing the echo_sync ${label} trigger`)
    if (capture && found.length > 1) drift(`has ${found.length} echo_sync capture triggers, expected one`)
    for (const t of found) {
      if (!FIRES.has(t.enabled) || t.type !== type || t.when || t.columnList) {
        drift(`echo_sync ${label} trigger ${describeTrigger(t)} is not enabled ${shape} without WHEN or column list`)
      }
    }
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
      drift(`declared column ${column.name} is missing (every wire row must carry it, SPEC §13)`)
      continue
    }
    if (c.type !== expectedType[column.kind])
      drift(`column ${column.name} is ${c.type}, declared ${expectedType[column.kind]}`)
    if (column.kind === "text" && c.collation !== "C") {
      drift(`column ${column.name} has collation ${c.collation ?? "none"}, declared "C" (SPEC §3)`)
    }
    if (c.notNull === column.nullable) {
      drift(
        `column ${column.name} is ${c.notNull ? "NOT NULL" : "nullable"}, declared ${column.nullable ? "nullable" : "NOT NULL"}`
      )
    }
  }

  const indexColumns = yield* sql<IndexColumnRow>`
      SELECT ic.relname AS index, k.ord::int AS ord, k.ord <= i.indnkeyatts AS key, a.attname AS column,
             i.indoption[k.ord - 1]::int AS option, co.collname AS collation,
             i.indisunique AS unique, i.indisprimary AS primary,
             (am.amname = 'btree' AND i.indpred IS NULL AND i.indexprs IS NULL) AS plain,
             (i.indisvalid AND i.indisready AND i.indislive) AS usable,
             i.indnullsnotdistinct AS "nullsNotDistinct"
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
  const kindOf = new Map(table.columns.map((c) => [c.name, c.kind]))

  const pk = indexes.find((rows) => rows[0]?.primary === true)
  const pkKey = pk === undefined ? [] : keyOf(pk)
  const idKey: ReadonlyArray<KeyColumn> = [
    { column: "id", option: 0, collation: collationOf(kindOf.get("id") ?? "text") }
  ]
  if (pk === undefined || pk[0]?.usable !== true || !sameKey(pkKey, idKey)) {
    drift(
      `primary key is ${pk === undefined ? "missing" : `(${describeKey(pkKey)})${pk[0]?.usable ? "" : " not valid"}`}, declared (${describeKey(idKey)})`
    )
  }

  for (const index of [
    ...Object.values(table.indexes),
    {
      name: "echo-scope",
      key: ["access_scope_id", "id"].flatMap((name) => table.columns.filter((c) => c.name === name))
    }
  ]) {
    const name = `${table.name}__${index.name}`
    const rows = byIndex.get(name)
    // SPEC §3: NULLS FIRST on declared fields, plain ASC (NULLS LAST) on the trailing id.
    const want: ReadonlyArray<KeyColumn> = index.key.map((c, i) => ({
      column: c.name,
      option: i === index.key.length - 1 ? 0 : INDOPTION_NULLS_FIRST,
      collation: collationOf(c.kind)
    }))
    if (rows === undefined) {
      drift(`declared index ${name} (${describeKey(want)}) is missing`)
      continue
    }
    const first = rows[0]
    const ok = first !== undefined && first.plain && !first.unique && first.usable && sameKey(keyOf(rows), want)
    if (!ok) {
      const flags = [
        first?.plain ? "" : " non-plain",
        first?.unique ? " unique" : "",
        first?.usable ? "" : " not valid/ready/live"
      ].join("")
      drift(`index ${name} is (${describeKey(keyOf(rows))})${flags}, declared (${describeKey(want)})`)
    }
  }

  for (const set of table.manifest.unique) {
    // The shape SyncTable's UNIQUE (…) constraint generates: default ASC NULLS LAST, the
    // column's collation, NULLS DISTINCT.
    const want: ReadonlyArray<KeyColumn> = set.map((column) => ({
      column,
      option: 0,
      collation: collationOf(kindOf.get(column) ?? "text")
    }))
    const backed = indexes.some((rows) => {
      const first = rows[0]
      return (
        first !== undefined &&
        first.unique &&
        !first.primary &&
        first.plain &&
        first.usable &&
        !first.nullsNotDistinct &&
        sameKey(keyOf(rows), want)
      )
    })
    if (!backed) {
      drift(`declared unique set (${set.join(", ")}) has no valid unique index with key exactly (${describeKey(want)})`)
    }
  }
})

export const verify = (registry: Registry.SyncRegistry) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    const mismatches: Array<Mismatch> = []
    for (const table of registry.tables) {
      // Every relation attach mapped this name to (capture trigger, enabled or not); attach
      // refuses same-name tables across schemas, so more than one is itself suspicious, and each
      // is checked. Fall back to search_path so an unattached table is still checked.
      const attached = yield* sql<Relation>`
        SELECT DISTINCT c.oid::int AS oid, n.nspname AS schema FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE t.tgfoid = 'echo_sync.echo_sync_capture_changes'::regproc AND c.relname = ${table.name}
        ORDER BY n.nspname`
      let relations: ReadonlyArray<Relation> = attached
      if (attached.length > 1) {
        // echo_sync.log.table_name is unqualified: two captured relations of one name write
        // indistinguishable log rows, however well-formed each is (attach refuses this).
        mismatches.push({
          table: table.name,
          problem: `captured in ${attached.length} schemas (${attached.map((r) => r.schema).join(", ")}); the log names tables unqualified, so exactly one may be attached`
        })
      }
      if (attached.length > 0) {
        // SPEC §13: the relation the unqualified name resolves to through search_path must be a
        // captured one, or app SQL writes a relation sync never sees.
        const [resolved] = yield* sql<Relation>`
          SELECT c.oid::int AS oid, n.nspname AS schema FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.oid = to_regclass(quote_ident(${table.name}))`
        if (resolved !== undefined && !attached.some((r) => r.oid === resolved.oid)) {
          mismatches.push({
            table: table.name,
            problem: `the unqualified name resolves through search_path to ${resolved.schema}.${table.name}, but the captured relation is in ${attached.map((r) => r.schema).join(", ")}; fix search_path or attach with an explicit schema`
          })
        }
      }
      if (attached.length === 0) {
        mismatches.push({
          table: table.name,
          problem: "not attached (no echo_sync capture trigger); run attach at startup"
        })
        relations = yield* sql<Relation>`
          SELECT c.oid::int AS oid, n.nspname AS schema FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.oid = to_regclass(quote_ident(${table.name}))`
        if (relations.length === 0) {
          mismatches.push({ table: table.name, problem: "table does not exist" })
          continue
        }
      }
      for (const relation of relations) {
        const where = relations.length > 1 ? `(in schema ${relation.schema}) ` : ""
        yield* checkRelation(table, relation, (problem) =>
          mismatches.push({ table: table.name, problem: where + problem })
        )
      }
    }
    if (mismatches.length > 0) return yield* new Drift({ mismatches })
  }).pipe(Telemetry.boundary)
