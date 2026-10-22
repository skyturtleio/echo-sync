/**
 * Schema evolution planning (DESIGN §11, "Generator and verifier contract"). Pure: no database.
 *
 * Input: the previous committed manifest snapshot (`migrations/manifest.json`), the registry's
 * current tables, and an explicit `Intent`. Output: a reviewable TypeScript migration module
 * (`NNN_name.ts`, default export an `Effect` over `SqlClient`, loadable by
 * `Migrator.fromRecord`) and the new snapshot text, both to be committed together.
 *
 * Only safe operations become DDL: create table, add a nullable column, add a NOT NULL column
 * with an intent default (added with the default, which is then dropped), `DROP NOT NULL`, and
 * index add / drop / change. Renames, column and table drops, lossy type changes, unique-set
 * changes and backfills become commented TODOs, never DDL, whatever the intent says, and so
 * does any step that references a table or column whose rename, drop or kind change is a TODO.
 *
 * `scope_grants` is the engine's table (created by `001_engine`) and is never planned here.
 */
import { Schema } from "effect"
import { ScopeGrants } from "../ScopeGrants.ts"
import { canonicalJson, pgType, type AnySyncTable, type ColumnKind } from "../SyncTable.ts"
import type { SyncRegistry } from "../Registry.ts"

// ---------------------------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------------------------

const ColumnKindSchema = Schema.Literals(["text", "integer", "double", "boolean", "timestamp", "json"])

const TableManifestSchema = Schema.Struct({
  name: Schema.String,
  columns: Schema.Array(Schema.Struct({ name: Schema.String, kind: ColumnKindSchema, nullable: Schema.Boolean })),
  indexes: Schema.Array(Schema.Struct({ name: Schema.String, columns: Schema.Array(Schema.String) })),
  unique: Schema.Array(Schema.Array(Schema.String)),
  dataHash: Schema.String,
  indexHash: Schema.String
})

/** The committed snapshot, with registry hashes (system tables included) at migration boundaries. */
export const Snapshot = Schema.Struct({
  version: Schema.Literal(1),
  tables: Schema.Array(TableManifestSchema),
  /** Missing in pre-gate snapshots; never invent history for their earlier migrations. */
  history: Schema.optionalKey(Schema.Array(Schema.Struct({ migration: Schema.String, dataHash: Schema.String })))
})
export type Snapshot = typeof Snapshot.Type

/** The snapshot before the first app migration. */
export const emptySnapshot: Snapshot = { version: 1, tables: [] }

/** The app tables of a registry (everything but the engine's `scope_grants`), sorted by name. */
const appTables = (registry: SyncRegistry): ReadonlyArray<AnySyncTable> =>
  registry.tables
    .filter((t) => t.name !== ScopeGrants.name)
    .toSorted((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))

/** The snapshot of a registry's app tables. */
export const snapshotOf = (registry: SyncRegistry): Snapshot => ({
  version: 1,
  tables: appTables(registry).map((t) => t.manifest)
})

/** Canonical JSON (sorted keys) with a trailing newline: the committed file's exact text. */
export const encodeSnapshot = (snapshot: Snapshot): string =>
  `${canonicalJson(Schema.encodeSync(Snapshot)(snapshot))}\n`

/** Parses and validates a committed snapshot file. Throws `MigrationPlanError` if malformed. */
export const decodeSnapshot = (text: string): Snapshot => {
  try {
    return Schema.decodeUnknownSync(Snapshot)(JSON.parse(text))
  } catch (error) {
    throw new MigrationPlanError(`manifest snapshot is malformed: ${String(error)}`)
  }
}

// ---------------------------------------------------------------------------------------------
// Intent and plan
// ---------------------------------------------------------------------------------------------

/**
 * What the generator cannot infer. Rename and drop entries never produce DDL; they only make
 * the generated TODO precise. `defaults` supplies the one-shot backfill value for a new NOT NULL
 * column (`"table.column": sqlExpression`, inserted verbatim into the DDL for review).
 */
export interface Intent {
  readonly defaults?: { readonly [tableDotColumn: string]: string }
  readonly renames?: {
    /** New table name → previous table name. */
    readonly tables?: { readonly [to: string]: string }
    /** Table (new name) → new column name → previous column name. */
    readonly columns?: { readonly [table: string]: { readonly [to: string]: string } }
  }
  /** `"table"` or `"table.column"`: the removal is intended (the TODO spells out the DROP). */
  readonly acknowledgeDrops?: ReadonlyArray<string>
}

/** An intent that refers to something the diff does not contain; nothing is guessed. */
export class MigrationPlanError extends Error {
  override readonly name = "MigrationPlanError"
}

export type Step =
  | { readonly _tag: "Ddl"; readonly sql: string; readonly comment?: ReadonlyArray<string> }
  | { readonly _tag: "Todo"; readonly lines: ReadonlyArray<string> }

const fail = (message: string): never => {
  throw new MigrationPlanError(message)
}
const setKey = (columns: ReadonlyArray<string>) => columns.join("\u0000")
const quote = (name: string) => `"${name}"`
const indexName = (table: string, index: string) => `${table}__${index}`
const columnType = (kind: ColumnKind) => pgType[kind]
const sameList = (a: ReadonlyArray<string>, b: ReadonlyArray<string>) =>
  a.length === b.length && a.every((x, i) => x === b[i])

const notNullSequence = (table: string, column: string): ReadonlyArray<string> => [
  `${table}.${column} is declared NOT NULL. Finish the DESIGN §11 sequence by hand, in later deploys:`,
  `  1. ship writers that always set ${column} (every insert carries every declared column);`,
  `  2. backfill existing rows in idempotent chunks, outside the Migrator transaction:`,
  `       UPDATE ${quote(table)} SET ${quote(column)} = <value> WHERE ${quote(column)} IS NULL AND id IN (<chunk>)`,
  `  3. validate: SELECT count(*) FROM ${quote(table)} WHERE ${quote(column)} IS NULL  -- must be 0`,
  `  4. enforce, in a new migration: ALTER TABLE ${quote(table)} ALTER COLUMN ${quote(column)} SET NOT NULL`,
  `Until step 4 the startup verifier reports ${table}.${column} as nullable (drift) and refuses to serve.`
]

/**
 * The steps that take the database from `previous` to the registry's app tables. Throws
 * `MigrationPlanError` when the intent names a table or column the diff does not have.
 */
export const plan = (previous: Snapshot, registry: SyncRegistry, intent: Intent = {}): ReadonlyArray<Step> => {
  const steps: Array<Step> = []
  const todo = (...lines: ReadonlyArray<string>) => steps.push({ _tag: "Todo", lines })
  const ddl = (sql: string, ...comment: ReadonlyArray<string>) =>
    steps.push(comment.length > 0 ? { _tag: "Ddl", sql, comment } : { _tag: "Ddl", sql })

  const before = new Map(previous.tables.map((m) => [m.name, m]))
  const tables = appTables(registry)
  const after = new Map(tables.map((t) => [t.name, t]))
  const drops = new Set(intent.acknowledgeDrops ?? [])
  const defaults = new Map(Object.entries(intent.defaults ?? {}))
  const usedDefaults = new Set<string>()
  const tableRenames = new Map(Object.entries(intent.renames?.tables ?? {}))
  const renamedFrom = new Set(tableRenames.values())

  for (const [to, from] of tableRenames) {
    if (!after.has(to) || before.has(to)) fail(`renames.tables: "${to}" is not a newly declared table`)
    if (!before.has(from) || after.has(from)) fail(`renames.tables: "${from}" is not a removed table`)
  }
  /** The previous manifest of a current table, resolved through a declared table rename. */
  const previousOf = (table: string) => before.get(table) ?? before.get(tableRenames.get(table) ?? "")
  for (const [table, columns] of Object.entries(intent.renames?.columns ?? {})) {
    const now = after.get(table)
    const was = previousOf(table)
    if (now === undefined || was === undefined) fail(`renames.columns: "${table}" is not a table in both manifests`)
    for (const [to, from] of Object.entries(columns)) {
      if (!now?.manifest.columns.some((c) => c.name === to) || was?.columns.some((c) => c.name === to)) {
        fail(`renames.columns: "${table}.${to}" is not a newly declared column`)
      }
      if (!was?.columns.some((c) => c.name === from) || now?.manifest.columns.some((c) => c.name === from)) {
        fail(`renames.columns: "${table}.${from}" is not a removed column`)
      }
    }
  }

  for (const table of tables) {
    const name = table.name
    const from = before.has(name) ? undefined : tableRenames.get(name)
    const was = previousOf(name)
    if (was === undefined) {
      ddl(
        table.postgres.createTable,
        `New table ${name}. Run attach(${name}) at server startup (outside this transaction).`
      )
      for (const index of table.postgres.createIndexes) ddl(index)
      continue
    }
    if (from !== undefined) {
      todo(
        `Table ${from} is declared renamed to ${name}. Renames are never generated; apply by hand:`,
        `  ALTER TABLE ${quote(from)} RENAME TO ${quote(name)};`,
        ...was.indexes.map(
          (i) => `  ALTER INDEX ${quote(indexName(from, i.name))} RENAME TO ${quote(indexName(name, i.name))};`
        ),
        `Re-run attach for ${name} at startup. The steps below for ${name} depend on this and are TODOs too;`,
        `they use the new index names.`
      )
    }

    const renames = new Map(Object.entries(intent.renames?.columns?.[name] ?? {}))
    const renameSources = new Set(renames.values())
    const old = new Map(was.columns.map((c) => [c.name, c]))
    const now = new Map(table.manifest.columns.map((c) => [c.name, c]))
    // Columns whose rename, drop or kind change is a TODO (old and new names): DDL that
    // references one cannot assume the column exists in its declared shape (§11), and neither
    // can any DDL on a table whose rename is a TODO.
    const blocked = new Set<string>([...renames.keys(), ...renameSources])
    for (const prior of was.columns) {
      const next = now.get(prior.name)
      if (next === undefined || next.kind !== prior.kind) blocked.add(prior.name)
    }
    /** The TODO a step referencing `refs` waits for, if any. */
    const dependency = (refs: ReadonlyArray<string>) => {
      const on = refs.filter((r) => blocked.has(r))
      if (from !== undefined) return `rename of table ${from}`
      return on.length > 0 ? `TODO for ${on.map((c) => `${name}.${c}`).join(", ")}` : undefined
    }
    const emit = (statement: string, refs: ReadonlyArray<string>, ...comment: ReadonlyArray<string>) => {
      const waits = dependency(refs)
      if (waits === undefined) ddl(statement, ...comment)
      else todo(`Depends on the ${waits} above; run by hand after it:`, `  ${statement};`)
    }
    // Columns in declaration order, so generated DDL follows the SyncTable.
    for (const column of table.columns) {
      const c = { name: column.name, kind: column.kind, nullable: column.nullable }
      const prior = old.get(c.name)
      const key = `${name}.${c.name}`
      if (prior === undefined) {
        const previousName = renames.get(c.name)
        if (previousName !== undefined) {
          const source = old.get(previousName)
          todo(
            `Column ${name}.${previousName} is declared renamed to ${c.name}. Renames are never generated; apply by hand:`,
            `  ALTER TABLE ${quote(name)} RENAME COLUMN ${quote(previousName)} TO ${quote(c.name)};`,
            ...(source !== undefined && (source.kind !== c.kind || source.nullable !== c.nullable)
              ? [
                  `  then reconcile the type/nullability change (${source.kind}${source.nullable ? " null" : ""} → ${c.kind}${c.nullable ? " null" : ""}).`
                ]
              : [])
          )
          continue
        }
        const type = columnType(c.kind)
        if (c.nullable) {
          emit(`ALTER TABLE ${quote(name)} ADD COLUMN ${quote(c.name)} ${type}`, [c.name])
          continue
        }
        const value = defaults.get(key)
        if (value !== undefined) {
          usedDefaults.add(key)
          emit(
            `ALTER TABLE ${quote(name)} ADD COLUMN ${quote(c.name)} ${type} NOT NULL DEFAULT ${value}`,
            [c.name],
            `The default backfills existing rows once and is dropped next: every insert carries every`,
            `declared column (§5), and the live catalog keeps no default the manifest does not know.`
          )
          emit(`ALTER TABLE ${quote(name)} ALTER COLUMN ${quote(c.name)} DROP DEFAULT`, [c.name])
          continue
        }
        emit(
          `ALTER TABLE ${quote(name)} ADD COLUMN ${quote(c.name)} ${type}`,
          [c.name],
          `Expand step: added nullable; see the TODO below to reach NOT NULL.`
        )
        todo(...notNullSequence(name, c.name))
        continue
      }
      if (prior.kind !== c.kind) {
        todo(
          `${key} changes kind ${prior.kind} → ${c.kind}. Type changes are never generated (they can lose data):`,
          `  add a new column, backfill it, switch readers and writers, then drop the old one, by hand.`,
          `  Dropping the old column also drops every index and unique constraint on it. The sequence`,
          `  must restore each declared one the drop removed, under its declared name, even where the`,
          `  declaration itself did not change; the startup verifier reports them as drift until then.`
        )
        continue
      }
      if (prior.nullable && !c.nullable) todo(...notNullSequence(name, c.name))
      if (!prior.nullable && c.nullable) {
        emit(
          `ALTER TABLE ${quote(name)} ALTER COLUMN ${quote(c.name)} DROP NOT NULL`,
          [c.name],
          `Loss-free, but dataHash changes: every client rebuilds its replica. A client still at the old`,
          `manifest cannot decode a null in ${key}, so ship clients before writing nulls.`
        )
      }
    }
    for (const prior of was.columns) {
      if (now.has(prior.name) || renameSources.has(prior.name)) continue
      const key = `${name}.${prior.name}`
      const goes = was.indexes.filter((i) => i.columns.includes(prior.name)).map((i) => indexName(name, i.name))
      todo(
        drops.has(key)
          ? `Column ${key} is no longer declared and its drop is acknowledged. Drops are never generated; when no reader needs it:`
          : `Column ${key} is no longer declared; its data is kept. Acknowledge with acknowledgeDrops: ["${key}"]. To drop by hand:`,
        `  ALTER TABLE ${quote(name)} DROP COLUMN ${quote(prior.name)};`,
        ...(goes.length > 0
          ? [`PostgreSQL drops ${goes.join(", ")} with the column; any replacement index is a TODO below.`]
          : [])
      )
    }

    // Indexes: inferred (plain DDL inside the Migrator transaction; indexHash rebuilds nothing).
    const oldIndexes = new Map(was.indexes.map((i) => [i.name, i]))
    const newIndexes = new Map(table.manifest.indexes.map((i) => [i.name, i]))
    const createFor = (index: string) => {
      const prefix = `CREATE INDEX ${quote(indexName(name, index))} `
      return (
        table.postgres.createIndexes.find((statement) => statement.startsWith(prefix)) ??
        fail(`no DDL for ${name}.${index}`)
      )
    }
    for (const prior of was.indexes) {
      const next = newIndexes.get(prior.name)
      if (next !== undefined && sameList(next.columns, prior.columns)) continue
      const drop = `DROP INDEX ${quote(indexName(name, prior.name))}`
      // A replacement is one unit: the CREATE reuses the name, so it can only run after the DROP.
      const waits = dependency([...prior.columns, ...(next?.columns ?? [])])
      if (waits === undefined) {
        ddl(drop)
        if (next !== undefined) ddl(createFor(next.name))
        continue
      }
      // An index on a dropped column goes with the column (the drop TODO says so): no DROP INDEX.
      // A kind change is hand-carried as add/backfill/drop, which may or may not have removed it.
      const goesWithColumn = prior.columns.some((c) => !now.has(c) && !renameSources.has(c))
      const changesKind = prior.columns.some((c) => {
        const current = now.get(c)
        return current !== undefined && old.get(c)?.kind !== current.kind
      })
      const statements = [
        ...(goesWithColumn ? [] : [changesKind ? drop.replace("DROP INDEX ", "DROP INDEX IF EXISTS ") : drop]),
        ...(next === undefined ? [] : [createFor(next.name)])
      ]
      if (statements.length > 0) {
        todo(`Depends on the ${waits} above; run by hand after it:`, ...statements.map((st) => `  ${st};`))
      }
    }
    for (const next of table.manifest.indexes) if (!oldIndexes.has(next.name)) emit(createFor(next.name), next.columns)

    // Unique sets: TODO only (adding can fail on data; removing needs the generated constraint name).
    const oldUnique = new Set(was.unique.map(setKey))
    const newUnique = new Set(table.manifest.unique.map(setKey))
    for (const cs of table.manifest.unique) {
      if (oldUnique.has(setKey(cs))) continue
      todo(
        `Unique set (${cs.join(", ")}) added on ${name}. Not generated: it fails on duplicate data. Check, then by hand:`,
        `  ALTER TABLE ${quote(name)} ADD UNIQUE (${cs.map(quote).join(", ")});`
      )
    }
    for (const cs of was.unique) {
      if (newUnique.has(setKey(cs))) continue
      todo(
        `Unique set (${cs.join(", ")}) removed from ${name}. Not generated: drop its constraint by hand`,
        `  (find the name with \\d ${name}): ALTER TABLE ${quote(name)} DROP CONSTRAINT <name>;`
      )
    }
  }

  for (const was of previous.tables) {
    if (after.has(was.name) || renamedFrom.has(was.name)) continue
    todo(
      drops.has(was.name)
        ? `Table ${was.name} is no longer declared and its drop is acknowledged. Drops are never generated; by hand:`
        : `Table ${was.name} is no longer declared; its data is kept. Acknowledge with acknowledgeDrops: ["${was.name}"]. To drop by hand:`,
      `  DROP TABLE ${quote(was.name)};`
    )
  }

  for (const k of defaults.keys())
    if (!usedDefaults.has(k)) fail(`defaults: "${k}" is not a newly declared NOT NULL column`)
  for (const d of drops) {
    const [table, column] = d.split(".")
    const removed =
      column === undefined
        ? before.has(d) && !after.has(d)
        : (previousOf(table ?? "")?.columns.some((c) => c.name === column) ?? false) &&
          !(after.get(table ?? "")?.manifest.columns.some((c) => c.name === column) ?? true)
    if (!removed) fail(`acknowledgeDrops: "${d}" is not a removed table or column`)
  }
  return steps
}

// ---------------------------------------------------------------------------------------------
// Module text
// ---------------------------------------------------------------------------------------------

/** Escapes text for a JS template literal. */
const templateText = (text: string) => text.replaceAll("\\", "\\\\").replaceAll("`", "\\`").replaceAll("${", "\\${")
const indent = (text: string, by: string) =>
  text
    .split("\n")
    .map((line) => by + line)
    .join("\n")

/** The migration module for `steps`: default export an Effect over `SqlClient`. */
export const render = (id: string, steps: ReadonlyArray<Step>): string => {
  const body: Array<string> = []
  const hasDdl = steps.some((s) => s._tag === "Ddl")
  if (hasDdl) body.push("  const sql = yield* SqlClient")
  for (const step of steps) {
    body.push("")
    if (step._tag === "Todo") {
      for (const line of step.lines) body.push(`  // TODO(migration): ${line}`)
      continue
    }
    for (const line of step.comment ?? []) body.push(`  // ${line}`)
    body.push(`  yield* sql.unsafe(\`\n${indent(templateText(step.sql), "    ")}\n  \`)`)
  }
  if (!hasDdl) body.push("  yield* Effect.void")
  return [
    "/**",
    ` * ${id}: generated by echo-sync Migrate.generate (DESIGN §11) from the committed manifest`,
    " * snapshot. Review every statement and TODO before committing; migration modules are",
    " * immutable once released.",
    " */",
    'import { Effect } from "effect"',
    ...(hasDdl ? ['import { SqlClient } from "effect/sql/SqlClient"'] : []),
    "",
    "export default Effect.gen(function* () {",
    ...body,
    "})",
    ""
  ].join("\n")
}

/** `NNN_name` after the highest of `existing` (ids like `"002_add_notes"` or file names). */
export const nextId = (existing: ReadonlyArray<string>, name: string): string => {
  if (!/^[a-z0-9_]+$/.test(name)) throw new MigrationPlanError(`migration name "${name}" must match /^[a-z0-9_]+$/`)
  const highest = Math.max(0, ...existing.map((e) => Number(/^(\d+)_/.exec(e.replace(/^.*\//, ""))?.[1] ?? 0)))
  return `${String(highest + 1).padStart(3, "0")}_${name}`
}

export interface Generated {
  /** `NNN_name`, the `fromRecord` key and file name (without `.ts`). */
  readonly id: string
  readonly source: string
  /** The new snapshot file text. */
  readonly snapshot: string
  readonly steps: ReadonlyArray<Step>
}

/** Plans and renders the next migration; commit `source` as `<id>.ts` together with `snapshot`. */
export const generate = (options: {
  readonly previous: Snapshot
  readonly registry: SyncRegistry
  readonly intent?: Intent
  /** Existing migration ids or file names, to number the next one. */
  readonly existing: ReadonlyArray<string>
  readonly name: string
}): Generated => {
  const id = nextId(options.existing, options.name)
  const steps = plan(options.previous, options.registry, options.intent)
  const snapshot: Snapshot = {
    ...snapshotOf(options.registry),
    history: [...(options.previous.history ?? []), { migration: id, dataHash: options.registry.dataHash }]
  }
  return { id, source: render(id, steps), snapshot: encodeSnapshot(snapshot), steps }
}
