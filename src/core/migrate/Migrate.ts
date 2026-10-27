/**
 * Schema evolution planning (SPEC §13, "Generator and verifier contract"). Pure: no database.
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
import { Array as Arr, Data, Effect, Option, pipe, Record as Rec, Result, Schema, String as Str } from "effect"
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
  scopeWalkIndex: Schema.optionalKey(Schema.Literal(true)),
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
  Arr.sort(
    Arr.filter([...registry.tables, ...registry.catalogs], (t) => t.name !== ScopeGrants.name),
    (a: AnySyncTable, b: AnySyncTable) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0)
  )

/** The snapshot of a registry's app tables. */
export const snapshotOf = (registry: SyncRegistry): Snapshot => ({
  version: 1,
  tables: Arr.map(appTables(registry), (t) => t.manifest)
})

const encode = Schema.encodeEffect(Snapshot)
const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(Snapshot))

/** Canonical JSON (sorted keys) with a trailing newline: the committed file's exact text. */
export const encodeSnapshot = Effect.fnUntraced(function* (snapshot: Snapshot) {
  const encoded = yield* encode(snapshot).pipe(
    Effect.mapError(
      (cause) =>
        new MigrationPlanError({ stage: "snapshot-encode", message: "manifest snapshot cannot be encoded", cause })
    )
  )
  return `${canonicalJson(encoded)}\n`
})

/** Lazily parses and validates a committed snapshot file, preserving the schema failure. */
export const decodeSnapshot = Effect.fnUntraced(function* (text: string) {
  return yield* decode(text).pipe(
    Effect.mapError(
      (cause) => new MigrationPlanError({ stage: "snapshot-decode", message: "manifest snapshot is malformed", cause })
    )
  )
})

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

/** An expected migration input refusal; unexpected programmer exceptions remain defects. */
export class MigrationPlanError extends Data.TaggedError("MigrationPlanError")<{
  readonly stage: "snapshot-decode" | "snapshot-encode" | "intent" | "name"
  readonly message: string
  readonly cause?: Schema.SchemaError
}> {}

export type Step =
  | { readonly _tag: "Ddl"; readonly sql: string; readonly comment?: ReadonlyArray<string> }
  | { readonly _tag: "Todo"; readonly lines: ReadonlyArray<string> }

const fail = (message: string) => Result.fail(new MigrationPlanError({ stage: "intent", message }))
const setKey = (columns: ReadonlyArray<string>) => Arr.join(columns, "\u0000")
const quote = (name: string) => `"${name}"`
const indexName = (table: string, index: string) => `${table}__${index}`
const columnType = (kind: ColumnKind) => pgType[kind]
const sameList = (a: ReadonlyArray<string>, b: ReadonlyArray<string>) =>
  a.length === b.length && Arr.every(a, (x, i) => x === b[i])

const notNullSequence = (table: string, column: string): ReadonlyArray<string> => [
  `${table}.${column} is declared NOT NULL. Finish the SPEC §13 sequence by hand, in later deploys:`,
  `  1. ship writers that always set ${column} (every insert carries every declared column);`,
  `  2. backfill existing rows in idempotent chunks, outside the Migrator transaction:`,
  `       UPDATE ${quote(table)} SET ${quote(column)} = <value> WHERE ${quote(column)} IS NULL AND id IN (<chunk>)`,
  `  3. validate: SELECT count(*) FROM ${quote(table)} WHERE ${quote(column)} IS NULL  -- must be 0`,
  `  4. enforce, in a new migration: ALTER TABLE ${quote(table)} ALTER COLUMN ${quote(column)} SET NOT NULL`,
  `Until step 4 the startup verifier reports ${table}.${column} as nullable (drift) and refuses to serve.`
]

/**
 * The steps that take the database from `previous` to the registry's app tables. Refuses
 * intent naming a table or column the diff does not have with an explicit Result failure.
 */
export const plan = (
  previous: Snapshot,
  registry: SyncRegistry,
  intent: Intent = {}
): Result.Result<ReadonlyArray<Step>, MigrationPlanError> => {
  const steps: Array<Step> = []
  const todo = (...lines: ReadonlyArray<string>) => steps.push({ _tag: "Todo", lines })
  const ddl = (sql: string, ...comment: ReadonlyArray<string>) =>
    steps.push(comment.length > 0 ? { _tag: "Ddl", sql, comment } : { _tag: "Ddl", sql })

  const before = new Map(Arr.map(previous.tables, (m) => [m.name, m]))
  const tables = appTables(registry)
  const after = new Map(Arr.map(tables, (t) => [t.name, t]))
  const drops = new Set(intent.acknowledgeDrops ?? [])
  const defaults = new Map(Rec.toEntries(intent.defaults ?? {}))
  const usedDefaults = new Set<string>()
  const tableRenames = new Map(Rec.toEntries(intent.renames?.tables ?? {}))
  const renamedFrom = new Set(tableRenames.values())

  for (const [to, from] of tableRenames) {
    if (!after.has(to) || before.has(to)) return fail(`renames.tables: "${to}" is not a newly declared table`)
    if (!before.has(from) || after.has(from)) return fail(`renames.tables: "${from}" is not a removed table`)
  }
  /** The previous manifest of a current table, resolved through a declared table rename. */
  const previousOf = (table: string) => before.get(table) ?? before.get(tableRenames.get(table) ?? "")
  for (const [table, columns] of Rec.toEntries(intent.renames?.columns ?? {})) {
    const now = after.get(table)
    const was = previousOf(table)
    if (now === undefined || was === undefined)
      return fail(`renames.columns: "${table}" is not a table in both manifests`)
    for (const [to, from] of Rec.toEntries(columns)) {
      if (!Arr.some(now.manifest.columns, (c) => c.name === to) || Arr.some(was.columns, (c) => c.name === to)) {
        return fail(`renames.columns: "${table}.${to}" is not a newly declared column`)
      }
      if (!Arr.some(was.columns, (c) => c.name === from) || Arr.some(now.manifest.columns, (c) => c.name === from)) {
        return fail(`renames.columns: "${table}.${from}" is not a removed column`)
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
        ...Arr.map(
          was.indexes,
          (i) => `  ALTER INDEX ${quote(indexName(from, i.name))} RENAME TO ${quote(indexName(name, i.name))};`
        ),
        ...(was.scopeWalkIndex === true
          ? [`  ALTER INDEX ${quote(indexName(from, "echo-scope"))} RENAME TO ${quote(indexName(name, "echo-scope"))};`]
          : []),
        `Re-run attach for ${name} at startup. The steps below for ${name} depend on this and are TODOs too;`,
        `they use the new index names.`
      )
    }

    const renames = new Map(Rec.toEntries(intent.renames?.columns?.[name] ?? {}))
    const renameSources = new Set(renames.values())
    const old = new Map(Arr.map(was.columns, (c) => [c.name, c]))
    const now = new Map(Arr.map(table.manifest.columns, (c) => [c.name, c]))
    // Columns whose rename, drop or kind change is a TODO (old and new names): DDL that
    // references one cannot assume the column exists in its declared shape (SPEC §13), and neither
    // can any DDL on a table whose rename is a TODO.
    const blocked = new Set<string>([...renames.keys(), ...renameSources])
    for (const prior of was.columns) {
      const next = now.get(prior.name)
      if (next === undefined || next.kind !== prior.kind) blocked.add(prior.name)
    }
    /** The TODO a step referencing `refs` waits for, if any. */
    const dependency = (refs: ReadonlyArray<string>) => {
      const on = Arr.filter(refs, (r) => blocked.has(r))
      if (from !== undefined) return `rename of table ${from}`
      return on.length > 0
        ? `TODO for ${Arr.join(
            Arr.map(on, (c) => `${name}.${c}`),
            ", "
          )}`
        : undefined
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
            `declared column (SPEC §3), and the live catalog keeps no default the manifest does not know.`
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
      const goes = Arr.map(
        Arr.filter(was.indexes, (i) => Arr.contains(i.columns, prior.name)),
        (i) => indexName(name, i.name)
      )
      todo(
        drops.has(key)
          ? `Column ${key} is no longer declared and its drop is acknowledged. Drops are never generated; when no reader needs it:`
          : `Column ${key} is no longer declared; its data is kept. Acknowledge with acknowledgeDrops: ["${key}"]. To drop by hand:`,
        `  ALTER TABLE ${quote(name)} DROP COLUMN ${quote(prior.name)};`,
        ...(goes.length > 0
          ? [`PostgreSQL drops ${Arr.join(goes, ", ")} with the column; any replacement index is a TODO below.`]
          : [])
      )
    }

    // Indexes: inferred (plain DDL inside the Migrator transaction; indexHash rebuilds nothing).
    const oldIndexes = new Map(Arr.map(was.indexes, (i) => [i.name, i]))
    const newIndexes = new Map(Arr.map(table.manifest.indexes, (i) => [i.name, i]))
    const createFor = (index: string) => {
      const prefix = `CREATE INDEX ${quote(indexName(name, index))} `
      return Option.getOrElse(
        Arr.findFirst(table.postgres.createIndexes, (statement) => Str.startsWith(prefix)(statement)),
        () => {
          throw new Error(`no DDL for ${name}.${index}`)
        }
      )
    }
    if (table.manifest.scopeWalkIndex === true && was.scopeWalkIndex !== true)
      emit(createFor("echo-scope"), ["access_scope_id", "id"])
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
      const goesWithColumn = Arr.some(prior.columns, (c) => !now.has(c) && !renameSources.has(c))
      const changesKind = Arr.some(prior.columns, (c) => {
        const current = now.get(c)
        return current !== undefined && old.get(c)?.kind !== current.kind
      })
      const statements = [
        ...(goesWithColumn ? [] : [changesKind ? Str.replace("DROP INDEX ", "DROP INDEX IF EXISTS ")(drop) : drop]),
        ...(next === undefined ? [] : [createFor(next.name)])
      ]
      if (statements.length > 0) {
        todo(`Depends on the ${waits} above; run by hand after it:`, ...Arr.map(statements, (st) => `  ${st};`))
      }
    }
    for (const next of table.manifest.indexes) if (!oldIndexes.has(next.name)) emit(createFor(next.name), next.columns)

    // Unique sets: TODO only (adding can fail on data; removing needs the generated constraint name).
    const oldUnique = new Set(Arr.map(was.unique, setKey))
    const newUnique = new Set(Arr.map(table.manifest.unique, setKey))
    for (const cs of table.manifest.unique) {
      if (oldUnique.has(setKey(cs))) continue
      todo(
        `Unique set (${Arr.join(cs, ", ")}) added on ${name}. Not generated: it fails on duplicate data. Check, then by hand:`,
        `  ALTER TABLE ${quote(name)} ADD UNIQUE (${Arr.join(Arr.map(cs, quote), ", ")});`
      )
    }
    for (const cs of was.unique) {
      if (newUnique.has(setKey(cs))) continue
      todo(
        `Unique set (${Arr.join(cs, ", ")}) removed from ${name}. Not generated: drop its constraint by hand`,
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
    if (!usedDefaults.has(k)) return fail(`defaults: "${k}" is not a newly declared NOT NULL column`)
  for (const d of drops) {
    const [table, column] = Str.split(d, ".")
    const removed =
      column === undefined
        ? before.has(d) && !after.has(d)
        : Option.match(Option.fromUndefinedOr(previousOf(table ?? "")), {
            onNone: () => false,
            onSome: (previousTable) => Arr.some(previousTable.columns, (c) => c.name === column)
          }) &&
          !Option.match(Option.fromUndefinedOr(after.get(table ?? "")), {
            onNone: () => true,
            onSome: (currentTable) => Arr.some(currentTable.manifest.columns, (c) => c.name === column)
          })
    if (!removed) return fail(`acknowledgeDrops: "${d}" is not a removed table or column`)
  }
  return Result.succeed(steps)
}

// ---------------------------------------------------------------------------------------------
// Module text
// ---------------------------------------------------------------------------------------------

/** Escapes text for a JS template literal. */
const templateText = (text: string) =>
  pipe(text, Str.replaceAll("\\", "\\\\"), Str.replaceAll("`", "\\`"), Str.replaceAll("${", "\\${"))
const indent = (text: string, by: string) =>
  Arr.join(
    Arr.map(Str.split(text, "\n"), (line) => by + line),
    "\n"
  )

/** The migration module for `steps`: default export an Effect over `SqlClient`. */
export const render = (id: string, steps: ReadonlyArray<Step>): string => {
  const body: Array<string> = []
  const hasDdl = Arr.some(steps, (s) => s._tag === "Ddl")
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
  return Arr.join(
    [
      "/**",
      ` * ${id}: generated by echo-sync Migrate.generate (SPEC §13) from the committed manifest`,
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
    ],
    "\n"
  )
}

/** `NNN_name` after the highest of `existing` (ids like `"002_add_notes"` or file names). */
export const nextId = (existing: ReadonlyArray<string>, name: string): Result.Result<string, MigrationPlanError> => {
  if (!/^[a-z0-9_]+$/.test(name))
    return Result.fail(
      new MigrationPlanError({ stage: "name", message: `migration name "${name}" must match /^[a-z0-9_]+$/` })
    )
  const highest = Math.max(
    0,
    ...Arr.map(existing, (e) => Number(/^(\d+)_/.exec(Str.replace(/^.*\//, "")(e))?.[1] ?? 0))
  )
  return Result.succeed(`${Str.padStart(3, "0")(String(highest + 1))}_${name}`)
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
export const generate = Effect.fnUntraced(function* (options: {
  readonly previous: Snapshot
  readonly registry: SyncRegistry
  readonly intent?: Intent
  /** Existing migration ids or file names, to number the next one. */
  readonly existing: ReadonlyArray<string>
  readonly name: string
}): Effect.fn.Return<Generated, MigrationPlanError> {
  const id = yield* Effect.fromResult(nextId(options.existing, options.name))
  const steps = yield* Effect.fromResult(plan(options.previous, options.registry, options.intent))
  const snapshot: Snapshot = {
    ...snapshotOf(options.registry),
    history: [...(options.previous.history ?? []), { migration: id, dataHash: options.registry.dataHash }]
  }
  return { id, source: render(id, steps), snapshot: yield* encodeSnapshot(snapshot), steps }
})
