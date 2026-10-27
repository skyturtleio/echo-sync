import type { DateTime } from "effect"
import {
  Array as Arr,
  Data,
  Option,
  Predicate,
  Record as Rec,
  Schema,
  SchemaAST,
  SchemaGetter,
  String as Str
} from "effect"
import type { AccessScopeId } from "./AccessScope.ts"

// ---------------------------------------------------------------------------------------------
// Column model
// ---------------------------------------------------------------------------------------------

/**
 * The finite set of column kinds (SPEC §3). Every field maps to exactly one; anything that
 * cannot be mapped unambiguously is rejected when the table is declared.
 *
 * | kind      | accepted field (classified on its *encoded* side)          | wire            | Postgres                | SQLite                         |
 * |-----------|------------------------------------------------------------|-----------------|-------------------------|--------------------------------|
 * | text      | `String` (any checks/brands), string literals, template lit | string          | `text COLLATE "C"`      | `TEXT` (BINARY)                |
 * | integer   | `Number` with `isInt` (`Schema.Int`), integer literals      | number          | `bigint`                | `INTEGER`                      |
 * | double    | `Number` with `isFinite` (`Schema.Finite`)                  | number          | `double precision`      | `REAL`                         |
 * | boolean   | `Boolean`, boolean literals                                 | boolean         | `boolean`               | `INTEGER` CHECK (0, 1)         |
 * | timestamp | `Schema.Date`, `Schema.DateTimeUtc`                         | epoch ms number | `timestamptz`           | `INTEGER` (epoch ms)           |
 * | json      | `Struct`, `Array`, `Tuple`, `Record`, non-literal `Union`   | JSON value      | `jsonb`                 | `TEXT` (JSON)                  |
 *
 * `NullOr(x)` makes the column nullable. Rejected: bare `Schema.Number`, `BigInt`, `Symbol`,
 * `Undefined`/optional keys, `Unknown`/`Any`, `Enum`, other declarations (`Uint8Array`, ...).
 * Timestamps are stored as epoch milliseconds, which is order-preserving in every dialect
 * (SPEC §3), and must lie within Postgres' `timestamptz` range.
 */
export type ColumnKind = "text" | "integer" | "double" | "boolean" | "timestamp" | "json"

/** A value a Postgres adapter binds or reads for a column (`@effect/sql-pg` decodes int8 as bigint). */
export type PgValue = string | number | boolean | bigint | globalThis.Date | Schema.Json | null
/** A value a SQLite adapter binds or reads for a column. */
export type SqliteValue = string | number | null

/** A row in wire form: snake_case column names, JSON-safe values. What every RowStore stores. */
export type EncodedRow = { readonly [column: string]: Schema.Json }
export const EncodedRow = Schema.Record(Schema.String, Schema.Json)

export interface Column {
  /** camelCase field name in the Row type. */
  readonly field: string
  /** snake_case column name on the wire and in SQL. */
  readonly name: string
  readonly kind: ColumnKind
  readonly nullable: boolean
  /** Field value ↔ wire value (`Codec<FieldType, Json>`). */
  readonly wire: Schema.Codec<unknown, Schema.Json>
  /** Wire value ↔ Postgres value. */
  readonly postgres: Schema.Codec<Schema.Json, PgValue>
  /** Wire value ↔ SQLite value. */
  readonly sqlite: Schema.Codec<Schema.Json, SqliteValue>
}

/** An index: its declared fields, then `id` as the tiebreak (INV-TBL-2). */
export interface Index<Fs extends ReadonlyArray<string> = ReadonlyArray<string>> {
  readonly name: string
  readonly fields: Fs
  /** Key columns in order: the declared fields' columns followed by the `id` column. */
  readonly key: ReadonlyArray<Column>
}

export interface Dialect<V> {
  readonly createTable: string
  readonly createIndexes: ReadonlyArray<string>
  /** Wire row ↔ dialect row (column-name keyed). Decode at the storage edge. */
  readonly row: Schema.Codec<EncodedRow, { readonly [column: string]: V }>
}

export interface ColumnManifest {
  readonly name: string
  readonly kind: ColumnKind
  readonly nullable: boolean
}

export interface TableManifest {
  readonly name: string
  readonly columns: ReadonlyArray<ColumnManifest>
  readonly indexes: ReadonlyArray<{ readonly name: string; readonly columns: ReadonlyArray<string> }>
  /** Server-only scope/id seek index; absent in snapshots predating its introduction. Not hashed. */
  readonly scopeWalkIndex?: true
  /** Declared unique column sets (column names, in declaration order). Part of `indexHash`. */
  readonly unique: ReadonlyArray<ReadonlyArray<string>>
  /**
   * Hash of the storage shape (table name; column names, kinds, nullability). Changes ⇒ local
   * migration and background refresh. Refinements, brands and nested JSON schemas are validation, not storage, and do not
   * contribute (INV-TBL-3); neither do indexes.
   */
  readonly dataHash: string
  /** Hash of the index set. Changing it must not force a replica re-download (INV-TBL-3). */
  readonly indexHash: string
}

/** The type-erased view of a table that stores and substrates work with. */
export interface AnySyncTable {
  readonly name: string
  readonly kind: "sync" | "catalog"
  readonly wire: Schema.Codec<{ readonly id: string; readonly accessScopeId?: AccessScopeId }, EncodedRow>
  readonly columns: ReadonlyArray<Column>
  readonly indexes: { readonly [name: string]: Index }
  readonly manifest: TableManifest
  readonly postgres: Dialect<PgValue>
  readonly sqlite: Dialect<SqliteValue>
}

/** Only scoped tables may be written by mutation handlers. */
export interface WritableTable extends AnySyncTable {
  readonly kind: "sync"
  readonly wire: Schema.Codec<{ readonly id: string; readonly accessScopeId: AccessScopeId }, EncodedRow>
}

// ---------------------------------------------------------------------------------------------
// Type-level derivation
// ---------------------------------------------------------------------------------------------

export type SnakeCase<S extends string> = S extends `${infer H}${infer T}`
  ? `${H extends Lowercase<H> ? H : `_${Lowercase<H>}`}${SnakeCase<T>}`
  : S

/** The wire type of a field, from its encoded type. Mirrors the runtime classification. */
export type WireValue<E> = E extends null
  ? null
  : E extends globalThis.Date | DateTime.Utc
    ? number
    : E extends string | number | boolean
      ? E
      : Schema.Json

type TableFields = Schema.Struct.Fields

export type Row<F extends TableFields> = Schema.Struct<F>["Type"]
export type Encoded<F extends TableFields> = {
  readonly [K in keyof F & string as SnakeCase<K>]: WireValue<F[K]["Encoded"]>
}

/** Fields usable in an index: those whose wire value is a scalar (not a JSON column). */
export type IndexableField<F extends TableFields> = {
  [K in keyof F & string]: [WireValue<F[K]["Encoded"]>] extends [string | number | boolean | null] ? K : never
}[keyof F & string]

export type IndexMap<F extends TableFields> = {
  readonly [name: string]: readonly [IndexableField<F>, ...ReadonlyArray<IndexableField<F>>]
}

/** Required fields of every table. */
export type RequiredFields = {
  readonly id: Schema.Constraint & { readonly Type: string; readonly Encoded: string }
  readonly accessScopeId: typeof AccessScopeId
}

type FieldProblem<K, S extends Schema.Constraint> = S["~type.optionality"] extends "optional"
  ? "SyncTable: optional keys are not supported; use Schema.NullOr"
  : S["~encoded.optionality"] extends "optional"
    ? "SyncTable: encoded-side defaults and optional keys are not supported; use Schema.NullOr"
    : [S["DecodingServices"] | S["EncodingServices"]] extends [never]
      ? [Exclude<S["Encoded"], null>] extends [never]
        ? "SyncTable: a field cannot be only null"
        : Exclude<S["Encoded"], null> extends bigint | symbol | undefined | Uint8Array
          ? "SyncTable: unsupported leaf schema (bigint, symbol, undefined and binary have no column mapping)"
          : K extends "id"
            ? string extends S["Type"]
              ? "SyncTable: id must be a branded string schema"
              : never
            : never
      : "SyncTable: field schemas must not require services (row codecs run with no context)"

/** Maps each invalid field to an explanatory string literal so the declaration fails to compile. */
export type ValidateFields<F extends TableFields> = {
  readonly [K in keyof F]: [FieldProblem<K, F[K]>] extends [never] ? F[K] : FieldProblem<K, F[K]>
}

/** A declared table. Concrete instances are assignable to `AnySyncTable`. */
export interface SyncTable<Name extends string, F extends TableFields, I extends IndexMap<F>> {
  readonly name: Name
  readonly kind: "sync"
  readonly fields: F
  /** The decoded row schema. Use it for query `success` schemas etc. */
  readonly Row: Schema.Struct<F>
  /** Row ↔ wire row (snake_case keys, JSON-safe values). */
  readonly wire: Schema.Codec<Row<F>, Encoded<F>>
  readonly indexes: { readonly [K in keyof I]: Index<I[K]> }
  readonly columns: ReadonlyArray<Column>
  readonly manifest: TableManifest
  readonly postgres: Dialect<PgValue>
  readonly sqlite: Dialect<SqliteValue>
}

export interface CatalogTable<Name extends string, F extends TableFields, I extends IndexMap<F>> extends Omit<
  SyncTable<Name, F, I>,
  "kind"
> {
  readonly kind: "catalog"
}

// ---------------------------------------------------------------------------------------------
// Declaration
// ---------------------------------------------------------------------------------------------

export class SyncTableDeclarationError extends Data.TaggedError("SyncTableDeclarationError")<{
  readonly message: string
}> {
  constructor(message: string) {
    super({ message })
  }
}

const identifier = /^[a-z][a-z0-9_]*$/
const fieldName = /^[a-z][a-zA-Z0-9]*$/

/** Postgres `timestamptz` lower bound (4714-11-24T00:00:00Z BC) and the JS `Date` upper bound. */
export const minTimestampMillis = -210_866_803_200_000
export const maxTimestampMillis = 8_640_000_000_000_000

const representationId = (ast: SchemaAST.AST): unknown => {
  const representation = ast.annotations?.["representation"]
  return Predicate.hasProperty(representation, "id") ? representation.id : undefined
}

const checkHas = (check: SchemaAST.Check<unknown>, id: string): boolean =>
  check._tag === "Filter"
    ? check.annotations?.representation?.id === id
    : Arr.some(check.checks, (inner) => checkHas(inner, id))

const hasCheck = (ast: SchemaAST.AST, id: string): boolean => Arr.some(ast.checks ?? [], (c) => checkHas(c, id))

type Leaf = { readonly kind: ColumnKind } | { readonly problem: string }

const leaf = (ast: SchemaAST.AST): Leaf => {
  switch (ast._tag) {
    case "String":
    case "TemplateLiteral":
      return { kind: "text" }
    case "Boolean":
      return { kind: "boolean" }
    case "Number":
      if (hasCheck(ast, "effect/schema/isInt")) return { kind: "integer" }
      if (hasCheck(ast, "effect/schema/isFinite")) return { kind: "double" }
      return {
        problem: "a bare Schema.Number is ambiguous (integer or double? NaN/Infinity?); use Schema.Int or Schema.Finite"
      }
    case "Literal":
      switch (typeof ast.literal) {
        case "string":
          return { kind: "text" }
        case "boolean":
          return { kind: "boolean" }
        case "number":
          return Number.isSafeInteger(ast.literal) ? { kind: "integer" } : { problem: "non-integer number literal" }
        default:
          return { problem: "bigint literals have no column mapping" }
      }
    case "Declaration": {
      const id = representationId(ast)
      if (id === "effect/schema/Date" || id === "effect/schema/DateTimeUtc") return { kind: "timestamp" }
      return { problem: `declaration ${String(id ?? "<anonymous>")} has no column mapping` }
    }
    case "Objects":
    case "Arrays":
    case "Suspend":
    case "Union":
      return { kind: "json" }
    case "BigInt":
      return { problem: "bigint has no portable column mapping; use Schema.BigIntFromString (stored as text)" }
    case "Enum":
      return { problem: "enums are not supported; use Schema.Literals" }
    default:
      return { problem: `${ast._tag} has no column mapping` }
  }
}

interface Classified {
  readonly kind: ColumnKind
  readonly nullable: boolean
}

/**
 * Optionality at the field level: the node itself or, through field-level unions (`NullOr`
 * etc.) and `suspend` (cycle-safe), any member. Does not descend into Objects/Arrays, so optional properties inside a JSON
 * column remain allowed.
 */
const optionalAtField = (ast: SchemaAST.AST, seen: ReadonlySet<SchemaAST.AST> = new Set()): boolean => {
  if (SchemaAST.isOptional(ast)) return true
  if (seen.has(ast)) return false
  const next = new Set(seen).add(ast)
  if (ast._tag === "Union") return Arr.some(ast.types, (t) => optionalAtField(t, next))
  if (ast._tag === "Suspend") return optionalAtField(ast.thunk(), next)
  return false
}

// NullOr(Literals(...)) retains a nested Union in Effect's AST. Classification
// and timestamp codec selection must inspect the same encoded leaves.
const unionLeaves = (node: SchemaAST.AST): ReadonlyArray<SchemaAST.AST> =>
  node._tag === "Union" ? Arr.flatMap(node.types, unionLeaves) : [node]

/**
 * Classifies a field by its *encoded* side (what is stored), so a transformed field (branded,
 * refined, or decoded into another type) maps by the column it actually occupies.
 */
const classify = (ast: SchemaAST.AST): Classified | string => {
  const encoded = SchemaAST.toEncoded(ast)
  // Both sides: an encoded-side default or optional key would let a stored row omit the key.
  if (optionalAtField(ast) || optionalAtField(encoded)) {
    return "optional keys and encoded-side defaults are not supported; use Schema.NullOr"
  }
  if (encoded._tag === "Null") return "a field cannot be only null"
  if (encoded._tag !== "Union") {
    const l = leaf(encoded)
    return "problem" in l ? l.problem : { kind: l.kind, nullable: false }
  }
  const types = unionLeaves(encoded)
  const members = Arr.filter(types, (t) => t._tag !== "Null")
  const nullable = members.length !== types.length
  const [first, ...rest] = members
  if (first === undefined) return "a field cannot be only null"
  if (rest.length === 0) {
    const l = leaf(first)
    return "problem" in l ? l.problem : { kind: l.kind, nullable }
  }
  const kinds = new Set(Arr.map(members, (m) => (m._tag === "Literal" ? JSON.stringify(leaf(m)) : "structural")))
  const [only] = kinds
  if (kinds.size === 1 && only !== undefined && only !== "structural") {
    const l = leaf(first)
    return "problem" in l ? l.problem : { kind: l.kind, nullable }
  }
  return { kind: "json", nullable }
}

const TimestampMillis = Schema.Int.check(Schema.isBetween({ minimum: minTimestampMillis, maximum: maxTimestampMillis }))
const DateFromWire = TimestampMillis.pipe(Schema.decodeTo(Schema.DateFromMillis))
const UtcFromWire = TimestampMillis.pipe(Schema.decodeTo(Schema.DateTimeUtcFromMillis))

/**
 * Field value ↔ wire value. The whole field schema stays the decoding target, so its own
 * transformations and checks run unchanged; only its stored (encoded) end is mapped to JSON.
 */
const wireFor = (field: Schema.Constraint, c: Classified): Schema.Constraint => {
  if (c.kind !== "timestamp") return Schema.toCodecJson(field)
  const encoded = SchemaAST.toEncoded(field.ast)
  const stored = Option.getOrElse(
    Arr.findFirst(unionLeaves(encoded), (t) => t._tag !== "Null"),
    () => encoded
  )
  const millis = representationId(stored) === "effect/schema/DateTimeUtc" ? UtcFromWire : DateFromWire
  return (c.nullable ? Schema.NullOr(millis) : millis).pipe(Schema.decodeTo(field))
}

const SqliteBoolean = Schema.Literals([0, 1]).pipe(
  Schema.decodeTo(Schema.Boolean, {
    decode: SchemaGetter.transform((n: 0 | 1) => n === 1),
    encode: SchemaGetter.transform((b: boolean) => (b ? 1 : 0))
  })
)
const PgInteger = Schema.BigInt.pipe(
  Schema.decodeTo(Schema.Int, {
    decode: SchemaGetter.transform((b: bigint) => Number(b)),
    encode: SchemaGetter.transform((n: number) => BigInt(n))
  })
)
const PgTimestamp = Schema.flip(DateFromWire)

const sqliteCodec = (kind: ColumnKind): Schema.Constraint => {
  switch (kind) {
    case "text":
      return Schema.String
    case "integer":
      return Schema.Int
    case "double":
      return Schema.Finite
    case "boolean":
      return SqliteBoolean
    case "timestamp":
      return TimestampMillis
    case "json":
      return Schema.fromJsonString(Schema.Json)
  }
}

const pgCodec = (kind: ColumnKind): Schema.Constraint => {
  switch (kind) {
    case "text":
      return Schema.String
    case "integer":
      return PgInteger
    case "double":
      return Schema.Finite
    case "boolean":
      return Schema.Boolean
    case "timestamp":
      return PgTimestamp
    case "json":
      return Schema.Json
  }
}

/** The Postgres column type for a column kind (text is `COLLATE "C"`, INV-TBL-2). */
export const pgType: Record<ColumnKind, string> = {
  text: 'text COLLATE "C"',
  integer: "bigint",
  double: "double precision",
  boolean: "boolean",
  timestamp: "timestamptz",
  json: "jsonb"
}
const sqliteType: Record<ColumnKind, string> = {
  text: "TEXT",
  integer: "INTEGER",
  double: "REAL",
  boolean: "INTEGER",
  timestamp: "INTEGER",
  json: "TEXT"
}

const quote = (sqlName: string): string => `"${sqlName}"`

/**
 * The one place derived runtime schemas meet their type-level mirror. The runtime structs are
 * built field by field from the classification above; their static types are computed by
 * `Row`/`Encoded`/`WireValue`, which TypeScript cannot relate to a dynamically built struct.
 * The codec round-trip tests (SyncTable.test.ts) pin that the two agree.
 */
const derived = <T, E>(schema: Schema.Constraint): Schema.Codec<T, E> =>
  // oxlint-disable-next-line typescript/consistent-type-assertions -- derived-schema edge, see doc comment
  schema as unknown as Schema.Codec<T, E>

/** Canonical JSON: object keys sorted, no whitespace. */
export const canonicalJson = (json: Schema.Json): string => {
  if (Arr.isArray<Schema.Json>(json)) return `[${Arr.join(Arr.map(json, canonicalJson), ",")}]`
  if (json !== null && typeof json === "object") {
    const entries = Arr.map(
      Arr.sort(Rec.toEntries(json), ([a]: [string, Schema.Json], [b]: [string, Schema.Json]) =>
        a < b ? -1 : a > b ? 1 : 0
      ),
      ([key, value]) => `${JSON.stringify(key)}:${canonicalJson(value)}`
    )
    return `{${Arr.join(entries, ",")}}`
  }
  return JSON.stringify(json)
}

/** FNV-1a 64-bit over UTF-8, hex. Detects change; not a security primitive. */
export const hash = (text: string): string => {
  let h = 0xcbf29ce484222325n
  for (const byte of new TextEncoder().encode(text)) h = BigInt.asUintN(64, (h ^ BigInt(byte)) * 0x100000001b3n)
  return Str.padStart(16, "0")(h.toString(16))
}

/**
 * Declares a synced table (SPEC §3). Throws `SyncTableDeclarationError` for anything that
 * cannot be mapped unambiguously; the type parameters already reject the cases visible to
 * TypeScript (missing `id`/`accessScopeId`, unbranded `id`, optional keys, bigint/symbol leaves,
 * unknown or JSON index fields).
 */
export const make = <
  const Name extends string,
  const F extends RequiredFields & TableFields,
  const I extends IndexMap<F> = Record<never, never>
>(
  name: Name,
  fields: F & ValidateFields<F>,
  options?: {
    readonly indexes?: I
    /** Column sets that must be unique (both dialects' DDL carry the constraint). */
    readonly unique?: ReadonlyArray<readonly [keyof F & string, ...ReadonlyArray<keyof F & string>]>
  }
): SyncTable<Name, F, I> => declare<Name, F, I, "sync">(name, fields, "sync", options)

/** A global, client-read-only reference table, independently versioned and downloaded. */
export const catalog = <
  const Name extends string,
  const F extends Pick<RequiredFields, "id"> & TableFields,
  const I extends IndexMap<F> = Record<never, never>
>(
  name: Name,
  fields: F & ValidateFields<F> & { readonly accessScopeId?: never },
  options?: { readonly indexes?: I }
): CatalogTable<Name, F, I> => declare<Name, F, I, "catalog">(name, fields, "catalog", options)

const declare = <Name extends string, F extends TableFields, I extends IndexMap<F>, Kind extends "sync" | "catalog">(
  name: Name,
  fields: F,
  kind: Kind,
  options?: {
    readonly indexes?: I
    readonly unique?: ReadonlyArray<readonly [keyof F & string, ...ReadonlyArray<keyof F & string>]>
  }
): Omit<SyncTable<Name, F, I>, "kind"> & { readonly kind: Kind } => {
  const fail = (message: string): never => {
    throw new SyncTableDeclarationError(`SyncTable "${name}": ${message}`)
  }
  if (!identifier.test(name) || name.length > 48) fail("table name must match /^[a-z][a-z0-9_]*$/, ≤ 48 chars")

  const all: TableFields = fields
  const columns: Array<Column> = []
  const wireFields: { [field: string]: Schema.Constraint } = {}
  const keyMap: { [field: string]: string } = {}
  const pgFields: { [column: string]: Schema.Constraint } = {}
  const sqliteFields: { [column: string]: Schema.Constraint } = {}
  const manifestColumns: Array<ColumnManifest> = []

  for (const [field, schema] of Rec.toEntries(all)) {
    const column = Str.camelToSnake(field)
    if (!fieldName.test(field) || Str.snakeToCamel(column) !== field || column.length > 63) {
      fail(`field "${field}": names must be camelCase (/^[a-z][a-zA-Z0-9]*$/) and round-trip through snake_case`)
    }
    const c = classify(schema.ast)
    if (typeof c === "string") return fail(`field "${field}": ${c}`)
    const wire = wireFor(schema, c)
    const nullable = <S extends Schema.Constraint>(s: S): Schema.Constraint => (c.nullable ? Schema.NullOr(s) : s)
    const pg = nullable(pgCodec(c.kind))
    const sqlite = nullable(sqliteCodec(c.kind))
    wireFields[field] = wire
    keyMap[field] = column
    pgFields[column] = pg
    sqliteFields[column] = sqlite
    columns.push({
      field,
      name: column,
      kind: c.kind,
      nullable: c.nullable,
      wire: derived(wire),
      postgres: derived(pg),
      sqlite: derived(sqlite)
    })
    manifestColumns.push({ name: column, kind: c.kind, nullable: c.nullable })
  }

  const columnOf = (field: string): Column =>
    Option.getOrElse(
      Arr.findFirst(columns, (c) => c.field === field),
      () => fail(`unknown field "${field}"`)
    )
  const idColumn = columnOf("id")
  if (idColumn.kind !== "text" || idColumn.nullable) fail(`"id" must be a non-null string schema`)
  if (kind === "sync") {
    const scopeColumn = columnOf("accessScopeId")
    if (scopeColumn.kind !== "text" || scopeColumn.nullable) fail(`"accessScopeId" must be AccessScopeId`)
  } else if (Arr.some(columns, (c) => c.field === "accessScopeId")) {
    fail("catalogs must not declare accessScopeId")
  }

  const declared: { readonly [index: string]: ReadonlyArray<string> } = options?.indexes ?? {}
  const indexes: { [index: string]: Index } = {}
  for (const [index, indexFields] of Rec.toEntries(declared)) {
    if (!identifier.test(index) || `${name}__${index}`.length > 63) fail(`index "${index}": invalid name`)
    if (indexFields.length === 0) fail(`index "${index}": needs at least one field`)
    if (new Set(indexFields).size !== indexFields.length) fail(`index "${index}": repeats a field`)
    const key = Arr.map(indexFields, columnOf)
    for (const c of key) {
      if (c.kind === "json") fail(`index "${index}": field "${c.field}" is a JSON column and cannot be indexed`)
      if (c.field === "id") fail(`index "${index}": "id" is always the implicit last key column`)
    }
    indexes[index] = { name: index, fields: indexFields, key: [...key, idColumn] }
  }

  const uniqueSets = Arr.map(options?.unique ?? [], (fs) => Arr.map(fs, (f) => columnOf(f).name))
  const uniques = Arr.map(uniqueSets, (cs) => `  UNIQUE (${Arr.join(Arr.map(cs, quote), ", ")})`)
  const pgColumns = [
    ...Arr.map(
      columns,
      (c) =>
        `  ${quote(c.name)} ${pgType[c.kind]}${c.nullable ? "" : " NOT NULL"}${c === idColumn ? " PRIMARY KEY" : ""}`
    ),
    ...uniques
  ]
  const sqliteColumns = [
    ...Arr.map(
      columns,
      (c) =>
        `  ${quote(c.name)} ${sqliteType[c.kind]}${c.nullable ? "" : " NOT NULL"}${c === idColumn ? " PRIMARY KEY" : ""}${
          c.kind === "boolean" ? ` CHECK (${quote(c.name)} IN (0, 1))` : ""
        }`
    ),
    ...uniques
  ]
  const indexDdl = (unique: string) =>
    Arr.map(
      Rec.values(indexes),
      (i) =>
        `CREATE INDEX ${quote(`${name}__${i.name}`)} ON ${quote(name)} (${Arr.join(
          Arr.map(i.key, (c) => `${quote(c.name)} ASC${unique === "pg" && c !== idColumn ? " NULLS FIRST" : ""}`),
          ", "
        )})`
    )

  const indexManifest = Arr.sort(
    Arr.map(Rec.values(indexes), (i) => ({ name: i.name, columns: Arr.map(i.key, (c) => c.name) })),
    (a: { readonly name: string }, b: { readonly name: string }) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0)
  )
  const sortedColumns = Arr.sort(manifestColumns, (a: ColumnManifest, b: ColumnManifest) =>
    a.name < b.name ? -1 : a.name > b.name ? 1 : 0
  )
  const manifest: TableManifest = {
    name,
    columns: sortedColumns,
    indexes: indexManifest,
    ...(kind === "sync" ? { scopeWalkIndex: true as const } : {}),
    unique: uniqueSets,
    dataHash: hash(
      canonicalJson({
        name,
        columns: Arr.map(sortedColumns, (c) => ({ name: c.name, kind: c.kind, nullable: c.nullable }))
      })
    ),
    indexHash: hash(
      canonicalJson({
        indexes: Arr.map(indexManifest, (i) => ({ name: i.name, columns: [...i.columns] })),
        unique: uniques
      })
    )
  }

  const wire = Schema.Struct(wireFields).pipe(Schema.encodeKeys(keyMap))
  const typedIndexes: { readonly [K in keyof I]: Index<I[K]> } = derivedIndexes(indexes)
  return {
    name,
    kind,
    fields,
    Row: Schema.Struct<F>(fields),
    wire: derived(wire),
    columns,
    indexes: typedIndexes,
    manifest,
    postgres: {
      createTable: `CREATE TABLE ${quote(name)} (\n${Arr.join(pgColumns, ",\n")}\n)`,
      createIndexes: [
        ...indexDdl("pg"),
        ...(kind === "sync"
          ? [
              `CREATE INDEX ${quote(`${name}__echo-scope`)} ON ${quote(name)} ("access_scope_id" ASC NULLS FIRST, "id" ASC)`
            ]
          : [])
      ],
      row: derived(Schema.Struct(pgFields))
    },
    sqlite: {
      createTable: `CREATE TABLE ${quote(name)} (\n${Arr.join(sqliteColumns, ",\n")}\n) STRICT`,
      createIndexes: indexDdl("sqlite"),
      row: derived(Schema.Struct(sqliteFields))
    }
  }
}

/** Same edge as `derived`: the index record is built from `options.indexes`, keyed identically. */
const derivedIndexes = <T>(indexes: { readonly [index: string]: Index }): T =>
  // oxlint-disable-next-line typescript/consistent-type-assertions -- built from the typed `indexes` option, same keys
  indexes as unknown as T
