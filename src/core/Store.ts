import { Array as Arr, type DateTime, Effect, Option, Schema } from "effect"
import {
  type AccessScopeId,
  AccessScopeId as AccessScopeIdSchema,
  Everyone,
  type PrincipalId,
  PrincipalId as PrincipalIdSchema,
  type Role,
  satisfies,
  strongest
} from "./AccessScope.ts"
import {
  type ConstraintViolated,
  CorruptStorage,
  InvalidRow,
  RowAlreadyExists,
  RowNotFound,
  ScopeForbidden,
  type StoreUnavailable
} from "./Errors.ts"
import { grantId, ScopeGrants } from "./ScopeGrants.ts"
import * as Search from "./Search.ts"
import type { AnySyncTable, Column, EncodedRow, Index } from "./SyncTable.ts"

// ---------------------------------------------------------------------------------------------
// Ordering: the one definition every RowStore must agree with (INV-TBL-2)
// ---------------------------------------------------------------------------------------------

/** A wire value in an index key column (index columns are never JSON). */
export type KeyValue = string | number | boolean | null

/** Code-point order (= UTF-8 byte order = SQLite BINARY = Postgres COLLATE "C"), not UTF-16. */
export const compareText = (a: string, b: string): number => {
  let i = 0
  let j = 0
  while (i < a.length && j < b.length) {
    const ca = a.codePointAt(i) ?? 0
    const cb = b.codePointAt(j) ?? 0
    if (ca !== cb) return ca < cb ? -1 : 1
    i += ca > 0xffff ? 2 : 1
    j += cb > 0xffff ? 2 : 1
  }
  return i < a.length ? 1 : j < b.length ? -1 : 0
}

/**
 * Total order on key values of one column: `null` is the smallest value; text by code point;
 * numbers (integer, double, timestamp ms) numerically; `false < true`.
 */
export const compareKeyValue = (a: Schema.Json, b: Schema.Json): number => {
  if (a === null || b === null) return a === b ? 0 : a === null ? -1 : 1
  if (typeof a === "string" && typeof b === "string") return compareText(a, b)
  if (typeof a === "number" && typeof b === "number") return a < b ? -1 : a > b ? 1 : 0
  if (typeof a === "boolean" && typeof b === "boolean") return a === b ? 0 : a ? 1 : -1
  throw new TypeError(`compareKeyValue: incomparable key values ${JSON.stringify(a)} / ${JSON.stringify(b)}`)
}

/** Lexicographic comparison of two rows on `key` columns (ascending). */
export const compareRows =
  (key: ReadonlyArray<Column>) =>
  (a: EncodedRow, b: EncodedRow): number => {
    for (const c of key) {
      const d = compareKeyValue(a[c.name] ?? null, b[c.name] ?? null)
      if (d !== 0) return d
    }
    return 0
  }

// ---------------------------------------------------------------------------------------------
// Index range plan: dialect-agnostic description of an index read
// ---------------------------------------------------------------------------------------------

export interface Bound {
  readonly value: string | number | boolean
  readonly inclusive: boolean
}

/**
 * An index read, in wire values. Semantics (identical in every RowStore):
 * - rows whose first `eq.length` key columns equal `eq` (`null` matches only `null`: IS NULL),
 * - and, on key column `eq.length`, within `lower`/`upper` (a bound never matches `null`),
 * - ordered by the full index key (`index.key`, which ends with `id`) in `order`;
 *   `asc` ⇒ NULLS FIRST, `desc` ⇒ NULLS LAST on every column.
 * `eq` and bound values are wire values; a SQL RowStore binds each through its key column's
 * dialect codec (`column.sqlite` / `column.postgres`), e.g. a timestamp bound is epoch ms on
 * the wire and a `Date` for Postgres.
 *
 * Scope and limit are part of the plan so the store stops after `limit` readable rows instead of
 * scanning past unreadable ones (SPEC §4):
 * - `readableBy` (`Some(principals)`): only rows whose `access_scope_id` has a `scope_grants` row
 *   for one of `principals` (every role satisfies `reader`, so any grant makes a scope readable),
 *   read in the same transaction as the rows; `None` = no scope constraint;
 * - `limit` (`Some(n)`, a non-negative integer): the first `n` rows, in plan order, of those that
 *   match everything else; `None` = all.
 */
export interface IndexRangePlan {
  readonly index: Index
  readonly text?: { readonly column: Column; readonly needle: string }
  readonly eq: ReadonlyArray<KeyValue>
  readonly lower: Option.Option<Bound>
  readonly upper: Option.Option<Bound>
  readonly order: "asc" | "desc"
  readonly readableBy: Option.Option<ReadonlyArray<PrincipalId>>
  readonly limit: Option.Option<number>
}

/** Reference predicate for `plan` (MemoryStore uses it; SQL stores must agree). */
export const planMatches = (plan: IndexRangePlan, row: EncodedRow): boolean => {
  if (plan.text !== undefined) {
    const value = row[plan.text.column.name]
    if (typeof value !== "string" || !value.includes(plan.text.needle)) return false
  }
  for (const [i, value] of plan.eq.entries()) {
    const c = plan.index.key[i]
    if (c === undefined || compareKeyValue(row[c.name] ?? null, value) !== 0) return false
  }
  const ranged = plan.index.key[plan.eq.length]
  if (ranged === undefined || (Option.isNone(plan.lower) && Option.isNone(plan.upper))) return true
  const value = row[ranged.name] ?? null
  if (value === null) return false
  if (Option.isSome(plan.lower)) {
    const d = compareKeyValue(value, plan.lower.value.value)
    if (d < 0 || (d === 0 && !plan.lower.value.inclusive)) return false
  }
  if (Option.isSome(plan.upper)) {
    const d = compareKeyValue(value, plan.upper.value.value)
    if (d > 0 || (d === 0 && !plan.upper.value.inclusive)) return false
  }
  return true
}

/** Reference result order for `plan`. */
export const planOrder = (plan: IndexRangePlan): ((a: EncodedRow, b: EncodedRow) => number) => {
  const asc = compareRows(plan.index.key)
  return plan.order === "asc" ? asc : (a, b) => asc(b, a)
}

/**
 * A plan limit (`take(n)`) must be a non-negative safe integer (SPEC §3). Anything else is a
 * programmer error: a defect with this one message in every store, never clamped or unlimited.
 */
export const checkLimit = (limit: Option.Option<number>): Effect.Effect<void> =>
  Option.isSome(limit) && !(Number.isSafeInteger(limit.value) && limit.value >= 0)
    ? Effect.die(new RangeError(`take(n) requires a non-negative safe integer, got ${limit.value}`))
    : Effect.void

const grantPair = (scope: unknown, principal: unknown) => JSON.stringify([scope, principal])

/**
 * Reference result of `plan` over `rows` (a table's rows) given `grants` (the `scope_grants`
 * table's rows): match, scope, order, then limit. MemoryStore uses it; SQL stores must agree.
 * A grant is matched by its `access_scope_id` and `principal_id` columns, as the SQL semi-join
 * does, never by its id (storage does not enforce `id = grantId(...)` for rows written around
 * the store).
 */
export const planResult = (
  plan: IndexRangePlan,
  rows: Iterable<EncodedRow>,
  grants: Iterable<EncodedRow>
): ReadonlyArray<EncodedRow> => {
  const granted = new Set(Array.from(grants, (g) => grantPair(g["access_scope_id"], g["principal_id"])))
  const readable = (row: EncodedRow) =>
    Option.match(plan.readableBy, {
      onNone: () => true,
      onSome: (principals) => principals.some((p) => granted.has(grantPair(row["access_scope_id"], p)))
    })
  const ordered = [...rows].filter((row) => planMatches(plan, row) && readable(row)).toSorted(planOrder(plan))
  return Option.match(plan.limit, { onNone: () => ordered, onSome: (n) => ordered.slice(0, n) })
}

/** The `id` column of a stored row, when it has one. */
const rowId = (row: unknown): string | undefined =>
  typeof row === "object" && row !== null && "id" in row && typeof row.id === "string" ? row.id : undefined

/**
 * Decode data the engine itself stored. It was written through `schema`, so a failure is corruption of trusted
 * storage, not a domain outcome: the `CorruptStorage` defect, naming the table and the row (`id`, by default
 * the row's own `id` column).
 */
export const decodeStored = <A>(
  schema: Schema.Decoder<A>,
  table: string,
  stored: unknown,
  id: string | undefined = rowId(stored)
): Effect.Effect<A> =>
  Schema.decodeUnknownEffect(schema)(stored).pipe(
    Effect.catch((cause) => Effect.die(new CorruptStorage({ table, id, cause })))
  )

// ---------------------------------------------------------------------------------------------
// RowStore ports (implemented per dialect)
// ---------------------------------------------------------------------------------------------

/**
 * Untyped row storage over wire-encoded rows. Implemented by Postgres, SQLite and the in-memory
 * reference. Contract for every method:
 * - runs inside a transaction the substrate owns; never begins, commits or rolls back;
 * - performs no authorization (the typed `EchoStore` does);
 * - reads fail only with the transient `StoreUnavailable`; writes may also fail permanently
 *   with `ConstraintViolated` when an authoritative database constraint refuses the write.
 */
export interface RowStore {
  readonly read: (table: AnySyncTable, id: string) => Effect.Effect<Option.Option<EncodedRow>, StoreUnavailable>
  /** Upsert (`Some`) or delete (`None`) the row with this id. */
  readonly write: (
    table: AnySyncTable,
    id: string,
    row: Option.Option<EncodedRow>
  ) => Effect.Effect<void, StoreUnavailable | ConstraintViolated>
  /** Rows matching `plan`: `planResult` (match, readable scope, plan order, limit). */
  readonly range: (
    table: AnySyncTable,
    plan: IndexRangePlan
  ) => Effect.Effect<ReadonlyArray<EncodedRow>, StoreUnavailable>
  /** All rows, ordered by `id` (code point). For small tables only. */
  readonly scan: (table: AnySyncTable) => Effect.Effect<ReadonlyArray<EncodedRow>, StoreUnavailable>
}

/** Replica-only operations (SQLite, MemoryStore). The server never purges or clears. */
export interface ReplicaRowStore extends RowStore {
  /** Delete every replicated row whose `access_scope_id` is `scopeId`, in every table. */
  readonly purgeScope: (scopeId: AccessScopeId) => Effect.Effect<void, StoreUnavailable>
  /** Delete every replicated row (not outbox, not staging). Used only for a null-cursor install. */
  readonly clearAll: Effect.Effect<void, StoreUnavailable>
}

// ---------------------------------------------------------------------------------------------
// Typed read API
// ---------------------------------------------------------------------------------------------

export type RowOf<T extends AnySyncTable> = T["wire"]["Type"]

type Head<Fs> = Fs extends readonly [infer H, ...ReadonlyArray<unknown>] ? H : never
type Tail<Fs> = Fs extends readonly [unknown, ...infer R] ? R : readonly []
type ValueOf<Row, K> = K extends keyof Row ? Row[K] : never

interface RangeState {
  readonly prefix?: readonly [field: string, value: string]
  readonly eq: ReadonlyArray<readonly [field: string, value: unknown]>
  readonly lower: Option.Option<readonly [field: string, value: unknown, inclusive: boolean]>
  readonly upper: Option.Option<readonly [field: string, value: unknown, inclusive: boolean]>
}

/** A finished index range. Every builder stage is one. */
export class IndexRange {
  readonly state: RangeState
  constructor(state: RangeState) {
    this.state = state
  }
}

/** After a lower bound on field `K`: only an upper bound on the same field may follow. */
export class LowerBounded<Row, K> extends IndexRange {
  lt(field: K & string, value: NonNullable<ValueOf<Row, K>>): IndexRange {
    return new IndexRange({ ...this.state, upper: Option.some([field, value, false]) })
  }
  lte(field: K & string, value: NonNullable<ValueOf<Row, K>>): IndexRange {
    return new IndexRange({ ...this.state, upper: Option.some([field, value, true]) })
  }
}

/**
 * Builder typed by the index's remaining fields `Fs`: `eq` consumes fields in order; a range
 * (`gt/gte/lt/lte`) applies to the next field and ends the equality prefix. Wrong field names,
 * out-of-order fields and wrongly typed values do not compile.
 */
export class IndexRangeBuilder<Row, Fs> extends IndexRange {
  /** Literal prefix of the next text index field; uses BINARY/C range bounds, not LIKE. */
  prefix(field: Head<Fs> & TextField<Row>, value: string): IndexRange {
    return new IndexRange({ ...this.state, prefix: [field, value] })
  }
  eq(field: Head<Fs> & string, value: ValueOf<Row, Head<Fs>>): IndexRangeBuilder<Row, Tail<Fs>> {
    return new IndexRangeBuilder<Row, Tail<Fs>>({ ...this.state, eq: [...this.state.eq, [field, value]] })
  }
  gt(field: Head<Fs> & string, value: NonNullable<ValueOf<Row, Head<Fs>>>): LowerBounded<Row, Head<Fs>> {
    return new LowerBounded<Row, Head<Fs>>({ ...this.state, lower: Option.some([field, value, false]) })
  }
  gte(field: Head<Fs> & string, value: NonNullable<ValueOf<Row, Head<Fs>>>): LowerBounded<Row, Head<Fs>> {
    return new LowerBounded<Row, Head<Fs>>({ ...this.state, lower: Option.some([field, value, true]) })
  }
  lt(field: Head<Fs> & string, value: NonNullable<ValueOf<Row, Head<Fs>>>): IndexRange {
    return new IndexRange({ ...this.state, upper: Option.some([field, value, false]) })
  }
  lte(field: Head<Fs> & string, value: NonNullable<ValueOf<Row, Head<Fs>>>): IndexRange {
    return new IndexRange({ ...this.state, upper: Option.some([field, value, true]) })
  }
}

export type IndexFields<T extends AnySyncTable, K extends keyof T["indexes"]> = T["indexes"][K]["fields"]

export type TextField<Row> = {
  [K in keyof Row & string]: NonNullable<Row[K]> extends string ? K : never
}[keyof Row & string]

export interface OrderedQuery<Row, E> {
  /** One literal substring predicate, applied before the limit. A later call replaces it. */
  readonly contains: (field: TextField<Row>, needle: string) => OrderedQuery<Row, E>
  readonly order: (order: "asc" | "desc") => OrderedQuery<Row, E>
  readonly take: (n: number) => Effect.Effect<ReadonlyArray<Row>, E>
  readonly first: Effect.Effect<Option.Option<Row>, E>
  readonly collect: Effect.Effect<ReadonlyArray<Row>, E>
}

export interface TableQuery<T extends AnySyncTable, E> {
  readonly withIndex: <K extends keyof T["indexes"] & string>(
    index: K,
    range?: (q: IndexRangeBuilder<RowOf<T>, IndexFields<T, K>>) => IndexRange
  ) => OrderedQuery<RowOf<T>, E>
}

/** Errors of reads. Rows in scopes the actor cannot read are skipped by queries. */
export type ReadError = RowNotFound | ScopeForbidden | InvalidRow | StoreUnavailable

/**
 * The typed, scope-checked store handed to mutators as `ctx.store` (SPEC §5) and, read-only,
 * to registered queries. Dialect-agnostic: built once by `make` over any `RowStore`.
 */
export interface EchoStore {
  readonly get: <T extends AnySyncTable>(
    table: T,
    id: RowOf<T>["id"]
  ) => Effect.Effect<RowOf<T>, RowNotFound | ScopeForbidden | StoreUnavailable>
  readonly insert: <T extends AnySyncTable>(
    table: T,
    row: RowOf<T>
  ) => Effect.Effect<void, RowAlreadyExists | ScopeForbidden | InvalidRow | StoreUnavailable | ConstraintViolated>
  readonly patch: <T extends AnySyncTable>(
    table: T,
    id: RowOf<T>["id"],
    patch: Partial<Omit<RowOf<T>, "id">>
  ) => Effect.Effect<void, RowNotFound | ScopeForbidden | InvalidRow | StoreUnavailable | ConstraintViolated>
  readonly delete: <T extends AnySyncTable>(
    table: T,
    id: RowOf<T>["id"]
  ) => Effect.Effect<void, RowNotFound | ScopeForbidden | StoreUnavailable | ConstraintViolated>
  readonly query: <T extends AnySyncTable>(table: T) => TableQuery<T, InvalidRow | StoreUnavailable>
  readonly scan: <T extends AnySyncTable>(table: T) => Effect.Effect<ReadonlyArray<RowOf<T>>, StoreUnavailable>
  /**
   * Creates the scope `scopeId` with the actor as its first owner (SPEC §4): claims the id
   * (`StoreOptions.claimScope`) and, only if the claim succeeds, writes
   * `{ id: grantId(scopeId, actor), role: "owner" }` through the normal write path (undo image,
   * log), skipping the owner check that every other `scope_grants` write needs. The grant is
   * always for `StoreOptions.actor`; a handler cannot name another principal.
   *
   * An id that is not fresh (it exists, including a scope whose grants were all revoked) fails
   * with the existing `ScopeForbidden { required: "owner" }`, not a new rejection: "already
   * exists" reads as "you are not its owner", and the wire does not change. The actor `*` is
   * refused the same way, before any claim (it would make everyone the owner).
   *
   * Mutation store only: not part of a query's read-only store.
   */
  readonly createScope: (
    scopeId: AccessScopeId
  ) => Effect.Effect<void, ScopeForbidden | InvalidRow | StoreUnavailable | ConstraintViolated>
}

/** The read half of `EchoStore`: what a registered query's handler gets (SPEC §12.3). */
export type ReadStore = Pick<EchoStore, "get" | "query" | "scan">

/** Pre-image of a row, captured before the first write to it in one store instance. */
export interface UndoImage {
  readonly table: AnySyncTable
  readonly id: string
  readonly before: Option.Option<EncodedRow>
}

export interface StoreOptions {
  /** The actor whose grants (own or `*`) make a scope readable to queries (pushed into the plan). */
  readonly actor: PrincipalId
  /**
   * The actor's effective role on a scope (own grant or `*`), if any. Must be the strongest role
   * among the actor's and `*`'s `scope_grants` rows, the rows queries filter by.
   */
  readonly roleFor: (scopeId: AccessScopeId) => Effect.Effect<Option.Option<Role>, StoreUnavailable>
  /**
   * Claims `scopeId` for `createScope`: true iff the id was fresh and is now taken by this
   * transaction. The server's claim is authoritative and atomic (an `access_scopes` insert in the
   * mutation's transaction); a replica's is optimistic (no `scope_grants` row for the scope).
   */
  readonly claimScope: (scopeId: AccessScopeId) => Effect.Effect<boolean, StoreUnavailable>
  /** The mutation's `ctx.now` (INV-MUT-8): `createScope`'s `grantedAt`, so a rebase replays it exactly. */
  readonly now: DateTime.Utc
}

export interface StoreInstance {
  readonly store: EchoStore
  /**
   * First-touch pre-images in first-write order, one per (table, id). An image is recorded
   * before its write is issued, so it is complete even if the handler fails midway.
   */
  readonly touched: Effect.Effect<ReadonlyArray<UndoImage>>
  /**
   * Names of every table this instance has read so far (INV-BRW-6): the tables of `get`, `query`
   * and `scan`, plus `scope_grants` whenever a role was looked up to authorize or filter a row.
   * A registered query's live subscription is reactive on exactly this set of its latest run.
   */
  readonly reads: Effect.Effect<ReadonlySet<string>>
}

/**
 * Grant identity is derived (INV-ACL-5): a scope_grants row whose id is not
 * `grantId(accessScopeId, principalId)` is rejected, so two grants for one pair cannot both be
 * written, and a patch that changes the pair (without changing the id) is rejected too.
 */
const checkGrantIdentity = (table: AnySyncTable, encoded: EncodedRow): Effect.Effect<void, InvalidRow> => {
  if (table.name !== ScopeGrants.name) return Effect.void
  const { id, access_scope_id, principal_id } = encoded
  const valid =
    typeof id === "string" &&
    typeof access_scope_id === "string" &&
    typeof principal_id === "string" &&
    id === grantId(AccessScopeIdSchema.make(access_scope_id), PrincipalIdSchema.make(principal_id))
  return valid
    ? Effect.void
    : Effect.fail(
        new InvalidRow({ table: table.name, message: "scope_grants id must be grantId(accessScopeId, principalId)" })
      )
}

const requiredRole = (table: AnySyncTable): Role => (table.name === ScopeGrants.name ? "owner" : "writer")

const decode = <T extends AnySyncTable>(table: T, row: EncodedRow): Effect.Effect<RowOf<T>> =>
  decodeStored(table.wire, table.name, row)

const encode = <T extends AnySyncTable>(table: T, row: unknown): Effect.Effect<EncodedRow, InvalidRow> =>
  Schema.encodeUnknownEffect(table.wire)(row).pipe(
    Effect.mapError((error) => new InvalidRow({ table: table.name, message: error.message }))
  )

const toKeyValue = (table: AnySyncTable, field: string, value: unknown) =>
  Effect.gen(function* () {
    const column = table.columns.find((c) => c.field === field)
    if (column === undefined) return yield* new InvalidRow({ table: table.name, message: `unknown field ${field}` })
    const encoded = yield* Schema.encodeUnknownEffect(column.wire)(value).pipe(
      Effect.mapError((error) => new InvalidRow({ table: table.name, message: error.message }))
    )
    if (encoded !== null && typeof encoded === "object") {
      return yield* new InvalidRow({ table: table.name, message: `field ${field} is not indexable` })
    }
    return encoded
  })

/**
 * A replica's optimistic scope claim (SPEC §4): fresh iff `rows` holds no `scope_grants` row for
 * the scope. The server's claim is authoritative; a rejection undoes the optimistic grant.
 */
export const claimFromGrants =
  (rows: RowStore) =>
  (scopeId: AccessScopeId): Effect.Effect<boolean, StoreUnavailable> =>
    Effect.map(rows.scan(ScopeGrants), (grants) => !grants.some((g) => g["access_scope_id"] === scopeId))

/** `actor`'s effective role on a scope, read from the `scope_grants` rows in `rows` (SPEC §4). */
export const roleFromGrants =
  (rows: RowStore, actor: PrincipalId) =>
  (scopeId: AccessScopeId): Effect.Effect<Option.Option<Role>, StoreUnavailable> =>
    Effect.gen(function* () {
      const roles: Array<Role> = []
      for (const who of [actor, Everyone]) {
        const row = yield* rows.read(ScopeGrants, grantId(scopeId, who))
        if (Option.isNone(row)) continue
        const grant = yield* decodeStored(ScopeGrants.wire, ScopeGrants.name, row.value)
        roles.push(grant.role)
      }
      return Option.fromNullishOr(strongest(roles))
    })

/** Restores pre-images in reverse write order. */
export const restore = (rows: RowStore, undo: ReadonlyArray<UndoImage>) =>
  Effect.forEach(Arr.reverse(undo), (image) => rows.write(image.table, image.id, image.before), { discard: true })

/** Build the typed store over `rows`. Create one instance per handler run. */
export const make = (rows: RowStore, options: StoreOptions): StoreInstance => {
  const images: Array<UndoImage> = []
  const seen = new Set<string>()
  const reads = new Set<string>()
  const read = (table: AnySyncTable) => Effect.sync(() => void reads.add(table.name))
  // Roles come from `scope_grants` rows, so every role lookup is a read of that table.
  const roleFor = (scopeId: AccessScopeId) => Effect.andThen(read(ScopeGrants), options.roleFor(scopeId))

  const authorize = Effect.fnUntraced(function* (scopeId: AccessScopeId, required: Role) {
    const held = yield* roleFor(scopeId)
    if (Option.isNone(held) || !satisfies(held.value, required)) {
      return yield* new ScopeForbidden({ scopeId, required, held: Option.getOrNull(held) })
    }
  })

  const write = Effect.fnUntraced(function* (
    table: AnySyncTable,
    id: string,
    before: Option.Option<EncodedRow>,
    after: Option.Option<EncodedRow>
  ) {
    const key = JSON.stringify([table.name, id])
    if (!seen.has(key)) {
      seen.add(key)
      images.push({ table, id, before })
    }
    yield* rows.write(table, id, after)
  })

  const readable = (scopes: Map<AccessScopeId, boolean>, scopeId: AccessScopeId) =>
    Effect.gen(function* () {
      const cached = scopes.get(scopeId)
      if (cached !== undefined) return cached
      const held = yield* roleFor(scopeId)
      const ok = Option.isSome(held) && satisfies(held.value, "reader")
      scopes.set(scopeId, ok)
      return ok
    })

  const visible = <T extends AnySyncTable>(table: T, encoded: ReadonlyArray<EncodedRow>) =>
    Effect.gen(function* () {
      const scopes = new Map<AccessScopeId, boolean>()
      const out: Array<RowOf<T>> = []
      for (const row of encoded) {
        const decoded = yield* decode(table, row)
        if (yield* readable(scopes, decoded.accessScopeId)) out.push(decoded)
      }
      return out
    })

  const store: EchoStore = {
    get: (table, id) =>
      Effect.gen(function* () {
        yield* read(table)
        const found = yield* rows.read(table, id)
        if (Option.isNone(found)) return yield* new RowNotFound({ table: table.name, id })
        const row = yield* decode(table, found.value)
        yield* authorize(row.accessScopeId, "reader")
        return row
      }),

    insert: (table, row) =>
      Effect.gen(function* () {
        const encoded = yield* encode(table, row)
        yield* checkGrantIdentity(table, encoded)
        yield* authorize(row.accessScopeId, requiredRole(table))
        const existing = yield* rows.read(table, row.id)
        if (Option.isSome(existing)) return yield* new RowAlreadyExists({ table: table.name, id: row.id })
        yield* write(table, row.id, existing, Option.some(encoded))
      }),

    patch: (table, id, patch) =>
      Effect.gen(function* () {
        const found = yield* rows.read(table, id)
        if (Option.isNone(found)) return yield* new RowNotFound({ table: table.name, id })
        const current = yield* decode(table, found.value)
        yield* authorize(current.accessScopeId, requiredRole(table))
        const next = { ...current, ...patch, id: current.id }
        const encoded = yield* encode(table, next)
        yield* checkGrantIdentity(table, encoded)
        // A scope move needs the right on both the old and the new scope (INV-ACL-2).
        if (next.accessScopeId !== current.accessScopeId) yield* authorize(next.accessScopeId, requiredRole(table))
        yield* write(table, id, found, Option.some(encoded))
      }),

    delete: (table, id) =>
      Effect.gen(function* () {
        const found = yield* rows.read(table, id)
        if (Option.isNone(found)) return yield* new RowNotFound({ table: table.name, id })
        const current = yield* decode(table, found.value)
        // Relinquishing one's own grant needs no role (INV-ACL-4). `*` is never a real actor;
        // excluding it keeps the `*` row owner-only even for a store built with actor `*`.
        const ownGrant =
          table.name === ScopeGrants.name && options.actor !== Everyone && found.value["principal_id"] === options.actor
        if (!ownGrant) yield* authorize(current.accessScopeId, requiredRole(table))
        yield* write(table, id, found, Option.none())
      }),

    createScope: (scopeId) =>
      Effect.gen(function* () {
        const forbidden = Effect.gen(function* () {
          const held = yield* roleFor(scopeId)
          return yield* new ScopeForbidden({ scopeId, required: "owner", held: Option.getOrNull(held) })
        })
        if (options.actor === Everyone) return yield* forbidden
        // A replica's claim reads scope_grants; record it like any role lookup (INV-BRW-6).
        yield* read(ScopeGrants)
        if (!(yield* options.claimScope(scopeId))) return yield* forbidden
        const id = grantId(scopeId, options.actor)
        const encoded = yield* encode(ScopeGrants, {
          id,
          accessScopeId: scopeId,
          principalId: options.actor,
          role: "owner",
          grantedAt: options.now
        })
        // A claimed id is fresh, so it has no grant rows: the pre-image is "absent".
        yield* write(ScopeGrants, id, Option.none(), Option.some(encoded))
      }),

    query: <T extends AnySyncTable>(table: T): TableQuery<T, InvalidRow | StoreUnavailable> => ({
      withIndex: (name, range) => {
        const index = table.indexes[name]
        const built = (range ?? ((q) => q))(
          new IndexRangeBuilder({ eq: [], lower: Option.none(), upper: Option.none() })
        ).state
        const run = (order: "asc" | "desc", limit: Option.Option<number>, text?: readonly [string, string]) =>
          Effect.gen(function* () {
            yield* read(table)
            if (index === undefined) {
              return yield* new InvalidRow({ table: table.name, message: `unknown index ${name}` })
            }
            const textColumn = (field: string, needle: string) => {
              const column = table.columns.find((c) => c.field === field)
              return column?.kind === "text" && Search.isPortable(needle)
                ? Effect.succeed(column)
                : Effect.fail(
                    new InvalidRow({
                      table: table.name,
                      message: "search requires a text column and portable Unicode without NUL"
                    })
                  )
            }
            const predicate =
              text === undefined
                ? undefined
                : {
                    column: yield* textColumn(text[0], text[1]),
                    needle: text[1]
                  }
            const eq: Array<KeyValue> = []
            for (const [field, value] of built.eq) eq.push(yield* toKeyValue(table, field, value))
            const bound = (b: Option.Option<readonly [string, unknown, boolean]>) =>
              Option.isNone(b)
                ? Effect.succeed(Option.none<Bound>())
                : toKeyValue(table, b.value[0], b.value[1]).pipe(
                    Effect.flatMap((value) =>
                      value === null
                        ? Effect.fail(new InvalidRow({ table: table.name, message: "range bounds cannot be null" }))
                        : Effect.succeed(Option.some({ value, inclusive: b.value[2] }))
                    )
                  )
            // The readable-scope filter and the limit are pushed into the plan (open item 8), so
            // the store stops after `limit` readable rows.
            const plan: IndexRangePlan = {
              index,
              ...(predicate === undefined ? {} : { text: predicate }),
              eq,
              lower: yield* bound(built.lower),
              upper: yield* bound(built.upper),
              order,
              readableBy: Option.some([options.actor, Everyone]),
              limit
            }
            let selected = plan
            if (built.prefix !== undefined) {
              const [field, value] = built.prefix
              const column = yield* textColumn(field, value)
              if (index.key[eq.length]?.name !== column.name) {
                return yield* new InvalidRow({ table: table.name, message: "prefix must name the next index field" })
              }
              const end = Search.prefixEnd(value)
              selected = {
                ...plan,
                lower: Option.some({ value, inclusive: true }),
                upper: end === undefined ? Option.none() : Option.some({ value: end, inclusive: false })
              }
            }
            // The semi-join reads `scope_grants`: a grant change can change this result (INV-BRW-6).
            if (Option.isSome(plan.readableBy)) yield* read(ScopeGrants)
            return yield* Effect.forEach(yield* rows.range(table, selected), (row) => decode(table, row))
          })
        const ordered = (
          order: "asc" | "desc",
          text?: readonly [string, string]
        ): OrderedQuery<RowOf<T>, InvalidRow | StoreUnavailable> => ({
          contains: (field, needle) => ordered(order, [field, needle]),
          order: (next) => ordered(next, text),
          take: (n) => run(order, Option.some(n), text),
          first: run(order, Option.some(1), text).pipe(Effect.map((rs) => Option.fromNullishOr(rs[0]))),
          collect: run(order, Option.none(), text)
        })
        return ordered("asc")
      }
    }),

    scan: (table) =>
      read(table).pipe(
        Effect.andThen(rows.scan(table)),
        Effect.flatMap((encoded) => visible(table, encoded))
      )
  }

  return { store, touched: Effect.sync(() => [...images]), reads: Effect.sync(() => new Set(reads)) }
}
