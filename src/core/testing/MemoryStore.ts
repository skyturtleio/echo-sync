/**
 * In-memory `RowStore`: the reference semantics for row storage (DESIGN §4 store layering,
 * §10 index ordering). Rows are kept in wire form, keyed by table then id.
 *
 * TEMPORARY shapes: `RangePlan` and `compareValues` mirror what `src/core` will export
 * (`IndexRangePlan`, `compareKeys`); switch to those when ws/foundation lands.
 */
import { Effect, Option } from "effect"
import type { EncodedRow } from "../../client/sync/Wire.ts"

export interface Bound {
  readonly value: unknown
  readonly inclusive: boolean
}

export interface RangePlan {
  /** Index columns in declaration order; `id` is the implicit final tiebreak. */
  readonly columns: ReadonlyArray<string>
  /** Equality values for a prefix of `columns` (`null` means IS NULL). */
  readonly eq: ReadonlyArray<unknown>
  /** Bounds on the column right after the `eq` prefix; never match NULL. */
  readonly lower: Option.Option<Bound>
  readonly upper: Option.Option<Bound>
  readonly order: "asc" | "desc"
  readonly limit: Option.Option<number>
}

/** Code-point order for text (not UTF-16 `<`), NULL lowest, numbers numeric, false < true. */
export const compareValues = (a: unknown, b: unknown): number => {
  if (a === b) return 0
  if (a === null || a === undefined) return -1
  if (b === null || b === undefined) return 1
  if (typeof a === "string" && typeof b === "string") {
    const ia = a[Symbol.iterator]()
    const ib = b[Symbol.iterator]()
    while (true) {
      const ca = ia.next()
      const cb = ib.next()
      if (ca.done === true) return cb.done === true ? 0 : -1
      if (cb.done === true) return 1
      const d = (ca.value.codePointAt(0) ?? 0) - (cb.value.codePointAt(0) ?? 0)
      if (d !== 0) return d
    }
  }
  if (typeof a === "boolean" && typeof b === "boolean") return a ? 1 : -1
  if (typeof a === "number" && typeof b === "number") return a - b
  // Mixed kinds cannot occur in one column of a declared schema.
  return 0
}

export type Tables = Map<string, Map<string, EncodedRow>>

export const cloneTables = (tables: Tables): Tables => new Map([...tables].map(([name, rows]) => [name, new Map(rows)]))

export interface MemoryStore {
  readonly tables: () => Tables
  readonly read: (table: string, id: string) => Effect.Effect<Option.Option<EncodedRow>>
  readonly write: (table: string, id: string, row: Option.Option<EncodedRow>) => Effect.Effect<void>
  readonly purgeScope: (scopeId: string) => Effect.Effect<void>
  readonly clearAll: Effect.Effect<void>
  readonly range: (table: string, plan: RangePlan) => Effect.Effect<ReadonlyArray<EncodedRow>>
  /** Runs `self` atomically: every write is rolled back if it fails or is interrupted. */
  readonly transaction: <A, E, R>(self: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
}

export const make = (initial: Tables = new Map()): MemoryStore => {
  let tables = cloneTables(initial)
  const tableOf = (name: string) => {
    const existing = tables.get(name)
    if (existing !== undefined) return existing
    const created = new Map<string, EncodedRow>()
    tables.set(name, created)
    return created
  }

  const range = (table: string, plan: RangePlan) =>
    Effect.sync(() => {
      const next = plan.columns[plan.eq.length]
      const matches = (row: EncodedRow) => {
        for (const [i, value] of plan.eq.entries()) {
          const column = plan.columns[i]
          if (column === undefined) return false
          const actual = row[column] ?? null
          if (value === null ? actual !== null : actual === null || compareValues(actual, value) !== 0) {
            return false
          }
        }
        if (next === undefined) return true
        const actual = row[next] ?? null
        if ((Option.isSome(plan.lower) || Option.isSome(plan.upper)) && actual === null) return false
        if (Option.isSome(plan.lower)) {
          const d = compareValues(actual, plan.lower.value.value)
          if (d < 0 || (d === 0 && !plan.lower.value.inclusive)) return false
        }
        if (Option.isSome(plan.upper)) {
          const d = compareValues(actual, plan.upper.value.value)
          if (d > 0 || (d === 0 && !plan.upper.value.inclusive)) return false
        }
        return true
      }
      const key = [...plan.columns, "id"]
      const sign = plan.order === "asc" ? 1 : -1
      const sorted = [...(tables.get(table)?.values() ?? [])].filter(matches).sort((a, b) => {
        for (const column of key) {
          const d = compareValues(a[column] ?? null, b[column] ?? null)
          if (d !== 0) return sign * d
        }
        return 0
      })
      return Option.isSome(plan.limit) ? sorted.slice(0, plan.limit.value) : sorted
    })

  return {
    tables: () => tables,
    read: (table, id) => Effect.sync(() => Option.fromNullishOr(tables.get(table)?.get(id))),
    write: (table, id, row) =>
      Effect.sync(() => {
        if (Option.isSome(row)) tableOf(table).set(id, row.value)
        else tables.get(table)?.delete(id)
      }),
    purgeScope: (scopeId) =>
      Effect.sync(() => {
        for (const rows of tables.values()) {
          for (const [id, row] of rows) if (row.access_scope_id === scopeId) rows.delete(id)
        }
      }),
    clearAll: Effect.sync(() => {
      tables = new Map()
    }),
    range,
    transaction: (self) =>
      Effect.suspend(() => {
        const before = cloneTables(tables)
        return self.pipe(
          Effect.onExit((exit) =>
            Effect.sync(() => {
              if (exit._tag === "Failure") tables = before
            })
          )
        )
      })
  }
}
