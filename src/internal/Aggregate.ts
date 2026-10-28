import { Array as Arr, Effect } from "effect"
import type { AggregateResult, AnySyncTable, Column } from "../core/SyncTable.ts"

/** One SQL aggregate row; casts avoid SQLite's signed-int64 SUM overflow. */
export const selectList = (columns: ReadonlyArray<Column>, dialect: "sqlite" | "postgres") =>
  Arr.join(
    [
      'COUNT(*) AS "__count"',
      ...Arr.flatMap(columns, (column, i) => {
        const value = `CAST(t."${column.name}" AS ${dialect === "sqlite" ? "REAL" : "NUMERIC"})`
        return [
          `SUM(${value}) AS "__sum${i}"`,
          `SUM(ABS(${value})) AS "__magnitude${i}"`,
          `COUNT(t."${column.name}") AS "__nonnull${i}"`
        ]
      })
    ],
    ", "
  )

const fail = (table: AnySyncTable, field: string): never => {
  throw new RangeError(`aggregate: ${table.name}.${field} exceeds the portable numeric range`)
}

const number = (row: Readonly<Record<string, unknown>>, key: string) => {
  const raw = row[key]
  return typeof raw === "number" || typeof raw === "string" || typeof raw === "bigint" ? Number(raw) : NaN
}

/** SQL drivers differ in number representation; never turn overflow into a null sum. */
export const decode = (table: AnySyncTable, columns: ReadonlyArray<Column>, row: Readonly<Record<string, unknown>>) =>
  Effect.sync((): AggregateResult<ReadonlyArray<string>> => {
    const count = number(row, "__count")
    if (!Number.isSafeInteger(count) || count < 0) return fail(table, "count")
    const sum: Record<string, number | null> = {}
    for (const [i, column] of columns.entries()) {
      const nonnull = number(row, `__nonnull${i}`)
      if (!Number.isSafeInteger(nonnull) || nonnull < 0 || nonnull > count) return fail(table, column.field)
      const value = nonnull === 0 ? null : number(row, `__sum${i}`)
      if (
        value !== null &&
        (!Number.isFinite(value) ||
          (column.kind === "integer" &&
            (!Number.isSafeInteger(value) || !(number(row, `__magnitude${i}`) <= Number.MAX_SAFE_INTEGER))))
      )
        return fail(table, column.field)
      Object.defineProperty(sum, column.field, { value, enumerable: true })
    }
    return { count, sum }
  })
