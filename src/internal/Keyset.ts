import { Array as Arr } from "effect"
import type { IndexRangePlan, KeyValue } from "../core/Store.ts"
import type { Column } from "../core/SyncTable.ts"

/** SQL shared by SQLite and Postgres: native seeks when non-null, total-order OR otherwise. */
export const predicates = (
  plan: IndexRangePlan,
  bind: (column: Column, value: KeyValue) => string
): ReadonlyArray<string> => {
  const result: Array<string> = []
  for (const side of ["after", "before"] as const) {
    const cursor = plan[side]
    if (cursor === undefined) continue
    const greater = (side === "after") === (plan.order === "asc")
    const op = greater ? ">" : "<"
    // An equal fixed prefix contributes nothing to comparison. Removing it lets SQLite
    // seek on the remaining tuple instead of scanning the entire equality group.
    const offset = Arr.every(plan.eq, (value, i) => value === cursor[i]) ? plan.eq.length : 0
    const keys = Arr.map(plan.index.key.slice(offset), (column, i) => {
      const value = cursor[offset + i]
      if (value === undefined) throw new Error("keyset cursor must contain every index key")
      return { column, name: `t."${column.name}"`, value, param: value === null ? "NULL" : bind(column, value) }
    })
    if (Arr.every(keys, (k) => !k.column.nullable)) {
      result.push(
        `(${Arr.join(
          Arr.map(keys, (k) => k.name),
          ", "
        )}) ${op} (${Arr.join(
          Arr.map(keys, (k) => k.param),
          ", "
        )})`
      )
      continue
    }
    const equal: Array<string> = []
    const alternatives: Array<string> = []
    for (const { name, value, param } of keys) {
      const strict =
        value === null
          ? greater
            ? `${name} IS NOT NULL`
            : "FALSE"
          : greater
            ? `${name} > ${param}`
            : `(${name} IS NULL OR ${name} < ${param})`
      alternatives.push(`(${Arr.join([...equal, strict], " AND ")})`)
      equal.push(value === null ? `${name} IS NULL` : `${name} = ${param}`)
    }
    result.push(`(${Arr.join(alternatives, " OR ")})`)
  }
  return result
}
