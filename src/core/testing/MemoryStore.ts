/**
 * In-memory `ReplicaRowStore`: the reference semantics for row storage (SPEC §4 store
 * layering, SPEC §3 index ordering). Rows are kept in wire form, keyed by table name then id.
 * Ordering and range matching come from core's single definition (`Store.planMatches`,
 * `Store.planOrder`, `Store.compareText`), never from JS string comparison.
 */
import { Array as Arr, Effect, Option, Record as Rec, Semaphore } from "effect"
import { ScopeGrants, Store } from "../index.ts"
import type { SyncTable } from "../index.ts"
import * as Aggregate from "../../internal/Aggregate.ts"

export type Tables = Map<string, Map<string, SyncTable.EncodedRow>>

export const cloneTables = (tables: Tables): Tables =>
  new Map(Arr.map([...tables], ([name, rows]) => [name, new Map(rows)]))

export interface MemoryStore extends Store.ReplicaRowStore {
  readonly tables: () => Tables
  /** Replaces every table (rollback of an enclosing transaction that owns the decision). */
  readonly replace: (tables: Tables) => void
  /**
   * Runs `self` serializably: transactions run one at a time (snapshot, body and rollback under
   * one permit), and every write is rolled back if the body fails or is interrupted. Not
   * re-entrant. Without serialization a rollback would restore a snapshot taken before another
   * transaction committed, erasing that commit.
   */
  readonly transaction: <A, E, R>(self: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
}

const byId = (a: SyncTable.EncodedRow, b: SyncTable.EncodedRow) => Store.compareText(String(a["id"]), String(b["id"]))

export const make = (initial: Tables = new Map()): MemoryStore => {
  let tables = cloneTables(initial)
  const lock = Semaphore.makeUnsafe(1)
  const tableOf = (name: string) => {
    const existing = tables.get(name)
    if (existing !== undefined) return existing
    const created = new Map<string, SyncTable.EncodedRow>()
    tables.set(name, created)
    return created
  }
  const rowsOf = (name: string) => [...(tables.get(name)?.values() ?? [])]

  return {
    tables: () => tables,
    replace: (next) => {
      tables = next
    },
    read: (table, id) => Effect.sync(() => Option.fromNullishOr(tables.get(table.name)?.get(id))),
    write: (table, id, row) =>
      Effect.sync(() => {
        if (Option.isSome(row)) tableOf(table.name).set(id, row.value)
        else tables.get(table.name)?.delete(id)
      }),
    writeMany: (table, rows) =>
      Effect.sync(() => {
        for (const row of rows) tableOf(table.name).set(String(row["id"]), row)
      }),
    range: (table, plan, columns) =>
      Effect.andThen(
        Store.checkLimit(plan.limit),
        Effect.sync(() => {
          const result = Store.planResult(
            plan,
            rowsOf(table.name),
            tables.get(ScopeGrants.ScopeGrants.name)?.values() ?? []
          )
          return columns === undefined
            ? result
            : Arr.map(result, (row) => Rec.filter(row, (_, key) => Arr.some(columns, (c) => c.name === key)))
        })
      ),
    aggregate: (table, plan, sums) =>
      Effect.suspend(() => {
        const readable = Store.planReadable(plan, tables.get(ScopeGrants.ScopeGrants.name)?.values() ?? [])
        const selected = { ...plan, limit: Option.none<number>() }
        const totals = Arr.map(sums, () => ({ sum: 0, magnitude: 0, nonnull: 0 }))
        let count = 0
        for (const row of tables.get(table.name)?.values() ?? []) {
          if (!readable(row) || !Store.planMatches(selected, row)) continue
          count++
          for (const [i, column] of sums.entries()) {
            const total = totals[i]
            const value = row[column.name]
            if (total === undefined || value === null) continue
            if (typeof value !== "number")
              return Effect.die(new Error(`aggregate: invalid stored ${table.name}.${column.field}`))
            total.sum += value
            total.magnitude += Math.abs(value)
            total.nonnull++
          }
        }
        const result: Record<string, unknown> = { __count: count }
        for (const [i, total] of totals.entries()) {
          result[`__sum${i}`] = total.sum
          result[`__magnitude${i}`] = total.magnitude
          result[`__nonnull${i}`] = total.nonnull
        }
        return Aggregate.decode(table, sums, result)
      }),
    scan: (table) =>
      Effect.sync(() =>
        Arr.sort(rowsOf(table.name), (a: SyncTable.EncodedRow, b: SyncTable.EncodedRow) => {
          const result = byId(a, b)
          return result < 0 ? -1 : result > 0 ? 1 : 0
        })
      ),
    purgeScope: (scopeId) =>
      Effect.sync(() => {
        for (const rows of tables.values()) {
          for (const [id, row] of rows) if (row["access_scope_id"] === scopeId) rows.delete(id)
        }
      }),
    clearAll: Effect.sync(() => {
      tables = new Map()
    }),
    transaction: (self) =>
      lock.withPermit(
        // One commit/rollback decision from the body's own exit: only the body is interruptible,
        // so no interruption can land between the body finishing and the decision.
        Effect.uninterruptibleMask((restore) =>
          Effect.suspend(() => {
            const before = cloneTables(tables)
            return restore(self).pipe(
              Effect.onExit((exit) =>
                Effect.sync(() => {
                  if (exit._tag === "Failure") tables = before
                })
              )
            )
          })
        )
      )
  }
}
