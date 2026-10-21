/**
 * In-memory `ReplicaRowStore`: the reference semantics for row storage (DESIGN §4 store
 * layering, §10 index ordering). Rows are kept in wire form, keyed by table name then id.
 * Ordering and range matching come from core's single definition (`Store.planMatches`,
 * `Store.planOrder`, `Store.compareText`), never from JS string comparison.
 */
import { Effect, Option, Semaphore } from "effect"
import { ScopeGrants, Store } from "../index.ts"
import type { SyncTable } from "../index.ts"

export type Tables = Map<string, Map<string, SyncTable.EncodedRow>>

export const cloneTables = (tables: Tables): Tables => new Map([...tables].map(([name, rows]) => [name, new Map(rows)]))

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
    range: (table, plan) =>
      Effect.sync(() =>
        Store.planResult(plan, rowsOf(table.name), (id) => tables.get(ScopeGrants.ScopeGrants.name)?.has(id) ?? false)
      ),
    scan: (table) => Effect.sync(() => rowsOf(table.name).toSorted(byId)),
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
