import { Clock, Duration, Effect } from "effect"
import { SqlClient } from "effect/sql/SqlClient"

/**
 * Deletes log entries older than `retention` (by the Effect `Clock`, so it is testable) and
 * advances the GC horizon. A pull whose cursor is below the horizon gets `ResnapshotRequired`.
 *
 * The horizon only moves forward, and every entry at or below it is deleted in the same
 * transaction, so a pull snapshot sees either the old log and old horizon or the new ones.
 * Ledger rows are not touched: their retention is a separate policy (DESIGN.md §16.5).
 */
export const collectGarbage = Effect.fnUntraced(function* (retention: Duration.Input) {
  const sql = yield* SqlClient
  const now = yield* Clock.currentTimeMillis
  const cutoff = new Date(now - Duration.toMillis(retention)).toISOString()
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const rows = yield* sql<{ gc_horizon: bigint; deleted: bigint }>`
        WITH target AS (
          SELECT max(position) AS h FROM echo_sync.log WHERE committed_at < ${cutoff}::timestamptz
        ), deleted AS (
          DELETE FROM echo_sync.log WHERE position <= (SELECT h FROM target) RETURNING 1
        )
        UPDATE echo_sync.engine_state
        SET gc_horizon = GREATEST(gc_horizon, (SELECT h FROM target))
        RETURNING gc_horizon, (SELECT count(*) FROM deleted) AS deleted`
      const row = rows[0]
      if (row === undefined) return yield* Effect.die("echo_sync.engine_state is empty; run migrate")
      return { horizon: row.gc_horizon, deleted: Number(row.deleted) }
    })
  )
})
