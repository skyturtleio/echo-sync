import { Clock, Duration, Effect, Schema } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import * as Telemetry from "../../internal/Telemetry.ts"

/**
 * Deletes log entries older than `retention` (by the Effect `Clock`, so it is testable) and
 * advances the GC horizon. A pull whose cursor is below the horizon gets `ResnapshotRequired`.
 *
 * The horizon only moves forward, and every entry at or below it is deleted in the same
 * transaction, so a pull snapshot sees either the old log and old horizon or the new ones.
 * Ledger rows are not touched: their retention is `expireLedger` (DESIGN.md §16 item 5).
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
}, Telemetry.boundary)

/** `expireLedger` was constructed with a retention below `minimumLedgerRetention`. */
export class LedgerRetentionTooShort extends Schema.TaggedError<LedgerRetentionTooShort>()("LedgerRetentionTooShort", {
  retentionMillis: Schema.Number,
  minimumMillis: Schema.Number
}) {}

/**
 * Twice the client's released-row window (§9, 24 h), with margin for a paused tab's clock. A
 * shorter retention could expire a row a late enqueue retry can still duplicate (DESIGN §16 item 5).
 */
export const minimumLedgerRetention = Duration.hours(48)

/**
 * Ledger expiry (DESIGN §16 item 5): one job run. It reclaims outcome payloads, not rows: for each
 * client, it clears `outcome` (and `acked_at`) on its ledger rows acked (`acked_at`, stamped by
 * pull) at or before `now − retention` by the Effect `Clock`, raises its floor (`expired_through`)
 * to the highest `seq` cleared and records the highest cleared position (`expired_position`); both
 * only move forward, in the same transaction. The row's identity `(client_id, mutation_id, seq,
 * position)` stays forever, so a retry of the id under any `seq` is still recognised (ledger.ts).
 * A row never acked is never expired, whatever its age. Floors and identities are never deleted.
 *
 * The cleared rows of a client are a `seq` prefix of its rows (it pushes in `seq` order, so
 * positions and ack times grow with `seq`). The job checks that no row below a raised floor still
 * has an outcome and dies, rolling everything back, if one does: a floor over a live row would
 * answer its resubmission `ClientExpired` although its outcome is still recorded.
 *
 * A retention below 48 h fails the Effect with `LedgerRetentionTooShort` (typed, before any SQL),
 * so a scheduled job fails on its first run and a caller can `catchTag` it. Default 7 days.
 */
export const expireLedger = (options?: { readonly retention?: Duration.Input | undefined }) =>
  Effect.gen(function* () {
    const retention = Duration.fromInputUnsafe(options?.retention ?? "7 days")
    if (Duration.isLessThan(retention, minimumLedgerRetention)) {
      return yield* new LedgerRetentionTooShort({
        retentionMillis: Duration.toMillis(retention),
        minimumMillis: Duration.toMillis(minimumLedgerRetention)
      })
    }
    const sql = yield* SqlClient
    const now = yield* Clock.currentTimeMillis
    const cutoff = new Date(now - Duration.toMillis(retention)).toISOString()
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const raised = yield* sql<{ client_id: string; expired_through: bigint; expired: bigint }>`
          WITH cleared AS (
            UPDATE echo_sync.mutations SET outcome = NULL, acked_at = NULL
            WHERE acked_at <= ${cutoff}::timestamptz AND outcome IS NOT NULL
            RETURNING client_id, seq, position
          ), per_client AS (
            SELECT client_id, max(seq) AS seq, max(position) AS position, count(*) AS n
            FROM cleared GROUP BY client_id
          )
          UPDATE echo_sync.clients c
          SET expired_through = GREATEST(c.expired_through, p.seq),
              expired_position = GREATEST(c.expired_position, p.position)
          FROM per_client p
          WHERE c.client_id = p.client_id
          RETURNING c.client_id, c.expired_through, p.n AS expired`
        if (raised.length === 0) return { clients: 0, expired: 0 }
        const holes = yield* sql<{ client_id: string; seq: bigint; expired_through: bigint }>`
          SELECT m.client_id, m.seq, c.expired_through
          FROM echo_sync.mutations m JOIN echo_sync.clients c USING (client_id)
          WHERE m.client_id = ANY(${raised.map((r) => r.client_id)}::text[]) AND m.seq <= c.expired_through
            AND m.outcome IS NOT NULL
          ORDER BY m.client_id, m.seq
          LIMIT 10`
        if (holes.length > 0) {
          return yield* Effect.die(
            new Error(
              `expireLedger: the expired rows are not a seq prefix; rolled back. Live rows at or below the new floor: ${holes
                .map((h) => `${h.client_id} seq ${h.seq} (floor ${h.expired_through})`)
                .join(", ")}`
            )
          )
        }
        return { clients: raised.length, expired: raised.reduce((n, r) => n + Number(r.expired), 0) }
      })
    )
  }).pipe(Telemetry.boundary)
