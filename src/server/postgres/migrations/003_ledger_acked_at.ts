import { Effect } from "effect"
import { SqlClient } from "effect/sql/SqlClient"

/**
 * Ledger expiry (DECISIONS 5): `acked_at` is when the client proved it installed the row's
 * ack, stamped by the first pull that presents a cursor at or past the row's position. `NULL` (every
 * row before this migration, and every row not yet acked) is never expired. Additive: existing rows
 * keep `NULL` until their client's next pull stamps them.
 *
 * Expiry clears `outcome` (and `acked_at`) and keeps the row's identity forever, so `outcome`
 * becomes nullable: `NULL` means processed, outcome reclaimed.
 *
 * The partial index serves `expireLedger`'s cutoff scan; the stamp uses `(client_id, position)`;
 * `clients_expired_position` serves pull's head, which must include expired positions.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`ALTER TABLE echo_sync.mutations ADD COLUMN acked_at timestamptz`
  yield* sql`ALTER TABLE echo_sync.mutations ALTER COLUMN outcome DROP NOT NULL`
  yield* sql`CREATE INDEX mutations_acked_at ON echo_sync.mutations (acked_at) WHERE acked_at IS NOT NULL`
  // pull's head includes max(expired_position): an expired row's position must not leave the head.
  yield* sql`CREATE INDEX clients_expired_position ON echo_sync.clients (expired_position)`
})
