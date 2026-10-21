import { Effect } from "effect"
import { SqlClient } from "effect/sql/SqlClient"

/**
 * Ledger expiry (DESIGN §16 item 5): `acked_at` is when the client proved it installed the row's
 * ack, stamped by the first pull that presents a cursor at or past the row's position. `NULL` (every
 * row before this migration, and every row not yet acked) is never expired. Additive: existing rows
 * keep `NULL` until their client's next pull stamps them.
 *
 * The partial index serves `expireLedger`'s cutoff scan; the stamp uses `(client_id, position)`.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`ALTER TABLE echo_sync.mutations ADD COLUMN acked_at timestamptz`
  yield* sql`CREATE INDEX mutations_acked_at ON echo_sync.mutations (acked_at) WHERE acked_at IS NOT NULL`
})
