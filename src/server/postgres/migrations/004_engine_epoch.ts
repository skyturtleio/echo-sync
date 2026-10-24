import { Effect } from "effect"
import { SqlClient } from "effect/sql/SqlClient"

/** Server-first rollout accepts legacy peers until the operator fences a restored timeline. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`ALTER TABLE echo_sync.engine_state
    ADD COLUMN epoch text NOT NULL DEFAULT gen_random_uuid()::text,
    ADD COLUMN require_epoch boolean NOT NULL DEFAULT false`
})
