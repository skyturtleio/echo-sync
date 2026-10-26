import { Effect } from "effect"
import { SqlClient } from "effect/sql/SqlClient"

/** App tables get this index through Migrate.generate; grants are engine-owned. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient
  // 001 uses the current SyncTable DDL, so fresh databases already have this index.
  yield* sql`CREATE INDEX IF NOT EXISTS "scope_grants__echo-scope"
    ON echo_sync.scope_grants (access_scope_id ASC NULLS FIRST, id ASC)`
})
