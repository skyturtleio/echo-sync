import { Effect } from "effect"
import { SqlClient } from "effect/sql/SqlClient"

/**
 * `scope_grants.id = grantId(access_scope_id, principal_id)` as a server-enforced invariant
 * (DESIGN §4), so a grant written around the store (raw SQL) cannot make an id lookup and a
 * column lookup disagree.
 *
 * core's `grantId` prefixes the JS length of the scope id (UTF-16 code units); `char_length` counts
 * code points. They agree exactly when the scope id has no character above U+FFFF, so such scope
 * ids are rejected rather than mis-derived (DESIGN §4). The range is built with `chr` to avoid
 * regex escapes; both functions are immutable.
 *
 * Validated, not `NOT VALID`: on an existing database a row that violates it fails this migration
 * (23514, naming the constraint), the migrator rolls back and startup stops.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    ALTER TABLE echo_sync.scope_grants ADD CONSTRAINT scope_grants_id_derived CHECK (
      access_scope_id !~ ('[' || chr(65536) || '-' || chr(1114111) || ']')
      AND id = char_length(access_scope_id)::text || ':' || access_scope_id || ':' || principal_id
    )`
})
