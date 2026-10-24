import { Effect } from "effect"
import { SqlClient } from "effect/sql/SqlClient"

/**
 * `scope_grants.id = grantId(access_scope_id, principal_id)` as a server-enforced invariant
 * (SPEC §4), so a grant written around the store (raw SQL) cannot make an id lookup and a
 * column lookup disagree.
 *
 * core's `grantId` prefixes the code-point count of the scope id, which is `char_length` only in
 * a UTF8 database (under SQL_ASCII, for one, it counts bytes). UTF8 is the supported contract, so
 * this migration refuses any other server encoding before adding the CHECK (SPEC §4).
 *
 * Validated, not `NOT VALID`: on an existing database a row that violates it fails this migration
 * (23514, naming the constraint), the migrator rolls back and startup stops.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    DO $$
    DECLARE enc text := (SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database());
    BEGIN
      IF enc <> 'UTF8' THEN
        RAISE EXCEPTION 'echo_sync requires a UTF8 database; % has encoding %', current_database(), enc
          USING ERRCODE = 'feature_not_supported',
                HINT = 'UTF8 is the only supported server encoding: grant ids count code points with char_length, which under SQL_ASCII counts bytes';
      END IF;
    END
    $$`
  yield* sql`
    ALTER TABLE echo_sync.scope_grants ADD CONSTRAINT scope_grants_id_derived CHECK (
      id = char_length(access_scope_id)::text || ':' || access_scope_id || ':' || principal_id
    )`
})
