import { Effect } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { ScopeGrants } from "echo-sync"

/**
 * Engine schema v1 (SPEC §4, §5, §6).
 *
 * Migration modules are immutable once released: change the engine with a new `NNN_name.ts`.
 * The `echo_sync` schema itself is created by `migrate` before the migrator runs, because the
 * migrator's history table (`echo_sync.migrations`) lives inside it.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient

  // One position space for log entries and ledger rows. Gaps (rolled-back allocations) are fine;
  // order is what matters, and order comes from the lock in `next_position`, not the sequence.
  yield* sql`CREATE SEQUENCE echo_sync.position_seq AS bigint`

  // The single mutable engine fact that cannot be derived: how far log GC has deleted.
  yield* sql`
    CREATE TABLE echo_sync.engine_state (
      singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
      gc_horizon bigint NOT NULL DEFAULT 0
    )`
  yield* sql`INSERT INTO echo_sync.engine_state DEFAULT VALUES`

  yield* sql`
    CREATE TABLE echo_sync.access_scopes (
      id text PRIMARY KEY,
      created_at timestamptz NOT NULL DEFAULT now()
    )`

  // A system synced table (SPEC §4): each grant is a row in the scope it grants, so it
  // replicates to that scope's readers and its changes are ordered with data changes in the log.
  // The table is core's SyncTable DDL (one definition, including UNIQUE (access_scope_id,
  // principal_id)), created inside `echo_sync`. Caveat: a fresh database runs this migration with
  // the core version it is deployed with, so a change to ScopeGrants' storage shape needs its own
  // migration for existing databases, like any SyncTable change (SPEC §13).
  const [{ search_path } = { search_path: "public" }] = yield* sql<{ search_path: string }>`SHOW search_path`
  yield* sql`SELECT set_config('search_path', 'echo_sync', true)`
  yield* sql.unsafe(ScopeGrants.ScopeGrants.postgres.createTable)
  for (const ddl of ScopeGrants.ScopeGrants.postgres.createIndexes) yield* sql.unsafe(ddl)
  yield* sql`SELECT set_config('search_path', ${search_path}, true)`
  // Engine-side constraints the SyncTable cannot express: grants die with their scope, and a
  // role outside the Role literals (from a script that bypasses the store) cannot be stored.
  yield* sql`
    ALTER TABLE echo_sync.scope_grants
      ADD FOREIGN KEY (access_scope_id) REFERENCES echo_sync.access_scopes (id) ON DELETE CASCADE,
      ADD CHECK (role IN ('reader', 'writer', 'owner'))`
  yield* sql`CREATE INDEX scope_grants_principal ON echo_sync.scope_grants (principal_id)`

  yield* sql`
    CREATE TABLE echo_sync.clients (
      client_id text PRIMARY KEY,
      principal_id text NOT NULL,
      cursor bigint,
      -- SPEC §11 expiry floor: seq ≤ expired_through is answered Rejected{clientExpired} at
      -- expired_position (the highest expired ledger row). No v1 job raises them.
      expired_through bigint NOT NULL DEFAULT 0,
      expired_position bigint NOT NULL DEFAULT 0,
      last_seen_at timestamptz NOT NULL DEFAULT now(),
      created_at timestamptz NOT NULL DEFAULT now()
    )`

  // The set of scopes whose contents a client holds as of a completed pull position (SPEC §7). The wire
  // cursor is a bare position, so this is how the next pull knows what to purge and what to walk.
  // Rows below the cursor a client presents are pruned on that pull; at most a couple per client.
  yield* sql`
    CREATE TABLE echo_sync.client_scopes (
      client_id text NOT NULL REFERENCES echo_sync.clients (client_id),
      position bigint NOT NULL,
      scope_ids text[] NOT NULL,
      PRIMARY KEY (client_id, position)
    )`

  yield* sql`
    CREATE TABLE echo_sync.mutations (
      client_id text NOT NULL REFERENCES echo_sync.clients (client_id),
      mutation_id text NOT NULL,
      seq bigint NOT NULL,
      position bigint NOT NULL UNIQUE,
      outcome jsonb NOT NULL,
      received_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (client_id, mutation_id)
    )`
  yield* sql`CREATE INDEX mutations_client_position ON echo_sync.mutations (client_id, position)`

  yield* sql`
    CREATE TABLE echo_sync.log (
      position bigint PRIMARY KEY,
      table_name text NOT NULL,
      row_id text NOT NULL,
      access_scope_id text NOT NULL,
      op text NOT NULL CHECK (op IN ('upsert', 'delete')),
      txid xid8 NOT NULL DEFAULT pg_current_xact_id(),
      committed_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )`
  yield* sql`CREATE INDEX log_scope_position ON echo_sync.log (access_scope_id, position)`

  // INV-POS-1: every position allocation takes the engine's transaction-scoped advisory lock first.
  // The lock is held until commit, so the next allocation can only happen after this
  // transaction is visible: positions are handed out in commit order.
  // Key = bytes of "echosync" as a signed int8.
  // The NOTIFY is delivered at commit (and deduplicated within a transaction): it is the poke
  // stream's hint that something may have changed (INV-POS-4). It carries no data.
  yield* sql`
    CREATE FUNCTION echo_sync.next_position() RETURNS bigint
    LANGUAGE plpgsql VOLATILE AS $$
    BEGIN
      PERFORM pg_advisory_xact_lock(7305797848413072995);
      PERFORM pg_notify('echo_sync', '');
      RETURN nextval('echo_sync.position_seq');
    END
    $$`

  // Row-level change capture for synced tables. Identity only, never payload.
  // A scope move or primary-key change is a tombstone in the old identity/scope followed by an
  // upsert in the new one.
  yield* sql`
    CREATE FUNCTION echo_sync.echo_sync_capture_changes() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP = 'DELETE'
         OR (TG_OP = 'UPDATE' AND (OLD.id IS DISTINCT FROM NEW.id
                                   OR OLD.access_scope_id IS DISTINCT FROM NEW.access_scope_id)) THEN
        INSERT INTO echo_sync.log (position, table_name, row_id, access_scope_id, op)
        VALUES (echo_sync.next_position(), TG_TABLE_NAME, OLD.id::text, OLD.access_scope_id, 'delete');
      END IF;
      IF TG_OP IN ('INSERT', 'UPDATE') THEN
        INSERT INTO echo_sync.log (position, table_name, row_id, access_scope_id, op)
        VALUES (echo_sync.next_position(), TG_TABLE_NAME, NEW.id::text, NEW.access_scope_id, 'upsert');
      END IF;
      RETURN NULL;
    END
    $$`

  // TRUNCATE fires no row triggers, so it would bypass the log: refuse it on captured tables.
  yield* sql`
    CREATE FUNCTION echo_sync.reject_truncate() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
      RAISE EXCEPTION 'echo_sync: TRUNCATE is not supported on synced table %', TG_TABLE_NAME
        USING ERRCODE = 'feature_not_supported';
    END
    $$`

  yield* sql`
    CREATE TRIGGER echo_sync_capture
    AFTER INSERT OR UPDATE OR DELETE ON echo_sync.scope_grants
    FOR EACH ROW EXECUTE FUNCTION echo_sync.echo_sync_capture_changes()`
  yield* sql`
    CREATE TRIGGER echo_sync_reject_truncate
    BEFORE TRUNCATE ON echo_sync.scope_grants
    FOR EACH STATEMENT EXECUTE FUNCTION echo_sync.reject_truncate()`
})
