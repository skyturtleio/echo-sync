import { Effect } from "effect"
import { SqlClient } from "effect/sql/SqlClient"

export default Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`CREATE TABLE echo_sync.catalog_generations (
    table_name text PRIMARY KEY,
    generation bigint NOT NULL DEFAULT 0
  )`
  yield* sql`CREATE FUNCTION echo_sync.catalog_changed() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
      INSERT INTO echo_sync.catalog_generations (table_name, generation) VALUES (TG_TABLE_NAME, 1)
      ON CONFLICT (table_name) DO UPDATE SET generation = echo_sync.catalog_generations.generation + 1;
      PERFORM pg_notify('echo_sync', '');
      RETURN NULL;
    END;
    $$`
})
