import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { attach } from "./schema.ts"
import { grant, pgAvailable, Replica, serverView, TestDatabase } from "./test-support.ts"

describe.skipIf(!pgAvailable)("change capture (§7)", () => {
  it.live("TRUNCATE on a synced table fails instead of silently bypassing the log", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* grant("p", "s1")
      yield* sql`INSERT INTO items (id, access_scope_id) VALUES ('a', 's1')`
      const replica = new Replica("p", "d")
      yield* replica.sync()
      const failure = yield* Effect.flip(sql`TRUNCATE items`)
      assert.strictEqual(failure._tag, "SqlError")
      yield* replica.sync()
      assert.deepStrictEqual(replica.view(), yield* serverView("p"))
      assert.isTrue(replica.rows.has("items/a"))
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("writes from outside any mutator (scripts, cascades) are captured like any other", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* grant("p", "s1")
      yield* sql`CREATE TABLE parents (id text PRIMARY KEY, access_scope_id text NOT NULL)`
      yield* sql`CREATE TABLE children (
        id text PRIMARY KEY, access_scope_id text NOT NULL,
        parent_id text NOT NULL REFERENCES parents (id) ON DELETE CASCADE)`
      yield* attach("parents")
      yield* attach("children")
      yield* sql`INSERT INTO parents VALUES ('p1', 's1')`
      yield* sql`INSERT INTO children VALUES ('c1', 's1', 'p1'), ('c2', 's1', 'p1')`
      const replica = new Replica("p", "d")
      yield* replica.sync()
      yield* sql`DELETE FROM parents WHERE id = 'p1'`
      yield* replica.sync()
      assert.isFalse(replica.rows.has("children/c1") || replica.rows.has("children/c2"))
      assert.deepStrictEqual(replica.view(), yield* serverView("p"))
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("attaching a second table with an already-synced name in another schema is refused", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* sql`CREATE SCHEMA other`
      yield* sql`CREATE TABLE other.items (id text PRIMARY KEY, access_scope_id text NOT NULL)`
      const failure = yield* Effect.flip(attach("other.items"))
      assert.strictEqual(failure._tag, "TableNameConflict")
    }).pipe(Effect.provide(TestDatabase))
  )
})
