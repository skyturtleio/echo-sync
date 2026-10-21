import { assert, describe, it } from "@effect/vitest"
import { Deferred, Effect, Exit, Fiber } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { attach, syncedTables } from "./schema.ts"
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

  it.live("concurrent attaches of one table name from two schemas cannot both succeed", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* sql`CREATE SCHEMA a`
      yield* sql`CREATE SCHEMA b`
      for (const schema of ["a", "b"]) {
        yield* sql`CREATE TABLE ${sql(schema)}.dup (id text PRIMARY KEY, access_scope_id text NOT NULL)`
      }
      // Widen the race: each attach's first trigger DDL sleeps, after its name check.
      yield* sql`
        CREATE FUNCTION slow_capture_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands()
                     WHERE object_identity LIKE 'echo_sync_capture on %') THEN
            PERFORM pg_sleep(0.3);
          END IF;
        END $$`
      yield* sql`CREATE EVENT TRIGGER slow_capture_ddl ON ddl_command_end
                 WHEN TAG IN ('CREATE TRIGGER') EXECUTE FUNCTION slow_capture_ddl()`
      const results = yield* Effect.forEach(["a.dup", "b.dup"], (table) => Effect.result(attach(table)), {
        concurrency: "unbounded"
      })
      assert.deepStrictEqual(results.filter((r) => r._tag === "Success").length, 1)
      const captured = (yield* syncedTables).filter((t) => t.name === "dup")
      assert.strictEqual(captured.length, 1)
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("an attach that fails between its two triggers leaves the table neither captured nor half-guarded", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* sql`CREATE TABLE half (id text PRIMARY KEY, access_scope_id text NOT NULL)`
      // Crash-point injection: fail the second DDL of attach (the TRUNCATE guard).
      yield* sql`
        CREATE FUNCTION fail_truncate_guard() RETURNS event_trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands()
                     WHERE object_identity LIKE 'echo_sync_reject_truncate%') THEN
            RAISE EXCEPTION 'injected failure';
          END IF;
        END $$`
      yield* sql`CREATE EVENT TRIGGER fail_truncate_guard ON ddl_command_end
                 WHEN TAG IN ('CREATE TRIGGER') EXECUTE FUNCTION fail_truncate_guard()`
      const failure = yield* Effect.flip(attach("half"))
      assert.strictEqual(failure._tag, "SqlError")
      const triggers = yield* sql<{ tgname: string }>`SELECT tgname FROM pg_trigger WHERE tgrelid = 'half'::regclass`
      assert.deepStrictEqual(triggers, [])
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "attach inside a caller's REPEATABLE READ transaction is refused (its stale snapshot would admit a duplicate name)",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        yield* sql`CREATE SCHEMA a`
        yield* sql`CREATE SCHEMA b`
        for (const schema of ["a", "b"]) {
          yield* sql`CREATE TABLE ${sql(schema)}.dup (id text PRIMARY KEY, access_scope_id text NOT NULL)`
        }
        // The reviewer's reproduction: T2 fixes a snapshot, T1 attaches a.dup and commits, T2 attaches b.dup.
        const snapshotTaken = yield* Deferred.make<void>()
        const otherAttached = yield* Deferred.make<void>()
        const t2 = yield* Effect.forkChild(
          Effect.exit(
            sql.withTransaction(
              Effect.gen(function* () {
                yield* sql`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`
                yield* sql`SELECT 1`
                yield* Deferred.succeed(snapshotTaken, undefined)
                yield* Deferred.await(otherAttached)
                yield* attach("b.dup")
              })
            )
          )
        )
        yield* Deferred.await(snapshotTaken)
        yield* attach("a.dup")
        yield* Deferred.succeed(otherAttached, undefined)
        const exit = yield* Fiber.join(t2)
        assert.isTrue(Exit.hasDies(exit))
        assert.strictEqual((yield* syncedTables).filter((t) => t.name === "dup").length, 1)
      }).pipe(Effect.provide(TestDatabase))
  )
})
