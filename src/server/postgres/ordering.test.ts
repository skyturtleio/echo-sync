import { assert, describe, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Random, Ref } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { grant, pgAvailable, Replica, TestDatabase } from "./test-support.ts"

/**
 * DESIGN.md §7: a client that has pulled through cursor c has seen every change with
 * position ≤ c. Writers allocate a position, then hold their transaction open for a random time,
 * so allocation order and commit order disagree unless the engine lock serializes them.
 */
const writersRaceAPullingReader = Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* grant("reader", "s1")
  const replica = new Replica("reader", "reader-device")
  yield* replica.sync()

  const writers = 40
  const done = yield* Ref.make(false)
  const readerFiber = yield* Effect.forkChild(
    Effect.gen(function* () {
      while (!(yield* Ref.get(done))) yield* replica.pullOnce(5)
    })
  )
  yield* Effect.forEach(
    Array.from({ length: writers }, (_, i) => i),
    (i) =>
      sql.withTransaction(
        Effect.gen(function* () {
          yield* Effect.sleep(`${yield* Random.nextIntBetween(0, 10)} millis`)
          yield* sql`INSERT INTO items (id, access_scope_id) VALUES (${`w${i}`}, 's1')`
          yield* Effect.sleep(`${yield* Random.nextIntBetween(0, 30)} millis`)
        })
      ),
    { concurrency: "unbounded", discard: true }
  )
  yield* Ref.set(done, true)
  yield* Fiber.join(readerFiber)
  // Anything still undelivered with a position above the cursor arrives now; a change whose
  // position the cursor already passed never will.
  yield* replica.sync()

  const received = new Set(replica.received.flatMap((c) => (c._tag === "upsert" ? [String(c.row["id"])] : [])))
  return Array.from({ length: writers }, (_, i) => `w${i}`).filter((id) => !received.has(id))
})

/**
 * The interleaving the engine lock exists to prevent, forced with Deferreds instead of timing:
 * writer A inserts (allocating its position) and holds its transaction open; writer B then inserts
 * and commits; the reader pulls once; A commits; the reader syncs. Without the lock B's position
 * is above A's and committed first, so the pull's cursor passes A's position before A is visible.
 * With the lock B's allocation queues behind A's transaction, so the pull cannot pass A. Returns
 * the rows the reader never received.
 */
const readerPullsBetweenOutOfOrderCommits = Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* grant("reader", "s1")
  const replica = new Replica("reader", "reader-device")
  yield* replica.sync()

  const aInserted = yield* Deferred.make<void>()
  const releaseA = yield* Deferred.make<void>()
  const bCommitted = yield* Deferred.make<void>()
  const writerA = yield* Effect.forkChild(
    sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO items (id, access_scope_id) VALUES ('a', 's1')`
        yield* Deferred.succeed(aInserted, undefined)
        yield* Deferred.await(releaseA)
      })
    )
  )
  yield* Deferred.await(aInserted)
  const writerB = yield* Effect.forkChild(
    sql
      .withTransaction(sql`INSERT INTO items (id, access_scope_id) VALUES ('b', 's1')`)
      .pipe(Effect.andThen(Deferred.succeed(bCommitted, undefined)))
  )
  // B either commits (no lock) or is queued on the engine's advisory lock in this database.
  while (!(yield* Deferred.isDone(bCommitted))) {
    const waiting = yield* sql<{ n: number }>`
      SELECT count(*)::int AS n FROM pg_locks
      WHERE locktype = 'advisory' AND NOT granted
        AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`
    if ((waiting[0]?.n ?? 0) > 0) break
    yield* Effect.sleep("5 millis")
  }
  yield* replica.pullOnce(100)
  yield* Deferred.succeed(releaseA, undefined)
  yield* Fiber.join(writerA)
  yield* Fiber.join(writerB)
  yield* replica.sync()

  const received = new Set(replica.received.flatMap((c) => (c._tag === "upsert" ? [String(c.row["id"])] : [])))
  return ["a", "b"].filter((id) => !received.has(id))
})

/** The engine's position allocation with the advisory lock removed; everything else unchanged. */
const withoutEngineLock = Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`
    CREATE OR REPLACE FUNCTION echo_sync.next_position() RETURNS bigint
    LANGUAGE sql VOLATILE AS $$ SELECT nextval('echo_sync.position_seq') $$`
})

describe.skipIf(!pgAvailable)("log ordering (§7)", () => {
  it.live(
    "a reader pulling after a later writer commits never skips an earlier writer still in flight",
    () =>
      Effect.gen(function* () {
        assert.deepStrictEqual(yield* readerPullsBetweenOutOfOrderCommits, [])
      }).pipe(Effect.provide(TestDatabase)),
    30_000
  )

  it.live(
    "without the trigger's advisory lock that reader skips the earlier writer (the ordering check can fail)",
    () =>
      Effect.gen(function* () {
        yield* withoutEngineLock
        assert.deepStrictEqual(yield* readerPullsBetweenOutOfOrderCommits, ["a"])
      }).pipe(Effect.provide(TestDatabase)),
    30_000
  )

  it.live(
    "a reader advancing its cursor during many random out-of-order commits never misses a change (stress)",
    () =>
      Effect.gen(function* () {
        const missed = yield* writersRaceAPullingReader
        assert.deepStrictEqual(missed, [])
      }).pipe(Effect.provide(TestDatabase)),
    30_000
  )
})
