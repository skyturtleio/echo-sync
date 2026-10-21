import { assert, describe, it } from "@effect/vitest"
import { Effect, Fiber, Random, Ref } from "effect"
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

  const received = new Set(replica.received.flatMap((c) => (c._tag === "Upsert" ? [c.rowId] : [])))
  return Array.from({ length: writers }, (_, i) => `w${i}`).filter((id) => !received.has(id))
})

describe.skipIf(!pgAvailable)("log ordering (§7)", () => {
  it.live(
    "a reader advancing its cursor during out-of-order commits never misses a change",
    () =>
      Effect.gen(function* () {
        const missed = yield* writersRaceAPullingReader
        assert.deepStrictEqual(missed, [])
      }).pipe(Effect.provide(TestDatabase)),
    30_000
  )

  it.live(
    "without the trigger's advisory lock the same reader misses changes (the ordering test can fail)",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        // Same trigger, same scenario; only the lock in position allocation is removed.
        yield* sql`
          CREATE OR REPLACE FUNCTION echo_sync.next_position() RETURNS bigint
          LANGUAGE sql VOLATILE AS $$ SELECT nextval('echo_sync.position_seq') $$`
        const missed = yield* writersRaceAPullingReader
        assert.isAbove(missed.length, 0, "expected the unlocked trigger to lose changes")
      }).pipe(Effect.provide(TestDatabase)),
    30_000
  )
})
