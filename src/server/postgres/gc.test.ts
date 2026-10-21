import { assert, describe, it } from "@effect/vitest"
import { Effect, Option } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { TestClock } from "effect/testing"
import { collectGarbage } from "./gc.ts"
import { pull } from "./pull.ts"
import { grant, pgAvailable, Replica, serverView, TestDatabase } from "./test-support.ts"

const insert = (id: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* sql`INSERT INTO items (id, access_scope_id) VALUES (${id}, 's1')`
  })

const databaseNow = Effect.flatMap(SqlClient, (sql) =>
  Effect.map(sql<{ ms: number }>`SELECT (extract(epoch FROM now()) * 1000)::float8 AS ms`, (rows) => rows[0]?.ms ?? 0)
)

/**
 * GC with a one-hour retention, run at `offset` ms after the database's current time on a
 * TestClock. Only GC sees the test clock; the database client keeps the live one.
 */
const gcAt = (offset: number) =>
  Effect.flatMap(databaseNow, (ms) =>
    Effect.andThen(TestClock.setTime(ms + offset), collectGarbage("1 hour")).pipe(Effect.provide(TestClock.layer()))
  )
const twoHours = 2 * 60 * 60 * 1000

describe.skipIf(!pgAvailable)("log GC (§7)", () => {
  it.live("entries younger than the retention window survive GC", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      yield* insert("a")
      const result = yield* gcAt(0)
      assert.strictEqual(result.deleted, 0)
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a client whose cursor GC passed must resnapshot and then converges; a client at head is unaffected", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      yield* insert("a")
      const behind = new Replica("p", "behind")
      yield* behind.sync()
      yield* insert("b")
      yield* insert("c")
      const current = new Replica("p", "current")
      yield* current.sync()

      const gc = yield* gcAt(twoHours)
      assert.isAbove(gc.deleted, 0)

      const failure = yield* Effect.flip(behind.sync())
      assert.deepStrictEqual(failure._tag === "ResnapshotRequired" && failure.reason, "gcHorizon")
      // The cursor at head is not below the horizon: incremental pulls keep working.
      yield* insert("d")
      yield* current.sync()
      assert.deepStrictEqual(current.view(), yield* serverView("p"))

      const fresh = new Replica("p", "behind")
      yield* fresh.sync()
      assert.deepStrictEqual(fresh.view(), yield* serverView("p"))
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("an unfinished page set whose log position GC passed must resnapshot instead of resuming", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      const replica = new Replica("p", "d")
      yield* replica.sync()
      for (const id of ["a", "b", "c", "d"]) yield* insert(id)
      const page = yield* replica.pullOnce(1)
      assert.strictEqual(page._tag, "PullPage")

      yield* gcAt(twoHours)
      const failure = yield* Effect.flip(replica.pullOnce(1))
      assert.deepStrictEqual(failure._tag === "ResnapshotRequired" && failure.reason, "gcHorizon")
    }).pipe(Effect.provide(TestDatabase))
  )
})

describe.skipIf(!pgAvailable)("pull continuation (§8)", () => {
  it.live("a continuation replayed by another client, principal, or cursor is refused", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      yield* grant("q", "s1")
      for (const id of ["a", "b", "c"]) yield* insert(id)
      const page = yield* pull({ principalId: "p", clientId: "d", cursor: null, continuation: Option.none(), limit: 1 })
      if (page._tag !== "PullPage") return assert.fail("expected a PullPage")
      const continuation = Option.some(page.continuation)
      const replays = [
        { principalId: "q", clientId: "q-device", cursor: null },
        { principalId: "p", clientId: "d2", cursor: null },
        { principalId: "p", clientId: "d", cursor: 0n }
      ]
      for (const replay of replays) {
        const failure = yield* Effect.flip(pull({ ...replay, continuation, limit: 10 }))
        assert.deepStrictEqual(failure._tag === "ResnapshotRequired" && failure.reason, "staleContinuation")
      }
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a cursor the server never completed a page set at is refused rather than trusted", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      yield* insert("a")
      const failure = yield* Effect.flip(
        pull({ principalId: "p", clientId: "d", cursor: 1n, continuation: Option.none(), limit: 10 })
      )
      assert.deepStrictEqual(failure._tag === "ResnapshotRequired" && failure.reason, "unknownCursor")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("re-pulling from the previous cursor after a lost PullDone yields the same converged state", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      yield* insert("a")
      const replica = new Replica("p", "d")
      yield* replica.sync()
      const cursor = replica.cursor
      yield* insert("b")
      // This response is lost: the client never applies it, then the next one also completes.
      yield* pull({ principalId: "p", clientId: "d", cursor, continuation: Option.none(), limit: 10 })
      yield* insert("c")
      yield* replica.sync()
      assert.deepStrictEqual(replica.view(), yield* serverView("p"))
    }).pipe(Effect.provide(TestDatabase))
  )
})
