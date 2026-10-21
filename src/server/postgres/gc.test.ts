import { assert, describe, it } from "@effect/vitest"
import { Effect, Option } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { TestClock } from "effect/testing"
import { collectGarbage } from "./gc.ts"
import { runMutation } from "./ledger.ts"
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
      // Deleted while `behind` is behind: its tombstone is GC'd and can never be delivered.
      yield* Effect.flatMap(SqlClient, (sql) => sql`DELETE FROM items WHERE id = 'a'`)
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

      // Resnapshot on the same replica: the null-cursor set replaces what it held, so the row
      // whose tombstone was collected is gone too.
      assert.isTrue(behind.rows.has("items/a"))
      behind.resnapshot()
      yield* behind.sync()
      assert.deepStrictEqual(behind.view(), yield* serverView("p"))
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("log GC never deletes ledger rows or held-scope records (would cause double apply after resnapshot)", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* grant("p", "s1")
      const replica = new Replica("p", "c1")
      yield* replica.sync()
      yield* runMutation({ principalId: "p", clientId: "c1", mutationId: "m1", seq: 1n }, insert("a"))
      yield* replica.sync()
      const snapshot = Effect.all([
        sql`SELECT client_id, mutation_id, position, outcome FROM echo_sync.mutations ORDER BY position`,
        sql`SELECT client_id, position, scope_ids FROM echo_sync.client_scopes ORDER BY client_id, position`,
        sql`SELECT client_id, principal_id FROM echo_sync.clients ORDER BY client_id`
      ])
      const before = yield* snapshot
      assert.isAbove(before[0].length + before[1].length, 1)
      const gc = yield* gcAt(twoHours)
      assert.isAbove(gc.deleted, 0)
      assert.deepStrictEqual(yield* snapshot, before)
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a page set interrupted mid row walk whose position GC passed must resnapshot instead of resuming", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      for (const id of ["a", "b", "c"]) yield* insert(id)
      const replica = new Replica("p", "d")
      const page = yield* replica.pullOnce(1)
      assert.strictEqual(page._tag, "PullPage")
      // Writes after the walk's head, then GC past them: the log the walk would continue with is gone.
      for (const id of ["d", "e"]) yield* insert(id)
      yield* gcAt(twoHours)
      const failure = yield* Effect.flip(replica.pullOnce(1))
      assert.deepStrictEqual(failure._tag === "ResnapshotRequired" && failure.reason, "gcHorizon")
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

/** A well-formed, decodable continuation for client `d` of principal `p`. */
const forge = (cursor: bigint | null, position: string) =>
  Option.some(
    JSON.stringify({
      clientId: "d",
      principalId: "p",
      cursor: cursor === null ? null : String(cursor),
      position,
      scopes: [],
      walking: []
    })
  )

const expectStale = (cursor: bigint | null, position: string) =>
  Effect.gen(function* () {
    const failure = yield* Effect.flip(
      pull({ principalId: "p", clientId: "d", cursor, continuation: forge(cursor, position), limit: 10 })
    )
    assert.deepStrictEqual(failure._tag === "ResnapshotRequired" && failure.reason, "staleContinuation")
  })

describe.skipIf(!pgAvailable)("pull continuation (§8)", () => {
  it.live("a continuation replayed by another client, principal, or cursor, or garbled, is refused", () =>
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
      const tampered = page.continuation.replace(/"position":"\d+"/, '"position":"x"')
      assert.notStrictEqual(tampered, page.continuation)
      for (const garbled of ["", "not json", tampered]) {
        const failure = yield* Effect.flip(
          pull({ principalId: "p", clientId: "d", cursor: null, continuation: Option.some(garbled), limit: 10 })
        )
        assert.deepStrictEqual(failure._tag === "ResnapshotRequired" && failure.reason, "staleContinuation")
      }
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a well-formed forged continuation cannot record a held set no snapshot produced", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      const heldSets = sql`SELECT position, scope_ids FROM echo_sync.client_scopes ORDER BY position`

      yield* grant("p", "A")
      yield* sql`INSERT INTO items (id, access_scope_id) VALUES ('a1', 'A')`
      yield* sql`INSERT INTO echo_sync.access_scopes (id) VALUES ('B')`
      yield* sql`INSERT INTO items (id, access_scope_id) VALUES ('b1', 'B')`
      const replica = new Replica("p", "d")
      yield* replica.sync()
      const cursor = replica.cursor
      if (cursor === null) return assert.fail("expected a cursor")
      const before = yield* heldSets

      // The reviewer's reproduction: a valid-JSON continuation at a future position, while the
      // client holds A, and again after A is revoked and B granted.
      yield* expectStale(null, "1000")
      yield* expectStale(cursor, "1000")
      yield* sql`DELETE FROM echo_sync.scope_grants WHERE access_scope_id = 'A'`
      yield* grant("p", "B")
      yield* expectStale(cursor, "1000")
      // Negative, and behind the originating cursor, are refused too.
      yield* expectStale(null, "-1")
      yield* expectStale(cursor, String(cursor - 1n))
      assert.deepStrictEqual(yield* heldSets, before)

      // The honest client then purges A and walks B; a later revoke of B still purges B.
      yield* replica.sync()
      assert.isTrue(replica.rows.has("items/b1") && !replica.rows.has("items/a1"))
      yield* sql`DELETE FROM echo_sync.scope_grants WHERE access_scope_id = 'B'`
      const received = replica.received.length
      yield* replica.sync()
      assert.deepStrictEqual(
        replica.received.slice(received).filter((c) => c._tag === "ScopePurged"),
        [{ _tag: "ScopePurged", scopeId: "B" }]
      )
      assert.deepStrictEqual(replica.view(), yield* serverView("p"))
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a resumed page set whose originating cursor the server never completed is refused", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      for (const id of ["a", "b"]) yield* insert(id)
      const continuation = Option.some(
        JSON.stringify({ clientId: "d", principalId: "p", cursor: "1", position: "1", scopes: ["s1"], walking: [] })
      )
      const failure = yield* Effect.flip(pull({ principalId: "p", clientId: "d", cursor: 1n, continuation, limit: 10 }))
      assert.deepStrictEqual(failure._tag === "ResnapshotRequired" && failure.reason, "unknownCursor")
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
