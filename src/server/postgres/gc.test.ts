import { assert, describe, it } from "@effect/vitest"
import * as PgMigrator from "@effect/sql-pg/PgMigrator"
import { Cause, type Duration, Effect, Exit, Option } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { TestClock } from "effect/testing"
import { collectGarbage, expireLedger, LedgerRetentionTooShort } from "./gc.ts"
import { runMutation } from "./ledger.ts"
import { pull } from "./pull.ts"
import engine from "./migrations/001_engine.ts"
import grantIdCheck from "./migrations/002_grant_id_check.ts"
import { migrate, migrationsTable } from "./schema.ts"
import { EmptyDatabase, grant, pgAvailable, Replica, serverView, TestDatabase, registry } from "./test-support.ts"

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

const day = 24 * 60 * 60 * 1000

/** `expireLedger` run at `offset` ms after the database's current time on a TestClock. */
const expireAt = (offset: number, retention?: Duration.Input) =>
  Effect.flatMap(databaseNow, (ms) =>
    Effect.andThen(
      TestClock.setTime(ms + offset),
      expireLedger(retention === undefined ? undefined : { retention })
    ).pipe(Effect.provide(TestClock.layer()))
  )

/** Moves every acked row's ack time back by `days` (the time between its ack and the job). */
const ageAcks = (days: number) =>
  Effect.flatMap(
    SqlClient,
    (sql) => sql`UPDATE echo_sync.mutations SET acked_at = acked_at - make_interval(days => ${days})
                 WHERE client_id = 'c1' AND acked_at IS NOT NULL`
  )

/**
 * One client `c1` of `p` that pushes `items.insert(id)` mutations through the real ledger and pulls
 * through a stand-in replica. `executions` counts bodies run, i.e. real (re-)executions.
 */
const ledgerClient = Effect.gen(function* () {
  yield* grant("p", "s1")
  const replica = new Replica("p", "c1")
  yield* replica.sync()
  const counter = { executions: 0 }
  const submit = (mutationId: string, seq: number, id = mutationId) =>
    runMutation(
      { principalId: "p", clientId: "c1", mutationId, seq: BigInt(seq) },
      Effect.andThen(
        Effect.sync(() => void counter.executions++),
        insert(id)
      )
    )
  const state = Effect.flatMap(SqlClient, (sql) =>
    Effect.all({
      rows: sql<{ seq: string; acked: boolean; live: boolean }>`
        SELECT seq::text, acked_at IS NOT NULL AS acked, outcome IS NOT NULL AS live
        FROM echo_sync.mutations WHERE client_id = 'c1' ORDER BY seq`,
      floor: sql<{ expired_through: string; expired_position: string }>`
        SELECT expired_through::text, expired_position::text FROM echo_sync.clients WHERE client_id = 'c1'`
    })
  )
  /** Rows that still hold their outcome; an expired row keeps only its identity. */
  const seqs = Effect.map(state, (s) => s.rows.filter((r) => r.live).map((r) => Number(r.seq)))
  /** Every identity in the ledger, expired or not: expiry never removes one. */
  const identities = Effect.map(state, (s) => s.rows.map((r) => Number(r.seq)))
  return { replica, counter, submit, state, seqs, identities, ageAcks }
})

describe.skipIf(!pgAvailable)("ledger expiry (DESIGN §16 item 5)", () => {
  it.live(
    "an acked row older than the retention expires, and its resubmission answers Rejected{ClientExpired} at the recorded position instead of executing again",
    () =>
      Effect.gen(function* () {
        const c = yield* ledgerClient
        const first = yield* c.submit("m1", 1, "a")
        assert.strictEqual(first.outcome._tag, "Applied")
        yield* c.replica.sync() // PullDone with m1's ack
        assert.isTrue(c.replica.acked.has("m1"))
        yield* c.replica.sync() // presents the cursor past m1: acked from now

        const run = yield* expireAt(7 * day + 60_000)
        assert.deepStrictEqual(run, { clients: 1, expired: 1 })
        assert.deepStrictEqual(yield* c.seqs, [])
        assert.deepStrictEqual(yield* c.identities, [1], "the identity outlives the outcome")

        const again = yield* c.submit("m1", 1, "a")
        assert.strictEqual(again.outcome._tag === "Rejected" && again.outcome.error._tag, "ClientExpired")
        assert.strictEqual(again.position, first.position)
        assert.strictEqual(c.counter.executions, 1)
        const rows = yield* Effect.flatMap(SqlClient, (sql) => sql`SELECT id FROM items`)
        assert.deepStrictEqual(rows, [{ id: "a" }])
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "a retry of an expired mutation under a fresh seq above the floor is answered Rejected{ClientExpired} at its original position, not executed again",
    () =>
      Effect.gen(function* () {
        const c = yield* ledgerClient
        const first = yield* c.submit("m1", 1, "a")
        yield* c.replica.sync()
        yield* c.replica.sync() // m1 acked now
        yield* ageAcks(10)
        assert.deepStrictEqual(yield* expireAt(0), { clients: 1, expired: 1 })
        // The client forgot m1 (its released-row marker was pruned) and enqueues it again as seq 2.
        const retry = yield* c.submit("m1", 2, "a")
        assert.strictEqual(retry.outcome._tag === "Rejected" && retry.outcome.error._tag, "ClientExpired")
        assert.strictEqual(retry.position, first.position)
        assert.strictEqual(c.counter.executions, 1)
        assert.deepStrictEqual(yield* c.identities, [1], "the retry recorded nothing")
        // A cleared row is never acked or expired again: later pulls and jobs leave it alone.
        yield* c.replica.sync()
        yield* c.replica.sync()
        yield* ageAcks(10)
        assert.deepStrictEqual(yield* expireAt(0), { clients: 0, expired: 0 })
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "a resubmitted expired id is answered at its own recorded position whatever seq it carries, and only an unknown id at or below the floor gets the floor's position",
    () =>
      Effect.gen(function* () {
        const c = yield* ledgerClient
        const m1 = yield* c.submit("m1", 1)
        const m2 = yield* c.submit("m2", 2)
        yield* c.replica.sync()
        yield* c.replica.sync() // both acked now
        yield* ageAcks(10)
        assert.deepStrictEqual(yield* expireAt(0), { clients: 1, expired: 2 })
        assert.deepStrictEqual((yield* c.state).floor, [
          { expired_through: "2", expired_position: String(m2.position) }
        ])
        const answer = (mutationId: string, seq: number) =>
          Effect.map(c.submit(mutationId, seq), (p) => [
            p.outcome._tag === "Rejected" ? p.outcome.error._tag : p.outcome._tag,
            p.position
          ])
        // At or below the floor (its own seq) and above it (a fresh seq): its own position both times.
        assert.deepStrictEqual(yield* answer("m1", 1), ["ClientExpired", m1.position])
        assert.deepStrictEqual(yield* answer("m1", 3), ["ClientExpired", m1.position])
        assert.deepStrictEqual(yield* answer("m2", 2), ["ClientExpired", m2.position])
        assert.deepStrictEqual(yield* answer("m2", 1), ["ClientExpired", m2.position])
        // Control: an id the server never recorded, at a seq at or below the floor, gets the floor's position.
        assert.deepStrictEqual(yield* answer("never-seen", 1), ["ClientExpired", m2.position])
        assert.strictEqual(c.counter.executions, 2)
        assert.deepStrictEqual(yield* c.identities, [1, 2], "the retries recorded nothing")
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "an acked row younger than the retention, and an unacked row of any age, survive; age is measured from the ack, not from receipt",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        const c = yield* ledgerClient
        yield* c.submit("m1", 1)
        yield* c.replica.sync()
        yield* c.replica.sync() // m1 acked now
        // Received long ago, acked only now: young by the rule.
        yield* sql`UPDATE echo_sync.mutations SET received_at = now() - interval '400 days' WHERE mutation_id = 'm1'`
        yield* c.submit("m2", 2) // pushed, never pulled: unacked
        yield* sql`UPDATE echo_sync.mutations SET received_at = now() - interval '400 days' WHERE mutation_id = 'm2'`

        assert.deepStrictEqual(yield* expireAt(7 * day - 60_000), { clients: 0, expired: 0 })
        assert.deepStrictEqual(yield* c.seqs, [1, 2])
        // A year on, m1 has been acked longer than the retention; m2 never was.
        yield* expireAt(365 * day)
        assert.deepStrictEqual(yield* c.seqs, [2])
        assert.strictEqual((yield* c.state).floor[0]?.expired_through, "1")
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "the floor after a job is the highest seq of an expired prefix, PullDone.expiredThrough reports it, and the recorded position is the highest expired row's",
    () =>
      Effect.gen(function* () {
        const c = yield* ledgerClient
        const p1 = yield* c.submit("m1", 1)
        const p2 = yield* c.submit("m2", 2)
        yield* c.replica.sync()
        yield* c.replica.sync() // m1, m2 acked now
        yield* c.ageAcks(10) // ... ten days before the job
        yield* c.submit("m3", 3)
        yield* c.replica.sync()
        yield* c.replica.sync() // m3 acked now
        yield* c.submit("m4", 4) // unacked

        assert.deepStrictEqual(yield* expireAt(0), { clients: 1, expired: 2 })
        assert.deepStrictEqual(yield* c.seqs, [3, 4])
        assert.deepStrictEqual((yield* c.state).floor, [
          { expired_through: "2", expired_position: String(p2.position) }
        ])
        assert.isBelow(Number(p1.position), Number(p2.position))
        const done = yield* pull(registry, {
          principalId: "p",
          clientId: "c1",
          cursor: c.replica.cursor,
          continuation: Option.none(),
          limit: 100
        })
        assert.strictEqual(done._tag === "PullDone" ? Number(done.expiredThrough) : "not PullDone", 2)
        // A later job that expires nothing never lowers it.
        yield* expireAt(0)
        assert.strictEqual((yield* c.state).floor[0]?.expired_through, "2")
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "expired rows that would not be a seq prefix fail the job loudly and roll back: no row deleted, no floor raised",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        const c = yield* ledgerClient
        yield* c.submit("m1", 1)
        yield* c.submit("m2", 2)
        // Impossible through the protocol (acks are installed in seq order); forged here.
        yield* sql`UPDATE echo_sync.mutations SET acked_at = now() - interval '30 days' WHERE mutation_id = 'm2'`
        const before = yield* c.state
        const exit = yield* Effect.exit(expireAt(0))
        assert.isTrue(Exit.isFailure(exit) && Cause.hasDies(exit.cause), "the job dies")
        assert.deepStrictEqual(yield* c.state, before)
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "a job run with no eligible rows changes nothing: young acks, unacked rows and earlier floors stay as they are",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        const c = yield* ledgerClient
        yield* c.submit("m1", 1)
        yield* c.replica.sync()
        yield* c.replica.sync()
        yield* c.ageAcks(10)
        yield* expireAt(0) // floor 1
        yield* c.submit("m2", 2)
        yield* c.replica.sync()
        yield* c.replica.sync() // m2 acked now
        yield* c.submit("m3", 3) // unacked
        const snapshot = Effect.all([
          sql`SELECT client_id, mutation_id, seq, position, outcome, acked_at FROM echo_sync.mutations ORDER BY position`,
          sql`SELECT * FROM echo_sync.clients ORDER BY client_id`
        ])
        const before = yield* snapshot
        assert.deepStrictEqual(yield* expireAt(0), { clients: 0, expired: 0 })
        assert.deepStrictEqual(yield* snapshot, before)
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "a late retry of an acked mutation under a fresh seq, landing within the retention of its ack, is answered from the ledger, not executed again",
    () =>
      Effect.gen(function* () {
        // The reconcile simulation's naive-GC double apply: the original is acked and released, its
        // released-row marker is pruned (24 h), and a late enqueue retry gets a fresh seq above any
        // floor. Only the ledger row recognises it, so it must outlive every retry the client can
        // still produce; the minimum retention is 48 h after the ack.
        const c = yield* ledgerClient
        const first = yield* c.submit("m1", 1, "a")
        yield* c.replica.sync()
        yield* c.replica.sync() // acked now
        yield* expireAt(48 * 60 * 60 * 1000 - 60_000, "48 hours")
        const retry = yield* c.submit("m1", 2, "a")
        assert.deepStrictEqual(retry, first)
        assert.strictEqual(c.counter.executions, 1)
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "expiring the newest ledger row never moves the head back: a cursor at its position does not regress and a page set resumed at it continues",
    () =>
      Effect.gen(function* () {
        const c = yield* ledgerClient
        yield* insert("a")
        yield* insert("b")
        // A mutation that writes nothing: its ledger row is the only record of the newest position.
        const last = yield* runMutation({ principalId: "p", clientId: "c1", mutationId: "m1", seq: 1n }, Effect.void)
        yield* c.replica.sync()
        assert.strictEqual(c.replica.cursor, last.position)
        yield* c.replica.sync() // stamps m1
        // Another client's initial walk is mid-set at that head.
        const other = new Replica("p", "d")
        assert.strictEqual((yield* other.pullOnce(1))._tag, "PullPage")

        yield* ageAcks(10)
        assert.deepStrictEqual(yield* expireAt(0), { clients: 1, expired: 1 })

        yield* c.replica.sync()
        assert.strictEqual(c.replica.cursor, last.position, "the cursor does not regress")
        yield* other.sync()
        assert.strictEqual(other.cursor, last.position)
        assert.deepStrictEqual(other.view(), yield* serverView("p"))
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "ledger rows written before migration 003 come through it unstamped and are never expired until a pull acks them",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        // A database deployed at 001 + 002, with a year-old ledger row.
        yield* sql`CREATE SCHEMA echo_sync`
        yield* PgMigrator.make({})({
          loader: PgMigrator.fromRecord({ "001_engine": engine, "002_grant_id_check": grantIdCheck }),
          table: migrationsTable
        })
        yield* sql`INSERT INTO echo_sync.clients (client_id, principal_id) VALUES ('c1', 'p')`
        yield* sql`INSERT INTO echo_sync.mutations (client_id, mutation_id, seq, position, outcome, received_at)
                 VALUES ('c1', 'm1', 1, 1, '{"_tag":"Applied"}', now() - interval '365 days')`
        yield* migrate
        assert.deepStrictEqual(yield* expireAt(0), { clients: 0, expired: 0 })
        const rows = yield* sql<{ mutation_id: string; acked: boolean }>`
        SELECT mutation_id, acked_at IS NOT NULL AS acked FROM echo_sync.mutations`
        assert.deepStrictEqual(rows, [{ mutation_id: "m1", acked: false }])
      }).pipe(Effect.provide(EmptyDatabase))
  )

  it("a retention below 48 h is refused when the job is constructed", () => {
    assert.throws(() => expireLedger({ retention: "47 hours" }), LedgerRetentionTooShort)
    assert.doesNotThrow(() => expireLedger({ retention: "48 hours" }))
    assert.doesNotThrow(() => expireLedger())
  })
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
      pull(registry, { principalId: "p", clientId: "d", cursor, continuation: forge(cursor, position), limit: 10 })
    )
    assert.deepStrictEqual(failure._tag === "ResnapshotRequired" && failure.reason, "staleContinuation")
  })

describe.skipIf(!pgAvailable)("pull continuation (§8)", () => {
  it.live("a continuation replayed by another client, principal, or cursor, or garbled, is refused", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      yield* grant("q", "s1")
      for (const id of ["a", "b", "c"]) yield* insert(id)
      const page = yield* pull(registry, {
        principalId: "p",
        clientId: "d",
        cursor: null,
        continuation: Option.none(),
        limit: 1
      })
      if (page._tag !== "PullPage") return assert.fail("expected a PullPage")
      const continuation = Option.some(page.continuation)
      const replays = [
        { principalId: "q", clientId: "q-device", cursor: null },
        { principalId: "p", clientId: "d2", cursor: null },
        { principalId: "p", clientId: "d", cursor: 0n }
      ]
      for (const replay of replays) {
        const failure = yield* Effect.flip(pull(registry, { ...replay, continuation, limit: 10 }))
        assert.deepStrictEqual(failure._tag === "ResnapshotRequired" && failure.reason, "staleContinuation")
      }
      const tampered = page.continuation.replace(/"position":"\d+"/, '"position":"x"')
      assert.notStrictEqual(tampered, page.continuation)
      for (const garbled of ["", "not json", tampered]) {
        const failure = yield* Effect.flip(
          pull(registry, {
            principalId: "p",
            clientId: "d",
            cursor: null,
            continuation: Option.some(garbled),
            limit: 10
          })
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
        replica.received.slice(received).filter((c) => c._tag === "scopePurged"),
        [{ _tag: "scopePurged", scopeId: "B" }]
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
      const failure = yield* Effect.flip(
        pull(registry, { principalId: "p", clientId: "d", cursor: 1n, continuation, limit: 10 })
      )
      assert.deepStrictEqual(failure._tag === "ResnapshotRequired" && failure.reason, "unknownCursor")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a cursor the server never completed a page set at is refused rather than trusted", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      yield* insert("a")
      const failure = yield* Effect.flip(
        pull(registry, { principalId: "p", clientId: "d", cursor: 1n, continuation: Option.none(), limit: 10 })
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
      yield* pull(registry, { principalId: "p", clientId: "d", cursor, continuation: Option.none(), limit: 10 })
      yield* insert("c")
      yield* replica.sync()
      assert.deepStrictEqual(replica.view(), yield* serverView("p"))
    }).pipe(Effect.provide(TestDatabase))
  )
})
