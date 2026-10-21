import { assert, describe, it } from "@effect/vitest"
import { Effect, Schedule } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { MutationRejected, runBatch, runMutation } from "./ledger.ts"
import { pgAvailable, Replica, TestDatabase } from "./test-support.ts"

const fastRetry = { conflictRetry: Schedule.recurs(2) }

/** A body with a visible side effect: appends to `items.body` of row `counter`. */
const appendEffect = (mark: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* sql`
      INSERT INTO items (id, access_scope_id, body) VALUES ('counter', 's1', ${mark})
      ON CONFLICT (id) DO UPDATE SET body = items.body || ${mark}`
  })

const counterBody = Effect.gen(function* () {
  const sql = yield* SqlClient
  const rows = yield* sql<{ body: string }>`SELECT body FROM items WHERE id = 'counter'`
  return rows[0]?.body ?? ""
})

/** A real Postgres serialization failure (SQLSTATE 40001). */
const serializationFailure = Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`DO $$ BEGIN RAISE EXCEPTION 'conflict' USING ERRCODE = '40001'; END $$`
})

describe.skipIf(!pgAvailable)("mutation ledger (§6)", () => {
  it.live("a resubmitted envelope (lost response) returns the recorded outcome without re-executing", () =>
    Effect.gen(function* () {
      const envelope = { principalId: "p", clientId: "c1", mutationId: 1 }
      const first = yield* runMutation(envelope, appendEffect("x"))
      // The client never saw `first` and retries the same identity, possibly with a body that
      // would now behave differently: it must not run.
      const second = yield* runMutation(
        envelope,
        Effect.fail(new MutationRejected({ reason: "domain", message: "no" }))
      )
      assert.deepStrictEqual(second, first)
      assert.strictEqual(yield* counterBody, "x")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("concurrent submissions of one envelope commit its effects once and agree on the outcome", () =>
    Effect.gen(function* () {
      const envelope = { principalId: "p", clientId: "c1", mutationId: 7 }
      const outcomes = yield* Effect.forEach(
        Array.from({ length: 8 }),
        () => runMutation(envelope, Effect.andThen(appendEffect("x"), Effect.sleep("20 millis"))),
        { concurrency: "unbounded" }
      )
      for (const outcome of outcomes) assert.deepStrictEqual(outcome, { _tag: "Applied" })
      assert.strictEqual(yield* counterBody, "x")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a rejected mutation records its outcome and leaves none of its writes behind", () =>
    Effect.gen(function* () {
      const envelope = { principalId: "p", clientId: "c1", mutationId: 1 }
      const outcome = yield* runMutation(
        envelope,
        Effect.andThen(appendEffect("x"), Effect.fail(new MutationRejected({ reason: "forbidden", message: "nope" })))
      )
      assert.deepStrictEqual(outcome, { _tag: "Rejected", reason: "forbidden", message: "nope" })
      assert.strictEqual(yield* counterBody, "")
      // Recorded: a retry returns the same rejection without running the (now succeeding) body.
      assert.deepStrictEqual(yield* runMutation(envelope, appendEffect("y")), outcome)
      assert.strictEqual(yield* counterBody, "")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "a batch stops unrecorded at a transient failure and its resubmission continues in client order",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        let attempts = 0
        const flaky = Effect.andThen(
          Effect.sync(() => attempts++),
          serializationFailure
        )
        const m1 = { mutationId: 1, body: appendEffect("1") }
        const m2 = { mutationId: 2, body: Effect.fail(new MutationRejected({ reason: "domain", message: "bad" })) }
        const m4 = { mutationId: 4, body: appendEffect("4") }
        const first = yield* runBatch("p", "c1", [m1, m2, { mutationId: 3, body: flaky }, m4], fastRetry)
        assert.deepStrictEqual(
          first.processed.map((p) => [p.mutationId, p.outcome._tag]),
          [
            [1, "Applied"],
            [2, "Rejected"]
          ]
        )
        assert.strictEqual(first.stopped?.mutationId, 3)
        assert.strictEqual(attempts, 3, "bounded retry: one attempt plus two retries")
        // Nothing for ≥ k: neither 3 nor 4 is recorded or applied.
        const recorded = yield* sql<{ mutation_id: bigint }>`
          SELECT mutation_id FROM echo_sync.mutations WHERE client_id = 'c1' ORDER BY position`
        assert.deepStrictEqual(
          recorded.map((r) => Number(r.mutation_id)),
          [1, 2]
        )
        assert.strictEqual(yield* counterBody, "1")

        // The client resubmits the whole unacknowledged batch; the conflict is gone now.
        const second = yield* runBatch("p", "c1", [m1, m2, { mutationId: 3, body: appendEffect("3") }, m4], fastRetry)
        assert.strictEqual(second.stopped, null)
        assert.deepStrictEqual(
          second.processed.map((p) => p.outcome._tag),
          ["Applied", "Rejected", "Applied", "Applied"]
        )
        assert.strictEqual(yield* counterBody, "134")

        // Acks arrive by pull, in client order, each exactly once.
        const replica = new Replica("p", "c1")
        yield* replica.sync()
        assert.deepStrictEqual([...replica.acked], [1, 2, 3, 4])
      }).pipe(Effect.provide(TestDatabase)),
    30_000
  )

  it.live("a server crash mid-mutation records nothing, keeps none of its writes, and the retry applies once", () =>
    Effect.gen(function* () {
      const envelope = { principalId: "p", clientId: "c1", mutationId: 1 }
      // The backend dies after the body wrote and before the ledger row: the closest a test can
      // get to the process crashing between "handler done" and "commit".
      const crashing = Effect.andThen(
        appendEffect("x"),
        Effect.flatMap(SqlClient, (sql) => sql`SELECT pg_terminate_backend(pg_backend_pid())`)
      )
      const result = yield* Effect.flip(runMutation(envelope, crashing))
      assert.strictEqual(result._tag, "MutationTransient")
      assert.strictEqual(yield* counterBody, "")
      assert.deepStrictEqual(yield* runMutation(envelope, appendEffect("x")), { _tag: "Applied" })
      assert.deepStrictEqual(yield* runMutation(envelope, appendEffect("x")), { _tag: "Applied" })
      assert.strictEqual(yield* counterBody, "x")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a mutation that writes nothing still gets an ack delivered by pull", () =>
    Effect.gen(function* () {
      const replica = new Replica("p", "c1")
      yield* replica.sync()
      yield* runMutation({ principalId: "p", clientId: "c1", mutationId: 1 }, Effect.void)
      yield* replica.sync()
      assert.deepStrictEqual([...replica.acked], [1])
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a client id registered to one principal is refused to another", () =>
    Effect.gen(function* () {
      yield* runMutation({ principalId: "alice", clientId: "c1", mutationId: 1 }, Effect.void)
      const result = yield* Effect.flip(
        runMutation({ principalId: "mallory", clientId: "c1", mutationId: 1 }, Effect.void)
      )
      assert.strictEqual(result._tag, "ClientForbidden")
    }).pipe(Effect.provide(TestDatabase))
  )
})
