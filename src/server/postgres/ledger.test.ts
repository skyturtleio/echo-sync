import { assert, describe, it } from "@effect/vitest"
import { Cause, Effect, Exit, Schedule } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { SqlError, UnknownError } from "effect/sql/SqlError"
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

const ledgerIds = Effect.gen(function* () {
  const sql = yield* SqlClient
  const rows = yield* sql<{ mutation_id: string }>`SELECT mutation_id FROM echo_sync.mutations ORDER BY position`
  return rows.map((r) => r.mutation_id)
})

/** A real Postgres serialization failure (SQLSTATE 40001). */
const serializationFailure = Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`DO $$ BEGIN RAISE EXCEPTION 'conflict' USING ERRCODE = '40001'; END $$`
})

const sqlDefect = new SqlError({ reason: new UnknownError({ cause: null, message: "connection lost" }) })
const programmerBug = new Error("programmer bug")

/** The attempt died with the programmer bug still in its cause, and no typed (retryable) failure. */
const diedWithBug = (exit: Exit.Exit<unknown, unknown>) =>
  Exit.isFailure(exit) &&
  exit.cause.reasons.some((r) => Cause.isDieReason(r) && r.defect === programmerBug) &&
  !exit.cause.reasons.some(Cause.isFailReason)

const env = (mutationId: string, seq: bigint) => ({ principalId: "p", clientId: "c1", mutationId, seq })
const reject = (message: string) => Effect.fail(new MutationRejected({ reason: "domain", message }))

describe.skipIf(!pgAvailable)("mutation ledger (§6)", () => {
  it.live("a resubmitted envelope (lost response) returns the recorded outcome and position without re-executing", () =>
    Effect.gen(function* () {
      const first = yield* runMutation(env("6f1c…uuid", 1n), appendEffect("x"))
      // The client never saw `first` and retries the same identity, possibly with a body that
      // would now behave differently: it must not run.
      const second = yield* runMutation(env("6f1c…uuid", 1n), reject("no"))
      assert.deepStrictEqual(second, first)
      assert.strictEqual(yield* counterBody, "x")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("concurrent submissions of one envelope run the body once and agree on outcome and position", () =>
    Effect.gen(function* () {
      let executions = 0
      const body = Effect.andThen(
        Effect.sync(() => executions++),
        Effect.andThen(appendEffect("x"), Effect.sleep("20 millis"))
      )
      const results = yield* Effect.forEach(Array.from({ length: 8 }), () => runMutation(env("m", 1n), body), {
        concurrency: "unbounded"
      })
      assert.strictEqual(executions, 1)
      for (const result of results) assert.deepStrictEqual(result, results[0])
      assert.strictEqual(yield* counterBody, "x")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a rejected mutation records its outcome and leaves none of its writes behind", () =>
    Effect.gen(function* () {
      const rejected = yield* runMutation(
        env("m", 1n),
        Effect.andThen(appendEffect("x"), Effect.fail(new MutationRejected({ reason: "forbidden", message: "nope" })))
      )
      assert.deepStrictEqual(rejected.outcome, { _tag: "Rejected", reason: "forbidden", message: "nope" })
      assert.strictEqual(yield* counterBody, "")
      // Recorded: a retry returns the same rejection without running the (now succeeding) body.
      assert.deepStrictEqual(yield* runMutation(env("m", 1n), appendEffect("y")), rejected)
      assert.strictEqual(yield* counterBody, "")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a batch stops at a transient failure without recording it, and its resubmission continues in order", () =>
    Effect.gen(function* () {
      let attempts = 0
      const flaky = Effect.andThen(
        Effect.sync(() => attempts++),
        serializationFailure
      )
      const m1 = { mutationId: "a", seq: 1n, body: appendEffect("1") }
      const m2 = { mutationId: "b", seq: 2n, body: reject("bad") }
      const m4 = { mutationId: "d", seq: 4n, body: appendEffect("4") }
      const first = yield* runBatch("p", "c1", [m1, m2, { mutationId: "c", seq: 3n, body: flaky }, m4], fastRetry)
      assert.deepStrictEqual(
        first.processed.map((p) => [p.mutationId, p.outcome._tag]),
        [
          ["a", "Applied"],
          ["b", "Rejected"]
        ]
      )
      assert.strictEqual(first.stopped?.mutationId, "c")
      assert.strictEqual(attempts, 3, "bounded retry: one attempt plus two retries")
      // Nothing for ≥ k: neither c nor d is recorded or applied.
      assert.deepStrictEqual(yield* ledgerIds, ["a", "b"])
      assert.strictEqual(yield* counterBody, "1")

      // The client resubmits the whole unacknowledged batch; the conflict is gone now.
      const second = yield* runBatch("p", "c1", [m1, m2, { mutationId: "c", seq: 3n, body: appendEffect("3") }, m4])
      assert.strictEqual(second.stopped, null)
      assert.deepStrictEqual(
        second.processed.map((p) => p.outcome._tag),
        ["Applied", "Rejected", "Applied", "Applied"]
      )
      // Replayed outcomes carry their original positions; new ones come after them.
      assert.deepStrictEqual(second.processed.slice(0, 2), first.processed)
      const positions = second.processed.map((p) => p.position)
      assert.isTrue(positions.every((position, i) => i === 0 || (positions[i - 1] ?? position) < position))
      assert.strictEqual(yield* counterBody, "134")

      // Acks arrive by pull, in client order, each exactly once.
      const replica = new Replica("p", "c1")
      yield* replica.sync()
      assert.deepStrictEqual([...replica.acked], ["a", "b", "c", "d"])
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a server crash mid-mutation records nothing, keeps none of its writes, and the retry applies once", () =>
    Effect.gen(function* () {
      // The backend dies after the body wrote and before the ledger row.
      const crashing = Effect.andThen(
        appendEffect("x"),
        Effect.flatMap(SqlClient, (sql) => sql`SELECT pg_terminate_backend(pg_backend_pid())`)
      )
      const result = yield* Effect.flip(runMutation(env("m", 1n), crashing))
      assert.strictEqual(result._tag, "MutationTransient")
      assert.strictEqual(yield* counterBody, "")
      const applied = yield* runMutation(env("m", 1n), appendEffect("x"))
      assert.deepStrictEqual(yield* runMutation(env("m", 1n), appendEffect("x")), applied)
      assert.strictEqual(yield* counterBody, "x")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "a connection lost after the body succeeded (savepoint cleanup fails) is a typed transient, not a defect",
    () =>
      Effect.gen(function* () {
        // The body finishes normally, but another connection kills its backend, so RELEASE
        // SAVEPOINT fails. The pinned SqlClient raises that as a defect.
        const killedAfterwards = Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* appendEffect("x")
          const [self] = yield* sql<{ pid: number }>`SELECT pg_backend_pid() AS pid`
          yield* Effect.scoped(
            Effect.flatMap(sql.reserve, (other) =>
              other.execute("SELECT pg_terminate_backend($1)", [self?.pid], undefined)
            )
          )
        })
        const result = yield* Effect.flip(runMutation(env("m", 1n), killedAfterwards))
        assert.strictEqual(result._tag, "MutationTransient")
        assert.deepStrictEqual(yield* ledgerIds, [])
        assert.strictEqual(yield* counterBody, "")
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live("runMutation inside a caller's transaction is refused (its outcome would not be committed)", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      const exit = yield* Effect.exit(sql.withTransaction(runMutation(env("m", 1n), appendEffect("x"))))
      assert.isTrue(Exit.hasDies(exit))
      assert.deepStrictEqual(yield* ledgerIds, [])
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a mutation that writes nothing still gets an ack delivered by pull", () =>
    Effect.gen(function* () {
      const replica = new Replica("p", "c1")
      yield* replica.sync()
      yield* runMutation(env("m", 1n), Effect.void)
      yield* replica.sync()
      assert.deepStrictEqual([...replica.acked], ["m"])
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a seq at or below the client's expiry floor is not executed and returns the expired position", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* runMutation(env("m1", 1n), Effect.void)
      // No v1 job raises the floor (§16.5); raise it by hand.
      yield* sql`UPDATE echo_sync.clients SET expired_through = 2, expired_position = 41 WHERE client_id = 'c1'`
      const expired = yield* runMutation(env("m2", 2n), appendEffect("x"))
      assert.deepStrictEqual(expired, {
        outcome: { _tag: "Rejected", reason: "clientExpired", message: "ledger expired for this seq" },
        position: 41n
      })
      assert.strictEqual(yield* counterBody, "")
      assert.deepStrictEqual(yield* ledgerIds, ["m1"])
      const next = yield* runMutation(env("m3", 3n), appendEffect("y"))
      assert.strictEqual(next.outcome._tag, "Applied")

      const replica = new Replica("p", "c1")
      const done = yield* replica.pullOnce(100)
      assert.deepStrictEqual(done._tag === "PullDone" && done.expiredThrough, 2n)
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a client id registered to one principal is refused to another", () =>
    Effect.gen(function* () {
      yield* runMutation(env("m", 1n), Effect.void)
      const result = yield* Effect.flip(
        runMutation({ principalId: "mallory", clientId: "c1", mutationId: "m", seq: 1n }, Effect.void)
      )
      assert.strictEqual(result._tag, "ClientForbidden")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a programmer defect combined with a SQL failure is never reported as a retryable transient", () =>
    Effect.gen(function* () {
      const bodies = [
        // SQL cleanup defect first, programmer bug in a finalizer, and the reverse order.
        Effect.die(sqlDefect).pipe(Effect.ensuring(Effect.die(programmerBug))),
        Effect.die(programmerBug).pipe(Effect.ensuring(Effect.die(sqlDefect))),
        Effect.andThen(appendEffect("x"), Effect.fail(sqlDefect)).pipe(Effect.ensuring(Effect.die(programmerBug)))
      ]
      for (const [i, body] of bodies.entries()) {
        const exit = yield* Effect.exit(runMutation(env(`m${i}`, BigInt(i + 1)), body))
        assert.isTrue(diedWithBug(exit), `ordering ${i}`)
      }
      assert.deepStrictEqual(yield* ledgerIds, [])
      assert.strictEqual(yield* counterBody, "")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a rejection combined with a defect is not ledgered as an ordinary rejection", () =>
    Effect.gen(function* () {
      const body = Effect.andThen(appendEffect("x"), reject("no")).pipe(Effect.ensuring(Effect.die(programmerBug)))
      const exit = yield* Effect.exit(runMutation(env("m", 1n), body))
      assert.isTrue(diedWithBug(exit))
      assert.deepStrictEqual(yield* ledgerIds, [])
      assert.strictEqual(yield* counterBody, "")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("seq < 1 is a malformed envelope: not executed, not recorded, not answered as clientExpired", () =>
    Effect.gen(function* () {
      for (const seq of [0n, -1n]) {
        const failure = yield* Effect.flip(runMutation(env(`bad${seq}`, seq), appendEffect("x")))
        assert.strictEqual(failure._tag, "MalformedEnvelope")
      }
      // One malformed item rejects the whole push before anything runs.
      const batch = yield* Effect.flip(
        runBatch("p", "c1", [
          { mutationId: "a", seq: 1n, body: appendEffect("a") },
          { mutationId: "z", seq: 0n, body: appendEffect("z") }
        ])
      )
      assert.strictEqual(batch._tag, "MalformedEnvelope")
      assert.deepStrictEqual(yield* ledgerIds, [])
      assert.strictEqual(yield* counterBody, "")
      // seq 1 is above the default floor 0 and is accepted.
      const ok = yield* runMutation(env("a", 1n), appendEffect("a"))
      assert.strictEqual(ok.outcome._tag, "Applied")
      assert.strictEqual(yield* counterBody, "a")
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "a conflict combined with a rejection is not retried and not ledgered (retry must not pick one failure)",
    () =>
      Effect.gen(function* () {
        let attempts = 0
        // First attempt: a real SQLSTATE 40001 and a MutationRejected in one cause. Second would succeed.
        const body = Effect.gen(function* () {
          attempts++
          if (attempts > 1) return yield* appendEffect("x")
          const conflict = yield* Effect.orDie(Effect.flip(serializationFailure))
          return yield* Effect.failCause(
            Cause.fromReasons<SqlError | MutationRejected>([
              Cause.makeFailReason(conflict),
              Cause.makeFailReason(new MutationRejected({ reason: "domain", message: "no" }))
            ])
          )
        })
        const exit = yield* Effect.exit(runMutation(env("m", 1n), body, fastRetry))
        assert.strictEqual(attempts, 1)
        assert.isTrue(Exit.isFailure(exit) && Cause.hasDies(exit.cause) && !exit.cause.reasons.some(Cause.isFailReason))
        assert.deepStrictEqual(yield* ledgerIds, [])
        assert.strictEqual(yield* counterBody, "")
      }).pipe(Effect.provide(TestDatabase))
  )
})
