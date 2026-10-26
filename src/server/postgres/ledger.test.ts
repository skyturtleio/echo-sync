import { assert, describe, it } from "@effect/vitest"
import {
  Array as Arr,
  Cause,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Option,
  Order,
  Ref,
  Schedule,
  Schema
} from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { SqlError, UnknownError } from "effect/sql/SqlError"
import { AccessScope, Ids, Mutation, Registry, Store, SyncTable } from "echo-sync"
import { runBatch, runMutation } from "./ledger.ts"
import * as RowStore from "./rowStore.ts"
import { resolveTables } from "./schema.ts"
import { Items, Notes, pgAvailable, Replica, RowId, TestDatabase } from "./test-support.ts"

/**
 * A barrier for `n` concurrent attempts: each waits until all `n` have arrived once. Retried
 * attempts pass straight through, so a conflict retry is not blocked forever.
 */
const makeBarrier = (n: number) =>
  Effect.gen(function* () {
    const arrived = yield* Ref.make(0)
    const open = yield* Deferred.make<void>()
    // oxlint-disable-next-line effecttsgo/return-effect-in-gen -- Deliberately allocate once and return the inner Effect for reuse by concurrent participants.
    return Effect.gen(function* () {
      if ((yield* Ref.updateAndGet(arrived, (k) => k + 1)) >= n) yield* Deferred.succeed(open, undefined)
      yield* Deferred.await(open)
    })
  })

const asOwner = {
  actor: AccessScope.PrincipalId.make("owner"),
  roleFor: () => Effect.succeed(Option.some<AccessScope.Role>("owner")),
  claimScope: () => Effect.succeed(false),
  now: DateTime.makeUnsafe(0)
}
const attachedRowStore = (registry: Registry.SyncRegistry) =>
  Effect.flatMap(resolveTables(registry), (tables) =>
    RowStore.make(new Map(Arr.map(tables, (t) => [t.table.name, t.schema])))
  )

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
  return Arr.map(rows, (r) => r.mutation_id)
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
  Arr.some(exit.cause.reasons, (r) => Cause.isDieReason(r) && r.defect === programmerBug) &&
  !Arr.some(exit.cause.reasons, Cause.isFailReason)

const rejection = new Mutation.DomainError({ mutation: "test", error: "no" })

/** Fails with a real SQLSTATE 40001 plus `other` in one compound cause. */
const conflictAnd = (other: Cause.Reason<Mutation.DomainError>) =>
  Effect.flatMap(Effect.orDie(Effect.flip(serializationFailure)), (conflict) =>
    Effect.failCause(Cause.fromReasons<SqlError | Mutation.DomainError>([Cause.makeFailReason(conflict), other]))
  )

const env = (mutationId: string, seq: bigint) => ({ principalId: "p", clientId: "c1", mutationId, seq })
const reject = (message: string) => Effect.fail(new Mutation.DomainError({ mutation: "test", error: message }))

const constrainItems = Effect.gen(function* () {
  const sql = yield* SqlClient
  yield* sql`CREATE UNIQUE INDEX items_unique_body ON items (body)`
  yield* sql`ALTER TABLE items ADD CONSTRAINT body_not_bad CHECK (body <> 'bad')`
})

/** The app execution path: mutation decoding/classification around the typed Store. */
const storedBody = (mutation: Mutation.AnyMutation, args: Schema.Json) =>
  Effect.gen(function* () {
    const rows = yield* attachedRowStore(Registry.make([Items, Notes]))
    const { store } = Store.make(rows, asOwner)
    yield* mutation.execute(
      {
        actor: asOwner.actor,
        now: asOwner.now,
        random: Mutation.seededRandom(Ids.ClientId.make("c1"), Ids.MutationId.make("test")),
        store
      },
      args
    )
  })

describe.skipIf(!pgAvailable)("mutation ledger (SPEC §5)", () => {
  it.live("a persistent unique violation is ledgered with a position instead of blocking later mutations", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* constrainItems
      let attempts = 0
      const insert = Mutation.make("insert", {
        args: Schema.Struct({ id: RowId, accessScopeId: AccessScope.AccessScopeId, body: Schema.String }),
        handler: ({ store }, row) =>
          Effect.gen(function* () {
            if (row.id === "second") attempts++
            yield* store.insert(Items, row)
          })
      })
      const result = yield* runBatch(
        "p",
        "c1",
        [
          {
            mutationId: "first",
            seq: 1n,
            body: storedBody(insert, { id: "first", accessScopeId: "s1", body: "same" })
          },
          {
            mutationId: "duplicate",
            seq: 2n,
            body: storedBody(insert, { id: "second", accessScopeId: "s1", body: "same" })
          },
          {
            mutationId: "third",
            seq: 3n,
            body: storedBody(insert, { id: "counter", accessScopeId: "s1", body: "unrelated" })
          }
        ],
        fastRetry
      )
      assert.strictEqual(result.stopped, null)
      assert.deepStrictEqual(
        Arr.map(result.processed, (p) => p.outcome._tag),
        ["Applied", "Rejected", "Applied"]
      )
      assert.strictEqual(attempts, 3, "a unique violation gets the bounded conflict retry")
      const second = result.processed[1]
      assert.isDefined(second)
      if (second?.outcome._tag !== "Rejected") return yield* Effect.die("missing rejection")
      assert.strictEqual(second.outcome.error._tag, "ConstraintViolated")
      if (second.outcome.error._tag !== "ConstraintViolated") return yield* Effect.die("wrong rejection")
      assert.strictEqual(second.outcome.error.kind, "unique")
      assert.strictEqual(second.outcome.error.constraint, "items_unique_body")
      assert.isAbove(second.outcome.error.message.length, 0)
      assert.isTrue(second.position > (result.processed[0]?.position ?? second.position))
      assert.isTrue(second.position < (result.processed[2]?.position ?? second.position))
      assert.deepStrictEqual(yield* ledgerIds, ["first", "duplicate", "third"])
      assert.strictEqual(yield* counterBody, "unrelated")
      assert.deepStrictEqual(yield* sql`SELECT id FROM items WHERE id = 'second'`, [])
      // Re-reading the ledger also exercises the JSON wire codec for the new rejection.
      assert.deepStrictEqual(yield* runMutation(env("duplicate", 2n), appendEffect("not run")), {
        outcome: second.outcome,
        position: second.position
      })
      // Raw SQL bodies retain the same safety net as the Store path.
      const raw = yield* runMutation(
        env("raw", 4n),
        sql`INSERT INTO items (id, access_scope_id, body) VALUES ('raw', 's1', 'same')`.pipe(Effect.asVoid),
        fastRetry
      )
      assert.deepStrictEqual(raw.outcome, second.outcome)
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a check constraint is rejected immediately without retries or partial writes", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* constrainItems
      yield* sql`INSERT INTO items (id, access_scope_id, body) VALUES ('target', 's1', 'good')`
      let attempts = 0
      const update = Mutation.make("check", {
        args: Schema.Struct({}),
        handler: ({ store }) =>
          Effect.gen(function* () {
            attempts++
            yield* store.insert(Items, {
              id: RowId.make("counter"),
              accessScopeId: AccessScope.AccessScopeId.make("s1"),
              body: "partial"
            })
            yield* store.patch(Items, RowId.make("target"), { body: "bad" })
          })
      })
      const body = storedBody(update, {})
      const result = yield* runBatch("p", "c1", [{ mutationId: "check", seq: 1n, body }], fastRetry)
      assert.strictEqual(result.stopped, null)
      assert.strictEqual(attempts, 1)
      const rejected = result.processed[0]
      if (rejected?.outcome._tag !== "Rejected") return yield* Effect.die("missing rejection")
      assert.strictEqual(rejected.outcome.error._tag, "ConstraintViolated")
      if (rejected.outcome.error._tag !== "ConstraintViolated") return yield* Effect.die("wrong rejection")
      assert.strictEqual(rejected.outcome.error.kind, "other")
      assert.strictEqual(rejected.outcome.error.constraint, null)
      assert.isAbove(rejected.outcome.error.message.length, 0)
      assert.isTrue(rejected.position > 0n)
      assert.strictEqual(yield* counterBody, "")
      assert.deepStrictEqual(yield* sql`SELECT body FROM items WHERE id = 'target'`, [{ body: "good" }])
      assert.deepStrictEqual(yield* ledgerIds, ["check"])
      assert.deepStrictEqual(yield* runMutation(env("check", 1n), Effect.void), {
        outcome: rejected.outcome,
        position: rejected.position
      })
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a unique violation from a stale snapshot retries to Applied instead of stopping the batch transient", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* constrainItems
      const readAbsent = yield* Deferred.make<void>()
      const competitorCommitted = yield* Deferred.make<void>()
      let attempts = 0
      let uniqueFailures = 0
      const insertIfAbsent = Mutation.make("insertIfAbsent", {
        args: Schema.Struct({}),
        handler: ({ store }) =>
          Effect.gen(function* () {
            attempts++
            const existing = yield* store.scan(Items)
            if (!Arr.some(existing, (row) => row.body === "shared")) {
              yield* Deferred.succeed(readAbsent, undefined)
              yield* Deferred.await(competitorCommitted)
              yield* store.insert(Items, {
                id: RowId.make("mutation"),
                accessScopeId: AccessScope.AccessScopeId.make("s1"),
                body: "shared"
              })
            }
          }).pipe(
            Effect.catchTag("ConstraintViolated", (error) => {
              if (error.kind === "unique") uniqueFailures++
              return Effect.fail(error)
            })
          )
      })
      const body = storedBody(insertIfAbsent, {})
      const mutation = yield* Effect.forkScoped(
        runBatch("p", "c1", [{ mutationId: "racing", seq: 1n, body }], fastRetry)
      )
      yield* Deferred.await(readAbsent)
      yield* sql.withTransaction(
        sql`INSERT INTO items (id, access_scope_id, body) VALUES ('competitor', 's1', 'shared')`
      )
      yield* Deferred.succeed(competitorCommitted, undefined)
      const result = yield* Fiber.join(mutation)
      assert.strictEqual(result.stopped, null)
      assert.strictEqual(attempts, 2)
      assert.strictEqual(uniqueFailures, 1, "the first attempt must actually hit 23505, not 40001")
      assert.deepStrictEqual(
        Arr.map(result.processed, (p) => p.outcome),
        [{ _tag: "Applied" }]
      )
      assert.isTrue((result.processed[0]?.position ?? 0n) > 0n)
      assert.deepStrictEqual(yield* ledgerIds, ["racing"])
      assert.deepStrictEqual(yield* sql`SELECT id, body FROM items`, [{ id: "competitor", body: "shared" }])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live("a resubmitted envelope (lost response) returns the recorded outcome and position without re-executing", () =>
    Effect.gen(function* () {
      const first = yield* runMutation(env("6f1c…uuid", 1n), appendEffect("x"))
      // The client never saw `first` and retries the same identity, possibly with a body that
      // would now behave differently: it must not run.
      const second = yield* runMutation(env("6f1c…uuid", 1n), reject("no"))
      assert.deepStrictEqual(second, first)
      assert.strictEqual(yield* counterBody, "x")
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
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
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("concurrent duplicates from an already-registered client commit one effect and one ledger row", () =>
    Effect.gen(function* () {
      // The client row exists before the race, so every duplicate goes straight to the row lock.
      yield* runMutation(env("first", 1n), Effect.void)
      const body = Effect.andThen(appendEffect("x"), Effect.sleep("20 millis"))
      const results = yield* Effect.forEach(Array.from({ length: 8 }), () => runMutation(env("m", 2n), body), {
        concurrency: "unbounded"
      })
      for (const result of results) assert.deepStrictEqual(result, results[0])
      // Committed effect, not handler invocations: an SSI retry may re-run a body that rolls back.
      assert.strictEqual(yield* counterBody, "x")
      assert.deepStrictEqual(yield* ledgerIds, ["first", "m"])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a rejected mutation records its outcome and leaves none of its writes behind", () =>
    Effect.gen(function* () {
      const rejected = yield* runMutation(
        env("m", 1n),
        Effect.andThen(appendEffect("x"), Effect.fail(new Mutation.DomainError({ mutation: "test", error: "nope" })))
      )
      assert.deepStrictEqual(rejected.outcome, {
        _tag: "Rejected",
        error: new Mutation.DomainError({ mutation: "test", error: "nope" })
      })
      assert.strictEqual(yield* counterBody, "")
      // Recorded: a retry returns the same rejection without running the (now succeeding) body.
      assert.deepStrictEqual(yield* runMutation(env("m", 1n), appendEffect("y")), rejected)
      assert.strictEqual(yield* counterBody, "")
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
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
        Arr.map(first.processed, (p) => [p.mutationId, p.outcome._tag]),
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
        Arr.map(second.processed, (p) => p.outcome._tag),
        ["Applied", "Rejected", "Applied", "Applied"]
      )
      // Replayed outcomes carry their original positions; new ones come after them.
      assert.deepStrictEqual(second.processed.slice(0, 2), first.processed)
      const positions = Arr.map(second.processed, (p) => p.position)
      assert.isTrue(Arr.every(positions, (position, i) => i === 0 || (positions[i - 1] ?? position) < position))
      assert.strictEqual(yield* counterBody, "134")

      // Acks arrive by pull, in client order, each exactly once.
      const replica = new Replica("p", "c1")
      yield* replica.sync()
      assert.deepStrictEqual([...replica.acked], ["a", "b", "c", "d"])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
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
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
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
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live("runMutation inside a caller's transaction is refused (its outcome would not be committed)", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      const exit = yield* Effect.exit(sql.withTransaction(runMutation(env("m", 1n), appendEffect("x"))))
      assert.isTrue(Exit.hasDies(exit))
      assert.deepStrictEqual(yield* ledgerIds, [])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a mutation that writes nothing still gets an ack delivered by pull", () =>
    Effect.gen(function* () {
      const replica = new Replica("p", "c1")
      yield* replica.sync()
      yield* runMutation(env("m", 1n), Effect.void)
      yield* replica.sync()
      assert.deepStrictEqual([...replica.acked], ["m"])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a seq at or below the client's expiry floor is not executed and returns the expired position", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* runMutation(env("m1", 1n), Effect.void)
      // No v1 job raises the floor (SPEC §11); raise it by hand.
      yield* sql`UPDATE echo_sync.clients SET expired_through = 2, expired_position = 41 WHERE client_id = 'c1'`
      const expired = yield* runMutation(env("m2", 2n), appendEffect("x"))
      assert.deepStrictEqual(expired, {
        outcome: { _tag: "Rejected", error: new Mutation.ClientExpired({ mutationId: Ids.MutationId.make("m2") }) },
        position: 41n
      })
      assert.strictEqual(yield* counterBody, "")
      assert.deepStrictEqual(yield* ledgerIds, ["m1"])
      const next = yield* runMutation(env("m3", 3n), appendEffect("y"))
      assert.strictEqual(next.outcome._tag, "Applied")

      const replica = new Replica("p", "c1")
      const done = yield* replica.pullOnce(100)
      assert.deepStrictEqual(done._tag === "PullDone" && done.expiredThrough, Ids.SeqFloor.make(2))
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a client id registered to one principal is refused to another", () =>
    Effect.gen(function* () {
      yield* runMutation(env("m", 1n), Effect.void)
      const result = yield* Effect.flip(
        runMutation({ principalId: "mallory", clientId: "c1", mutationId: "m", seq: 1n }, Effect.void)
      )
      assert.strictEqual(result._tag, "ClientForbidden")
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
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
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a rejection combined with a defect is not ledgered as an ordinary rejection", () =>
    Effect.gen(function* () {
      const body = Effect.andThen(appendEffect("x"), reject("no")).pipe(Effect.ensuring(Effect.die(programmerBug)))
      const exit = yield* Effect.exit(runMutation(env("m", 1n), body))
      assert.isTrue(diedWithBug(exit))
      assert.deepStrictEqual(yield* ledgerIds, [])
      assert.strictEqual(yield* counterBody, "")
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
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
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "a conflict combined with a rejection is retried, and the retry's outcome is ledgered (transient beats rejection)",
    () =>
      Effect.gen(function* () {
        let attempts = 0
        const body = Effect.suspend(() => {
          attempts++
          return attempts > 1 ? appendEffect("x") : conflictAnd(Cause.makeFailReason(rejection))
        })
        const result = yield* runMutation(env("m", 1n), body, fastRetry)
        assert.strictEqual(attempts, 2)
        assert.deepStrictEqual(result.outcome, { _tag: "Applied" })
        assert.deepStrictEqual(yield* ledgerIds, ["m"])
        assert.strictEqual(yield* counterBody, "x")
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "a conflict combined with a rejection on every attempt stops the batch as transient with nothing ledgered",
    () =>
      Effect.gen(function* () {
        let attempts = 0
        const body = Effect.suspend(() => {
          attempts++
          return Effect.andThen(appendEffect("x"), conflictAnd(Cause.makeFailReason(rejection)))
        })
        const result = yield* runBatch(
          "p",
          "c1",
          [
            { mutationId: "m", seq: 1n, body },
            { mutationId: "n", seq: 2n, body: appendEffect("n") }
          ],
          fastRetry
        )
        assert.strictEqual(attempts, 3, "one attempt plus two retries")
        assert.deepStrictEqual(result.processed, [])
        assert.strictEqual(result.stopped?.mutationId, "m")
        assert.deepStrictEqual(yield* ledgerIds, [])
        assert.strictEqual(yield* counterBody, "")
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a conflict combined with a programmer defect dies without retry (defect beats transient)", () =>
    Effect.gen(function* () {
      let attempts = 0
      const body = Effect.suspend(() => {
        attempts++
        return conflictAnd(Cause.makeDieReason(programmerBug))
      })
      const exit = yield* Effect.exit(runMutation(env("m", 1n), body, fastRetry))
      assert.strictEqual(attempts, 1)
      assert.isTrue(diedWithBug(exit))
      assert.deepStrictEqual(yield* ledgerIds, [])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("two clients' concurrent read-modify-write mutations on one row both take effect (no lost update)", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* sql`INSERT INTO items (id, access_scope_id, body) VALUES ('n', 's1', '0')`
      const barrier = yield* makeBarrier(2)
      // Both read before either writes; the write is computed from the read.
      const increment = Effect.gen(function* () {
        const [row] = yield* sql<{ body: string }>`SELECT body FROM items WHERE id = 'n'`
        yield* barrier
        yield* sql`UPDATE items SET body = ${String(Number(row?.body ?? "0") + 1)} WHERE id = 'n'`
      })
      const results = yield* Effect.all(
        Arr.map(["c1", "c2"], (clientId) =>
          runMutation({ principalId: "p", clientId, mutationId: `inc-${clientId}`, seq: 1n }, increment)
        ),
        { concurrency: "unbounded" }
      )
      assert.deepStrictEqual(
        Arr.map(results, (r) => r.outcome._tag),
        ["Applied", "Applied"]
      )
      const [row] = yield* sql<{ body: string }>`SELECT body FROM items WHERE id = 'n'`
      assert.strictEqual(row?.body, "2")
      assert.strictEqual((yield* ledgerIds).length, 2)
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("two clients' concurrent inserts of one id through the Store: one applies, the other is rejected", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      const barrier = yield* makeBarrier(2)
      const insert = (clientId: string) =>
        Effect.gen(function* () {
          const rows = yield* attachedRowStore(Registry.make([Items, Notes]))
          yield* barrier
          const { store } = Store.make(rows, asOwner)
          yield* store.insert(Items, {
            id: RowId.make("x"),
            accessScopeId: AccessScope.AccessScopeId.make("s1"),
            body: clientId
          })
        })
      const results = yield* Effect.all(
        Arr.map(["c1", "c2"], (clientId) =>
          runMutation({ principalId: "p", clientId, mutationId: `ins-${clientId}`, seq: 1n }, insert(clientId))
        ),
        { concurrency: "unbounded" }
      )
      const outcomes = Arr.map(results, (r) => (r.outcome._tag === "Rejected" ? r.outcome.error._tag : r.outcome._tag))
      assert.deepStrictEqual(Arr.sort(outcomes, Order.String), ["Applied", "RowAlreadyExists"])
      const winner = results[0]?.outcome._tag === "Applied" ? "c1" : "c2"
      const rows = yield* sql<{ body: string }>`SELECT body FROM items WHERE id = 'x'`
      assert.deepStrictEqual(rows, [{ body: winner }])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a registry table without capture attached is never written: the mutation dies, nothing ledgered", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      const Unattached = SyncTable.make("unattached", {
        id: RowId,
        accessScopeId: AccessScope.AccessScopeId,
        body: Schema.String
      })
      yield* sql.unsafe(Unattached.postgres.createTable)
      const body = Effect.gen(function* () {
        const rows = yield* attachedRowStore(Registry.make([Items, Notes, Unattached]))
        yield* rows.write(Unattached, "u1", Option.some({ id: "u1", access_scope_id: "s1", body: "lost" }))
      })
      const exit = yield* Effect.exit(runMutation(env("m", 1n), body))
      assert.isTrue(Exit.hasDies(exit))
      assert.deepStrictEqual(yield* ledgerIds, [])
      assert.deepStrictEqual(yield* sql`SELECT id FROM unattached`, [])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )
})
