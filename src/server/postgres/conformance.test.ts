import { assert, describe, it } from "@effect/vitest"
import { Cause, Effect, Exit } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { isSqlError, SqlError, UnknownError } from "effect/sql/SqlError"
import { Errors } from "echo-sync"
import * as Conformance from "../../core/testing/Conformance.ts"
import { claimScope } from "./rpc.ts"
import * as RowStore from "./rowStore.ts"
import { attach } from "./schema.ts"
import { pgAvailable, TestDatabase } from "./test-support.ts"

/**
 * A fresh, empty store per case: the suite's tables are dropped and recreated from their SyncTable
 * DDL and attached (so every write goes through the capture trigger and the engine lock, as in
 * production). The transaction is the substrate's: `withTransaction`, SQL failures as
 * `StoreUnavailable`.
 */
const subject = Effect.gen(function* () {
  const sql = yield* SqlClient
  for (const table of Conformance.tables) {
    yield* sql`DROP TABLE IF EXISTS ${sql(table.name)}`
    yield* sql.unsafe(table.postgres.createTable)
    for (const ddl of table.postgres.createIndexes) yield* sql.unsafe(ddl)
    yield* attach(table)
  }
  // The engine's scope_grants (range plans filter readable scopes through it) starts empty too.
  yield* sql`DELETE FROM echo_sync.scope_grants`
  for (const scope of Conformance.grantedScopes) {
    yield* sql`INSERT INTO echo_sync.access_scopes (id) VALUES (${scope}) ON CONFLICT DO NOTHING`
  }
  const rows = yield* RowStore.make(
    new Map([...Conformance.tables.map((t) => [t.name, "public"] as const), ["scope_grants", "echo_sync"] as const])
  )
  const made: Conformance.RowStoreSubject = {
    rows,
    claimScope: claimScope(sql),
    transaction: <A, E, R>(self: Effect.Effect<A, E, R>) =>
      sql.withTransaction(self).pipe(
        // Whole cause (Cause.map touches Fail reasons only): a rollback-cleanup defect next to the
        // body's failure must reach the suite, not be dropped as `mapError` would.
        Effect.catchCause((cause: Cause.Cause<E | SqlError>) =>
          Effect.failCause(
            Cause.map(cause, (error: E | SqlError): E | Errors.StoreUnavailable =>
              isSqlError(error) ? RowStore.unavailable(error) : error
            )
          )
        ),
        Effect.provideService(SqlClient, sql)
      )
  }
  return made
})

describe.skipIf(!pgAvailable)("Postgres RowStore conforms to Store.RowStore (SPEC §17)", () => {
  for (const check of Conformance.rowStore(subject)) {
    it.live(
      check.name,
      () => Effect.map(check.run, (cases) => assert.isAbove(cases, 0)).pipe(Effect.provide(TestDatabase)),
      120_000
    )
  }

  it.live("the subject's transaction keeps every reason of a mixed cause (a cleanup defect is never dropped)", () =>
    Effect.gen(function* () {
      const { transaction } = yield* subject
      const bug = new Error("rollback cleanup failed")
      const sqlError = new SqlError({ reason: new UnknownError({ cause: null, message: "conflict" }) })
      const exit = yield* Effect.exit(
        transaction(
          Effect.failCause(
            Cause.fromReasons<string | SqlError>([
              Cause.makeFailReason("abort"),
              Cause.makeFailReason(sqlError),
              Cause.makeDieReason(bug)
            ])
          )
        )
      )
      if (Exit.isSuccess(exit)) return assert.fail("transaction succeeded")
      const reasons = exit.cause.reasons.map((r) =>
        Cause.isFailReason(r)
          ? r.error === "abort"
            ? "fail:abort"
            : r.error instanceof Errors.StoreUnavailable
              ? "fail:StoreUnavailable"
              : "fail:other"
          : Cause.isDieReason(r) && r.defect === bug
            ? "die:bug"
            : "other"
      )
      assert.deepStrictEqual(reasons, ["fail:abort", "fail:StoreUnavailable", "die:bug"])
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )
})
