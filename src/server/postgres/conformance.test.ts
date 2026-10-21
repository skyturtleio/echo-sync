import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { isSqlError } from "effect/sql/SqlError"
import * as Conformance from "../../core/testing/Conformance.ts"
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
  const rows = yield* RowStore.make(new Map(Conformance.tables.map((t) => [t.name, "public"])))
  const made: Conformance.RowStoreSubject = {
    rows,
    transaction: (self) =>
      sql.withTransaction(self).pipe(
        Effect.mapError((error) => (isSqlError(error) ? RowStore.unavailable(error) : error)),
        Effect.provideService(SqlClient, sql)
      )
  }
  return made
})

describe.skipIf(!pgAvailable)("Postgres RowStore conforms to Store.RowStore (DESIGN §14)", () => {
  for (const check of Conformance.rowStore(subject)) {
    it.live(
      check.name,
      () => Effect.map(check.run, (cases) => assert.isAbove(cases, 0)).pipe(Effect.provide(TestDatabase)),
      120_000
    )
  }
})
