import { Effect, Stream } from "effect"
import * as SqlClient from "effect/sql/SqlClient"
import type { Connection } from "effect/sql/SqlConnection"
import type { Compiler } from "effect/sql/Statement"

/** Test-only, healthy-path client over a real reserved driver connection. Counts executions,
 * not statement construction, including transaction controls. No trigger-internal SQL or
 * physical rows scanned: `rows` counts rows returned by SELECT to the engine. */
export const countingClient = Effect.fnUntraced(function* (source: SqlClient.SqlClient, compiler: Compiler) {
  const connection = yield* source.reserve
  const statements: Array<{ sql: string; params: ReadonlyArray<unknown>; rows: number }> = []
  const track = <A, E, R>(sql: string, params: ReadonlyArray<unknown>, work: Effect.Effect<A, E, R>) =>
    Effect.suspend(() => {
      const statement = { sql, params, rows: 0 }
      statements.push(statement)
      return Effect.tap(work, (result) =>
        Effect.sync(() => {
          if (/^\s*SELECT\b/i.test(sql) && Array.isArray(result)) statement.rows = result.length
        })
      )
    })
  const counted: Connection = {
    execute: (sql, params, transform) => track(sql, params, connection.execute(sql, params, transform)),
    executeUnprepared: (sql, params, transform) =>
      track(sql, params, connection.executeUnprepared(sql, params, transform)),
    executeRaw: (sql, params) => track(sql, params, connection.executeRaw(sql, params)),
    executeValues: (sql, params) => track(sql, params, connection.executeValues(sql, params)),
    executeValuesUnprepared: (sql, params) => track(sql, params, connection.executeValuesUnprepared(sql, params)),
    executeStream: (sql, params, transform) =>
      Stream.unwrap(
        Effect.sync(() => {
          const statement = { sql, params, rows: 0 }
          statements.push(statement)
          return Stream.tap(connection.executeStream(sql, params, transform), () => Effect.sync(() => statement.rows++))
        })
      )
  }
  // Same compiler and success-path controls as pinned PgClient / SqliteClient. This facade
  // deliberately serves serial fixtures only, not pool/concurrency or commit-failure tests.
  const client = yield* SqlClient.make({
    acquirer: Effect.succeed(counted),
    compiler,
    spanAttributes: [],
    releaseSavepoint: (name) => `RELEASE SAVEPOINT ${name}`
  })
  return {
    client,
    statements,
    reset: () => {
      statements.length = 0
    }
  }
})
