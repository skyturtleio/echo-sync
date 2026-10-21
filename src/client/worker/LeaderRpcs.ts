/**
 * Tab ↔ leader contract. Every request a tab makes against the local replica/outbox goes through
 * this `RpcGroup` over a `MessagePort` brokered by the router (see `docs/browser-substrate.md`).
 * Payloads cross a structured-clone boundary, so every success/error is a Schema.
 */
import * as Schema from "effect/Schema"
import { Rpc, RpcGroup } from "effect/rpc"

/** A SQLite statement failed inside the leader. The transaction (if any) was rolled back. */
export class SqlFailure extends Schema.TaggedError<SqlFailure>()("SqlFailure", {
  message: Schema.String
}) {}

/**
 * The leader that was serving this request went away (tab closed / worker crashed) or no leader can be
 * reached. The request may or may not have been applied: `enqueue` is idempotent on `mutationId`, so it
 * is safe to retry; `exec` is only safe to retry if the statement is.
 */
export class LeaderUnavailable extends Schema.TaggedError<LeaderUnavailable>()("LeaderUnavailable", {
  reason: Schema.Literals(["LeaderLost", "LeaderFailed", "Unsupported", "SecondaryTabWithoutRouter"]),
  message: Schema.String
}) {}

export const SqlParams = Schema.Array(Schema.Union([Schema.String, Schema.Number, Schema.Null]))
export type SqlParams = typeof SqlParams.Type

export const Rows = Schema.Array(Schema.Record(Schema.String, Schema.Unknown))
export type Rows = typeof Rows.Type

/**
 * What a tab hands to `enqueue`. `mutationId` is the idempotency key chosen by the caller (unique per
 * client); `args` is the already-encoded mutation args. The leader stamps the client id and the local
 * sequence number.
 */
export const OutboxEnvelope = Schema.Struct({
  mutationId: Schema.String,
  name: Schema.String,
  args: Schema.Json
})
export type OutboxEnvelope = typeof OutboxEnvelope.Type

export const Enqueued = Schema.Struct({
  clientId: Schema.String,
  mutationId: Schema.String,
  seq: Schema.Number
})
export type Enqueued = typeof Enqueued.Type

export const Statement = Schema.Struct({ sql: Schema.String, params: SqlParams })
export type Statement = typeof Statement.Type

/**
 * A live query: a fixed list of read statements plus the tables they depend on (table-level
 * dependencies, DESIGN §10). Typed `Query.make` handlers will compile down to this shape.
 */
export const LiveQuery = Schema.Struct({
  statements: Schema.Array(Statement),
  tables: Schema.Array(Schema.String)
})
export type LiveQuery = typeof LiveQuery.Type

/** One emission of a live query: `results[i]` are the rows of `statements[i]`, all read in one transaction. */
export const QueryResult = Schema.Struct({ results: Schema.Array(Rows) })
export type QueryResult = typeof QueryResult.Type

export class LeaderRpcs extends RpcGroup.make(
  /** Raw statement execution for the dev harness and diagnostics. */
  Rpc.make("exec", {
    payload: { sql: Schema.String, params: SqlParams },
    success: Rows,
    error: SqlFailure
  }),
  /** Durably append an intent to the outbox. Resolves only after the SQLite transaction committed. */
  Rpc.make("enqueue", {
    payload: { envelope: OutboxEnvelope },
    success: Enqueued,
    error: SqlFailure
  }),
  /** Live query: emits the current result, then a fresh result after every write to `tables`. */
  Rpc.make("subscribe", {
    payload: { query: LiveQuery },
    success: QueryResult,
    error: SqlFailure,
    stream: true
  }),
  /** Identity of the serving leader (diagnostics / tests). */
  Rpc.make("leaderInfo", {
    success: Schema.Struct({ leaderId: Schema.String, clientId: Schema.String, dbName: Schema.String })
  })
) {}
