/**
 * Tab ↔ leader contract. Every request a tab makes against the local replica/outbox goes through
 * this `RpcGroup` over a `MessagePort` brokered by the router (see `docs/browser-substrate.md`).
 * Payloads cross a structured-clone boundary, so every success/error is a Schema.
 */
import * as Schema from "effect/Schema"
import { Rpc, RpcGroup } from "effect/rpc"
import { AccessScope, Errors, Ids, Mutation, Protocol, Query } from "echo-sync"
import { LocallyRejected } from "../sync/Engine.ts"
import { Notice } from "../sync/SqliteSyncStorage.ts"
import { SyncStatus } from "../sync/SyncLoop.ts"

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
 * `exec` on a leader started without `allowRawExec` (DESIGN §10.1): raw writes bypass the outbox and
 * break the replica invariant, so only the dev harness enables it.
 */
export class RawExecDisabled extends Schema.TaggedError<RawExecDisabled>()("RawExecDisabled", {}) {}

/** This leader was started without a `syncUrl`: a local-only replica, nothing syncs. */
export class SyncDisabled extends Schema.TaggedError<SyncDisabled>()("SyncDisabled", {}) {}

/** This tab already has an open `presence` stream for the room (one entry per tab per room, DESIGN §12). */
export class PresenceAlreadyOpen extends Schema.TaggedError<PresenceAlreadyOpen>()("PresenceAlreadyOpen", {
  room: Schema.String
}) {}

/** A tab's presence in one room (DESIGN §9.1): the tab id is the server's entry key with the clientId. */
export const PresencePayload = Schema.Struct({
  room: AccessScope.AccessScopeId,
  tabId: Protocol.TabId,
  state: Schema.Json
})

/**
 * What a tab hands to `enqueue`. `mutationId` is the idempotency key chosen by the caller (unique per
 * client); `name` is a mutation in the leader's registry and `args` its encoded args. The leader runs the
 * handler optimistically, stamps the client id and the local sequence number.
 */
export const OutboxEnvelope = Schema.Struct({
  mutationId: Ids.MutationId,
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
 * A raw live query, for the dev harness only (served only with `allowRawExec`, like `exec`): a fixed list
 * of read statements plus the tables they depend on (table-level dependencies). The leader runs it with
 * `PRAGMA query_only` on, which catches accidental writes but is no security boundary: caller SQL can
 * switch it off. Registered queries (`subscribe`) are the read path for apps (DESIGN §10.1).
 */
export const LiveQuery = Schema.Struct({
  statements: Schema.Array(Statement),
  tables: Schema.Array(Schema.String)
})
export type LiveQuery = typeof LiveQuery.Type

/** One emission of a live query: `results[i]` are the rows of `statements[i]`, all read in one transaction. */
export const QueryResult = Schema.Struct({ results: Schema.Array(Rows) })
export type QueryResult = typeof QueryResult.Type

/** One pending outbox entry, as exported for recovery (DESIGN §9). */
export const PendingExport = Schema.Struct({ envelope: Mutation.MutationEnvelope, createdAt: Schema.Int })
export type PendingExport = typeof PendingExport.Type

export const StoredNotice = Schema.Struct({ id: Schema.Int, notice: Notice })
export type StoredNotice = typeof StoredNotice.Type

export const LeaderInfo = Schema.Struct({
  leaderId: Schema.String,
  clientId: Schema.String,
  dbName: Schema.String,
  /** Whether this leader runs a sync loop (it was given a `syncUrl`). */
  syncing: Schema.Boolean
})
export type LeaderInfo = typeof LeaderInfo.Type

export class LeaderRpcs extends RpcGroup.make(
  /** Raw statement execution for the dev harness and diagnostics. */
  /** Raw statement execution for the dev harness; `RawExecDisabled` unless the leader allows it. */
  Rpc.make("exec", {
    payload: { sql: Schema.String, params: SqlParams },
    success: Rows,
    error: Schema.Union([SqlFailure, RawExecDisabled])
  }),
  /**
   * Run a mutation optimistically and durably append it to the outbox, in one SQLite transaction
   * (DESIGN §9). Resolves only after that transaction committed. A known `mutationId` (pending or
   * released) returns its original seq and runs nothing. `LocallyRejected`: the handler rejected it
   * against the local replica; nothing was saved.
   */
  Rpc.make("enqueue", {
    payload: { envelope: OutboxEnvelope },
    success: Enqueued,
    error: Schema.Union([SqlFailure, LocallyRejected])
  }),
  /**
   * A registered query (DESIGN §10.1), by name with its encoded args. Emits the encoded result of a run,
   * then of a fresh run after every write to a table the latest run read. Each run, and the dependency
   * set derived from it, comes from one read transaction.
   */
  Rpc.make("subscribe", {
    payload: { name: Schema.String, args: Schema.Json },
    success: Schema.Json,
    error: Schema.Union([
      SqlFailure,
      Query.UnknownQuery,
      Query.QueryArgsDecodeFailed,
      Errors.RowNotFound,
      Errors.ScopeForbidden,
      Errors.InvalidRow
    ]),
    stream: true
  }),
  /**
   * Raw live query: emits the current result, then a fresh result after every write to `tables`.
   * `RawExecDisabled` unless the leader allows raw SQL (DESIGN §10.1).
   */
  Rpc.make("subscribeRaw", {
    payload: { query: LiveQuery },
    success: QueryResult,
    error: Schema.Union([SqlFailure, RawExecDisabled]),
    stream: true
  }),
  /** Identity of the serving leader (diagnostics / tests). */
  Rpc.make("leaderInfo", { success: LeaderInfo }),
  /** The sync loop's status: the current value, then every change (push halted, last transient error). */
  Rpc.make("syncStatus", { success: SyncStatus, error: SyncDisabled, stream: true }),
  /** Wake the sync loop (coalesced); completes once the wake is queued, never clears a halted push. */
  Rpc.make("requestSync", { error: SyncDisabled }),
  /**
   * Join `room` as this tab and observe it (DESIGN §9.1 Presence): room snapshots only (heartbeats stay on
   * the leader). The leader keeps the upstream stream up (idle timeout, rejoin with the latest tracked
   * state); the entry lives exactly while this stream is open. `Unauthorized`: no grant in the room's scope.
   */
  Rpc.make("presence", {
    payload: PresencePayload,
    success: Protocol.RoomSnapshot,
    error: Schema.Union([SyncDisabled, Protocol.Unauthorized]),
    stream: true
  }),
  /** Replace this tab's state in `room`; a no-op unless its `presence` stream is open on this leader. */
  Rpc.make("track", { payload: PresencePayload, error: SyncDisabled }),
  /**
   * Retained notices (rejected / outcome-unknown mutations), oldest first: the current list, then the
   * list after every change to `echo_notices`. The engine never removes one; only `dismissNotices` does.
   */
  Rpc.make("notices", { success: Schema.Array(StoredNotice), error: SqlFailure, stream: true }),
  /** Delete the notices with these ids (DESIGN §9.1). Unknown ids are ignored, so a retry is harmless. */
  Rpc.make("dismissNotices", { payload: { ids: Schema.Array(Schema.Int) }, error: SqlFailure }),
  /** Every pending outbox entry in seq order. Always available, also while push is halted (DESIGN §6). */
  Rpc.make("exportPending", { success: Schema.Array(PendingExport), error: SqlFailure })
) {}
