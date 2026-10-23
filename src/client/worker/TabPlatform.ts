/**
 * What a tab client (`LeaderClient`) needs from the browser: capabilities, Web Locks, persistent storage
 * and an RPC connection to the leader over a `MessagePort`. The default is the browser itself; tests
 * provide their own, so lock grants and releases, the topology mode and the leader's answers are under
 * the test's control without replacing modules. Internal: not exported from `echo-sync/client`.
 */
import * as BrowserWorker from "@effect/platform-browser/BrowserWorker"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Scope from "effect/Scope"
import type * as Stream from "effect/Stream"
import type * as Schema from "effect/Schema"
import { RpcClient } from "effect/rpc"
import type { RpcClientError } from "effect/rpc/RpcClientError"
import type { WorkerError } from "effect/workers/WorkerError"
import type { Errors, Protocol, Query } from "echo-sync"
import type { LocallyRejected } from "../sync/Engine.ts"
import type { SyncStatus } from "../sync/SyncLoop.ts"
import * as LeaderRpcs from "./LeaderRpcs.ts"
import * as Platform from "./platform.ts"

type PresencePayload = typeof LeaderRpcs.PresencePayload.Type

/**
 * One leader's `LeaderRpcs` as the tab calls them; a transport failure is `RpcClientError`. The browser
 * default below assigns the real RPC client to this, so the compiler checks it against `LeaderRpcs`.
 */
export interface LeaderConnection {
  readonly exec: (input: {
    readonly sql: string
    readonly params: LeaderRpcs.SqlParams
  }) => Effect.Effect<LeaderRpcs.Rows, LeaderRpcs.SqlFailure | LeaderRpcs.RawExecDisabled | RpcClientError>
  readonly enqueue: (input: {
    readonly envelope: LeaderRpcs.OutboxEnvelope
  }) => Effect.Effect<LeaderRpcs.Enqueued, LeaderRpcs.SqlFailure | LocallyRejected | RpcClientError>
  readonly subscribe: (input: {
    readonly name: string
    readonly args: Schema.Json
  }) => Stream.Stream<
    Schema.Json,
    | LeaderRpcs.SqlFailure
    | Query.UnknownQuery
    | Query.QueryArgsDecodeFailed
    | Errors.RowNotFound
    | Errors.ScopeForbidden
    | Errors.InvalidRow
    | RpcClientError
  >
  readonly subscribeRaw: (input: {
    readonly query: LeaderRpcs.LiveQuery
  }) => Stream.Stream<LeaderRpcs.QueryResult, LeaderRpcs.SqlFailure | LeaderRpcs.RawExecDisabled | RpcClientError>
  readonly leaderInfo: () => Effect.Effect<LeaderRpcs.LeaderInfo, RpcClientError>
  readonly syncStatus: () => Stream.Stream<SyncStatus, LeaderRpcs.SyncDisabled | RpcClientError>
  readonly requestSync: () => Effect.Effect<void, LeaderRpcs.SyncDisabled | RpcClientError>
  readonly presence: (
    input: PresencePayload
  ) => Stream.Stream<Protocol.RoomSnapshot, LeaderRpcs.SyncDisabled | Protocol.Forbidden | RpcClientError>
  readonly track: (input: PresencePayload) => Effect.Effect<void, LeaderRpcs.SyncDisabled | RpcClientError>
  readonly notices: () => Stream.Stream<ReadonlyArray<LeaderRpcs.StoredNotice>, LeaderRpcs.SqlFailure | RpcClientError>
  readonly dismissNotices: (input: {
    readonly ids: ReadonlyArray<number>
  }) => Effect.Effect<void, LeaderRpcs.SqlFailure | RpcClientError>
  readonly exportPending: () => Effect.Effect<
    ReadonlyArray<LeaderRpcs.PendingExport>,
    LeaderRpcs.SqlFailure | RpcClientError
  >
}

export interface TabPlatform {
  readonly capability: () => { readonly unsupported: string | undefined; readonly sharedWorker: boolean }
  readonly holdLock: (name: string) => Effect.Effect<void, Platform.LockError, Scope.Scope>
  readonly isLockHeld: (name: string) => Effect.Effect<boolean, Platform.LockError>
  readonly awaitLockReleased: (name: string) => Effect.Effect<void, Platform.LockError>
  /** Best effort: ask the browser to keep this origin's storage (OPFS) under pressure. */
  readonly persistStorage: Effect.Effect<void>
  /** The RPC client to the leader on the other end of `port`, open for the enclosing scope. */
  readonly connect: (port: MessagePort) => Effect.Effect<LeaderConnection, WorkerError, Scope.Scope>
}

const browser: TabPlatform = {
  capability: Platform.tabCapability,
  holdLock: Platform.holdLock,
  isLockHeld: Platform.isLockHeld,
  awaitLockReleased: Platform.awaitLockReleased,
  persistStorage: Effect.sync(() => void navigator.storage.persist().catch(() => false)),
  connect: (port) =>
    Effect.gen(function* () {
      // Built in the caller's scope, so the protocol lives exactly as long as the connection.
      const protocol = yield* Layer.build(
        // One port, multiplexed: a request holds its pool lease until its Exit, so the default per-item
        // concurrency of 1 would let one open `subscribe` stream block every other request.
        RpcClient.layerProtocolWorker({ size: 1, concurrency: Number.MAX_SAFE_INTEGER }).pipe(
          Layer.provide(BrowserWorker.layer(() => port))
        )
      )
      return yield* RpcClient.make(LeaderRpcs.LeaderRpcs).pipe(Effect.provide(protocol))
    })
}

export const TabPlatform = Context.Reference<TabPlatform>("echo-sync/client/worker/TabPlatform", {
  defaultValue: () => browser
})
