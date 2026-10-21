/**
 * Tab-side handle to the replica (DESIGN §9). Spawns this tab's leader candidate, finds the current
 * leader through the router SharedWorker, and talks to it with Effect RPC.
 *
 * Handover semantics (see docs/browser-substrate.md):
 * - Leader loss is detected through the leader's epoch Web Lock, never by waiting on a dead port.
 * - In-flight requests fail with `LeaderUnavailable { reason: "LeaderLost" }`. The request may or may
 *   not have been applied before the leader died; `enqueue` is idempotent on `mutationId`, so retry it.
 * - Requests issued while no leader is connected wait for the next one.
 * - Open `subscribe` streams terminate with `LeaderUnavailable { reason: "LeaderLost" }`. They do not
 *   resume silently: the caller re-subscribes and receives a fresh full result from the new leader.
 */
import * as BrowserWorker from "@effect/platform-browser/BrowserWorker"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { RpcClient } from "effect/rpc"
import { RpcClientError } from "effect/rpc/RpcClientError"
import type { LocallyRejected } from "../sync/Engine.ts"
import type { SyncStatus } from "../sync/SyncLoop.ts"
import {
  type Enqueued,
  type LeaderInfo,
  LeaderRpcs,
  type PendingExport,
  type StoredNotice,
  type SyncDisabled,
  LeaderUnavailable,
  type LiveQuery,
  type OutboxEnvelope,
  type QueryResult,
  type RawExecDisabled,
  type Rows,
  type SqlFailure,
  type SqlParams
} from "./LeaderRpcs.ts"
import { isFromLeader, isFromRouter, type ToLeader, type ToRouter } from "./messages.ts"
import * as Platform from "./platform.ts"

export interface LeaderClientConfig {
  readonly appId: string
  readonly profile: string
  /** The principal this replica acts as (DESIGN §4). One profile belongs to one principal. */
  readonly principal: string
  /** e.g. `(name) => new Worker(new URL("./leader.worker.ts", import.meta.url), { type: "module", name })` */
  readonly spawnLeader: (name: string) => Worker
  /** e.g. `(name) => new SharedWorker(new URL("./router.worker.ts", import.meta.url), { type: "module", name })` */
  readonly spawnRouter: (name: string) => SharedWorker
}

/**
 * - `shared-worker`: every tab reaches the single leader through the router.
 * - `single-tab`: no SharedWorker; only the tab whose own worker is leader is served, every other tab
 *   fails closed with `SecondaryTabWithoutRouter` (never a second database owner).
 * - `unsupported`: a required capability is missing; every request fails with `Unsupported`.
 */
export type Mode = "shared-worker" | "single-tab" | "unsupported"

export interface Status {
  readonly mode: Mode
  readonly tabId: string
  /** Whether this tab's own worker currently is the leader. */
  readonly isLeaderTab: boolean
  readonly leaderId: string | undefined
}

export class LeaderClient extends Context.Service<
  LeaderClient,
  {
    /** Raw SQL for the dev harness; `RawExecDisabled` unless the leader was started with `allowRawExec`. */
    readonly exec: (
      sql: string,
      params?: SqlParams
    ) => Effect.Effect<Rows, SqlFailure | RawExecDisabled | LeaderUnavailable>
    /** Runs the mutation optimistically; resolves only after the outbox entry is committed in SQLite. */
    readonly enqueue: (
      envelope: OutboxEnvelope
    ) => Effect.Effect<Enqueued, SqlFailure | LocallyRejected | LeaderUnavailable>
    /** Raw live query for the dev harness; `RawExecDisabled` unless the leader allows raw SQL. */
    readonly subscribe: (
      query: LiveQuery
    ) => Stream.Stream<QueryResult, SqlFailure | RawExecDisabled | LeaderUnavailable>
    readonly leaderInfo: Effect.Effect<LeaderInfo, LeaderUnavailable>
    /** The leader's sync status; ends with `LeaderLost` on handover (re-subscribe, like `subscribe`). */
    readonly syncStatus: Stream.Stream<SyncStatus, SyncDisabled | LeaderUnavailable>
    readonly notices: Effect.Effect<ReadonlyArray<StoredNotice>, SqlFailure | LeaderUnavailable>
    /** Pending outbox entries for export (DESIGN §9); available while push is halted. */
    readonly exportPending: Effect.Effect<ReadonlyArray<PendingExport>, SqlFailure | LeaderUnavailable>
    readonly status: Effect.Effect<Status>
  }
>()("echo-sync/client/LeaderClient") {}

const makeRpcClient = RpcClient.make(LeaderRpcs)
type Client = Effect.Success<typeof makeRpcClient>

type Connection =
  | {
      readonly _tag: "Connected"
      readonly leaderId: string
      readonly client: Client
      readonly lost: Deferred.Deferred<void>
    }
  | { readonly _tag: "Unavailable"; readonly error: LeaderUnavailable }

const isRpcClientError = (u: unknown): u is RpcClientError => u instanceof RpcClientError

const lostError = (leaderId: string, detail?: string) =>
  new LeaderUnavailable({
    reason: "LeaderLost",
    message: `leader ${leaderId} went away${detail === undefined ? "" : `: ${detail}`}`
  })

export const make = (config: LeaderClientConfig): Effect.Effect<LeaderClient["Service"], never, Scope.Scope> =>
  Effect.gen(function* () {
    const identity: Platform.ReplicaIdentity = { appId: config.appId, profile: config.profile }
    const capability = Platform.tabCapability()
    const unsupported = Platform.validateIdentity(identity) ?? capability.unsupported
    const mode: Mode =
      unsupported !== undefined ? "unsupported" : capability.sharedWorker ? "shared-worker" : "single-tab"
    const tabId = crypto.randomUUID()
    const scope = yield* Effect.scope
    const fork = <A, E>(effect: Effect.Effect<A, E>) => Fiber.runIn(Effect.runFork(effect), scope)

    // `current` is pending while no leader is connected; requests wait on it.
    let current = Deferred.makeUnsafe<Connection>()
    interface Active {
      readonly leaderId: string
      readonly scope: Scope.Closeable
      readonly lost: Deferred.Deferred<void>
    }
    // Async work started for a connection only acts while that exact connection object is `active`.
    let active: Active | undefined
    let isLeaderTab = false
    const publish = (connection: Connection | undefined) => {
      if (Deferred.isDoneUnsafe(current)) current = Deferred.makeUnsafe()
      if (connection !== undefined) Deferred.doneUnsafe(current, Effect.succeed(connection))
    }

    const withLeader = <A, E>(
      f: (client: Client) => Effect.Effect<A, E | RpcClientError>
    ): Effect.Effect<A, Exclude<E, RpcClientError> | LeaderUnavailable> =>
      Effect.suspend(() => Deferred.await(current)).pipe(
        Effect.flatMap((connection) =>
          connection._tag === "Unavailable"
            ? Effect.fail(connection.error)
            : f(connection.client).pipe(
                Effect.catchIf(isRpcClientError, (e) => Effect.fail(lostError(connection.leaderId, e.message))),
                Effect.raceFirst(
                  Deferred.await(connection.lost).pipe(Effect.andThen(Effect.fail(lostError(connection.leaderId))))
                )
              )
        )
      )

    /** A stream from the current leader that ends with `LeaderLost` when that leader goes away. */
    const streamWithLeader = <A, E>(
      f: (client: Client) => Stream.Stream<A, E | RpcClientError>
    ): Stream.Stream<A, Exclude<E, RpcClientError> | LeaderUnavailable> =>
      Stream.unwrap(
        Effect.suspend(() => Deferred.await(current)).pipe(
          Effect.map((connection) =>
            connection._tag === "Unavailable"
              ? Stream.fail(connection.error)
              : f(connection.client).pipe(
                  Stream.catchIf(isRpcClientError, (e) => Stream.fail(lostError(connection.leaderId, e.message))),
                  Stream.interruptWhen(
                    Deferred.await(connection.lost).pipe(Effect.andThen(Effect.fail(lostError(connection.leaderId))))
                  )
                )
          )
        )
      )

    const service = LeaderClient.of({
      exec: (sql, params = []) => withLeader((client) => client.exec({ sql, params })),
      enqueue: (envelope) => withLeader((client) => client.enqueue({ envelope })),
      leaderInfo: withLeader((client) => client.leaderInfo()),
      subscribe: (query) => streamWithLeader((client) => client.subscribeRaw({ query })),
      syncStatus: streamWithLeader((client) => client.syncStatus()),
      notices: withLeader((client) => client.notices()),
      exportPending: withLeader((client) => client.exportPending()),
      status: Effect.sync(() => ({ mode, tabId, isLeaderTab, leaderId: active?.leaderId }))
    })

    if (unsupported !== undefined) {
      publish({ _tag: "Unavailable", error: new LeaderUnavailable({ reason: "Unsupported", message: unsupported }) })
      return service
    }

    // Held for the tab's lifetime: the leader and the router drop this tab's port when it is released.
    const tabLock = yield* Effect.exit(Platform.holdLock(Platform.tabLockName(identity, tabId)))
    if (Exit.isFailure(tabLock)) {
      publish({
        _tag: "Unavailable",
        error: new LeaderUnavailable({ reason: "Unsupported", message: `tab lock: ${String(tabLock.cause)}` })
      })
      return service
    }
    void navigator.storage.persist().catch(() => false)

    /**
     * Fail everything in flight on `connection` with a typed `LeaderLost` first, then tear down its RPC
     * client: closing the scope first would surface as interruption instead.
     */
    const displace = (connection: Active) => {
      Deferred.doneUnsafe(connection.lost, Effect.void)
      fork(Effect.andThen(Effect.yieldNow, Scope.close(connection.scope, Exit.void)))
    }

    const connect = (leaderId: string, port: MessagePort) => {
      if (active !== undefined) displace(active)
      const connection: Active = { leaderId, scope: Scope.forkUnsafe(scope), lost: Deferred.makeUnsafe<void>() }
      active = connection
      // Requests wait for this connection's handshake rather than using the displaced one.
      publish(undefined)
      fork(
        Platform.awaitLockReleased(Platform.leaderEpochLockName(identity, leaderId)).pipe(
          Effect.andThen(
            Effect.sync(() => {
              if (active === connection) {
                active = undefined
                publish(undefined)
              }
              displace(connection)
            })
          ),
          Effect.catchCause((cause) => Effect.logError("echo: watching leader epoch failed", cause))
        )
      )
      fork(
        Effect.gen(function* () {
          const protocol = yield* Layer.buildWithScope(
            // One port, multiplexed: a request holds its pool lease until its Exit, so the default per-item
            // concurrency of 1 would let one open `subscribe` stream block every other request.
            RpcClient.layerProtocolWorker({ size: 1, concurrency: Number.MAX_SAFE_INTEGER }).pipe(
              Layer.provide(BrowserWorker.layer(() => port))
            ),
            connection.scope
          )
          const client = yield* makeRpcClient.pipe(Effect.provide(protocol), Scope.provide(connection.scope))
          if (active === connection) publish({ _tag: "Connected", leaderId, client, lost: connection.lost })
        }).pipe(
          // The handshake never completes if the leader died first.
          Effect.raceFirst(Deferred.await(connection.lost)),
          Effect.catchCause((cause) => Effect.logError("echo: connecting to leader failed", cause))
        )
      )
    }

    const router =
      mode === "shared-worker" ? config.spawnRouter(`echo:${config.appId}:${config.profile}:router`) : undefined
    const worker = config.spawnLeader(`echo:${config.appId}:${config.profile}:leader-candidate`)
    yield* Scope.addFinalizer(
      scope,
      Effect.sync(() => worker.terminate())
    )

    worker.addEventListener("message", (event) => {
      const message: unknown = event.data
      if (!isFromLeader(message)) return
      switch (message._tag) {
        case "Waiting": {
          if (router === undefined) {
            publish({
              _tag: "Unavailable",
              error: new LeaderUnavailable({
                reason: "SecondaryTabWithoutRouter",
                message: "Another tab owns the database and this browser has no SharedWorker to reach it"
              })
            })
          }
          return
        }
        case "Elected": {
          isLeaderTab = true
          const channel = new MessageChannel()
          if (router !== undefined) {
            const routerPort: ToLeader = { _tag: "RouterPort", port: channel.port1 }
            worker.postMessage(routerPort, { transfer: [channel.port1] })
            const leader: ToRouter = { _tag: "Leader", leaderId: message.leaderId, port: channel.port2 }
            router.port.postMessage(leader, { transfer: [channel.port2] })
          } else {
            const direct: ToLeader = { _tag: "Connect", tabId, port: channel.port1 }
            worker.postMessage(direct, { transfer: [channel.port1] })
            connect(message.leaderId, channel.port2)
          }
          return
        }
        case "Failed": {
          if (active === undefined) {
            publish({
              _tag: "Unavailable",
              error: new LeaderUnavailable({ reason: "LeaderFailed", message: message.message })
            })
          }
          return
        }
      }
    })

    if (router !== undefined) {
      router.port.addEventListener("message", (event) => {
        const message: unknown = event.data
        if (isFromRouter(message)) connect(message.leaderId, message.port)
      })
      router.port.start()
      const hello: ToRouter = { _tag: "Hello", appId: config.appId, profile: config.profile, tabId }
      router.port.postMessage(hello, { transfer: [] })
    }
    const init: ToLeader = { _tag: "Init", appId: config.appId, profile: config.profile, principal: config.principal }
    worker.postMessage(init, { transfer: [] })

    return service
  })

export const layer = (config: LeaderClientConfig): Layer.Layer<LeaderClient> => Layer.effect(LeaderClient)(make(config))
