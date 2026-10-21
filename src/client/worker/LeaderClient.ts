/**
 * Tab-side handle to the replica (DESIGN §9). Spawns this tab's leader candidate, finds the current
 * leader through the router SharedWorker, and talks to it with Effect RPC.
 *
 * Handover semantics (see docs/browser-substrate.md):
 * - Leader loss is detected through the leader's epoch Web Lock, never by waiting on a dead port.
 * - In-flight requests fail with `LeaderUnavailable { reason: "LeaderLost" }`. The request may or may
 *   not have been applied before the leader died; `enqueue` is idempotent on `mutationId`, so retry it.
 * - Requests issued while no leader is connected wait for the next one.
 * - `mutate` fixes its `mutationId` before the first attempt and retries `LeaderLost` with it, so a
 *   mutation that committed on the lost leader lands on the §9 idempotency rule: one outbox row.
 * - `subscribe` (registered queries) and `notices` re-subscribe to the next leader on `LeaderLost`; the
 *   stream emits nothing in between, so a consumer keeps the last emission until a fresh one arrives.
 * - Raw `subscribeRaw` streams and `syncStatus` terminate with `LeaderUnavailable { reason: "LeaderLost" }`;
 *   the caller re-subscribes and receives a fresh full result from the new leader.
 */
import * as BrowserWorker from "@effect/platform-browser/BrowserWorker"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import { type Errors, Ids, type Mutation, type Query } from "echo-sync"
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
    /**
     * The one write API (DESIGN §9.1): encodes `args`, generates the `mutationId` once, and enqueues,
     * retrying `LeaderLost` only. Resolves after the leader committed the optimistic apply and the outbox
     * entry. `options.mutationId` is for replay and tests; apps never mint ids.
     */
    readonly mutate: MutateFn
    /**
     * The replay/test primitive under `mutate`: one attempt with a caller-chosen `mutationId` (replay of an
     * exported entry, tests; DESIGN §9.1). Apps use `mutate`; a UI adapter does not export this.
     * Resolves only after the outbox entry is committed in SQLite.
     */
    readonly enqueue: (
      envelope: OutboxEnvelope
    ) => Effect.Effect<Enqueued, SqlFailure | LocallyRejected | LeaderUnavailable>
    /** A registered query's live result, decoded with its `success` schema; survives handover. */
    readonly subscribe: SubscribeFn
    /**
     * The raw read escape hatch for the dev harness: declared tables, statements run under `query_only`;
     * `RawExecDisabled` unless the leader allows raw SQL. Ends on handover.
     */
    readonly subscribeRaw: (
      query: LiveQuery
    ) => Stream.Stream<QueryResult, SqlFailure | RawExecDisabled | LeaderUnavailable>
    readonly leaderInfo: Effect.Effect<LeaderInfo, LeaderUnavailable>
    /** The leader's sync status; ends with `LeaderLost` on handover (re-subscribe, like `subscribeRaw`). */
    readonly syncStatus: Stream.Stream<SyncStatus, SyncDisabled | LeaderUnavailable>
    /** Retained notices, current list then every change; survives handover. */
    readonly notices: Stream.Stream<ReadonlyArray<StoredNotice>, SqlFailure | LeaderUnavailable>
    /** The only way a notice is removed (DESIGN §9.1). Idempotent, so retried across `LeaderLost`. */
    readonly dismissNotices: (ids: ReadonlyArray<number>) => Effect.Effect<void, SqlFailure | LeaderUnavailable>
    /** Pending outbox entries for export (DESIGN §9); available while push is halted. */
    readonly exportPending: Effect.Effect<ReadonlyArray<PendingExport>, SqlFailure | LeaderUnavailable>
    /** This tab's view of the topology: the current value, then every change. */
    readonly status: Stream.Stream<Status>
  }
>()("echo-sync/client/LeaderClient") {}

type MutateFn = <A extends Mutation.PureSchema, E extends Mutation.PureSchema>(
  mutation: Mutation.Mutation<string, A, E>,
  args: A["Type"],
  options?: { readonly mutationId?: Ids.MutationId }
) => Effect.Effect<Enqueued, SqlFailure | LocallyRejected | LeaderUnavailable>

/** Errors a registered query's stream can end with (a run's `ReadError`s, minus the transient one). */
export type SubscribeError =
  | SqlFailure
  | LeaderUnavailable
  | Query.UnknownQuery
  | Query.QueryArgsDecodeFailed
  | Errors.RowNotFound
  | Errors.ScopeForbidden
  | Errors.InvalidRow

type SubscribeFn = <A extends Mutation.PureSchema, S extends Mutation.PureSchema>(
  query: Query.Query<string, A, S>,
  args: A["Type"]
) => Stream.Stream<S["Type"], SubscribeError>

const isLeaderLost = (e: unknown): e is LeaderUnavailable => e instanceof LeaderUnavailable && e.reason === "LeaderLost"

// No tight loop while the old connection is not yet marked lost: the next attempt waits for a new leader.
const lostBackoff = Schedule.spaced("50 millis")

/** Re-open `open` on the next leader whenever it ends with `LeaderLost`, emitting nothing in between. */
const resubscribing = <A, E>(
  open: Stream.Stream<A, E | LeaderUnavailable>
): Stream.Stream<A, E | LeaderUnavailable> => {
  const loop: Stream.Stream<A, E | LeaderUnavailable> = open.pipe(
    Stream.catchIf(isLeaderLost, () =>
      Stream.unwrap(
        Effect.as(
          Effect.sleep("50 millis"),
          Stream.suspend(() => loop)
        )
      )
    )
  )
  return loop
}

/** The leader RPCs the tab API is built on, each failing with `LeaderLost` when its leader goes away. */
export interface LeaderCalls {
  readonly enqueue: (
    envelope: OutboxEnvelope
  ) => Effect.Effect<Enqueued, SqlFailure | LocallyRejected | LeaderUnavailable>
  readonly subscribe: (name: string, args: Schema.Json) => Stream.Stream<Schema.Json, SubscribeError>
  readonly notices: Stream.Stream<ReadonlyArray<StoredNotice>, SqlFailure | LeaderUnavailable>
  readonly dismissNotices: (ids: ReadonlyArray<number>) => Effect.Effect<void, SqlFailure | LeaderUnavailable>
}

/**
 * The DESIGN §9.1 semantics over `calls`: id generation, `LeaderLost` retry and re-subscription, and the
 * tab-side encode/decode. Exported so they are tested against a real leader without a browser.
 */
export const tabApi = (calls: LeaderCalls) => {
  const mutate: MutateFn = (mutation, args, options) =>
    Effect.gen(function* () {
      // A bad value of the mutation's own `args` type is the caller's bug, not an outcome.
      const encoded: Schema.Json = yield* Effect.orDie(
        Schema.encodeUnknownEffect(Schema.toCodecJson(mutation.args))(args)
      )
      // Fixed once, before the first attempt: every retry presents the same idempotency key (DESIGN §9).
      const mutationId = options?.mutationId ?? Ids.MutationId.make(crypto.randomUUID())
      return yield* calls
        .enqueue({ mutationId, name: mutation.name, args: encoded })
        .pipe(Effect.retry({ while: isLeaderLost, schedule: lostBackoff }))
    })
  const subscribe: SubscribeFn = (query, args) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const encoded: Schema.Json = yield* Effect.orDie(
          Schema.encodeUnknownEffect(Schema.toCodecJson(query.args))(args)
        )
        const decode = Schema.decodeUnknownEffect(Schema.toCodecJson(query.success))
        // A leader result that does not decode is a version skew between tab and leader code: a defect.
        return resubscribing(calls.subscribe(query.name, encoded)).pipe(
          Stream.mapEffect((result) => Effect.orDie(decode(result)))
        )
      })
    )
  return {
    mutate,
    subscribe,
    notices: resubscribing(calls.notices),
    dismissNotices: (ids: ReadonlyArray<number>) =>
      calls.dismissNotices(ids).pipe(Effect.retry({ while: isLeaderLost, schedule: lostBackoff }))
  }
}

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
    /**
     * The epoch (leaderId) this tab's own worker was elected with, while that epoch lasts. Ownership ends
     * with the epoch, not with the tab: a worker can die while its tab survives and connects to another
     * tab's replacement leader.
     */
    let localEpoch: string | undefined
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

    const status = yield* SubscriptionRef.make<Status>({ mode, tabId, isLeaderTab: false, leaderId: undefined })
    // Called from DOM callbacks after every change to `active` or `localEpoch`; `set` never suspends here.
    const publishStatus = () =>
      Effect.runSync(
        SubscriptionRef.set(status, { mode, tabId, isLeaderTab: localEpoch !== undefined, leaderId: active?.leaderId })
      )

    const enqueue = (envelope: OutboxEnvelope) => withLeader((client) => client.enqueue({ envelope }))
    const service = LeaderClient.of({
      exec: (sql, params = []) => withLeader((client) => client.exec({ sql, params })),
      enqueue,
      ...tabApi({
        enqueue,
        subscribe: (name, args) => streamWithLeader((client) => client.subscribe({ name, args })),
        notices: streamWithLeader((client) => client.notices()),
        dismissNotices: (ids) => withLeader((client) => client.dismissNotices({ ids }))
      }),
      leaderInfo: withLeader((client) => client.leaderInfo()),
      subscribeRaw: (query) => streamWithLeader((client) => client.subscribeRaw({ query })),
      syncStatus: streamWithLeader((client) => client.syncStatus()),
      exportPending: withLeader((client) => client.exportPending()),
      status: SubscriptionRef.changes(status)
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
      publishStatus()
      // Requests wait for this connection's handshake rather than using the displaced one.
      publish(undefined)
      fork(
        Platform.awaitLockReleased(Platform.leaderEpochLockName(identity, leaderId)).pipe(
          Effect.andThen(
            Effect.sync(() => {
              if (active === connection) {
                active = undefined
                publishStatus()
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
          const epoch = message.leaderId
          localEpoch = epoch
          publishStatus()
          // The epoch lock is held by this worker until it dies, also when the tab outlives it.
          fork(
            Platform.awaitLockReleased(Platform.leaderEpochLockName(identity, epoch)).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  if (localEpoch !== epoch) return
                  localEpoch = undefined
                  publishStatus()
                })
              ),
              Effect.catchCause((cause) => Effect.logError("echo: watching this tab's leader epoch failed", cause))
            )
          )
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
