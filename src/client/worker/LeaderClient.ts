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
 * - `syncStatus` also re-subscribes on `LeaderLost` (DESIGN §9.1); `requestSync` retries it like `mutate`.
 * - `presence` re-joins the next leader on `LeaderLost` with this tab's latest tracked state; `track`
 *   updates that state (DESIGN §9.1 Presence). See `tabApi`.
 * - Raw `subscribeRaw` streams terminate with `LeaderUnavailable { reason: "LeaderLost" }`;
 *   the caller re-subscribes and receives a fresh full result from the new leader.
 *
 * Reset (DESIGN §16.24): `resetReplica`'s signal puts the client into a terminal state. Every request and
 * stream fails `LeaderUnavailable { reason: "Reset" }`, which is not a handover (never retried or
 * resubscribed); `status` reports `reset: true` and stays open. See `enterReset`.
 */
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as Telemetry from "../../internal/Telemetry.ts"
import { type AccessScope, type Errors, Ids, type Mutation, Protocol, type Query } from "echo-sync"
import type { LocallyRejected } from "../sync/Engine.ts"
import type { SyncStatus } from "../sync/SyncLoop.ts"
// Values, so apps get the runtime schemas (and their types) without RPC introspection (DESIGN §16.19).
export { SyncError, SyncStatus } from "../sync/SyncLoop.ts"
export { SyncErrorKind } from "../sync/SyncFailure.ts"
import {
  type Enqueued,
  type LeaderInfo,
  type PendingExport,
  PresenceAlreadyOpen,
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
import {
  isFromLeader,
  isFromRouter,
  isResetSignal,
  isRouterBroadcast,
  type ToLeader,
  type ToRouter
} from "./messages.ts"
import * as Platform from "./platform.ts"
import * as LeaderRoute from "./LeaderRoute.ts"
import { TabPlatform } from "./TabPlatform.ts"

export interface LeaderClientConfig extends TabApiOptions {
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
  /**
   * `resetReplica` closed this client (DESIGN §16.24): terminal. Its leader candidate is terminated, every
   * request fails `LeaderUnavailable { reason: "Reset" }`, and no leader is started or joined for it again
   * (`isLeaderTab` false, `leaderId` undefined). Build a new client to use the replica again.
   */
  readonly reset: boolean
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
    /**
     * The leader's sync status, current value then every change; survives handover like `subscribe` (the
     * last emission stands until the new leader's first). `SyncDisabled` on a local-only replica.
     */
    readonly syncStatus: Stream.Stream<SyncStatus, SyncDisabled | LeaderUnavailable>
    /**
     * Wake the leader's sync loop (coalesced) and complete once the wake is accepted (DESIGN §9.1). Implies
     * no network success and never clears a halted push; retried across `LeaderLost`.
     */
    readonly requestSync: Effect.Effect<void, SyncDisabled | LeaderUnavailable>
    /**
     * This tab's presence in `room` (DESIGN §9.1): its entry exists exactly while the stream runs. Emits the
     * room's snapshots; survives handover (re-joined on the new leader with the latest `track`ed state).
     */
    readonly presence: PresenceFn
    /**
     * Record this tab's desired state in a room it has joined through `presence` (a no-op otherwise).
     * Completes once this tab has recorded it, before any message to the leader; the open stream then
     * delivers the latest state, eventually, while it stays open (DESIGN §9.1).
     */
    readonly track: TrackFn
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

type PresenceFn = (
  room: AccessScope.AccessScopeId,
  state: Schema.Json
) => Stream.Stream<Protocol.RoomSnapshot, PresenceError>
type TrackFn = (room: AccessScope.AccessScopeId, state: Schema.Json) => Effect.Effect<void>

/** `Forbidden`: no grant in the room's scope. `PresenceAlreadyOpen`: one stream per room per tab. */
export type PresenceError = SyncDisabled | Protocol.Forbidden | PresenceAlreadyOpen | LeaderUnavailable

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

/**
 * Only a cause whose failures are all `LeaderLost` and that has no defect is a handover (DESIGN §16.23): a
 * defect means the leader died of a bug, which a silent resubscription would hide.
 */
const isHandover = <E>(cause: Cause.Cause<E>): boolean =>
  Cause.hasFails(cause) &&
  cause.reasons.every(
    (reason) => Cause.isInterruptReason(reason) || (Cause.isFailReason(reason) && isLeaderLost(reason.error))
  )

/**
 * Re-open `open` on the next leader whenever it ends with a handover, emitting nothing in between. Every other
 * cause passes through unchanged: `Stream.catchIf` would re-fail the bare error and drop a defect beside it.
 */
const resubscribing = <A, E>(
  open: Stream.Stream<A, E | LeaderUnavailable>
): Stream.Stream<A, E | LeaderUnavailable> => {
  const loop: Stream.Stream<A, E | LeaderUnavailable> = open.pipe(
    Stream.catchCauseIf(isHandover, () =>
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
  readonly syncStatus: Stream.Stream<SyncStatus, SyncDisabled | LeaderUnavailable>
  readonly requestSync: Effect.Effect<void, SyncDisabled | LeaderUnavailable>
  /** `room`/`track` with this tab's id filled in; `revision` is the tab's (DESIGN §9.1). */
  readonly presence: (
    room: AccessScope.AccessScopeId,
    state: Schema.Json,
    revision: number
  ) => Stream.Stream<Protocol.RoomSnapshot, SyncDisabled | Protocol.Forbidden | LeaderUnavailable>
  readonly track: (
    room: AccessScope.AccessScopeId,
    state: Schema.Json,
    revision: number
  ) => Effect.Effect<void, SyncDisabled | LeaderUnavailable>
}

/**
 * The DESIGN §9.1 semantics over `calls`: id generation, `LeaderLost` retry and re-subscription, and the
 * tab-side encode/decode. Exported so they are tested against a real leader without a browser.
 */
export interface TabApiOptions {
  /** Deadline of one presence send to the leader (DESIGN §9.1 sender). Default 45 s, the default `idleTimeout`. */
  readonly presenceSendTimeout?: Duration.Input
  /** First retry delay of a failed presence send; doubles up to 30 s. Default 1 s. */
  readonly presenceRetryDelay?: Duration.Input
}

export const tabApi = (calls: LeaderCalls, tuning: TabApiOptions = {}) => {
  const sendTimeout = tuning.presenceSendTimeout ?? "45 seconds"
  const retryBase = Duration.toMillis(Duration.fromInputUnsafe(tuning.presenceRetryDelay ?? "1 second"))
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
  /**
   * The latest desired state per joined room, kept in the tab: a new leader knows nothing of the old one's
   * (DESIGN §9.1, 07a8f48). `track` completes once this tab has recorded it, before any leader IPC. The open
   * stream owns delivery to the leader: every (re)join carries the latest state, and a supervised sender
   * scoped to that join sends any newer one after the join's first snapshot, one at a time, latest wins,
   * never blocking snapshots.
   */
  interface Cell {
    state: Schema.Json
    /** The revision `state` was given (`nextRevision`); every join and send carries the pair. */
    revision: number
    wake: Deferred.Deferred<void>
  }
  const joined = new Map<string, Cell>()
  /**
   * One revision counter per room for the life of this tab client (DESIGN §9.1, 6c7027f), outside any
   * stream's cell: closing and reopening a room never resets it. Every event the server may see out of
   * order takes the next value: an open, an accepted `track`, a handover rejoin.
   */
  const revisions = new Map<string, number>()
  const nextRevision = (room: AccessScope.AccessScopeId) => {
    const next = (revisions.get(room) ?? -1) + 1
    revisions.set(room, next)
    return next
  }
  /**
   * Each attempt has a deadline; one that fails (`LeaderLost` included: the retry goes to whichever leader
   * is current), dies or times out is retried with backoff and the latest state. It never ends the stream,
   * and never waits for the stream to fail or rejoin. Only the join's scope ends it.
   */
  const deliver = (room: AccessScope.AccessScopeId, cell: Cell, sent: number) =>
    Effect.gen(function* () {
      let delay = retryBase
      while (true) {
        const wake = Deferred.makeUnsafe<void>()
        cell.wake = wake
        if (cell.revision <= sent) {
          yield* Deferred.await(wake)
          continue
        }
        const { state, revision } = cell
        const attempt = yield* Effect.exit(calls.track(room, state, revision).pipe(Effect.timeout(sendTimeout)))
        if (Exit.isSuccess(attempt)) {
          sent = revision
          delay = retryBase
          continue
        }
        yield* Effect.sleep(Duration.millis(delay))
        delay = Math.min(delay * 2, 30_000)
      }
    })
  const presence: PresenceFn = (room, state) =>
    Stream.unwrap(
      Effect.gen(function* () {
        // Check and insert in one synchronous step, released on every exit; release only removes itself.
        const cell = yield* Effect.acquireRelease(
          Effect.sync((): Cell | undefined => {
            if (joined.has(room)) return undefined
            const fresh: Cell = { state, revision: nextRevision(room), wake: Deferred.makeUnsafe<void>() }
            joined.set(room, fresh)
            return fresh
          }),
          (registered) =>
            Effect.sync(() => {
              if (registered !== undefined && joined.get(room) === registered) joined.delete(room)
            })
        )
        if (cell === undefined) return Stream.fail<PresenceError>(new PresenceAlreadyOpen({ room }))
        let rejoin = false
        const open = Stream.unwrap(
          Effect.gen(function* () {
            // A handover rejoin takes a fresh revision, above anything the old leader may still deliver.
            if (rejoin) cell.revision = nextRevision(room)
            rejoin = true
            // One synchronous read: the join carries a genuine (state, revision) pair, the sender's baseline.
            const { state: joinedState, revision: read } = cell
            const first = Deferred.makeUnsafe<void>()
            // Scoped to this join: its end (a lost leader, a closed stream) interrupts a send in flight.
            yield* Effect.forkScoped(Effect.andThen(Deferred.await(first), deliver(room, cell, read)))
            return calls
              .presence(room, joinedState, read)
              .pipe(Stream.tap(() => Effect.sync(() => Deferred.doneUnsafe(first, Effect.void))))
          })
        )
        return resubscribing(open)
      })
    )
  // Records the desired state; the open stream delivers it. A room this tab has not joined: dropped.
  const track: TrackFn = (room, state) =>
    Effect.sync(() => {
      const cell = joined.get(room)
      if (cell === undefined) return
      cell.state = state
      cell.revision = nextRevision(room)
      Deferred.doneUnsafe(cell.wake, Effect.void)
    })
  return {
    mutate,
    subscribe,
    presence,
    track,
    notices: resubscribing(calls.notices),
    syncStatus: resubscribing(calls.syncStatus),
    requestSync: calls.requestSync.pipe(Effect.retry({ while: isLeaderLost, schedule: lostBackoff })),
    dismissNotices: (ids: ReadonlyArray<number>) =>
      calls.dismissNotices(ids).pipe(Effect.retry({ while: isLeaderLost, schedule: lostBackoff }))
  }
}

export const make = (config: LeaderClientConfig): Effect.Effect<LeaderClient["Service"], never, Scope.Scope> =>
  Effect.gen(function* () {
    const platform = yield* TabPlatform
    const identity: Platform.ReplicaIdentity = { appId: config.appId, profile: config.profile }
    const capability = platform.capability()
    const unsupported = Platform.validateIdentity(identity) ?? capability.unsupported
    const mode: Mode =
      unsupported !== undefined ? "unsupported" : capability.sharedWorker ? "shared-worker" : "single-tab"
    const tabId = crypto.randomUUID()
    const scope = yield* Effect.scope
    /**
     * Everything that keeps this client in the topology: the tab lock, lock watchers, router acceptance and
     * leader connections. Closed by a reset (DESIGN §16.24), which releases or dequeues every lock this tab
     * held or waited on; the client itself stays usable until `scope` closes, failing every request.
     */
    const topology = Scope.forkUnsafe(scope)
    const context = yield* Effect.context<never>()
    /** Reset once by the reset signal, never undone: no path below starts, elects or joins a leader after it. */
    const route = LeaderRoute.makeUnsafe()
    /**
     * Completed once a reset's close of `topology` has finished. Closing a scope detaches it from its parent
     * before its finalizers run, so a close of `scope` racing that reset close would no longer see `topology`
     * and could finish while the tab lock and the lock watchers are still being released. This finalizer makes
     * the client's scope close wait for it: once `scope` is closed, every lock this tab held or queued is gone.
     */
    const topologyClosed = Deferred.makeUnsafe<void>()
    yield* Scope.addFinalizer(
      scope,
      Effect.suspend(() => (route.isReset() ? Deferred.await(topologyClosed) : Effect.void))
    )
    const routerPorts = new Set<MessagePort>()
    const pendingConnections = new Set<MessagePort>()
    yield* Scope.addFinalizer(
      scope,
      Effect.sync(() => {
        for (const port of routerPorts) port.close()
        for (const port of pendingConnections) port.close()
        pendingConnections.clear()
      })
    )
    /**
     * Whether this client may still act in the topology: not reset and its scope not closed (a scope is
     * marked closed before its finalizers run, so this is false from the first finalizer on).
     */
    const live = () => !route.isReset() && scope.state._tag !== "Closed"
    // DOM callbacks start their Effect work here: owned by `topology`, so a reset or the client's close ends it.
    const fork = <A, E>(effect: Effect.Effect<A, E>) =>
      live() ? Fiber.runIn(Effect.runForkWith(context)(effect), topology) : undefined

    interface Active {
      readonly leaderId: string
      readonly scope: Scope.Closeable
      readonly lost: Deferred.Deferred<LeaderUnavailable>
    }
    // Async work started for a connection only acts while that exact connection object is `active`.
    let active: Active | undefined
    /**
     * The epoch (leaderId) this tab's own worker was elected with, while that epoch lasts. Ownership ends
     * with the epoch, not with the tab: a worker can die while its tab survives and connects to another
     * tab's replacement leader.
     */
    let localEpoch: string | undefined
    const status = yield* SubscriptionRef.make<Status>({
      mode,
      tabId,
      isLeaderTab: false,
      leaderId: undefined,
      reset: false
    })
    // Called from DOM callbacks after every change to `active` or `localEpoch`; `set` never suspends here.
    // Never claims a leader this tab does not own: while connected to another epoch, `isLeaderTab` is false
    // even if this tab's own epoch has not been seen to end yet (its lock callback can arrive later). With no
    // connection yet (right after election, before the handshake) it is true, so a fresh leader tab does not
    // flicker to false on its way to connecting to its own worker.
    const publishStatus = () =>
      Effect.runSyncWith(context)(
        SubscriptionRef.set(status, {
          mode,
          tabId,
          isLeaderTab:
            !route.isReset() && localEpoch !== undefined && (active === undefined || active.leaderId === localEpoch),
          leaderId: route.isReset() ? undefined : active?.leaderId,
          reset: route.isReset()
        })
      )

    const enqueue = (envelope: OutboxEnvelope) => route.withLeader((client) => client.enqueue({ envelope }))
    const service = LeaderClient.of({
      exec: (sql, params = []) => route.withLeader((client) => client.exec({ sql, params })),
      enqueue,
      ...tabApi(
        {
          enqueue,
          subscribe: (name, args) => route.streamWithLeader((client) => client.subscribe({ name, args })),
          notices: route.streamWithLeader((client) => client.notices()),
          dismissNotices: (ids) => route.withLeader((client) => client.dismissNotices({ ids })),
          syncStatus: route.streamWithLeader((client) => client.syncStatus()),
          requestSync: route.withLeader((client) => client.requestSync()),
          presence: (room, state, revision) =>
            route.streamWithLeader((client) =>
              client.presence({ room, tabId: Protocol.TabId.make(tabId), state, revision })
            ),
          track: (room, state, revision) =>
            route.withLeader((client) => client.track({ room, tabId: Protocol.TabId.make(tabId), state, revision }))
        },
        config
      ),
      leaderInfo: route.withLeader((client) => client.leaderInfo()),
      subscribeRaw: (query) => route.streamWithLeader((client) => client.subscribeRaw({ query })),
      exportPending: route.withLeader((client) => client.exportPending()),
      status: SubscriptionRef.changes(status)
    })

    if (unsupported !== undefined) {
      route.publish({
        _tag: "Unavailable",
        error: new LeaderUnavailable({ reason: "Unsupported", message: unsupported })
      })
      return service
    }

    /** This tab's leader candidate, once spawned. */
    let candidate: Worker | undefined
    // The current router SharedWorker; `undefined` in `single-tab` mode and after a reset. Replaced by
    // `reconnectRouter`.
    let router: SharedWorker | undefined
    /**
     * The terminal reset state (DESIGN §16.24), entered synchronously on the signal. Requests waiting for a
     * leader and those in flight fail `Reset` (not `LeaderLost`, so nothing retries or resubscribes), the
     * candidate is terminated (the browser releases its leader and epoch locks and drops a queued leader
     * request), and closing `topology` releases the tab lock, dequeues every lock watcher and closes the
     * connections. Every path that could start, elect or join a leader checks `reset` first.
     */
    const enterReset = () => {
      const error = route.resetError
      route.reset()
      candidate?.terminate()
      localEpoch = undefined
      const connection = active
      active = undefined
      // Fails in-flight requests before the connection's scope closes, as `displace` does.
      if (connection !== undefined) Deferred.doneUnsafe(connection.lost, Effect.succeed(error))
      for (const port of pendingConnections) port.close()
      pendingConnections.clear()
      for (const port of routerPorts) port.close()
      routerPorts.clear()
      router = undefined
      publishStatus()
      Effect.runForkWith(context)(
        Effect.andThen(Effect.yieldNow, Scope.close(topology, Exit.void)).pipe(
          Effect.ensuring(Deferred.done(topologyClosed, Exit.void))
        )
      )
    }
    const resetSignals = new BroadcastChannel(Platform.resetChannelName(identity))
    resetSignals.addEventListener("message", (event) => {
      const message: unknown = event.data
      // The channel name already scopes the identity; the payload is checked too, so no name collision can
      // ever close a client of another identity.
      if (!isResetSignal(message) || message.appId !== config.appId || message.profile !== config.profile) return
      if (live()) enterReset()
    })
    yield* Scope.addFinalizer(
      scope,
      Effect.sync(() => resetSignals.close())
    )

    // Held for the tab's lifetime: the leader and the router drop this tab's port when it is released.
    // Waited on in a fiber owned by `topology`, so a reset during the wait dequeues the request (DESIGN §16.24).
    const tabLockFiber = fork(platform.holdLock(Platform.tabLockName(identity, tabId)).pipe(Scope.provide(topology)))
    if (tabLockFiber === undefined) return service
    const tabLock = yield* Fiber.await(tabLockFiber)
    if (route.isReset()) return service
    if (Exit.isFailure(tabLock)) {
      route.publish({
        _tag: "Unavailable",
        error: new LeaderUnavailable({ reason: "Unsupported", message: `tab lock: ${String(tabLock.cause)}` })
      })
      return service
    }
    yield* platform.persistStorage

    /**
     * Fail everything in flight on `connection` with a typed `LeaderLost` first, then tear down its RPC
     * client: closing the scope first would surface as interruption instead.
     */
    const displace = (connection: Active, error: LeaderUnavailable = LeaderRoute.lostError(connection.leaderId)) => {
      Deferred.doneUnsafe(connection.lost, Effect.succeed(error))
      fork(Effect.andThen(Effect.yieldNow, Scope.close(connection.scope, Exit.void)))
    }

    const connect = (leaderId: string, port: MessagePort) => {
      if (route.isReset()) {
        port.close()
        return
      }
      if (active !== undefined) displace(active)
      const connection: Active = {
        leaderId,
        scope: Scope.forkUnsafe(topology),
        lost: Deferred.makeUnsafe<LeaderUnavailable>()
      }
      active = connection
      publishStatus()
      // Requests wait for this connection's handshake rather than using the displaced one.
      route.publish(undefined)
      fork(
        platform.awaitLockReleased(Platform.leaderEpochLockName(identity, leaderId)).pipe(
          Effect.andThen(
            Effect.suspend(() => {
              const lost = active === connection
              const cleanup = Effect.sync(() => {
                if (lost) {
                  active = undefined
                  publishStatus()
                  route.publish(undefined)
                }
                displace(connection)
                // The router may have died with that leader's tab (WebKit ends a SharedWorker with its creating
                // tab); reconnecting relaunches it if so, and is a no-op re-Hello on a live router.
                reconnectRouter()
              })
              return lost
                ? Telemetry.observe(
                    cleanup.pipe(
                      Effect.andThen(Telemetry.count("echo.leader.loss.total", "lost")),
                      Effect.andThen(route.awaitConnection),
                      Effect.flatMap((replacement) =>
                        Telemetry.annotate({ "echo.result": replacement._tag === "Connected" ? "elected" : "lost" })
                      )
                    ),
                    "echo.leader.handover",
                    Telemetry.serverFailure
                  )
                : cleanup
            })
          ),
          Effect.catchCause((cause) => Telemetry.log("Error", "echo: watching leader epoch failed", cause))
        )
      )
      fork(
        Effect.gen(function* () {
          yield* Scope.addFinalizer(
            connection.scope,
            Effect.sync(() => port.close())
          )
          pendingConnections.delete(port)
          const client = yield* platform.connect(port).pipe(Scope.provide(connection.scope))
          if (active === connection) route.publish({ _tag: "Connected", leaderId, client, lost: connection.lost })
        }).pipe(
          // The handshake never completes if the leader died first.
          Effect.raceFirst(Deferred.await(connection.lost)),
          Effect.catchCause((cause) => Telemetry.log("Error", "echo: connecting to leader failed", cause))
        )
      )
    }

    // A reset signal that arrived while this client was being built: never start a candidate for it.
    if (route.isReset()) return service
    const worker = config.spawnLeader(`echo:${config.appId}:${config.profile}:leader-candidate`)
    candidate = worker
    yield* Scope.addFinalizer(
      scope,
      Effect.sync(() => worker.terminate())
    )

    /**
     * Install a `Connected` only if it can be the live route. Deliveries from different router ports (a dead
     * instance's and its relaunch's) are not ordered with each other, so one can arrive late. Acceptance is
     * serialized across all ports, and each one is checked when it is processed:
     * - the epoch this tab is already connected (or handshaking) to: a duplicate, which must not displace it;
     * - an epoch whose lock is not held exclusively: its leader is dead, and installing it would displace the
     *   live connection while the router, which already linked this tab to the successor, never relinks it.
     * One fiber owned by `topology` takes them in arrival order; a port still queued when it ends is closed
     * through `pendingConnections`.
     */
    const routerConnects = yield* Queue.unbounded<{ readonly leaderId: string; readonly port: MessagePort }>()
    const acceptConnected = (leaderId: string, port: MessagePort) =>
      Effect.gen(function* () {
        const acceptable =
          live() &&
          active?.leaderId !== leaderId &&
          (yield* platform.isLockHeld(Platform.leaderEpochLockName(identity, leaderId)))
        if (acceptable) connect(leaderId, port)
        else port.close()
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            if (pendingConnections.delete(port)) port.close()
          })
        ),
        Effect.catchCause((cause) => Telemetry.log("Error", "echo: accepting a router connection failed", cause))
      )
    const onRouterMessage = (event: MessageEvent) => {
      const message: unknown = event.data
      if (!isFromRouter(message)) return
      if (!live()) {
        message.port.close()
        return
      }
      pendingConnections.add(message.port)
      Queue.offerUnsafe(routerConnects, { leaderId: message.leaderId, port: message.port })
    }
    fork(
      Effect.forever(Effect.flatMap(Queue.take(routerConnects), (next) => acceptConnected(next.leaderId, next.port)))
    )
    /** Announce this tab's worker as leader `leaderId` to the current router, over a fresh port pair. */
    const announce = (leaderId: string) => {
      if (router === undefined) return
      const channel = new MessageChannel()
      const routerPort: ToLeader = { _tag: "RouterPort", port: channel.port1 }
      worker.postMessage(routerPort, { transfer: [channel.port1] })
      const leader: ToRouter = { _tag: "Leader", leaderId, port: channel.port2 }
      router.port.postMessage(leader, { transfer: [channel.port2] })
    }
    /**
     * (Re)connect to the router and register this tab; the tab whose worker leads re-announces it. Constructing
     * the SharedWorker relaunches the router if the browser ended it. Idempotent on a live router: `Hello`
     * names the leader this tab is already connected to, and the router's `accept` fences a re-announcement.
     * Old router ports stay open: a `Connected` already in flight on one is still delivered.
     */
    const reconnectRouter = () => {
      if (!live() || mode !== "shared-worker") return
      const next = config.spawnRouter(Platform.routerName(identity))
      routerPorts.add(next.port)
      next.port.addEventListener("message", onRouterMessage)
      next.port.start()
      router = next
      const hello: ToRouter = {
        _tag: "Hello",
        appId: config.appId,
        profile: config.profile,
        tabId,
        connectedTo: active?.leaderId ?? null
      }
      next.port.postMessage(hello, { transfer: [] })
      if (localEpoch !== undefined) announce(localEpoch)
    }

    worker.addEventListener("message", (event) => {
      if (!live()) return
      const message: unknown = event.data
      if (!isFromLeader(message)) return
      switch (message._tag) {
        case "Waiting": {
          if (router === undefined) {
            route.publish({
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
            platform.awaitLockReleased(Platform.leaderEpochLockName(identity, epoch)).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  if (localEpoch !== epoch) return
                  localEpoch = undefined
                  publishStatus()
                })
              ),
              Effect.catchCause((cause) =>
                Telemetry.log("Error", "echo: watching this tab's leader epoch failed", cause)
              )
            )
          )
          if (router !== undefined) {
            // Reconnect rather than announce on the current port: the router may have died before this tab was
            // ever connected, and then no connected epoch ends to trigger a reconnect.
            reconnectRouter()
          } else {
            const channel = new MessageChannel()
            const direct: ToLeader = { _tag: "Connect", tabId, port: channel.port1 }
            worker.postMessage(direct, { transfer: [channel.port1] })
            connect(message.leaderId, channel.port2)
          }
          return
        }
        case "Failed": {
          if (active === undefined) {
            route.publish({
              _tag: "Unavailable",
              error: new LeaderUnavailable({ reason: "LeaderFailed", message: message.message })
            })
          }
          return
        }
      }
    })

    if (mode === "shared-worker") {
      const started = new BroadcastChannel(Platform.routerName(identity))
      started.addEventListener("message", (event) => {
        if (isRouterBroadcast(event.data)) reconnectRouter()
      })
      yield* Scope.addFinalizer(
        scope,
        Effect.sync(() => started.close())
      )
      reconnectRouter()
    }
    const init: ToLeader = { _tag: "Init", appId: config.appId, profile: config.profile, principal: config.principal }
    worker.postMessage(init, { transfer: [] })

    return service
  }).pipe(Telemetry.boundary)

export const layer = (config: LeaderClientConfig): Layer.Layer<LeaderClient> => Layer.effect(LeaderClient)(make(config))
