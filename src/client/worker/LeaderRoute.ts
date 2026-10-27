/**
 * The route from this tab's requests to the current leader connection. Requests wait while no leader is
 * connected, fail with the connection's `LeaderLost` when it is displaced, and fail `Reset` for good once
 * the route is reset. Owns no browser state: `LeaderClient` decides when a connection is published,
 * displaced or reset.
 */
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import type { RpcClientError } from "effect/rpc/RpcClientError"
import * as Telemetry from "../../internal/Telemetry.ts"
import { type LeaderInfo, LeaderUnavailable } from "./LeaderRpcs.ts"
import type { LeaderConnection, LeaderMethodError } from "./TabPlatform.ts"

type RoutedConnection = {
  readonly [K in keyof LeaderConnection]: LeaderConnection[K] extends (
    ...args: infer P
  ) => Effect.Effect<infer A, infer E>
    ? (...args: P) => Effect.Effect<A, Exclude<E, RpcClientError> | LeaderUnavailable>
    : LeaderConnection[K] extends (...args: infer P) => Stream.Stream<infer A, infer E>
      ? (...args: P) => Stream.Stream<A, Exclude<E, RpcClientError> | LeaderUnavailable>
      : never
}

export interface Connected {
  readonly _tag: "Connected"
  readonly leaderId: string
  readonly client: LeaderConnection
  /** Completed with the error in-flight requests fail with when this connection is displaced. */
  readonly lost: Deferred.Deferred<LeaderUnavailable>
}

export type Connection = Connected | { readonly _tag: "Unavailable"; readonly error: LeaderUnavailable }

export interface LeaderRoute {
  /** Whether `reset` was called. Never cleared. */
  readonly isReset: () => boolean
  /** The error every request fails with after `reset`. */
  readonly resetError: LeaderUnavailable
  /**
   * Route requests to `connection` (or a terminal `Unavailable`); `undefined` makes requests wait for the
   * next one. A no-op after `reset`.
   */
  readonly publish: (connection: Connection | undefined) => void
  /**
   * Fails every waiting and later request with `resetError`, and maps the teardown interruption of a request
   * in flight to it too. Only failing `lost` of the connection in flight is left to the caller.
   */
  readonly reset: () => void
  /** The next published connection: the current one if any, otherwise the next `publish`. */
  readonly awaitConnection: Effect.Effect<Connection>
  readonly withLeader: <A, E>(
    f: (client: RoutedConnection) => Effect.Effect<A, E>
  ) => Effect.Effect<A, E | LeaderUnavailable>
  /** A stream from the current leader that ends with its `lost` error when that leader goes away. */
  readonly streamWithLeader: <A, E>(
    f: (client: RoutedConnection) => Stream.Stream<A, E>
  ) => Stream.Stream<A, E | LeaderUnavailable>
}

export const lostError = (leaderId: string, detail?: string) =>
  new LeaderUnavailable({
    reason: "LeaderLost",
    message: `leader ${leaderId} went away${detail === undefined ? "" : `: ${detail}`}`
  })

const routeClient = (
  client: LeaderConnection,
  unavailable: (error: RpcClientError) => LeaderUnavailable
): RoutedConnection => {
  // Only declared RPC method failures enter here; arbitrary withLeader callback errors do not.
  const normalize = <E extends LeaderMethodError>(error: E | RpcClientError): E | LeaderUnavailable =>
    error._tag === "RpcClientError" ? unavailable(error) : error
  const call = <A, E extends LeaderMethodError>(self: Effect.Effect<A, E | RpcClientError>) =>
    Effect.catchCause(self, (cause) => Effect.failCause(Cause.map(cause, normalize<E>)))
  const stream = <A, E extends LeaderMethodError>(self: Stream.Stream<A, E | RpcClientError>) =>
    Stream.catchCause(self, (cause) => Stream.failCause(Cause.map(cause, normalize<E>)))
  return {
    exec: (input) => call(client.exec(input)),
    enqueue: (input) => call(client.enqueue(input)),
    subscribe: (input) => stream(client.subscribe(input)),
    subscribeSnapshot: (input) => stream(client.subscribeSnapshot(input)),
    subscribeRaw: (input) => stream(client.subscribeRaw(input)),
    leaderInfo: () => call<LeaderInfo, never>(client.leaderInfo()),
    syncStatus: () => stream(client.syncStatus()),
    requestSync: () => call(client.requestSync()),
    presence: (input) => stream(client.presence(input)),
    track: (input) => call(client.track(input)),
    notices: () => stream(client.notices()),
    dismissNotices: (input) => call(client.dismissNotices(input)),
    exportPending: () => call(client.exportPending())
  }
}

export const makeUnsafe = (): LeaderRoute => {
  // Pending while no leader is connected; requests wait on it. After a reset it stays the `Reset` failure.
  let current = Deferred.makeUnsafe<Connection>()
  let isReset = false
  const resetError = new LeaderUnavailable({
    reason: "Reset",
    message: "resetReplica closed this client; build a new client to use the replica again"
  })
  /**
   * After a reset a call that captured a connection before it fails `Reset`, never the shut-down RPC
   * client's interruption (`RpcClient` answers `Effect.interrupt` once its scope closed) or `LeaderLost`.
   */
  const isResetTeardown = <E>(cause: Cause.Cause<E>) => isReset && Cause.hasInterruptsOnly(cause)
  const transportError = (leaderId: string, e: RpcClientError) =>
    isReset ? resetError : lostError(leaderId, e.message)

  const publish = (connection: Connection | undefined) => {
    if (isReset) return
    if (Deferred.isDoneUnsafe(current)) current = Deferred.makeUnsafe()
    if (connection !== undefined) Deferred.doneUnsafe(current, Effect.succeed(connection))
  }

  return {
    isReset: () => isReset,
    resetError,
    publish,
    reset: () => {
      publish({ _tag: "Unavailable", error: resetError })
      isReset = true
    },
    awaitConnection: Effect.suspend(() => Deferred.await(current)),
    withLeader: (f) =>
      Effect.suspend(() => Deferred.await(current)).pipe(
        Effect.flatMap((connection) =>
          connection._tag === "Unavailable"
            ? Effect.fail(connection.error)
            : Effect.suspend(() =>
                isReset
                  ? Effect.fail(resetError)
                  : // The recorded loss runs first: a connection displaced before the call starts fails with its
                    // reason at once, and the RPC on a closed client is never started.
                    Effect.raceFirst(
                      Effect.flatMap(Deferred.await(connection.lost), Effect.fail),
                      f(routeClient(connection.client, (error) => transportError(connection.leaderId, error)))
                    )
              ).pipe(Effect.catchCauseIf(isResetTeardown, () => Effect.fail(resetError)))
        ),
        Telemetry.boundary
      ),
    streamWithLeader: <A, E>(f: (client: RoutedConnection) => Stream.Stream<A, E>) =>
      Stream.unwrap(
        Effect.suspend(() => Deferred.await(current)).pipe(
          Effect.map((connection) =>
            connection._tag === "Unavailable"
              ? Stream.fail(connection.error)
              : Stream.suspend(() =>
                  // Checked when the stream starts, not only when `current` was read (INV-BRW-7).
                  isReset
                    ? Stream.fail(resetError)
                    : f(routeClient(connection.client, (error) => transportError(connection.leaderId, error))).pipe(
                        Stream.interruptWhen(Effect.flatMap(Deferred.await(connection.lost), Effect.fail))
                      )
                ).pipe(Stream.catchCauseIf(isResetTeardown, () => Stream.fail(resetError)))
          )
        )
      ).pipe(Telemetry.stream)
  }
}
