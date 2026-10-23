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
import { RpcClientError } from "effect/rpc/RpcClientError"
import * as Telemetry from "../../internal/Telemetry.ts"
import { LeaderUnavailable } from "./LeaderRpcs.ts"
import type { LeaderConnection } from "./TabPlatform.ts"

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
    f: (client: LeaderConnection) => Effect.Effect<A, E | RpcClientError>
  ) => Effect.Effect<A, Exclude<E, RpcClientError> | LeaderUnavailable>
  /** A stream from the current leader that ends with its `lost` error when that leader goes away. */
  readonly streamWithLeader: <A, E>(
    f: (client: LeaderConnection) => Stream.Stream<A, E | RpcClientError>
  ) => Stream.Stream<A, E | LeaderUnavailable>
}

export const lostError = (leaderId: string, detail?: string) =>
  new LeaderUnavailable({
    reason: "LeaderLost",
    message: `leader ${leaderId} went away${detail === undefined ? "" : `: ${detail}`}`
  })

const isRpcClientError = (u: unknown): u is RpcClientError => u instanceof RpcClientError

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
                      f(connection.client).pipe(
                        Effect.catchIf(isRpcClientError, (e) => Effect.fail(transportError(connection.leaderId, e)))
                      )
                    )
              ).pipe(Effect.catchCauseIf(isResetTeardown, () => Effect.fail(resetError)))
        ),
        Telemetry.boundary
      ),
    streamWithLeader: <A, E>(f: (client: LeaderConnection) => Stream.Stream<A, E | RpcClientError>) =>
      Stream.unwrap(
        Effect.suspend(() => Deferred.await(current)).pipe(
          Effect.map((connection) =>
            connection._tag === "Unavailable"
              ? Stream.fail(connection.error)
              : Stream.suspend(() =>
                  // Checked when the stream starts, not only when `current` was read (DESIGN §16.24).
                  isReset
                    ? Stream.fail(resetError)
                    : f(connection.client).pipe(
                        // Maps only the transport failures, keeping every other reason (a defect included): a
                        // `Stream.catchIf` would re-fail any other error bare (DESIGN §16.23).
                        Stream.catchCause((cause) =>
                          Stream.failCause(
                            Cause.map(cause, (e: E | RpcClientError): E | LeaderUnavailable =>
                              isRpcClientError(e) ? transportError(connection.leaderId, e) : e
                            )
                          )
                        ),
                        Stream.interruptWhen(Effect.flatMap(Deferred.await(connection.lost), Effect.fail))
                      )
                ).pipe(Stream.catchCauseIf(isResetTeardown, () => Stream.fail(resetError)))
          )
        )
      ).pipe(Telemetry.stream)
  }
}
