/**
 * The engine's `Transport` over core's `SyncRpcs` (DESIGN §6, §8, §12), on the leader.
 *
 * This module owns the RPC client and so its error union, and is where every failure is classified
 * once: each push failure becomes a `SyncFailure.PushTransportError` and each pull and pokes failure a
 * `SyncFailure.PullTransportError`, whose `reason` is the innermost recognized evidence (a credential
 * failure or an HTTP 401/403 inside an `RpcClientError` is that reason, never `network`). What a reason means (its status kind, whether a
 * push halts) is decided in `SyncFailure`. A push halts (DESIGN §6, a68a727) on:
 * - core's `MalformedEnvelope`: the server refused the batch;
 * - `PushIntegrityError{invalidPayload}`: the outbox batch is not a valid `Protocol.PushRequest` before
 *   it is sent (an honest client cannot produce one, and the server would refuse it at payload decode);
 * - `PushIntegrityError{serverDefect}`: the push call ended in a defect. On the wire this is how the
 *   server reports a push payload it cannot decode (`RpcServer` answers the request with
 *   `Exit.die(<formatted issue>)`; there is no failure tag for it) and how a handler defect arrives
 *   (fatal defects are broadcast as `Defect`). §6 says a defect is never retried and never ledgered, so
 *   retrying the same batch could only repeat it, and skipping it would open a seq gap.
 *
 * `ClientOutdated` is a separate compatibility stop for both directions until a new leader (§11).
 * Other reasons are transient and retried by the loop. Every pull defect (a handler defect, or a
 * response the RPC client cannot decode, which it raises as a defect) becomes the transient
 * `PullProtocolError`, so a bad page never kills the loop's fiber; core's `ResnapshotRequired` passes
 * through unwrapped for the engine to handle.
 */
import { Cause, Context, Effect, Layer, Predicate, Schema, type Scope, Stream } from "effect"
import { FetchHttpClient, type Headers, HttpClient, HttpClientError, HttpClientRequest } from "effect/http"
import { RpcClient, RpcClientError, RpcSerialization } from "effect/rpc"
import { type AccessScope, type Ids, Protocol } from "echo-sync"
import type * as Presence from "./Presence.ts"
import type * as Engine from "./Engine.ts"
import {
  CredentialsUnavailable,
  PullProtocolError,
  PushIntegrityError,
  PullTransportError,
  PushTransportError
} from "./SyncFailure.ts"

/**
 * `error`, or the more specific evidence an `RpcClientError` wraps (DESIGN §9.1): the
 * `CredentialsUnavailable` that `layerProtocolHttp` puts inside the HTTP `TransportError`, or the HTTP
 * 401/403 that its status filter raises.
 */
const lift = <E extends { readonly _tag: string }>(
  error: E | RpcClientError.RpcClientError
): E | RpcClientError.RpcClientError | CredentialsUnavailable | Protocol.Unauthorized => {
  if (!(error instanceof RpcClientError.RpcClientError) || error.reason._tag !== "HttpError") return error
  const http = error.reason.cause
  if (http instanceof HttpClientError.TransportError && http.cause instanceof CredentialsUnavailable) return http.cause
  if (
    http instanceof HttpClientError.StatusCodeError &&
    (http.response.status === 401 || http.response.status === 403)
  ) {
    return new Protocol.Unauthorized({ message: `HTTP ${http.response.status}` })
  }
  return error
}

const toPushRequest = Schema.decodeUnknownEffect(Schema.toType(Protocol.PushRequest))
/** Defects become `onDefect`; interruption and typed failures pass through unchanged. */
const defectsAs =
  <E2>(onDefect: (message: string) => E2) =>
  <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, E | E2, R> =>
    Effect.catchCause(self, (cause): Effect.Effect<never, E | E2> =>
      Cause.hasInterrupts(cause) || !Cause.hasDies(cause)
        ? Effect.failCause(cause)
        : Effect.fail(onDefect(Cause.pretty(cause)))
    )

/** The leader's `syncHeaders` for this protocol, provided by `layerProtocolHttp`; unset without one. */
const SyncHeaders = Context.Reference<Effect.Effect<Headers.Input> | undefined>(
  "echo-sync/client/Transport/SyncHeaders",
  {
    defaultValue: () => undefined
  }
)

/**
 * Headers a stream (re)connect already resolved before its request (DESIGN §6 Liveness): the HTTP
 * layer uses them instead of evaluating `syncHeaders` again. Unset for push and pull.
 */
const PreparedHeaders = Context.Reference<Headers.Input | undefined>("echo-sync/client/Transport/PreparedHeaders", {
  defaultValue: () => undefined
})

/**
 * Runs `syncHeaders`; a defect becomes `CredentialsUnavailable`. Any cause with an interruption,
 * including interruption plus a defect from a finalizer, passes through unchanged.
 */
const acquire = (headers: Effect.Effect<Headers.Input>) =>
  headers.pipe(defectsAs((message) => new CredentialsUnavailable({ message: `syncHeaders failed: ${message}` })))

/**
 * The engine's transport plus the pokes stream, which the leader follows with `SyncLoop.followPokes`.
 * `pokes` is prepare-then-stream: the Effect resolves this connect's headers (before any `idleTimeout`
 * clock, DESIGN §6 Liveness), and the stream it returns sends the request with exactly those.
 */
export interface SyncTransport extends Engine.Transport {
  /** `principal`: the replica's, asserted to the server like the engine's push and pull (DESIGN §16.21). */
  readonly pokes: (
    clientId: Ids.ClientId,
    principal: AccessScope.PrincipalId
  ) => Effect.Effect<Stream.Stream<typeof Protocol.PokesElement.Type, PullTransportError>, PullTransportError>
  /** `Protocol.PresenceRpcs` on the same mount (the server merges both groups, DESIGN §12). */
  readonly presence: (clientId: Ids.ClientId, principal: AccessScope.PrincipalId) => Presence.Upstream<PresenceError>
}

export type PresenceError =
  | RpcClientError.RpcClientError
  | Protocol.Unauthorized
  | Protocol.ServerUnavailable
  | Protocol.Forbidden
  | CredentialsUnavailable

/**
 * Only a missing grant (`Forbidden`) ends a presence subscription. Everything else is rejoined, including
 * `Unauthorized`: a credential failure, retried like every other one (DESIGN §9.1 Credentials).
 */
export const isFatalPresence = (error: PresenceError): error is Protocol.Forbidden =>
  Predicate.isTagged(error, "Forbidden")

export const make: Effect.Effect<SyncTransport, never, RpcClient.Protocol | Scope.Scope> = Effect.gen(function* () {
  const client = yield* RpcClient.make(Protocol.SyncRpcs)
  const headers = yield* SyncHeaders
  const presenceClient = yield* RpcClient.make(Protocol.PresenceRpcs)
  return {
    push: (request) =>
      Effect.gen(function* () {
        const payload = yield* toPushRequest(request).pipe(
          Effect.mapError((error) => new PushIntegrityError({ reason: "invalidPayload", message: error.message }))
        )
        return yield* client.push(payload)
      }).pipe(
        defectsAs((message) => new PushIntegrityError({ reason: "serverDefect", message })),
        Effect.mapError((error) => new PushTransportError({ reason: lift(error) }))
      ),

    // The request is built by the engine from core types; the response is decoded by the RPC client.
    pull: (request) =>
      client.pull(request).pipe(
        defectsAs((message) => new PullProtocolError({ message })),
        Effect.mapError((error) =>
          error._tag === "ResnapshotRequired" ? error : new PullTransportError({ reason: lift(error) })
        )
      ),

    // Any end of this stream (failure, defect, completion) is handled by reconnecting (followPokes).
    pokes: (clientId, principal) => {
      const open = (prepared: Headers.Input | undefined) => {
        const stream = client
          .pokes({ clientId, principal })
          .pipe(Stream.mapError((error) => new PullTransportError({ reason: lift(error) })))
        return prepared === undefined ? stream : stream.pipe(Stream.provideService(PreparedHeaders, prepared))
      }
      return headers === undefined
        ? Effect.succeed(open(undefined))
        : acquire(headers).pipe(
            Effect.mapError((reason) => new PullTransportError({ reason })),
            Effect.map(open)
          )
    },

    presence: (clientId, principal) => ({
      // Prepare-then-stream like `pokes`, so the leader's `idleTimeout` starts at send.
      room: (payload) =>
        headers === undefined
          ? Effect.succeed(presenceClient.room({ clientId, principal, ...payload }))
          : Effect.map(acquire(headers), (prepared) =>
              presenceClient
                .room({ clientId, principal, ...payload })
                .pipe(Stream.provideService(PreparedHeaders, prepared))
            ),
      track: (payload) => presenceClient.track({ clientId, principal, ...payload })
    })
  }
})

/**
 * RPC over HTTP POST with NDJSON framing, using the platform `fetch`. The server must mount
 * `Protocol.SyncRpcs` with `RpcServer.layerHttp({ protocol: "http" })` and `RpcSerialization.layerNdjson`.
 *
 * `headers` (DESIGN §9.1 Credentials) is evaluated on every outgoing HTTP request: each push, each
 * pull, and each (re)connect of a stream. It is applied to the HTTP client under the RPC protocol, so
 * every RPC on this protocol (including streams added later, such as presence `room`) carries the
 * credential current at the moment it is sent; nothing is cached across requests. A stream's
 * (re)connect resolves them first (`SyncTransport.pokes`, presence `room`) and its request uses that
 * result.
 *
 * Anything `headers` raises is a local acquisition failure (DESIGN §9.1): the request is not sent
 * and the call fails with a transport `RpcClientError` (an HTTP `TransportError` carrying
 * `CredentialsUnavailable`), retried like a network failure and never a server defect.
 */
export const layerProtocolHttp = (
  url: string,
  headers?: Effect.Effect<Headers.Input>
): Layer.Layer<RpcClient.Protocol> => {
  // `RpcClient.layerProtocolHttp` with the request transform inlined: its `transformClient` option
  // cannot add the HTTP error the credential step may raise.
  const protocol = Layer.effect(RpcClient.Protocol)(
    Effect.flatMap(HttpClient.HttpClient, (base) => {
      // Refuse authentication statuses before RPC reads the body; leave all other statuses alone.
      const client = HttpClient.mapRequest(base, HttpClientRequest.prependUrl(url)).pipe(
        HttpClient.filterStatus((status) => status !== 401 && status !== 403)
      )
      if (headers === undefined) return RpcClient.makeProtocolHttp(client)
      return RpcClient.makeProtocolHttp(
        HttpClient.mapRequestEffect(client, (request) =>
          Effect.gen(function* () {
            const prepared = yield* PreparedHeaders
            const current =
              prepared ??
              (yield* acquire(headers).pipe(
                Effect.mapError(
                  (cause) =>
                    new HttpClientError.HttpClientError({
                      reason: new HttpClientError.TransportError({ request, cause, description: cause.message })
                    })
                )
              ))
            return HttpClientRequest.setHeaders(request, current)
          })
        )
      )
    })
  )
  return headers === undefined
    ? protocol.pipe(Layer.provide([RpcSerialization.layerNdjson, FetchHttpClient.layer]))
    : Layer.merge(protocol, Layer.succeed(SyncHeaders)(headers)).pipe(
        Layer.provide([RpcSerialization.layerNdjson, FetchHttpClient.layer])
      )
}
