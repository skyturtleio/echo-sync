/**
 * The engine's `Transport` over core's `SyncRpcs` (SPEC §8, §7, §10), on the leader.
 *
 * This module owns credential preparation and the RPC client. Only HTTP 401/403 are lifted
 * out of an `RpcClientError`; arbitrary fetch rejections are always network failures.
 * What an error means (its status kind, whether a
 * push halts) is decided in `SyncFailure`. A push halts (INV-LOOP-3) on:
 * - core's `MalformedEnvelope`: the server refused the batch;
 * - `PushIntegrityError{invalidPayload}`: the outbox batch is not a valid `Protocol.PushRequest` before
 *   it is sent (an honest client cannot produce one, and the server would refuse it at payload decode);
 * - `PushIntegrityError{serverDefect}`: the push call ended in a defect. On the wire this is how the
 *   server reports a push payload it cannot decode (`RpcServer` answers the request with
 *   `Exit.die(<formatted issue>)`; there is no failure tag for it) and how a handler defect arrives
 *   (fatal defects are broadcast as `Defect`). INV-LOOP-3 says a defect is never retried and never ledgered, so
 *   retrying the same batch could only repeat it, and skipping it would open a seq gap.
 *
 * `ClientOutdated` is a separate compatibility stop for both directions until a new leader (SPEC §0.2, §10).
 * Other reasons are transient and retried by the loop. Every pull defect (a handler defect, or a
 * response the RPC client cannot decode, which it raises as a defect) becomes the transient
 * `PullProtocolError`, so a bad page never kills the loop's fiber; core's `ResnapshotRequired` passes
 * through unwrapped for the engine to handle.
 */
import { Array as Arr, Cause, Context, Effect, Layer, Predicate, Schema, type Scope, Stream } from "effect"
import { FetchHttpClient, type Headers, HttpClient, HttpClientError, HttpClientRequest } from "effect/http"
import { RpcClient, RpcClientError, RpcSerialization } from "effect/rpc"
import { type AccessScope, type Ids, Protocol } from "echo-sync"
import type * as Presence from "./Presence.ts"
import type * as Engine from "./Engine.ts"
import { CredentialsUnavailable, PullProtocolError, PushIntegrityError, type PullReason } from "./SyncFailure.ts"

/**
 * Lift only an actual HTTP status failure. Fetch rejections are nested in TransportError,
 * never inspected for engine-shaped causes.
 */
const lift = <E extends { readonly _tag: string }>(
  error: E | RpcClientError.RpcClientError
): E | RpcClientError.RpcClientError | Protocol.Unauthorized => {
  // oxlint-disable-next-line effecttsgo/instance-of-schema -- Only this RPC implementation's errors cross the infrastructure boundary, not same-identifier classes.
  if (!(error instanceof RpcClientError.RpcClientError) || error.reason._tag !== "HttpError") return error
  const http = error.reason.cause
  if (
    http instanceof HttpClientError.StatusCodeError &&
    (http.response.status === 401 || http.response.status === 403)
  ) {
    return new Protocol.Unauthorized({ reason: "httpUnauthorized", message: `HTTP ${http.response.status}` })
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
 * Headers this request already resolved before entering RPC (INV-LOOP-8).
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
 * clock, INV-LOOP-8), and the stream it returns sends the request with exactly those.
 */
export interface SyncTransport extends Engine.Transport {
  /** `principal`: the replica's, asserted to the server like the engine's push and pull (SPEC §5). */
  readonly pokes: (
    clientId: Ids.ClientId,
    principal: AccessScope.PrincipalId,
    epoch?: Effect.Effect<{ readonly epoch?: string | null }>
  ) => Effect.Effect<Stream.Stream<typeof Protocol.PokesElement.Type, PullReason>, PullReason>
  /** `Protocol.PresenceRpcs` on the same mount (the server merges both groups, SPEC §10). */
  readonly presence: (
    clientId: Ids.ClientId,
    principal: AccessScope.PrincipalId,
    epoch?: Effect.Effect<{ readonly epoch?: string | null }>
  ) => Presence.Upstream<PresenceError>
}

export type PresenceError =
  | RpcClientError.RpcClientError
  | Protocol.ResnapshotRequired
  | Protocol.ClientOutdated
  | Protocol.Unauthorized
  | Protocol.ServerUnavailable
  | Protocol.Forbidden
  | CredentialsUnavailable

/**
 * Only a missing grant (`Forbidden`) ends a presence subscription. Everything else is rejoined, including
 * `Unauthorized`: a credential failure, retried like every other one (SPEC §10.3 Credentials).
 */
export const isFatalPresence = (error: PresenceError): error is Protocol.Forbidden =>
  Predicate.isTagged(error, "Forbidden")

export const make: Effect.Effect<SyncTransport, never, RpcClient.Protocol | Scope.Scope> = Effect.gen(function* () {
  const client = yield* RpcClient.make(Protocol.SyncRpcs)
  const headers = yield* SyncHeaders
  const presenceClient = yield* RpcClient.make(Protocol.PresenceRpcs)
  const prepared = headers === undefined ? Effect.succeed(undefined) : acquire(headers)
  const prepareEffect = <A, E, R>(self: Effect.Effect<A, E, R>) =>
    Effect.flatMap(prepared, (current) =>
      current === undefined ? self : Effect.provideService(self, PreparedHeaders, current)
    )
  const prepareStream = <A, E, R>(self: Stream.Stream<A, E, R>) =>
    Effect.map(prepared, (current) =>
      current === undefined ? self : Stream.provideService(self, PreparedHeaders, current)
    )
  return {
    push: (request) =>
      Effect.gen(function* () {
        const payload = yield* toPushRequest(request).pipe(
          Effect.mapError((error) => new PushIntegrityError({ reason: "invalidPayload", message: error.message }))
        )
        return yield* client.push(payload)
      }).pipe(
        defectsAs((message) => new PushIntegrityError({ reason: "serverDefect", message })),
        Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, lift))),
        prepareEffect
      ),

    // The request is built by the engine from core types; the response is decoded by the RPC client.
    pull: (request) =>
      Stream.unwrap(
        prepareStream(
          client.pull(request, { streamBufferSize: 1 }).pipe(
            Stream.catchCause((cause): Stream.Stream<never, PullReason> =>
              Cause.hasInterrupts(cause) || !Cause.hasDies(cause)
                ? Stream.failCause(cause)
                : Stream.fail(new PullProtocolError({ message: Cause.pretty(cause) }))
            ),
            Stream.catchCause((cause) => Stream.failCause(Cause.map(cause, lift)))
          )
        )
      ),

    // Any end of this stream (failure, defect, completion) is handled by reconnecting (followPokes).
    pokes: (clientId, principal, epoch = Effect.succeed({})) =>
      prepareStream(
        Stream.unwrap(Effect.map(epoch, (fence) => client.pokes({ clientId, principal, ...fence }))).pipe(
          // A restored server may be otherwise quiet. Wake pull to negotiate its timeline.
          Stream.catchCause((cause) =>
            cause.reasons.length > 0 &&
            Arr.every(cause.reasons, (r) => Cause.isFailReason(r) && r.error._tag === "ResnapshotRequired")
              ? Stream.succeed(Protocol.Poke.make({}))
              : Stream.failCause(Cause.map(cause, lift))
          )
        )
      ),

    presence: (clientId, principal, epoch = Effect.succeed({})) => ({
      // Prepare-then-stream like `pokes`, so the leader's `idleTimeout` starts at send.
      room: (payload) =>
        Effect.flatMap(epoch, (fence) =>
          prepareStream(
            presenceClient
              .room({ clientId, principal, ...payload, ...fence })
              .pipe(Stream.catchCause((cause) => Stream.failCause(Cause.map(cause, lift))))
          )
        ),
      track: (payload) =>
        prepareEffect(
          Effect.flatMap(epoch, (fence) => presenceClient.track({ clientId, principal, ...payload, ...fence })).pipe(
            Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, lift)))
          )
        )
    })
  }
})

/**
 * RPC over HTTP POST with NDJSON framing, using the platform `fetch`. The server must mount
 * `Protocol.SyncRpcs` with `RpcServer.layerHttp({ protocol: "http" })` and `RpcSerialization.layerNdjson`.
 *
 * `headers` (INV-LOOP-8 Credentials) is evaluated on every outgoing HTTP request: each push, each
 * pull, and each (re)connect of a stream. Transport.make resolves them before RPC, then the HTTP
 * client applies the prepared value. Nothing is cached across requests.
 *
 * Anything `headers` raises is a local acquisition failure (SPEC §10): the request is not sent
 * and the call fails with `CredentialsUnavailable`, never an error inferred from a fetch rejection.
 */
export const layerProtocolHttp = (
  url: string,
  headers?: Effect.Effect<Headers.Input>
): Layer.Layer<RpcClient.Protocol> => {
  // Credential execution belongs to make, before the RPC boundary, not to this HTTP transform.
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
            return prepared === undefined ? request : HttpClientRequest.setHeaders(request, prepared)
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
