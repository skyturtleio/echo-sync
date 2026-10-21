/**
 * The engine's `Transport` over core's `SyncRpcs` (DESIGN §6, §8, §12), on the leader.
 *
 * Fatal push classification (DESIGN §6, a68a727) happens here, because this module owns the RPC
 * client and so its error union. A push is fatal (`isFatalPush`) when:
 * - the server refuses the batch with core's `MalformedEnvelope` (tag `"MalformedEnvelope"`);
 * - the outbox batch is not a valid `Protocol.PushRequest` before it is sent (an honest client cannot
 *   produce one, and the server would refuse it at payload decode): `PushIntegrityError{invalidPayload}`;
 * - the push call ends in a defect (`PushIntegrityError{serverDefect}`). On the wire this is how the
 *   server reports a push payload it cannot decode (`RpcServer` answers the request with
 *   `Exit.die(<formatted issue>)`; there is no failure tag for it) and how a handler defect arrives
 *   (fatal defects are broadcast as `Defect`). §6 says a defect is never retried and never ledgered, so
 *   retrying the same batch could only repeat it, and skipping it would open a seq gap.
 *
 * Everything else is transient and retried by the loop: `RpcClientError` (HTTP, socket, protocol),
 * `Unauthorized` (credentials may be renewed), interruption. Every pull defect (a handler defect, or a
 * response the RPC client cannot decode, which it raises as a defect) becomes the transient
 * `PullProtocolError`, so a bad page never kills the loop's fiber; core's `ResnapshotRequired` passes through for the engine to handle.
 */
import { Cause, Context, Effect, Layer, Predicate, Schema, type Scope, Stream } from "effect"
import { FetchHttpClient, type Headers, HttpClient, HttpClientError, HttpClientRequest } from "effect/http"
import { RpcClient, type RpcClientError, RpcSerialization } from "effect/rpc"
import { type Ids, Protocol } from "echo-sync"
import type * as Presence from "./Presence.ts"
import type * as Engine from "./Engine.ts"

/** The push batch cannot be sent as is, or the server ended it with a defect. Fatal. */
export class PushIntegrityError extends Schema.TaggedError<PushIntegrityError>()("PushIntegrityError", {
  reason: Schema.Literals(["invalidPayload", "serverDefect"]),
  message: Schema.String
}) {}

/** The pull call ended in a defect. Transient: nothing was installed. */
export class PullProtocolError extends Schema.TaggedError<PullProtocolError>()("PullProtocolError", {
  message: Schema.String
}) {}

export type TransportError =
  | RpcClientError.RpcClientError
  | Protocol.Unauthorized
  | Protocol.MalformedEnvelope
  | Protocol.ServerUnavailable
  | PushIntegrityError
  | PullProtocolError

/** The fatal-push rule for `SyncLoop.make({ isFatalPush })`; storage errors are never fatal. */
export const isFatalPush = (error: unknown): boolean =>
  Predicate.isTagged(error, "MalformedEnvelope") || Predicate.isTagged(error, "PushIntegrityError")

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

/**
 * `syncHeaders` raised (a defect: its error channel is `never`) while the leader was preparing a
 * request (DESIGN §9.1 Credentials). Local and transient, like a network failure: nothing was sent.
 */
export class CredentialsUnavailable extends Schema.TaggedError<CredentialsUnavailable>()("CredentialsUnavailable", {
  message: Schema.String
}) {}

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

/** A `Transport` over an RPC client `Protocol` (see `layerProtocolHttp`). */
/**
 * The engine's transport plus the pokes stream, which the leader follows with `SyncLoop.followPokes`.
 * `pokes` is prepare-then-stream: the Effect resolves this connect's headers (before any `idleTimeout`
 * clock, DESIGN §6 Liveness), and the stream it returns sends the request with exactly those.
 */
export interface SyncTransport extends Engine.Transport<TransportError> {
  readonly pokes: (
    clientId: Ids.ClientId
  ) => Effect.Effect<Stream.Stream<typeof Protocol.PokesElement.Type, unknown>, CredentialsUnavailable>
  /** `Protocol.PresenceRpcs` on the same mount (the server merges both groups, DESIGN §12). */
  readonly presence: (clientId: Ids.ClientId) => Presence.Upstream<PresenceError>
}

export type PresenceError = RpcClientError.RpcClientError | Protocol.Unauthorized | Protocol.ServerUnavailable

/** Only a missing grant ends a presence subscription; everything else is rejoined. */
export const isFatalPresence = (error: PresenceError): error is Protocol.Unauthorized =>
  Predicate.isTagged(error, "Unauthorized")

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
      }).pipe(defectsAs((message) => new PushIntegrityError({ reason: "serverDefect", message }))),

    // The request is built by the engine from core types; the response is decoded by the RPC client.
    pull: (request) => client.pull(request).pipe(defectsAs((message) => new PullProtocolError({ message }))),

    // Any end of this stream (failure, defect, completion) is handled by reconnecting (followPokes).
    pokes: (clientId) =>
      headers === undefined
        ? Effect.succeed(client.pokes({ clientId }))
        : Effect.map(acquire(headers), (prepared) =>
            client.pokes({ clientId }).pipe(Stream.provideService(PreparedHeaders, prepared))
          ),

    presence: (clientId) => ({
      room: (payload) => presenceClient.room({ clientId, ...payload }),
      track: (payload) => presenceClient.track({ clientId, ...payload })
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
 * (re)connect resolves them first (`SyncTransport.pokes`) and its request uses that result.
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
      const client = HttpClient.mapRequest(base, HttpClientRequest.prependUrl(url))
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
