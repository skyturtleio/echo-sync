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
import { Cause, Effect, Layer, Predicate, Schema, type Scope } from "effect"
import { FetchHttpClient } from "effect/http"
import { RpcClient, type RpcClientError, RpcSerialization } from "effect/rpc"
import { Protocol } from "echo-sync"
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

/** A `Transport` over an RPC client `Protocol` (see `layerProtocolHttp`). */
export const make: Effect.Effect<
  Engine.Transport<TransportError>,
  never,
  RpcClient.Protocol | Scope.Scope
> = Effect.gen(function* () {
  const client = yield* RpcClient.make(Protocol.SyncRpcs)
  return {
    push: (request) =>
      Effect.gen(function* () {
        const payload = yield* toPushRequest(request).pipe(
          Effect.mapError((error) => new PushIntegrityError({ reason: "invalidPayload", message: error.message }))
        )
        return yield* client.push(payload)
      }).pipe(defectsAs((message) => new PushIntegrityError({ reason: "serverDefect", message }))),

    // The request is built by the engine from core types; the response is decoded by the RPC client.
    pull: (request) => client.pull(request).pipe(defectsAs((message) => new PullProtocolError({ message })))
  }
})

/**
 * RPC over HTTP POST with NDJSON framing, using the platform `fetch`. The server must mount
 * `Protocol.SyncRpcs` with `RpcServer.layerHttp({ protocol: "http" })` and `RpcSerialization.layerNdjson`.
 */
export const layerProtocolHttp = (url: string): Layer.Layer<RpcClient.Protocol> =>
  RpcClient.layerProtocolHttp({ url }).pipe(Layer.provide([RpcSerialization.layerNdjson, FetchHttpClient.layer]))
