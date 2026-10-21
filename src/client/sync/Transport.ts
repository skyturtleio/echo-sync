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
 * `Unauthorized` (credentials may be renewed), interruption. Pull defects become the transient
 * `PullProtocolError` so a bad page never kills the loop's fiber.
 *
 * Until reconcile swaps the engine onto core's types, values cross between the engine's `Wire` shapes
 * and core's Protocol here, and core's `ResnapshotRequired` is re-raised as the engine's class (the
 * engine recognizes it with `instanceof`).
 */
import { Cause, Effect, Layer, Predicate, Schema, type Scope } from "effect"
import { FetchHttpClient } from "effect/http"
import { RpcClient, type RpcClientError, RpcSerialization } from "effect/rpc"
import { Protocol } from "../../core/index.ts"
import type * as Engine from "./Engine.ts"
import * as Wire from "./Wire.ts"

/** The push batch cannot be sent as is, or the server ended it with a defect. Fatal. */
export class PushIntegrityError extends Schema.TaggedError<PushIntegrityError>()("PushIntegrityError", {
  reason: Schema.Literals(["invalidPayload", "serverDefect"]),
  message: Schema.String
}) {}

/** A pull request or response that does not fit the protocol. Transient: nothing was installed. */
export class PullProtocolError extends Schema.TaggedError<PullProtocolError>()("PullProtocolError", {
  message: Schema.String
}) {}

export type TransportError =
  | RpcClientError.RpcClientError
  | Protocol.Unauthorized
  | Protocol.MalformedEnvelope
  | PushIntegrityError
  | PullProtocolError

/** The fatal-push rule for `SyncLoop.make({ isFatalPush })`; storage errors are never fatal. */
export const isFatalPush = (error: unknown): boolean =>
  Predicate.isTagged(error, "MalformedEnvelope") || Predicate.isTagged(error, "PushIntegrityError")

const toPushRequest = Schema.decodeUnknownEffect(Schema.toType(Protocol.PushRequest))
const toPullRequest = Schema.decodeUnknownEffect(Schema.toType(Protocol.PullRequest))
const toWireRow = Schema.decodeUnknownEffect(
  Schema.StructWithRest(Schema.Struct({ id: Schema.String, access_scope_id: Schema.String }), [
    Schema.Record(Schema.String, Schema.Unknown)
  ])
)

/** Defects become `onDefect`; interruption and typed failures pass through unchanged. */
const defectsAs =
  <E2>(onDefect: (message: string) => E2) =>
  <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, E | E2, R> =>
    Effect.catchCause(self, (cause): Effect.Effect<never, E | E2> =>
      Cause.hasInterrupts(cause) || !Cause.hasDies(cause)
        ? Effect.failCause(cause)
        : Effect.fail(onDefect(Cause.pretty(cause)))
    )

/** Core's rejection classes become plain tagged records (`_tag` is an own property). */
const toWireOutcome = (outcome: Protocol.Ack["outcome"]): Wire.MutationOutcome =>
  outcome._tag === "Applied" ? outcome : { _tag: "Rejected", error: { ...outcome.error } }

const toWireAck = (ack: Protocol.Ack): Wire.Ack => ({ ...ack, outcome: toWireOutcome(ack.outcome) })

const toWireChange = (change: Protocol.Change): Effect.Effect<Wire.Change, Schema.SchemaError> =>
  change._tag === "upsert"
    ? Effect.map(toWireRow(change.row), (row) => ({ _tag: "upsert" as const, table: change.table, row }))
    : Effect.succeed(change)

const toWireResponse = (response: Protocol.PullResponse): Effect.Effect<Wire.PullResponse, Schema.SchemaError> =>
  Effect.map(Effect.forEach(response.changes, toWireChange), (changes) => ({
    ...response,
    changes,
    acks: response.acks.map(toWireAck)
  }))

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
        const response = yield* client.push(payload)
        return { results: response.results.map(toWireAck) }
      }).pipe(defectsAs((message) => new PushIntegrityError({ reason: "serverDefect", message }))),

    pull: (request) =>
      Effect.gen(function* () {
        const payload = yield* toPullRequest(request).pipe(
          Effect.mapError((error) => new PullProtocolError({ message: error.message }))
        )
        const response = yield* client
          .pull(payload)
          .pipe(
            Effect.catchTag("ResnapshotRequired", (error) =>
              Effect.fail(new Wire.ResnapshotRequired({ reason: error.reason }))
            )
          )
        return yield* toWireResponse(response).pipe(
          Effect.mapError((error) => new PullProtocolError({ message: error.message }))
        )
      }).pipe(defectsAs((message) => new PullProtocolError({ message })))
  }
})

/**
 * RPC over HTTP POST with NDJSON framing, using the platform `fetch`. The server must mount
 * `Protocol.SyncRpcs` with `RpcServer.layerHttp({ protocol: "http" })` and `RpcSerialization.layerNdjson`.
 */
export const layerProtocolHttp = (url: string): Layer.Layer<RpcClient.Protocol> =>
  RpcClient.layerProtocolHttp({ url }).pipe(Layer.provide([RpcSerialization.layerNdjson, FetchHttpClient.layer]))
