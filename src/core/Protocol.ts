import { Schema } from "effect"
import { Rpc, RpcGroup } from "effect/rpc"
import { AccessScopeId } from "./AccessScope.ts"
import { ClientId, Cursor, MutationId, Position, Seq, SeqFloor } from "./Ids.ts"
import { MutationOutcome } from "./Mutation.ts"
import { EncodedRow } from "./SyncTable.ts"

// ---------------------------------------------------------------------------------------------
// Push (DESIGN §6)
// ---------------------------------------------------------------------------------------------

/** One mutation in a push batch; `clientId` is carried once on the request. Pushed in `seq` order. */
export const PushMutation = Schema.Struct({
  mutationId: MutationId,
  seq: Seq,
  name: Schema.String,
  args: Schema.Json
})
export type PushMutation = typeof PushMutation.Type

export const PushRequest = Schema.Struct({
  clientId: ClientId,
  mutations: Schema.NonEmptyArray(PushMutation).check(Schema.isMaxLength(100))
})
export type PushRequest = typeof PushRequest.Type

/** Outcome of one processed mutation and the ledger position it was recorded at (DESIGN §6, §8). */
export const PushResult = Schema.Struct({
  mutationId: MutationId,
  position: Position,
  outcome: MutationOutcome
})
export type PushResult = typeof PushResult.Type

/**
 * Results for the processed prefix of the batch, in order. Fewer results than mutations means
 * a transient failure stopped the batch there; the client retries from the first missing one.
 */
export const PushResponse = Schema.Struct({ results: Schema.Array(PushResult) })
export type PushResponse = typeof PushResponse.Type

// ---------------------------------------------------------------------------------------------
// Pull (DESIGN §8)
// ---------------------------------------------------------------------------------------------

/** Opaque to the client: resend verbatim. The server owns its schema (JSON inside the string). */
export const PullContinuation = Schema.String.pipe(Schema.brand("echo-sync/PullContinuation"))
export type PullContinuation = typeof PullContinuation.Type

export const Upsert = Schema.TaggedStruct("upsert", { table: Schema.String, row: EncodedRow })
export const Tombstone = Schema.TaggedStruct("tombstone", { table: Schema.String, id: Schema.String })
export const ScopePurged = Schema.TaggedStruct("scopePurged", { scopeId: AccessScopeId })
/** Applied strictly in order. */
export const Change = Schema.Union([Upsert, Tombstone, ScopePurged])
export type Change = typeof Change.Type

export const Ack = Schema.Struct({ mutationId: MutationId, position: Position, outcome: MutationOutcome })
export type Ack = typeof Ack.Type

export const PullRequest = Schema.Struct({
  clientId: ClientId,
  cursor: Cursor,
  continuation: Schema.OptionFromNullOr(PullContinuation)
})
export type PullRequest = typeof PullRequest.Type

/** More pages follow: stage, do not apply. */
export const PullPage = Schema.TaggedStruct("PullPage", {
  changes: Schema.Array(Change),
  acks: Schema.Array(Ack),
  continuation: PullContinuation
})
/** Page set complete: apply the staged set (acks included) atomically, persist `nextCursor`. */
export const PullDone = Schema.TaggedStruct("PullDone", {
  changes: Schema.Array(Change),
  acks: Schema.Array(Ack),
  nextCursor: Position,
  /** The server's ledger-expiry floor for this client (DESIGN §16.5); 0 when nothing expired. */
  expiredThrough: SeqFloor
})
export const PullResponse = Schema.Union([PullPage, PullDone])
export type PullResponse = typeof PullResponse.Type

// ---------------------------------------------------------------------------------------------
// Errors and RPC group (DESIGN §12)
// ---------------------------------------------------------------------------------------------

/** The client must discard staged pages and resync from a null cursor (DESIGN §8). */
export class ResnapshotRequired extends Schema.TaggedError<ResnapshotRequired>()("ResnapshotRequired", {
  reason: Schema.Literals(["gcHorizon", "unknownCursor", "staleContinuation"])
}) {}

/** The request's principal could not be established or does not own `clientId`. */
export class Unauthorized extends Schema.TaggedError<Unauthorized>()("Unauthorized", {
  message: Schema.String
}) {}

/** A hint that something may have changed; never data (DESIGN §8). */
export const Poke = Schema.Struct({})

export const SyncRpcs = RpcGroup.make(
  Rpc.make("push", { payload: PushRequest, success: PushResponse, error: Unauthorized }),
  Rpc.make("pull", {
    payload: PullRequest,
    success: PullResponse,
    error: Schema.Union([ResnapshotRequired, Unauthorized])
  }),
  Rpc.make("pokes", { payload: { clientId: ClientId }, success: Poke, error: Unauthorized, stream: true })
)
