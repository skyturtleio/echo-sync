import { Schema } from "effect"
import { Rpc, RpcGroup } from "effect/rpc"
import { AccessScopeId, PrincipalId } from "./AccessScope.ts"
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

/**
 * Request-level push rejection (DESIGN §6): an envelope in the batch is malformed (e.g.
 * `seq < 1`), so the whole push is refused; nothing is executed or ledgered.
 */
export class MalformedEnvelope extends Schema.TaggedError<MalformedEnvelope>()("MalformedEnvelope", {
  clientId: ClientId,
  mutationId: MutationId,
  message: Schema.String
}) {}

/**
 * Transient server infrastructure failure on `pull` or `pokes` (DESIGN §6): retry with backoff.
 * A defect from the server is always a bug; push reports this case as a short result list.
 */
export class ServerUnavailable extends Schema.TaggedError<ServerUnavailable>()("ServerUnavailable", {
  message: Schema.String
}) {}

/** A hint that something may have changed; never data (DESIGN §8). Wakes the client's sync loop. */
export const Poke = Schema.TaggedStruct("poke", {})

/** Liveness only (DESIGN §6 Liveness): proves the stream is alive and never wakes the sync loop. */
export const Heartbeat = Schema.TaggedStruct("heartbeat", {})

/** One `pokes` stream element; the first element of every stream is a `Poke`. */
export const PokesElement = Schema.Union([Poke, Heartbeat])

export const SyncRpcs = RpcGroup.make(
  Rpc.make("push", {
    payload: PushRequest,
    success: PushResponse,
    error: Schema.Union([MalformedEnvelope, Unauthorized])
  }),
  Rpc.make("pull", {
    payload: PullRequest,
    success: PullResponse,
    error: Schema.Union([ResnapshotRequired, ServerUnavailable, Unauthorized])
  }),
  Rpc.make("pokes", {
    payload: { clientId: ClientId },
    success: PokesElement,
    error: Schema.Union([ServerUnavailable, Unauthorized]),
    stream: true
  })
)

// ---------------------------------------------------------------------------------------------
// Presence (DESIGN §12): in-memory, connection-scoped, never on the durable path
// ---------------------------------------------------------------------------------------------

/** One tab of a client. A presence entry is keyed by `(clientId, tabId)`. */
export const TabId = Schema.String.check(Schema.isNonEmpty()).pipe(Schema.brand("echo-sync/TabId"))
export type TabId = typeof TabId.Type

export const PresenceEntry = Schema.Struct({
  principalId: PrincipalId,
  clientId: ClientId,
  tabId: TabId,
  state: Schema.Json
})
export type PresenceEntry = typeof PresenceEntry.Type

/** Every entry of a room, ordered by `(principalId, clientId, tabId)`. */
export const RoomSnapshot = Schema.TaggedStruct("snapshot", {
  room: AccessScopeId,
  entries: Schema.Array(PresenceEntry)
})
export type RoomSnapshot = typeof RoomSnapshot.Type

/**
 * One `room` stream element: a `RoomSnapshot` (the first element of every stream) or a
 * `Heartbeat` (DESIGN §16 item 13, the §6 Liveness shape), which is never a snapshot and
 * changes no room state.
 */
export const RoomElement = Schema.Union([RoomSnapshot, Heartbeat])
export type RoomElement = typeof RoomElement.Type

/** The entry's identity and state. `room` is an access scope the principal must be able to read. */
export const PresencePayload = Schema.Struct({
  clientId: ClientId,
  room: AccessScopeId,
  tabId: TabId,
  state: Schema.Json,
  /**
   * Tab-assigned, strictly increasing per `(room, tab)` for the life of its `presence` stream (DESIGN §9.1,
   * §12 revision fencing): `track` is a no-op unless it is greater than the entry's, so a late request can
   * never overwrite a newer state. A join installs its `(state, revision)` unconditionally.
   */
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
})

/**
 * The principal holds no grant in the room's scope (DESIGN §9.1 Presence). Unlike `Unauthorized` (a
 * credential failure, retried), this is a fact about access, and the only error that ends a presence stream.
 */
export class Forbidden extends Schema.TaggedError<Forbidden>()("Forbidden", { room: AccessScopeId }) {}

export const PresenceRpcs = RpcGroup.make(
  /**
   * Joins `room` with `state` and observes it: a `RoomSnapshot` on join and after every change
   * (coalescing allowed), and a `Heartbeat` every server `heartbeatInterval`. The entry lives
   * exactly while this stream is open.
   */
  Rpc.make("room", {
    payload: PresencePayload,
    success: RoomElement,
    error: Schema.Union([ServerUnavailable, Unauthorized, Forbidden]),
    stream: true
  }),
  /**
   * Replaces the state of this `(clientId, tabId)` entry in `room`; a no-op without one, or when `revision`
   * is not greater than the entry's.
   */
  Rpc.make("track", {
    payload: PresencePayload,
    error: Schema.Union([ServerUnavailable, Unauthorized])
  })
)
