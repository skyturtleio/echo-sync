import { Schema } from "effect"
import { Rpc, RpcGroup } from "effect/rpc"
import { AccessScopeId, PrincipalId } from "./AccessScope.ts"
import { ClientId, Cursor, MutationId, Position, Seq, SeqFloor } from "./Ids.ts"
import { MutationOutcome } from "./Mutation.ts"
import { EncodedRow } from "./SyncTable.ts"

/** Engine wire contract, independent of the app's registry hash. Not an equality gate. */
export const wireVersion = 2
export const ClientVersion = Schema.Struct({
  dataHash: Schema.String,
  wireVersion: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
})
export type ClientVersion = typeof ClientVersion.Type

// ---------------------------------------------------------------------------------------------
// Push (SPEC §8)
// ---------------------------------------------------------------------------------------------

/** One mutation in a push batch; `clientId` is carried once on the request. Pushed in `seq` order. */
export const PushMutation = Schema.Struct({
  mutationId: MutationId,
  seq: Seq,
  /** Original writer clock, never an ordering key. Absent only on pre-field clients (SPEC §5). */
  createdAt: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: -210_866_803_200_000, maximum: 8_640_000_000_000_000 }))
  ),
  name: Schema.String,
  args: Schema.Json
})
export type PushMutation = typeof PushMutation.Type

/** Reject, never clamp, writer clocks further ahead than this (SPEC §18). */
export const maxMutationFutureSkewMillis = 30_000

export const PushRequest = Schema.Struct({
  clientId: ClientId,
  /** null bootstraps an epoch-aware client; absent identifies a legacy client. */
  epoch: Schema.optionalKey(Schema.NullOr(Schema.String)),
  /** Absent only for pre-gate clients; never infer compatibility from absence. */
  clientVersion: Schema.optionalKey(ClientVersion),
  /**
   * The principal this replica acts as (SPEC §5). A consistency assertion, never a credential:
   * the server refuses the request `Unauthorized` unless it equals the authenticated principal, and
   * never uses it as the actor. Absent only for clients that predate it.
   */
  principal: Schema.optionalKey(PrincipalId),
  mutations: Schema.NonEmptyArray(PushMutation).check(Schema.isMaxLength(100))
})
export type PushRequest = typeof PushRequest.Type

/** Outcome of one processed mutation and the ledger position it was recorded at (SPEC §5, §7). */
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
// Pull (SPEC §7)
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
  epoch: Schema.optionalKey(Schema.NullOr(Schema.String)),
  clientVersion: Schema.optionalKey(ClientVersion),
  /** As on `PushRequest` (SPEC §5). */
  principal: Schema.optionalKey(PrincipalId),
  cursor: Cursor,
  continuation: Schema.OptionFromNullOr(PullContinuation)
})
export type PullRequest = typeof PullRequest.Type

/** More pages follow: stage, do not apply. */
export const PullPage = Schema.TaggedStruct("PullPage", {
  epoch: Schema.optionalKey(Schema.String),
  changes: Schema.Array(Change),
  acks: Schema.Array(Ack),
  continuation: PullContinuation
})
/** Page set complete: apply the staged set (acks included) atomically, persist `nextCursor`. */
export const PullDone = Schema.TaggedStruct("PullDone", {
  epoch: Schema.optionalKey(Schema.String),
  changes: Schema.Array(Change),
  acks: Schema.Array(Ack),
  nextCursor: Position,
  /** The server's ledger-expiry floor for this client (SPEC §11); 0 when nothing expired. */
  expiredThrough: SeqFloor
})
export const PullResponse = Schema.Union([PullPage, PullDone])
export type PullResponse = typeof PullResponse.Type

// ---------------------------------------------------------------------------------------------
// Errors and RPC group (SPEC §10)
// ---------------------------------------------------------------------------------------------

/** The client must discard staged pages and resync from a null cursor (INV-CLI-10). */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class ResnapshotRequired extends Schema.TaggedError<ResnapshotRequired>()("ResnapshotRequired", {
  /** Present for a history fence; discard old replica metadata and retry under this epoch. */
  epoch: Schema.optionalKey(Schema.String),
  reason: Schema.Literals(["gcHorizon", "unknownCursor", "staleContinuation"])
}) {}

/** Closed diagnostic vocabulary for every `Unauthorized` condition (SPEC §15). */
export const UnauthorizedReason = Schema.Literals([
  "authenticationRefused",
  "principalMismatch",
  "clientPrincipalMismatch",
  "wildcardPrincipal",
  "httpUnauthorized"
])
export type UnauthorizedReason = typeof UnauthorizedReason.Type

/**
 * The request's principal could not be established, disagrees with the replica, or does not own
 * `clientId`. `reason` is optional on the wire so a new client can decode an old server's error.
 */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class Unauthorized extends Schema.TaggedError<Unauthorized>()("Unauthorized", {
  reason: Schema.optionalKey(UnauthorizedReason),
  message: Schema.String
}) {}

/** App policy refused this client. Stop sync until reload, without changing local storage. */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class ClientOutdated extends Schema.TaggedError<ClientOutdated>()("ClientOutdated", {
  message: Schema.String
}) {}

/**
 * Request-level push rejection (INV-MUT-7): an envelope in the batch is malformed (e.g.
 * `seq < 1`), so the whole push is refused; nothing is executed or ledgered.
 */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class MalformedEnvelope extends Schema.TaggedError<MalformedEnvelope>()("MalformedEnvelope", {
  clientId: ClientId,
  mutationId: MutationId,
  message: Schema.String
}) {}

/**
 * Transient server infrastructure failure on `pull` or `pokes` (SPEC §10): retry with backoff.
 * A defect from the server is always a bug; push reports this case as a short result list.
 */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class ServerUnavailable extends Schema.TaggedError<ServerUnavailable>()("ServerUnavailable", {
  message: Schema.String
}) {}

/** A hint that something may have changed; never data (INV-LOOP-7). Wakes the client's sync loop. */
export const Poke = Schema.TaggedStruct("poke", {})

/** Liveness only (INV-LOOP-7): proves the stream is alive and never wakes the sync loop. */
export const Heartbeat = Schema.TaggedStruct("heartbeat", {})

/** One `pokes` stream element; the first element of every stream is a `Poke`. */
export const PokesElement = Schema.Union([Poke, Heartbeat])

export const SyncRpcs = RpcGroup.make(
  Rpc.make("push", {
    payload: PushRequest,
    success: PushResponse,
    error: Schema.Union([MalformedEnvelope, Unauthorized, ClientOutdated, ResnapshotRequired])
  }),
  Rpc.make("pull", {
    payload: PullRequest,
    success: PullResponse,
    error: Schema.Union([ResnapshotRequired, ServerUnavailable, Unauthorized, ClientOutdated]),
    stream: true
  }),
  Rpc.make("pokes", {
    /** `principal` as on `PushRequest` (SPEC §5). */
    payload: {
      clientId: ClientId,
      principal: Schema.optionalKey(PrincipalId),
      epoch: Schema.optionalKey(Schema.NullOr(Schema.String))
    },
    success: PokesElement,
    error: Schema.Union([ServerUnavailable, Unauthorized, ClientOutdated, ResnapshotRequired]),
    stream: true
  })
)

// ---------------------------------------------------------------------------------------------
// Presence (SPEC §10): in-memory, connection-scoped, never on the durable path
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
 * One `room` stream element: a `RoomSnapshot` (the first element of every stream that installed its
 * entry; a superseded join, with a revision lower than the stored entry's, ends with no element) or a
 * `Heartbeat` (SPEC §10.2), which is never a snapshot and
 * changes no room state.
 */
export const RoomElement = Schema.Union([RoomSnapshot, Heartbeat])
export type RoomElement = typeof RoomElement.Type

/** The entry's identity and state. `room` is an access scope the principal must be able to read. */
export const PresencePayload = Schema.Struct({
  clientId: ClientId,
  epoch: Schema.optionalKey(Schema.NullOr(Schema.String)),
  /** As on `PushRequest` (SPEC §5); not part of the entry. */
  principal: Schema.optionalKey(PrincipalId),
  room: AccessScopeId,
  tabId: TabId,
  state: Schema.Json,
  /**
   * Tab-assigned, never decreasing per `(room, tab)` for the life of the tab client (SPEC §10.3
   * revision fencing): `track` is a no-op unless it is greater than the entry's; a join installs its
   * `(state, revision)` when it is at least the entry's and is a superseded attempt otherwise. A late
   * request therefore never displaces a newer state.
   */
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
})

/**
 * The principal holds no grant in the room's scope (SPEC §10.3 Presence). Unlike `Unauthorized` (a
 * credential failure, retried), this is a fact about access, and the only error that ends a presence stream.
 */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
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
    error: Schema.Union([ServerUnavailable, Unauthorized, Forbidden, ClientOutdated, ResnapshotRequired]),
    stream: true
  }),
  /**
   * Replaces the state of this `(clientId, tabId)` entry in `room`; a no-op without one, or when `revision`
   * is not greater than the entry's.
   */
  Rpc.make("track", {
    payload: PresencePayload,
    error: Schema.Union([ServerUnavailable, Unauthorized, ClientOutdated, ResnapshotRequired])
  })
)
