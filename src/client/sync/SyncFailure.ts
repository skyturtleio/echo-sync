/**
 * Every way a sync cycle can fail, and what each failure means (SPEC §10).
 *
 * Transport failures retain their original tags. The RPC-derived unions make additions require a
 * catalog mapping; whole-cause precedence remains separate from each error's meaning (SPEC §15).
 */
import { Cause, Match, Option, Schema } from "effect"
import type { Outcome } from "../../internal/Telemetry.ts"
import type { Rpc, RpcClientError, RpcGroup } from "effect/rpc"
import { type Errors, Protocol } from "echo-sync"
import { failureMeaning } from "../../internal/ErrorCatalog.ts"

/**
 * `syncHeaders` raised (a defect: its error channel is `never`) while the leader was preparing a
 * request (INV-LOOP-8 Credentials). Local and transient, like a network failure: nothing was sent.
 */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class CredentialsUnavailable extends Schema.TaggedError<CredentialsUnavailable>()("CredentialsUnavailable", {
  message: Schema.String
}) {}

/** The push batch cannot be sent as is, or the server ended it with a defect. Fatal. */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class PushIntegrityError extends Schema.TaggedError<PushIntegrityError>()("PushIntegrityError", {
  reason: Schema.Literals(["invalidPayload", "serverDefect"]),
  message: Schema.String
}) {}

/** The pull call ended in a defect. Transient: nothing was installed. */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class PullProtocolError extends Schema.TaggedError<PullProtocolError>()("PullProtocolError", {
  message: Schema.String
}) {}

/**
 * A pull change for a table this client knows does not decode with that table's schema. The
 * install transaction fails as a whole (no rows, acks, cursor or staging change). Per INV-CLI-9
 * and SPEC §10 it is a transient pull failure: the sync loop retries with backoff, never halts.
 */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class MalformedChange extends Schema.TaggedError<MalformedChange>()("MalformedChange", {
  table: Schema.String,
  message: Schema.String
}) {}

/** A known table's server payload lacks a declared wire column. Retry, never rebuild. */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class ServerOutdated extends Schema.TaggedError<ServerOutdated>()("ServerOutdated", {
  table: Schema.String
}) {}

/**
 * Why a push failed in the transport:
 * - `CredentialsUnavailable`: `syncHeaders` raised; nothing was sent.
 * - `Unauthorized`: the server rejected the credential (its `Unauthorized`, or HTTP 401/403).
 * - `ClientOutdated`: compatibility policy refused this client (SPEC §0.2).
 * - `MalformedEnvelope`: the server refused the batch (INV-LOOP-3). Halts push.
 * - `PushIntegrityError`: the batch is not a valid push request, or the push ended in a defect. Halts push.
 * - `RpcClientError`: no usable RPC result (transport, HTTP, an empty or unreadable response).
 */
export type PushReason =
  | CredentialsUnavailable
  | Rpc.ErrorExit<Extract<RpcGroup.Rpcs<typeof Protocol.SyncRpcs>, { readonly _tag: "push" }>>
  | PushIntegrityError
  | RpcClientError.RpcClientError

/**
 * Why a pull or a pokes stream failed in the transport:
 * - `CredentialsUnavailable`, `Unauthorized`, `ClientOutdated`, `RpcClientError`: as for a push.
 * - `ServerUnavailable`: the server reported a transient infrastructure failure.
 * - `PullProtocolError`: the pull ended in a defect.
 */
export type PullReason =
  | CredentialsUnavailable
  | Rpc.ErrorExit<Extract<RpcGroup.Rpcs<typeof Protocol.SyncRpcs>, { readonly _tag: "pull" | "pokes" }>>
  | PullProtocolError
  | RpcClientError.RpcClientError

/** The safe diagnostic reason an `Unauthorized` carries, if it decided a transport failure. */
export const unauthorizedReason = (
  cause: Cause.Cause<PushFailure | PullFailure>
): Option.Option<Protocol.UnauthorizedReason> => {
  for (const failure of failures(cause)) {
    if (failure instanceof Protocol.Unauthorized && failure.reason !== undefined) return Option.some(failure.reason)
  }
  return Option.none()
}

/** A failure of `Engine.push`. */
export type PushFailure = Errors.StoreUnavailable | Errors.ConstraintViolated | PushReason

/** A failure of `Engine.pull`. */
export type PullFailure =
  | Errors.StoreUnavailable
  | Errors.ConstraintViolated
  | PullReason
  | MalformedChange
  | ServerOutdated

/**
 * What kind of failure `lastError` reports (SPEC §10), so an app can tell "sign in again"
 * from "you are offline" without parsing text:
 * - `credentials`: `syncHeaders` raised; nothing was sent.
 * - `unauthorized`: the server rejected the credential (`Unauthorized` or HTTP 401/403).
 * - `unavailable`: the server reported `ServerUnavailable`.
 * - `network`: no usable RPC result (transport, HTTP, or a response body that was empty, incomplete
 *   or unreadable after an HTTP response arrived).
 * - `server`: the fallback for any other transient failure: a stopped push batch, a pull defect, a
 *   local `StoreUnavailable`.
 * - `outdated`: compatibility policy refused this client; sync stays stopped until a new leader.
 * - `server-outdated`: a known-table payload lacks a declared column; back off without rebuilding.
 */
export const SyncErrorKind = Schema.Literals([
  "credentials",
  "unauthorized",
  "unavailable",
  "network",
  "server",
  "outdated",
  "server-outdated"
])
export type SyncErrorKind = typeof SyncErrorKind.Type

/** What one failure means: its status kind and its telemetry outcome (SPEC §14), which is finer. */
interface Meaning {
  readonly kind: SyncErrorKind
  readonly outcome: Outcome
}

/** A push failure's meaning; `halt` is the library-controlled `Halted` reason when it stops pushing (INV-LOOP-3). */
interface PushMeaning extends Meaning {
  readonly halt: Option.Option<string>
}

const pushMeaning = (failure: PushFailure): PushMeaning => ({
  ...failureMeaning(failure),
  halt: Match.valueTags(failure, {
    StoreUnavailable: () => Option.none(),
    ConstraintViolated: () => Option.none(),
    CredentialsUnavailable: () => Option.none(),
    Unauthorized: () => Option.none(),
    ClientOutdated: () => Option.none(),
    ResnapshotRequired: () => Option.none(),
    RpcClientError: () => Option.none(),
    MalformedEnvelope: () => Option.some("the server refused the push batch as malformed"),
    PushIntegrityError: (error) =>
      Option.some(
        error.reason === "invalidPayload"
          ? "the outbox batch is not a valid push request"
          : "the server ended the push with a defect"
      )
  })
})

const failures = <E>(cause: Cause.Cause<E>): ReadonlyArray<E> =>
  cause.reasons.filter(Cause.isFailReason).map((reason) => reason.error)

/**
 * `kind` and `halt` decide the status; `outcome` names the typed failures for telemetry (interruption and
 * defects are named by `Telemetry.outcome` before it is consulted).
 */
export interface PushVerdict {
  readonly kind: SyncErrorKind
  /** The `Halted` reason when this push stops pushing (INV-LOOP-3). */
  readonly halt: Option.Option<string>
  readonly outcome: Outcome
}

/**
 * Whole-cause verdict on a push (INV-LOOP-5: defect > integrity > first failure), never just its first
 * failure: a defect anywhere halts, even beside a transient failure, since an RPC request decode failure
 * arrives as an untagged defect. Interruption is the caller's to handle first.
 */
export const classifyPush = (cause: Cause.Cause<PushFailure>): PushVerdict => {
  const meanings = failures(cause).map(pushMeaning)
  const halting = meanings.find((m) => Option.isSome(m.halt))
  const decisive = halting ?? meanings[0]
  const dies = Cause.hasDies(cause)
  return {
    kind: dies || decisive === undefined ? "server" : decisive.kind,
    halt: halting !== undefined ? halting.halt : dies ? Option.some("push failed with a defect") : Option.none(),
    outcome: decisive?.outcome ?? "server"
  }
}

export interface PullVerdict {
  readonly kind: SyncErrorKind
  readonly outcome: Outcome
}

/** Verdict on a pull: its first failure; a defect anywhere is a transient `server` failure (it never halts). */
export const classifyPull = (cause: Cause.Cause<PullFailure>): PullVerdict => {
  const first = failures(cause).map(failureMeaning)[0]
  return {
    kind: Cause.hasDies(cause) || first === undefined ? "server" : first.kind,
    outcome: first?.outcome ?? "server"
  }
}
