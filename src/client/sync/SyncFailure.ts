/**
 * Every way a sync cycle can fail, and what each failure means (DESIGN §6, §9.1).
 *
 * One closed union, classified exhaustively in one place: the public status `kind`, whether a push
 * halts, and the telemetry outcome. The transport is the only module that sees RPC and HTTP errors; it
 * builds a `SyncTransportError` whose `reason` names what happened (the innermost evidence already
 * lifted out: a credential failure or an HTTP 401/403 inside an `RpcClientError`). The engine adds its
 * own local failures. `SyncLoop` and telemetry only read the verdicts here, so a new reason does not
 * compile until every verdict covers it.
 */
import { Cause, Data, Match, Option, Schema } from "effect"
import type { RpcClientError } from "effect/rpc"
import type { Errors, Protocol } from "echo-sync"

/**
 * `syncHeaders` raised (a defect: its error channel is `never`) while the leader was preparing a
 * request (DESIGN §9.1 Credentials). Local and transient, like a network failure: nothing was sent.
 */
export class CredentialsUnavailable extends Schema.TaggedError<CredentialsUnavailable>()("CredentialsUnavailable", {
  message: Schema.String
}) {}

/** The push batch cannot be sent as is, or the server ended it with a defect. Fatal. */
export class PushIntegrityError extends Schema.TaggedError<PushIntegrityError>()("PushIntegrityError", {
  reason: Schema.Literals(["invalidPayload", "serverDefect"]),
  message: Schema.String
}) {}

/** The pull call ended in a defect. Transient: nothing was installed. */
export class PullProtocolError extends Schema.TaggedError<PullProtocolError>()("PullProtocolError", {
  message: Schema.String
}) {}

/**
 * A pull change for a table this client knows does not decode with that table's schema. The
 * install transaction fails as a whole (no rows, acks, cursor or staging change). Per DESIGN §8
 * and §6 it is a transient pull failure: the sync loop retries with backoff, never halts.
 */
export class MalformedChange extends Schema.TaggedError<MalformedChange>()("MalformedChange", {
  table: Schema.String,
  message: Schema.String
}) {}

/** A known table's server payload lacks a declared wire column. Retry, never rebuild. */
export class ServerOutdated extends Schema.TaggedError<ServerOutdated>()("ServerOutdated", {
  table: Schema.String
}) {}

/**
 * What a transport failure was:
 * - `CredentialsUnavailable`: `syncHeaders` raised; nothing was sent.
 * - `Unauthorized`: the server rejected the credential (its `Unauthorized`, or HTTP 401/403).
 * - `ServerUnavailable`: the server reported a transient infrastructure failure.
 * - `ClientOutdated`: compatibility policy refused this client (DESIGN §11).
 * - `MalformedEnvelope`: the server refused the push batch (DESIGN §6).
 * - `PushIntegrityError`: the batch is not a valid push request, or the push ended in a defect.
 * - `PullProtocolError`: the pull ended in a defect.
 * - `RpcClientError`: no usable RPC result (transport, HTTP, an empty or unreadable response).
 */
export type TransportReason =
  | CredentialsUnavailable
  | Protocol.Unauthorized
  | Protocol.ServerUnavailable
  | Protocol.ClientOutdated
  | Protocol.MalformedEnvelope
  | PushIntegrityError
  | PullProtocolError
  | RpcClientError.RpcClientError

/** The one error a sync transport fails with (`ResnapshotRequired` aside, which the engine handles). */
export class SyncTransportError extends Data.TaggedError("SyncTransportError")<{ readonly reason: TransportReason }> {
  override get message(): string {
    return `${this.reason._tag}: ${this.reason.message}`
  }
}

/** A failure of `Engine.push`. */
export type PushFailure = Errors.StoreUnavailable | Errors.ConstraintViolated | SyncTransportError

/** A failure of `Engine.pull`. */
export type PullFailure = PushFailure | Protocol.ResnapshotRequired | MalformedChange | ServerOutdated

/**
 * What kind of failure `lastError` reports (DESIGN §9.1), so an app can tell "sign in again"
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

/**
 * What one failure means. `halt` is set exactly for push integrity failures (DESIGN §6): pushing stops
 * for this leader, with that library-controlled reason. `outcome` is the telemetry vocabulary
 * (docs/observability.md), which names some failures more finely than `kind`.
 */
interface Meaning {
  readonly kind: SyncErrorKind
  readonly outcome: string
  readonly halt: Option.Option<string>
}

const as = (kind: SyncErrorKind, outcome: string = kind): Meaning => ({ kind, outcome, halt: Option.none() })
const halts = (halt: string): Meaning => ({ kind: "server", outcome: "halted", halt: Option.some(halt) })

const meaningOf = (failure: PullFailure): Meaning =>
  Match.valueTags(failure, {
    StoreUnavailable: () => as("server"),
    ConstraintViolated: () => as("server"),
    ResnapshotRequired: () => as("server", "resnapshot"),
    MalformedChange: () => as("server", "malformed"),
    ServerOutdated: () => as("server-outdated"),
    SyncTransportError: ({ reason }) =>
      Match.valueTags(reason, {
        CredentialsUnavailable: () => as("credentials"),
        Unauthorized: () => as("unauthorized"),
        ServerUnavailable: () => as("unavailable"),
        ClientOutdated: () => as("outdated"),
        RpcClientError: () => as("network"),
        PullProtocolError: () => as("server"),
        MalformedEnvelope: () => halts("the server refused the push batch as malformed"),
        PushIntegrityError: (error) =>
          halts(
            error.reason === "invalidPayload"
              ? "the outbox batch is not a valid push request"
              : "the server ended the push with a defect"
          )
      })
  })

export interface Verdict {
  readonly kind: SyncErrorKind
  /** The `Halted` reason when this failure stops pushing (DESIGN §6); only a push can halt. */
  readonly halt: Option.Option<string>
}

/**
 * Whole-cause verdict (DESIGN §6: defect > integrity > anything else), never just the first failure:
 * a defect anywhere halts a push, even beside a transient failure, since an RPC request decode failure
 * arrives as an untagged defect. A pull's caller ignores `halt` (a pull defect is transient). The cause
 * must not be interruption-only; interruption is the caller's to re-raise.
 */
export const classify = (cause: Cause.Cause<PullFailure>): Verdict => {
  const failures = cause.reasons.filter(Cause.isFailReason).map((reason) => meaningOf(reason.error))
  const fatal = failures.find((meaning) => Option.isSome(meaning.halt))
  const dies = Cause.hasDies(cause)
  const first = fatal ?? failures[0]
  return {
    kind: dies || first === undefined ? "server" : first.kind,
    halt: fatal !== undefined ? fatal.halt : dies ? Option.some("push failed with a defect") : Option.none()
  }
}

/** The telemetry outcome of a typed failure cause: `halted` if any failure halts, else the first's. */
export const outcome = (cause: Cause.Cause<PullFailure>): string => {
  const failures = cause.reasons.filter(Cause.isFailReason).map((reason) => meaningOf(reason.error))
  return (failures.find((meaning) => Option.isSome(meaning.halt)) ?? failures[0])?.outcome ?? "server"
}
