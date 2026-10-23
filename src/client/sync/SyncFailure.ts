/**
 * Every way a sync cycle can fail, and what each failure means (DESIGN §6, §9.1).
 *
 * Two closed unions, one per direction, each classified exhaustively in one place: the public status
 * `kind`, whether a push halts (only a push can), and the telemetry outcome. The transport is the only
 * module that sees RPC and HTTP errors; it builds a `PushTransportError` or `PullTransportError` whose
 * `reason` names what happened (the innermost evidence already
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
 * Why a push failed in the transport:
 * - `CredentialsUnavailable`: `syncHeaders` raised; nothing was sent.
 * - `Unauthorized`: the server rejected the credential (its `Unauthorized`, or HTTP 401/403).
 * - `ClientOutdated`: compatibility policy refused this client (DESIGN §11).
 * - `MalformedEnvelope`: the server refused the batch (DESIGN §6). Halts push.
 * - `PushIntegrityError`: the batch is not a valid push request, or the push ended in a defect. Halts push.
 * - `RpcClientError`: no usable RPC result (transport, HTTP, an empty or unreadable response).
 */
export type PushReason =
  | CredentialsUnavailable
  | Protocol.Unauthorized
  | Protocol.ClientOutdated
  | Protocol.MalformedEnvelope
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
  | Protocol.Unauthorized
  | Protocol.ClientOutdated
  | Protocol.ServerUnavailable
  | PullProtocolError
  | RpcClientError.RpcClientError

/** The transport's push failure. */
export class PushTransportError extends Data.TaggedError("PushTransportError")<{ readonly reason: PushReason }> {
  override get message(): string {
    return `${this.reason._tag}: ${this.reason.message}`
  }
}

/** The transport's pull or pokes failure (`ResnapshotRequired` aside, which the engine handles). */
export class PullTransportError extends Data.TaggedError("PullTransportError")<{ readonly reason: PullReason }> {
  override get message(): string {
    return `${this.reason._tag}: ${this.reason.message}`
  }
}

/** A failure of `Engine.push`. */
export type PushFailure = Errors.StoreUnavailable | Errors.ConstraintViolated | PushTransportError

/** A failure of `Engine.pull`. */
export type PullFailure =
  | Errors.StoreUnavailable
  | Errors.ConstraintViolated
  | PullTransportError
  | Protocol.ResnapshotRequired
  | MalformedChange
  | ServerOutdated

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

/** What one failure means: its status kind and its telemetry outcome (docs/observability.md), which is finer. */
interface Meaning {
  readonly kind: SyncErrorKind
  readonly outcome: string
}

/** A push failure's meaning; `halt` is the library-controlled `Halted` reason when it stops pushing (DESIGN §6). */
interface PushMeaning extends Meaning {
  readonly halt: Option.Option<string>
}

const meaning = (kind: SyncErrorKind, outcome: string = kind): Meaning => ({ kind, outcome })
const continues = (kind: SyncErrorKind): PushMeaning => ({ kind, outcome: kind, halt: Option.none() })
const halts = (halt: string): PushMeaning => ({ kind: "server", outcome: "halted", halt: Option.some(halt) })

const pushMeaning = (failure: PushFailure): PushMeaning =>
  Match.valueTags(failure, {
    StoreUnavailable: () => continues("server"),
    ConstraintViolated: () => continues("server"),
    PushTransportError: ({ reason }) =>
      Match.valueTags(reason, {
        CredentialsUnavailable: () => continues("credentials"),
        Unauthorized: () => continues("unauthorized"),
        ClientOutdated: () => continues("outdated"),
        RpcClientError: () => continues("network"),
        MalformedEnvelope: () => halts("the server refused the push batch as malformed"),
        PushIntegrityError: (error) =>
          halts(
            error.reason === "invalidPayload"
              ? "the outbox batch is not a valid push request"
              : "the server ended the push with a defect"
          )
      })
  })

const pullMeaning = (failure: PullFailure): Meaning =>
  Match.valueTags(failure, {
    StoreUnavailable: () => meaning("server"),
    ConstraintViolated: () => meaning("server"),
    ResnapshotRequired: () => meaning("server", "resnapshot"),
    MalformedChange: () => meaning("server", "malformed"),
    ServerOutdated: () => meaning("server-outdated"),
    PullTransportError: ({ reason }) =>
      Match.valueTags(reason, {
        CredentialsUnavailable: () => meaning("credentials"),
        Unauthorized: () => meaning("unauthorized"),
        ClientOutdated: () => meaning("outdated"),
        RpcClientError: () => meaning("network"),
        ServerUnavailable: () => meaning("unavailable"),
        PullProtocolError: () => meaning("server")
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
  /** The `Halted` reason when this push stops pushing (DESIGN §6). */
  readonly halt: Option.Option<string>
  readonly outcome: string
}

/**
 * Whole-cause verdict on a push (DESIGN §6: defect > integrity > first failure), never just its first
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
  readonly outcome: string
}

/** Verdict on a pull: its first failure; a defect anywhere is a transient `server` failure (it never halts). */
export const classifyPull = (cause: Cause.Cause<PullFailure>): PullVerdict => {
  const first = failures(cause).map(pullMeaning)[0]
  return {
    kind: Cause.hasDies(cause) || first === undefined ? "server" : first.kind,
    outcome: first?.outcome ?? "server"
  }
}
