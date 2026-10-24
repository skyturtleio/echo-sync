/** SPEC §15: the status and telemetry projections of typed sync failures. */
import type { PullFailure, PushFailure, SyncErrorKind } from "../client/sync/SyncFailure.ts"
import type { Outcome } from "./Telemetry.ts"

const failures = {
  CredentialsUnavailable: { kind: "credentials", outcome: "credentials" },
  Unauthorized: { kind: "unauthorized", outcome: "unauthorized" },
  ClientOutdated: { kind: "outdated", outcome: "outdated" },
  MalformedEnvelope: { kind: "server", outcome: "halted" },
  PushIntegrityError: { kind: "server", outcome: "halted" },
  RpcClientError: { kind: "network", outcome: "network" },
  StoreUnavailable: { kind: "server", outcome: "server" },
  ConstraintViolated: { kind: "server", outcome: "server" },
  ResnapshotRequired: { kind: "server", outcome: "resnapshot" },
  ServerUnavailable: { kind: "unavailable", outcome: "unavailable" },
  PullProtocolError: { kind: "server", outcome: "server" },
  MalformedChange: { kind: "server", outcome: "malformed" },
  ServerOutdated: { kind: "server-outdated", outcome: "server-outdated" }
} satisfies Record<(PushFailure | PullFailure)["_tag"], { readonly kind: SyncErrorKind; readonly outcome: Outcome }>

export const failureMeaning = (error: PushFailure | PullFailure) => failures[error._tag]

/** Payloads inside successful responses are not failures; operation aggregation stays in §14. */
export const failureOutcome = (error: PushFailure | PullFailure | { readonly _tag: "LocallyRejected" }): Outcome =>
  error._tag === "LocallyRejected" ? "rejected" : failureMeaning(error).outcome
