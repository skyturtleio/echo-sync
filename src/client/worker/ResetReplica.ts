/**
 * `resetReplica` (DESIGN §9.1, §16 item 11a): delete one identity's replica, for example on sign-out.
 *
 * A static function, not a `LeaderClient` method: it must run while no client for the identity is alive.
 * The app closes its tab clients first (its own cross-tab sign-out signal); a live leader makes the reset
 * fail with `ReplicaInUse` and touch nothing. The work runs in a leader-candidate worker started in reset
 * mode (the app's own `spawnLeader`), so the OPFS pool is opened through the same host the leader uses;
 * see `reset` in `Leader.ts`.
 *
 * Interrupting the reset terminates that worker, and the browser then releases its lock and access
 * handles. A removal it had already begun may be partial; what a leader makes of such a directory is not
 * established, but a reset with `discardPending` never opens the database, so it can be retried on it.
 */
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { isFromLeader, type ResetOutcome, type ToLeader } from "./messages.ts"
import * as Platform from "./platform.ts"

/** A leader holds the identity's leader lock (some tab still has a client open); nothing was touched. */
export class ReplicaInUse extends Schema.TaggedError<ReplicaInUse>()("ReplicaInUse", {}) {}

/** The outbox holds `count` unreleased mutations and `discardPending` was not set; nothing was deleted. */
export class PendingWrites extends Schema.TaggedError<PendingWrites>()("PendingWrites", { count: Schema.Int }) {}

/** The reset could not run or did not finish (unsupported browser, invalid identity, an OPFS error). */
export class ResetFailed extends Schema.TaggedError<ResetFailed>()("ResetFailed", { message: Schema.String }) {}

export interface ResetReplicaConfig extends Platform.ReplicaIdentity {
  /** The same factory the app passes to `LeaderClient.make`. */
  readonly spawnLeader: (name: string) => Worker
}

export const resetReplica = (
  config: ResetReplicaConfig,
  options: { readonly discardPending?: true } = {}
): Effect.Effect<void, ReplicaInUse | PendingWrites | ResetFailed> =>
  Effect.gen(function* () {
    const identity: Platform.ReplicaIdentity = { appId: config.appId, profile: config.profile }
    const invalid = Platform.validateIdentity(identity) ?? Platform.tabCapability().unsupported
    if (invalid !== undefined) return yield* new ResetFailed({ message: invalid })
    const outcome = yield* Effect.acquireUseRelease(
      // A synchronous throw (a bad worker URL, a CSP refusal) is a failed reset, not a defect.
      Effect.try({
        try: () => config.spawnLeader(`echo:${config.appId}:${config.profile}:reset`),
        catch: (cause) => new ResetFailed({ message: `spawnLeader failed: ${String(cause)}` })
      }),
      (worker) =>
        Effect.gen(function* () {
          const answered = yield* Deferred.make<ResetOutcome>()
          const onMessage = (event: MessageEvent) => {
            const message: unknown = event.data
            if (isFromLeader(message) && message._tag === "ResetOutcome") {
              Deferred.doneUnsafe(answered, Effect.succeed(message.outcome))
            }
          }
          const onError = (event: ErrorEvent) =>
            Deferred.doneUnsafe(
              answered,
              Effect.succeed({ _tag: "ResetFailed", message: `reset worker failed: ${event.message}` })
            )
          const command: ToLeader = {
            _tag: "Reset",
            appId: config.appId,
            profile: config.profile,
            discardPending: options.discardPending === true
          }
          // A throwing postMessage is a failed reset, not a defect; interruption is never caught here. The
          // listeners are added inside the thunk, so the `ensuring` below is installed before they exist.
          return yield* Effect.try({
            try: () => {
              worker.addEventListener("message", onMessage)
              worker.addEventListener("error", onError)
              worker.postMessage(command, { transfer: [] })
            },
            catch: (cause) => new ResetFailed({ message: `posting to the reset worker failed: ${String(cause)}` })
          }).pipe(
            Effect.andThen(Deferred.await(answered)),
            // On every exit (answer, failure, interruption), not only interruption.
            Effect.ensuring(
              Effect.sync(() => {
                worker.removeEventListener("message", onMessage)
                worker.removeEventListener("error", onError)
              })
            )
          )
        }),
      (worker) => Effect.sync(() => worker.terminate())
    )
    switch (outcome._tag) {
      case "Done":
        return
      case "ReplicaInUse":
        return yield* new ReplicaInUse()
      case "PendingWrites":
        return yield* new PendingWrites({ count: outcome.count })
      case "ResetFailed":
        return yield* new ResetFailed({ message: outcome.message })
    }
  })
