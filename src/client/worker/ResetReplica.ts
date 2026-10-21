/**
 * `resetReplica` (DESIGN §9.1, §16 item 11a): delete one identity's replica, for example on sign-out.
 *
 * A static function, not a `LeaderClient` method: it must run while no client for the identity is alive.
 * The app closes its tab clients first (its own cross-tab sign-out signal); a live leader makes the reset
 * fail with `ReplicaInUse` and touch nothing. The work runs in a leader-candidate worker started in reset
 * mode (the app's own `spawnLeader`), so the OPFS pool is opened through the same host the leader uses;
 * see `reset` in `Leader.ts`.
 *
 * Interrupting the reset terminates that worker. The browser then releases its lock and access handles;
 * a removal it had already begun may be partial, which leaves a replica that the next leader either opens
 * or rebuilds. Retry the reset in that case.
 */
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
      Effect.sync(() => config.spawnLeader(`echo:${config.appId}:${config.profile}:reset`)),
      (worker) =>
        Effect.callback<ResetOutcome>((resume) => {
          const onMessage = (event: MessageEvent) => {
            const message: unknown = event.data
            if (isFromLeader(message) && message._tag === "ResetOutcome") resume(Effect.succeed(message.outcome))
          }
          const onError = (event: ErrorEvent) =>
            resume(Effect.succeed({ _tag: "ResetFailed", message: `reset worker failed: ${event.message}` }))
          worker.addEventListener("message", onMessage)
          worker.addEventListener("error", onError)
          const command: ToLeader = {
            _tag: "Reset",
            appId: config.appId,
            profile: config.profile,
            discardPending: options.discardPending === true
          }
          worker.postMessage(command, { transfer: [] })
          return Effect.sync(() => {
            worker.removeEventListener("message", onMessage)
            worker.removeEventListener("error", onError)
          })
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
