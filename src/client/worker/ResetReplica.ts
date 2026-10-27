/**
 * `resetReplica` (SPEC §12): delete one identity's replica, for example on sign-out.
 *
 * A static function, not a `LeaderClient` method: it also runs when no client for the identity is alive.
 * It closes the identity's tab clients itself (INV-BRW-7): once the reset worker is spawned it broadcasts
 * `ResetSignal` on `Platform.resetChannelName(identity)`, and every `LeaderClient` of the identity in this
 * browser, the caller's own included, enters its terminal reset state and terminates its leader candidate.
 * The worker then waits at most `closeTimeout` (default 5 s) for the leader lock; a leader that does not let
 * go in time makes the reset fail with `ReplicaInUse` and touch nothing. The work runs in a leader-candidate
 * worker started in reset mode (the app's own `spawnLeader`), so the OPFS pool is opened through the same host
 * the leader uses; see `reset` in `Leader.ts`.
 *
 * Interrupting the reset terminates that worker, and the browser then releases its lock and access
 * handles. The removal is durable across that (INV-BRW-8): once the pending decision is made, the worker
 * writes an engine-owned tombstone before touching the directory and removes it last. Interrupted before the
 * tombstone, the replica is intact; after it, the next leader of the identity (before opening its pool) or the
 * next reset (answering `Done` without a pending count) completes the removal, and a leader then starts on a
 * fresh replica with a new clientId.
 */
import * as Data from "effect/Data"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import { isFromLeader, type ResetOutcome, type ResetSignal, type ToLeader } from "./messages.ts"
import * as Platform from "./platform.ts"

/**
 * A leader still held the identity's leader lock after `closeTimeout` (a tab that did not honour the reset
 * signal: an old build, a frozen tab); nothing was touched.
 */
export class ReplicaInUse extends Data.TaggedError("ReplicaInUse")<{}> {}

/** The outbox holds `count` unreleased mutations and `discardPending` was not set; nothing was deleted. */
export class PendingWrites extends Data.TaggedError("PendingWrites")<{ readonly count: number }> {}

/** The reset could not run or did not finish (unsupported browser, invalid identity, an OPFS error). */
export class ResetFailed extends Data.TaggedError("ResetFailed")<{ readonly message: string }> {}

export interface ResetReplicaConfig extends Platform.ReplicaIdentity {
  /** The same factory the app passes to `LeaderClient.make`. */
  readonly spawnLeader: (name: string) => Worker
}

export interface ResetReplicaOptions {
  /** Skip the outbox check and delete pending writes too; the database is never opened. */
  readonly discardPending?: true
  /**
   * How long to wait for the identity's clients to let go of the leader lock after the reset signal before
   * failing `ReplicaInUse`; the whole budget for the lock request, so a near-zero value can refuse even a free
   * lock. Must be finite (an infinite one fails `ResetFailed`, nothing touched). Default 5 s.
   */
  readonly closeTimeout?: Duration.Input
}

export const resetReplica = (
  config: ResetReplicaConfig,
  options: ResetReplicaOptions = {}
): Effect.Effect<void, ReplicaInUse | PendingWrites | ResetFailed> =>
  Effect.gen(function* () {
    const identity: Platform.ReplicaIdentity = { appId: config.appId, profile: config.profile }
    const invalid = Platform.validateIdentity(identity) ?? Platform.tabCapability().unsupported
    if (invalid !== undefined) return yield* new ResetFailed({ message: invalid })
    // Bounded by contract (INV-BRW-7): an unhonoured signal must surface as `ReplicaInUse`, never as a reset
    // that waits forever. Refused before anything is spawned or broadcast.
    // `Duration` reads a numeric NaN as 0 and throws on malformed input: both are refused here, typed.
    const closeTimeout = options.closeTimeout ?? "5 seconds"
    const closeTimeoutMillis = yield* Effect.try({
      try: () =>
        typeof closeTimeout === "number" && Number.isNaN(closeTimeout) ? NaN : Duration.toMillis(closeTimeout),
      catch: () => new ResetFailed({ message: "closeTimeout is not a valid duration" })
    })
    if (!Number.isFinite(closeTimeoutMillis)) {
      return yield* new ResetFailed({ message: "closeTimeout must be finite" })
    }
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
            discardPending: options.discardPending === true,
            closeTimeoutMillis
          }
          const signal: ResetSignal = { _tag: "ResetSignal", appId: config.appId, profile: config.profile }
          // A throwing postMessage is a failed reset, not a defect; interruption is never caught here. The
          // listeners are added inside the thunk, so the `ensuring` below is installed before they exist.
          return yield* Effect.try({
            try: () => {
              worker.addEventListener("message", onMessage)
              worker.addEventListener("error", onError)
              // Only once the reset worker exists, so a `spawnLeader` that throws closes no client.
              const channel = new BroadcastChannel(Platform.resetChannelName(identity))
              try {
                // oxlint-disable-next-line unicorn/require-post-message-target-origin -- BroadcastChannel has no target origin
                channel.postMessage(signal)
              } finally {
                channel.close()
              }
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
