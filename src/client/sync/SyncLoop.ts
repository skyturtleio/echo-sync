/**
 * The leader's sync loop (DESIGN §8, §9): runs `Engine.push` then `Engine.pull` whenever woken (local
 * enqueue, poke, timer) and retries transient failures with backoff.
 *
 * Fatal push (DESIGN §6, a68a727): a push error the caller's `isFatalPush` classifies as an integrity
 * failure (`MalformedEnvelope`, or the server failing to decode the push payload) halts pushing for the
 * lifetime of this loop: no automatic retry, no outbox entry is released (that would leave a seq gap,
 * §16.5). Pull keeps running. The halt is surfaced through `status`, and the pending outbox stays
 * exportable. The engine writes nothing durable when its transport fails, so classifying in the caller
 * (who owns the transport's error union) loses nothing.
 */
import { Duration, Effect, Option, Queue, Schema, type Stream, SubscriptionRef } from "effect"
import type * as Engine from "./Engine.ts"

export const PushState = Schema.Union([
  Schema.TaggedStruct("Active", {}),
  /** Integrity failure; pushing stays stopped until the leader restarts. */
  Schema.TaggedStruct("Halted", { message: Schema.String })
])
export type PushState = typeof PushState.Type

export const SyncStatus = Schema.Struct({
  push: PushState,
  /** Last transient failure, cleared by the next cycle that completes cleanly. */
  lastError: Schema.NullOr(Schema.String)
})
export type SyncStatus = typeof SyncStatus.Type

export interface SyncLoop {
  /** Request a cycle soon (coalesced: many wakes while one is queued run one cycle). */
  readonly wake: Effect.Effect<void>
  readonly status: Effect.Effect<SyncStatus>
  readonly changes: Stream.Stream<SyncStatus>
  /** Runs forever; fork it in the leader's scope. Fails only with defects. */
  readonly run: Effect.Effect<never>
}

const describe = (error: unknown): string =>
  error instanceof Error ? `${error.name}: ${error.message}` : typeof error === "string" ? error : JSON.stringify(error)

export const make = <E, TE>(options: {
  readonly engine: Engine.Engine<E, TE>
  /** Whether a push failure is an integrity error that must stop pushing (see module doc). */
  readonly isFatalPush: (error: E | TE) => boolean
  /** First retry delay after a transient failure; doubles up to `maxRetryDelay`. */
  readonly retryDelay?: Duration.Input
  readonly maxRetryDelay?: Duration.Input
}): Effect.Effect<SyncLoop> =>
  Effect.gen(function* () {
    const { engine } = options
    const base = Duration.toMillis(Duration.fromInputUnsafe(options.retryDelay ?? "1 second"))
    const cap = Duration.toMillis(Duration.fromInputUnsafe(options.maxRetryDelay ?? "30 seconds"))
    const status = yield* SubscriptionRef.make<SyncStatus>({ push: { _tag: "Active" }, lastError: null })
    const wakes = yield* Queue.sliding<void>(1)

    /** One push+pull cycle; returns whether it hit a transient failure. */
    const cycle = Effect.gen(function* () {
      let transient = Option.none<string>()
      if ((yield* SubscriptionRef.get(status)).push._tag === "Active") {
        const pushed = yield* Effect.result(engine.push)
        if (pushed._tag === "Failure") {
          if (options.isFatalPush(pushed.failure)) {
            const message = describe(pushed.failure)
            yield* Effect.logError(`echo-sync: push halted by an integrity error: ${message}`)
            const halted: PushState = { _tag: "Halted", message }
            yield* SubscriptionRef.update(status, (s) => ({ ...s, push: halted }))
          } else {
            transient = Option.some(describe(pushed.failure))
          }
        } else if (pushed.success.stopped) {
          transient = Option.some("server stopped the push batch at a transient failure")
        }
      }
      const pulled = yield* Effect.result(engine.pull)
      if (pulled._tag === "Failure" && Option.isNone(transient)) transient = Option.some(describe(pulled.failure))
      yield* SubscriptionRef.update(status, (s) => ({ ...s, lastError: Option.getOrNull(transient) }))
      return Option.isSome(transient)
    })

    const run = Effect.gen(function* () {
      let delay = base
      while (true) {
        const failed = yield* cycle
        if (failed) {
          // Retry after the backoff unless something wakes the loop first.
          yield* Effect.raceFirst(Queue.take(wakes), Effect.sleep(Duration.millis(delay)))
          delay = Math.min(delay * 2, cap)
        } else {
          delay = base
          yield* Queue.take(wakes)
        }
      }
    })

    return {
      wake: Effect.asVoid(Queue.offer(wakes, undefined)),
      status: SubscriptionRef.get(status),
      changes: SubscriptionRef.changes(status),
      run
    }
  })
