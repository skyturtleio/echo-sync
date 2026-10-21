/**
 * The leader's sync loop (DESIGN §8, §9): runs `Engine.push` then `Engine.pull` whenever woken (local
 * enqueue, poke) and retries transient failures with backoff; `followPokes` keeps the pokes stream up.
 *
 * Fatal push (DESIGN §6, a68a727): a push error the caller's `isFatalPush` classifies as an integrity
 * failure (`MalformedEnvelope`, or the server failing to decode the push payload) halts pushing for the
 * lifetime of this loop: no automatic retry, no outbox entry is released (that would leave a seq gap,
 * §16.5). Pull keeps running. The halt is surfaced through `status`, and the pending outbox stays
 * exportable. The engine writes nothing durable when its transport fails, so classifying in the caller
 * (who owns the transport's error union) loses nothing.
 */
import { Cause, Duration, Effect, Exit, Option, Queue, Schema, Stream, SubscriptionRef } from "effect"
import type { Errors } from "echo-sync"
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

/**
 * Classifies a whole failure `Cause` (DESIGN §6: defect > transient > rejection), never just its
 * first `Fail`: a defect anywhere halts push, even beside a transient failure, since an RPC request
 * decode failure arrives as an untagged defect. Interruption is re-raised (the loop is stopping).
 */
const classify = <E>(cause: Cause.Cause<E>, isFatal: ((error: E) => boolean) | undefined) =>
  Effect.gen(function* () {
    if (Cause.hasInterrupts(cause)) return yield* Effect.interrupt
    const failures = cause.reasons.filter(Cause.isFailReason).map((reason) => reason.error)
    const fatalFailure = isFatal === undefined ? undefined : failures.find(isFatal)
    const halts = isFatal !== undefined && (Cause.hasDies(cause) || fatalFailure !== undefined)
    const first = fatalFailure ?? failures[0]
    return { halts, message: Cause.hasDies(cause) || first === undefined ? Cause.pretty(cause) : describe(first) }
  })

export const make = <TE>(options: {
  readonly engine: Engine.Engine<TE>
  /** Whether a push failure is an integrity error that must stop pushing (see module doc). */
  readonly isFatalPush: (error: Errors.StoreUnavailable | TE) => boolean
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
        const pushed = yield* Effect.exit(engine.push)
        if (Exit.isFailure(pushed)) {
          const { halts, message } = yield* classify(pushed.cause, options.isFatalPush)
          if (halts) {
            yield* Effect.logError(`echo-sync: push halted by an integrity error: ${message}`)
            const halted: PushState = { _tag: "Halted", message }
            yield* SubscriptionRef.update(status, (s) => ({ ...s, push: halted }))
          } else {
            transient = Option.some(message)
          }
        } else if (pushed.value.stopped) {
          transient = Option.some("server stopped the push batch at a transient failure")
        }
      }
      // Every pull failure, a defect included, is transient (§6): nothing was installed.
      const pulled = yield* Effect.exit(engine.pull)
      if (Exit.isFailure(pulled)) {
        const { message } = yield* classify(pulled.cause, undefined)
        if (Option.isNone(transient)) transient = Option.some(message)
      }
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

/**
 * Follows the server's pokes stream (DESIGN §6, §8, 81a07db): every `poke` wakes the loop, a
 * `heartbeat` only proves liveness (DESIGN §6 Liveness) and never wakes it, and the stream is
 * reconnected after ANY end (failure, defect, clean completion, or `idleTimeout` with no element at
 * all) following a backoff that starts at `retryDelay`, doubles up to `maxRetryDelay`, and resets once
 * a connection delivered a poke. The idle timeout is what notices a connection that died silently (a
 * proxy that keeps the response open); it must exceed the server's `heartbeatInterval`.
 * Reconnecting does not wake the loop itself: a wake before the server's LISTEN is confirmed could pull
 * before a change that the new stream then never announces. The server emits one poke on every stream
 * right after LISTEN (DESIGN §6, aec6cd4), and that first element is what covers anything missed while
 * disconnected. Pokes are only hints (§8), so nothing here is fatal. Runs until interrupted.
 */
export const followPokes = <E>(options: {
  readonly connect: Stream.Stream<{ readonly _tag: "poke" | "heartbeat" }, E>
  readonly wake: Effect.Effect<void>
  readonly retryDelay?: Duration.Input
  readonly maxRetryDelay?: Duration.Input
  /** No element for this long ends the stream (then reconnects). Default 45 s. */
  readonly idleTimeout?: Duration.Input | undefined
}): Effect.Effect<never> =>
  Effect.gen(function* () {
    const base = Duration.toMillis(Duration.fromInputUnsafe(options.retryDelay ?? "1 second"))
    const cap = Duration.toMillis(Duration.fromInputUnsafe(options.maxRetryDelay ?? "30 seconds"))
    const connect = Stream.timeout(options.connect, options.idleTimeout ?? "45 seconds")
    let delay = base
    while (true) {
      const connection = { poked: false }
      const ended = yield* Effect.exit(
        Stream.runForEach(connect, (element) =>
          element._tag === "poke"
            ? Effect.andThen(
                Effect.sync(() => (connection.poked = true)),
                options.wake
              )
            : Effect.void
        )
      )
      if (connection.poked) delay = base
      yield* Effect.logDebug(
        `echo-sync: pokes stream ended (${Exit.isSuccess(ended) ? "completed" : Cause.pretty(ended.cause)}); reconnecting in ${delay} ms`
      )
      yield* Effect.sleep(Duration.millis(delay))
      delay = Math.min(delay * 2, cap)
    }
  })
