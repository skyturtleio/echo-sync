/**
 * The leader's sync loop (DESIGN §8, §9): runs `Engine.push` then `Engine.pull` whenever woken (local
 * enqueue, poke) and retries transient failures with backoff; `followPokes` keeps the pokes stream up.
 *
 * Fatal push (DESIGN §6, a68a727): a push failure that `SyncFailure.classifyPush` says halts (an integrity
 * failure: `MalformedEnvelope`, an invalid batch, or a push defect) halts pushing for the lifetime of this
 * loop: no automatic retry, no outbox entry is released (that would leave a seq gap, §16.5). Pull keeps
 * running. The halt is surfaced through `status`, and the pending outbox stays exportable.
 */
import {
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  Exit,
  Option,
  Pull,
  Queue,
  Result,
  Schema,
  Stream,
  SubscriptionRef
} from "effect"
import type { Errors } from "echo-sync"
import type * as Engine from "./Engine.ts"
import * as SyncFailure from "./SyncFailure.ts"
import { type PullTransportError, SyncErrorKind } from "./SyncFailure.ts"
import * as Telemetry from "../../internal/Telemetry.ts"

export const PushState = Schema.Union([
  Schema.TaggedStruct("Active", {}),
  /** Integrity failure; pushing stays stopped until the leader restarts. */
  Schema.TaggedStruct("Halted", { message: Schema.String })
])
export type PushState = typeof PushState.Type

export const SyncPhase = Schema.Literals(["idle", "pushing", "pulling", "backoff"])
export type SyncPhase = typeof SyncPhase.Type

/**
 * A failure: its kind and a library-controlled description of it (DESIGN §9.1). Never raw
 * error text, so never header values, bodies, URLs or tokens; the leader logs the full cause.
 */
export const SyncError = Schema.Struct({ kind: SyncErrorKind, message: Schema.String })
export type SyncError = typeof SyncError.Type

/** The leader's sync state (DESIGN §9.1). No `connected` flag: a finished cycle proves nothing about now. */
export const SyncStatus = Schema.Struct({
  push: PushState,
  /** Last failure, cleared by a clean cycle; `outdated` persists for this leader's lifetime. */
  lastError: Schema.NullOr(SyncError),
  /** Unreleased outbox entries (pushed-but-not-installed included); follows every append and release. */
  pendingMutations: Schema.Int,
  /** Epoch ms of the last cycle whose push and `PullDone` install both completed; `null` until one in this leader. */
  lastSyncedAt: Schema.NullOr(Schema.Number),
  phase: SyncPhase,
  /** Epoch ms of the next retry while `phase === "backoff"`, otherwise `null`. */
  nextRetryAt: Schema.NullOr(Schema.Number)
})
export type SyncStatus = typeof SyncStatus.Type

export interface SyncLoop {
  /**
   * Request a cycle soon (coalesced: many wakes while one is queued run one cycle). During backoff it
   * cuts the wait short; it never clears a `Halted` push or a terminal `outdated` stop.
   */
  readonly wake: Effect.Effect<void>
  readonly status: Effect.Effect<SyncStatus>
  readonly changes: Stream.Stream<SyncStatus>
  /** Runs forever; fork it in the leader's scope. Fails only with defects. */
  readonly run: Effect.Effect<never>
}

/**
 * The public text for each kind (DESIGN §9.1, 418d688): library-controlled constants. No raw error
 * message, name, unrecognized tag, string cause, URL or `Cause.pretty` ever reaches `syncStatus`, since
 * any of them can carry a token, an echoed header or a request body. Detail goes to the leader's log.
 */
const messages: Record<SyncErrorKind, string> = {
  credentials: "the app's syncHeaders failed; the request was not sent",
  unauthorized: "the server rejected the credential",
  unavailable: "the server is temporarily unavailable",
  network: "the request produced no usable response",
  server: "a transient failure; retrying",
  outdated: "this client requires an update; sync is stopped",
  "server-outdated": "the server is missing a declared column; retrying"
}

const stoppedBatch: SyncError = { kind: "server", message: "the server stopped the push batch at a transient failure" }

/**
 * The status consequences of a failure cause (DESIGN §6: defect > integrity > transient), never just
 * its first failure. Interruption is re-raised (the loop is stopping). The returned strings are
 * constants; the cause itself is only ever logged.
 */
const classify = <E, V>(cause: Cause.Cause<E>, verdict: (cause: Cause.Cause<E>) => V) =>
  Cause.hasInterrupts(cause) ? Effect.interrupt : Effect.succeed(verdict(cause))

const errorOf = (kind: SyncErrorKind): SyncError => ({ kind, message: messages[kind] })

export const make = (options: {
  readonly engine: Engine.Engine
  /** First retry delay after a transient failure; doubles up to `maxRetryDelay`. */
  readonly retryDelay?: Duration.Input
  readonly maxRetryDelay?: Duration.Input
  /**
   * The current count of unreleased outbox entries, then the count after every outbox write (the leader
   * passes a reactive count over `echo_outbox`). Required. Followed while `run` runs; a failure restarts it after
   * a second. `status` and `changes` wait for its first value: a status with an unknown count would tell
   * a consumer there are no pending writes (DESIGN §9.1).
   */
  readonly pendingMutations: Stream.Stream<number, Errors.StoreUnavailable>
}): Effect.Effect<SyncLoop> =>
  Effect.gen(function* () {
    const { engine } = options
    const base = Duration.toMillis(Duration.fromInputUnsafe(options.retryDelay ?? "1 second"))
    const cap = Duration.toMillis(Duration.fromInputUnsafe(options.maxRetryDelay ?? "30 seconds"))
    const status = yield* SubscriptionRef.make<SyncStatus>({
      push: { _tag: "Active" },
      lastError: null,
      pendingMutations: 0,
      lastSyncedAt: null,
      phase: "idle",
      nextRetryAt: null
    })
    const setPhase = (phase: SyncPhase, nextRetryAt: number | null = null) =>
      SubscriptionRef.update(status, (s) => ({ ...s, phase, nextRetryAt }))
    const wakes = yield* Queue.sliding<void>(1)
    // Completed once `pendingMutations` holds a real count; nothing reads the status before that.
    const counted = yield* Deferred.make<void>()
    // Policy refusal is terminal for this leader, unlike the push-only integrity halt.
    let outdated = false
    const stopOutdated = (error: SyncError) => {
      outdated = true
      return SubscriptionRef.update(status, (s): SyncStatus => ({
        ...s,
        lastError: error,
        phase: "idle",
        nextRetryAt: null
      }))
    }

    /** One push+pull cycle; returns whether it hit a transient failure. */
    const cycle = Effect.gen(function* () {
      if (outdated) return false
      let transient = Option.none<SyncError>()
      // A cycle whose push did not run (halted) or did not finish cleanly is not a completed sync.
      let pushedCleanly = false
      if ((yield* SubscriptionRef.get(status)).push._tag === "Active") {
        yield* setPhase("pushing")
        // The whole final cause decides, including failures an outer finalizer added after the batch
        // span (which classified the batch's own cause the same way) had ended.
        const pushed = yield* Effect.exit(engine.push)
        if (Exit.isFailure(pushed)) {
          const { kind, halt } = yield* classify(pushed.cause, SyncFailure.classifyPush)
          if (Option.isNone(halt) && kind === "outdated") {
            yield* stopOutdated(errorOf(kind))
            return false
          }
          if (Option.isSome(halt)) {
            yield* Telemetry.log("Error", "echo-sync: push halted by an integrity error", pushed.cause)
            const halted: PushState = { _tag: "Halted", message: halt.value }
            yield* SubscriptionRef.update(status, (s) => ({ ...s, push: halted }))
          } else {
            yield* Telemetry.log("Debug", "echo-sync: push failed; retrying", pushed.cause)
            transient = Option.some(errorOf(kind))
          }
        } else if (pushed.value.stopped) {
          transient = Option.some(stoppedBatch)
        } else {
          pushedCleanly = true
        }
      }
      yield* setPhase("pulling")
      // Nothing was installed on failure. Policy refusal stops sync; other failures back off.
      const pulled = yield* Effect.exit(engine.pull)
      if (Exit.isFailure(pulled)) {
        // A pull never halts: a pull defect is transient.
        const { kind } = yield* classify(pulled.cause, SyncFailure.classifyPull)
        if (kind === "outdated") {
          yield* stopOutdated(errorOf(kind))
          return false
        }
        yield* Telemetry.log("Debug", "echo-sync: pull failed; retrying", pulled.cause)
        if (Option.isNone(transient) || kind === "server-outdated") transient = Option.some(errorOf(kind))
      }
      // `engine.pull` succeeds only once a `PullDone` set is installed.
      const syncedAt = pushedCleanly && Exit.isSuccess(pulled) ? yield* Clock.currentTimeMillis : undefined
      yield* SubscriptionRef.update(status, (s) => ({
        ...s,
        lastError: Option.getOrNull(transient),
        lastSyncedAt: syncedAt ?? s.lastSyncedAt
      }))
      return Option.isSome(transient)
    })

    const followPending = Stream.runForEach(options.pendingMutations, (n) =>
      // The count is in the ref before readers are let in, so their first (replayed) value carries it.
      Effect.andThen(
        SubscriptionRef.update(status, (s) => ({ ...s, pendingMutations: n })),
        Effect.andThen(Deferred.succeed(counted, undefined), Telemetry.pending(n))
      )
    ).pipe(
      Effect.catchCause((cause) => Telemetry.log("Warn", "echo-sync: pending count failed; restarting", cause)),
      Effect.andThen(Effect.sleep("1 second")),
      Effect.forever
    )

    const run = Effect.gen(function* () {
      yield* Effect.forkChild(followPending)
      let delay = base
      while (true) {
        const failed = yield* cycle
        if (failed) {
          // Retry after the backoff unless something wakes the loop first.
          yield* setPhase("backoff", (yield* Clock.currentTimeMillis) + delay)
          yield* Effect.raceFirst(Queue.take(wakes), Effect.sleep(Duration.millis(delay)))
          delay = Math.min(delay * 2, cap)
        } else {
          delay = base
          yield* setPhase("idle")
          yield* Queue.take(wakes)
        }
      }
    })

    return {
      wake: Effect.asVoid(Queue.offer(wakes, undefined)),
      // Both wait for the first count, which `run` produces: a reader before `run` waits, never deadlocks it.
      status: Effect.andThen(Deferred.await(counted), SubscriptionRef.get(status)),
      changes: Stream.unwrap(Effect.as(Deferred.await(counted), SubscriptionRef.changes(status))),
      run: Telemetry.boundary(run)
    }
  })

/**
 * A first pull of the pokes stream that ends it (`Cause.Done`) is a clean server end, not a failure, unless
 * the idle timeout ended it.
 */
const pokesOutcome =
  (timedOut: () => boolean) =>
  (cause: Cause.Cause<PullTransportError | Cause.Done>): Telemetry.Outcome =>
    Result.match(Pull.filterNoDone(cause), {
      onSuccess: (failure) => SyncFailure.classifyPull(failure).outcome,
      onFailure: () => (timedOut() ? "idle-timeout" : "server-end")
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
export const followPokes = (options: {
  /**
   * One (re)connect: the Effect prepares it (resolves credentials, DESIGN §9.1) and the stream it
   * returns sends the request when run. `idleTimeout` covers only the stream (DESIGN §6 Liveness).
   */
  readonly connect: Effect.Effect<
    Stream.Stream<{ readonly _tag: "poke" | "heartbeat" }, PullTransportError>,
    PullTransportError
  >
  readonly wake: Effect.Effect<void>
  readonly retryDelay?: Duration.Input
  readonly maxRetryDelay?: Duration.Input
  /** No element for this long ends the stream (then reconnects). Default 45 s. */
  readonly idleTimeout?: Duration.Input | undefined
}): Effect.Effect<never> =>
  Effect.gen(function* () {
    const base = Duration.toMillis(Duration.fromInputUnsafe(options.retryDelay ?? "1 second"))
    const cap = Duration.toMillis(Duration.fromInputUnsafe(options.maxRetryDelay ?? "30 seconds"))
    const idleTimeout = options.idleTimeout ?? "45 seconds"
    // Preparation runs first and is never counted as silence; the clock starts when the stream is
    // first pulled, which is when its request is sent, and bounds the first element and every gap.
    let delay = base
    while (true) {
      const connection = { poked: false, timedOut: false }
      const connect = Stream.transformPull(
        Stream.unwrap(
          Effect.map(options.connect, (stream) =>
            Stream.timeoutOrElse(stream, {
              duration: idleTimeout,
              orElse: () =>
                Stream.fromEffect(
                  Effect.sync(() => {
                    connection.timedOut = true
                  })
                ).pipe(Stream.drain)
            })
          )
        ),
        (pull) => {
          let first = true
          return Effect.succeed(
            Effect.suspend(() => {
              if (!first) return pull
              first = false
              return Telemetry.observe(
                pull.pipe(
                  Effect.tap((elements) =>
                    Telemetry.annotate({
                      "echo.pokes": elements.filter((element) => element._tag === "poke").length,
                      "echo.heartbeats": elements.filter((element) => element._tag === "heartbeat").length
                    })
                  ),
                  Effect.tapCause(() =>
                    connection.timedOut ? Telemetry.annotate({ "echo.reason": "idle-timeout" }) : Effect.void
                  )
                ),
                "echo.pokes",
                pokesOutcome(() => connection.timedOut)
              )
            })
          )
        }
      )
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
      yield* Telemetry.count(
        "echo.pokes.reconnect.total",
        connection.timedOut
          ? "idle-timeout"
          : Exit.isSuccess(ended)
            ? "server-end"
            : Telemetry.outcome(ended, (cause) => SyncFailure.classifyPull(cause).outcome)
      )
      yield* Telemetry.log(
        "Debug",
        "echo-sync: pokes stream ended; reconnecting",
        Exit.isFailure(ended) ? ended.cause : undefined
      )
      yield* Effect.sleep(Duration.millis(delay))
      delay = Math.min(delay * 2, cap)
    }
  }).pipe(Telemetry.boundary)
