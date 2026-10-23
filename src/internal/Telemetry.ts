/** Engine telemetry boundary. No dependency span, event, result or cause crosses it. */
import {
  Cause,
  Channel,
  Clock,
  Context,
  Duration,
  Effect,
  Exit,
  Logger,
  Metric,
  Option,
  Predicate,
  References,
  Scope,
  Stream,
  Tracer
} from "effect"

export type Name =
  | "echo.enqueue"
  | "echo.push"
  | "echo.pull"
  | "echo.install"
  | "echo.rebase"
  | "echo.pokes"
  | "echo.leader.election"
  | "echo.leader.handover"
  | "echo.router.handover"
  | "echo.server.startup"
  | "echo.server.push"
  | "echo.server.pull"
  | "echo.server.pokes"

const names = new Set<string>([
  "echo.enqueue",
  "echo.push",
  "echo.pull",
  "echo.install",
  "echo.rebase",
  "echo.pokes",
  "echo.leader.election",
  "echo.leader.handover",
  "echo.router.handover",
  "echo.server.startup",
  "echo.server.push",
  "echo.server.pull",
  "echo.server.pokes"
])
const outcomes = new Set<string>([
  "ok",
  "credentials",
  "unauthorized",
  "unavailable",
  "network",
  "server",
  "outdated",
  "server-outdated",
  "malformed",
  "resnapshot",
  "interrupted",
  "defect",
  "rejected",
  "forbidden",
  "halted",
  "stopped",
  "saved",
  "duplicate",
  "elected",
  "lost",
  "idle-timeout",
  "server-end",
  "page",
  "done"
])
const counts = new Set<string>([
  "echo.batch_size",
  "echo.processed",
  "echo.applied",
  "echo.rejected",
  "echo.pages",
  "echo.resnapshot_count",
  "echo.pending",
  "echo.replayed",
  "echo.released",
  "echo.notices_rejected",
  "echo.notices_unknown",
  "echo.pokes",
  "echo.heartbeats",
  "echo.changes",
  "echo.acks"
])
const approved = (key: string, value: unknown) =>
  key === "echo.outcome" || key === "echo.result" || key === "echo.reason"
    ? typeof value === "string" && outcomes.has(value)
    : key === "echo.resnapshot"
      ? typeof value === "boolean"
      : counts.has(key) && typeof value === "number" && Number.isSafeInteger(value) && value >= 0

/** A sink is host code. A failed callback drops only that telemetry operation. */
const guard = <A>(call: () => A): A | undefined => {
  try {
    return call()
  } catch {
    return undefined
  }
}

// Shared across nested boundaries so SQL/RPC parents collapse to the nearest exported ancestor.
const forwarded = new WeakMap<Tracer.Span, Tracer.Span | undefined>()
const parentFor = (
  parent: Option.Option<Tracer.AnySpan>,
  external: Option.Option<Tracer.AnySpan>
): Option.Option<Tracer.AnySpan> => {
  let current = parent
  while (Option.isSome(current) && current.value._tag === "Span" && forwarded.has(current.value)) {
    const exported = forwarded.get(current.value)
    if (exported !== undefined) return Option.some(exported)
    if (current.value instanceof SafeSpan && current.value.root && Option.isNone(current.value.parent))
      return Option.none()
    current = current.value.parent
  }
  return Option.isSome(current) ? current : external
}

class SafeSpan extends Tracer.NativeSpan {
  readonly target: Tracer.Span | undefined
  readonly root: boolean
  constructor(options: Parameters<Tracer.Tracer["span"]>[0], target: Tracer.Span | undefined) {
    const visible = target ?? Option.getOrUndefined(parentFor(options.parent, Option.none()))
    super({ ...options, sampled: visible?.sampled ?? options.sampled })
    this.target = target
    this.root = options.root
    // The proxy and suppressed dependency spans propagate the exported identity, not a phantom one.
    if (visible !== undefined) {
      this._spanId = visible.spanId
      this._traceId = visible.traceId
    }
    forwarded.set(this, target)
  }
  override attribute(key: string, value: unknown): void {
    if (!approved(key, value)) return
    super.attribute(key, value)
    if (this.target !== undefined) guard(() => this.target?.attribute(key, value))
  }
  override end(time: bigint, _exit: Exit.Exit<unknown, unknown>): void {
    super.end(time, Exit.void)
    if (this.target !== undefined) guard(() => this.target?.end(time, Exit.void))
  }
  override event(): void {}
  override addLinks(): void {}
}

const wrapped = new WeakSet<Tracer.Tracer>()
const wrappedLoggers = new WeakSet<Logger.Logger<unknown, unknown>>()
const messages = new Set([
  "echo-sync: diagnostics",
  "echo-sync: unknown table change skipped",
  "echo-sync: push halted by an integrity error",
  "echo-sync: push failed; retrying",
  "echo-sync: pull failed; retrying",
  "echo-sync: pending count failed; restarting",
  "echo-sync: pokes stream ended; reconnecting",
  "echo-sync: presence track failed; retrying",
  "echo leader candidate cannot serve",
  "echo: connecting to leader failed",
  "echo: accepting a router connection failed",
  "echo: watching this tab's leader epoch failed",
  "echo: watching leader epoch failed",
  "echo router: watching leader epoch failed",
  "echo router: watching tab failed",
  "echo router: leader announcement failed",
  "presence revocation watch restarting"
])

/** Install before capturing callback runners, not just around handler construction. */
export const boundary = <A, E, R>(work: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.withFiber((fiber) => {
    const host = fiber.getRef(Tracer.Tracer)
    const external = Context.getOption(fiber.context, Tracer.ParentSpan)
    const tracer = wrapped.has(host)
      ? host
      : Tracer.make({
          span(options) {
            const target = names.has(options.name)
              ? guard(() =>
                  host.span({
                    ...options,
                    parent: parentFor(options.parent, options.root ? Option.none() : external),
                    annotations: Context.empty(),
                    links: []
                  })
                )
              : undefined
            return new SafeSpan({ ...options, annotations: Context.empty(), links: [] }, target)
          }
        })
    wrapped.add(tracer)
    const loggers = new Set(
      [...fiber.getRef(Logger.CurrentLoggers)].map((logger) => {
        if (wrappedLoggers.has(logger)) return logger
        const safe = Logger.make<unknown, void>((options) => {
          const diagnostics = options.fiber.getRef(References.CurrentLogAnnotations)["echo.diagnostics"] === true
          // Dependency log messages may themselves contain errors, bodies or SQL, not just Causes.
          if (
            !diagnostics &&
            !(
              Array.isArray(options.message) &&
              options.message.every((message: unknown) => typeof message === "string" && messages.has(message))
            )
          )
            return
          guard(() =>
            logger.log({
              ...options,
              cause: diagnostics ? options.cause : Cause.empty
            })
          )
        })
        wrappedLoggers.add(safe)
        return safe
      })
    )
    // tracerLogger may run between operations, when the current parent is host-owned. Shadow it
    // locally as well: no diagnostics event may be written into that external parent's span.
    const protectedWork =
      Option.isSome(external) && external.value._tag === "Span" && !forwarded.has(external.value)
        ? work.pipe(
            Effect.provideService(
              Tracer.ParentSpan,
              new SafeSpan(
                {
                  name: "engine boundary",
                  parent: external,
                  annotations: Context.empty(),
                  links: [],
                  startTime: 0n,
                  kind: "internal",
                  root: false,
                  sampled: external.value.sampled
                },
                undefined
              )
            )
          )
        : work
    return protectedWork.pipe(
      Effect.provideService(Tracer.Tracer, tracer),
      Effect.provideService(Logger.CurrentLoggers, loggers)
    )
  })

/** Stream execution outlives construction; carry the boundary into its pulls and finalizers. */
export const stream = <A, E, R>(work: Stream.Stream<A, E, R>): Stream.Stream<A, E, R> =>
  Stream.fromChannel(
    Channel.fromTransformBracket((_upstream, _scope, channelScope) =>
      boundary(
        Effect.gen(function* () {
          const owned = yield* Scope.make()
          const context = Context.add(yield* Effect.context<R>(), Scope.Scope, owned)
          // Channel.provideService only protects initialization/pulls. Scope finalizers do not
          // capture their registering fiber's context, so close the owned scope explicitly in it.
          yield* Scope.addFinalizerExit(channelScope, (exit) =>
            Scope.close(owned, exit).pipe(Effect.provideContext(context))
          )
          return yield* Stream.toPull(work).pipe(Effect.provideContext(context))
        })
      )
    )
  )

/**
 * The outcome of an exit. Interruption, then defects, take precedence; a typed failure is named by
 * the operation's own closed classifier (`failure`) when it has one, otherwise by its tag. Only
 * recognized tags are mapped; never copy a tag, error message or input into telemetry.
 */
export const outcome = <A, E>(exit: Exit.Exit<A, E>, failure?: (cause: Cause.Cause<E>) => string): string => {
  if (Exit.isSuccess(exit)) return "ok"
  if (Cause.hasInterrupts(exit.cause)) return "interrupted"
  if (Cause.hasDies(exit.cause)) return "defect"
  if (failure !== undefined) return failure(exit.cause)
  const failures = exit.cause.reasons.filter(Cause.isFailReason).map((reason) => kind(reason.error))
  return failures.includes("halted") ? "halted" : (failures[0] ?? "server")
}

const kind = (error: unknown): string => {
  if (Predicate.isTagged(error, "Unauthorized")) return "unauthorized"
  if (Predicate.isTagged(error, "ServerUnavailable")) return "unavailable"
  if (Predicate.isTagged(error, "ClientOutdated")) return "outdated"
  if (Predicate.isTagged(error, "ResnapshotRequired")) return "resnapshot"
  if (Predicate.isTagged(error, "LocallyRejected")) return "rejected"
  if (Predicate.isTagged(error, "Forbidden")) return "forbidden"
  if (Predicate.isTagged(error, "MalformedEnvelope")) return "halted"
  if (Predicate.isTagged(error, "TimeoutError")) return "idle-timeout"
  if (Predicate.isTagged(error, "Done")) return "server-end"
  return "server"
}

export const annotate = (attributes: Record<string, unknown>) =>
  Effect.withFiber((fiber) => {
    let parent = Context.getOption(fiber.context, Tracer.ParentSpan)
    while (Option.isSome(parent) && parent.value instanceof SafeSpan) {
      if (names.has(parent.value.name))
        return Effect.annotateCurrentSpan(attributes).pipe(Effect.provideService(Tracer.ParentSpan, parent.value))
      parent = parent.value.parent
    }
    return Effect.void
  })

/** Raw causes have one explicit, sensitive channel. Hosts must filter this annotation. */
export const diagnostic = (cause: Cause.Cause<unknown>) =>
  Effect.logDebug("echo-sync: diagnostics", cause).pipe(Effect.annotateLogs({ "echo.diagnostics": true }), boundary)

export const log = (level: "Debug" | "Warn" | "Error", message: string, cause?: Cause.Cause<unknown>) =>
  Effect.logWithLevel(cause !== undefined && Cause.hasInterrupts(cause) ? "Debug" : level)(message).pipe(
    Effect.andThen(cause === undefined ? Effect.void : diagnostic(cause)),
    boundary
  )

export const count = (name: string, result: string) =>
  Effect.withFiber((fiber) => {
    guard(() =>
      Metric.counter(name, { attributes: { "echo.outcome": outcomes.has(result) ? result : "server" } }).updateUnsafe(
        1,
        fiber.context
      )
    )
    return Effect.void
  })
export const pending = (n: number) =>
  Effect.withFiber((fiber) => {
    guard(() => Metric.gauge("echo.outbox.pending").updateUnsafe(n, fiber.context))
    return Effect.void
  })
export const histogram = (name: "echo.push.batch_size" | "echo.pull.pages", n: number) =>
  Effect.withFiber((fiber) => {
    guard(() =>
      Metric.histogram(name, {
        boundaries: name === "echo.push.batch_size" ? [0, 1, 10, 25, 50, 100] : [1, 2, 5, 10, 25, 50, 100, 500]
      }).updateUnsafe(n, fiber.context)
    )
    return Effect.void
  })

/** Stream counterpart: the same proxy sanitizes its scoped span's exit. */
export const observeStream = <A, E, R>(work: Stream.Stream<A, E, R>, name: Name): Stream.Stream<A, E, R> =>
  stream(
    Stream.unwrap(
      Effect.map(Effect.context<R>(), (context) =>
        work.pipe(
          Stream.onExit((exit) =>
            Effect.gen(function* () {
              const value = Exit.isSuccess(exit) ? "server-end" : outcome(exit)
              yield* annotate({ "echo.outcome": value })
              yield* count(`${name}.total`, value)
              if (Exit.isFailure(exit)) yield* diagnostic(exit.cause)
            }).pipe(Effect.provideContext(context))
          )
        )
      )
    ).pipe(Stream.withSpan(name))
  )

/**
 * onExit preserves the original value/cause and masks only its own bookkeeping. `failure` is the
 * operation's own closed classifier of a typed failure (see `outcome`).
 */
export const observe = <A, E, R>(
  work: Effect.Effect<A, E, R>,
  name: Name,
  failure?: (cause: Cause.Cause<E>) => string
): Effect.Effect<A, E, R> =>
  boundary(
    Effect.suspend(() => {
      let started = 0
      return Effect.flatMap(Clock.currentTimeMillis, (time) => {
        started = time
        return work.pipe(
          Effect.onExit((exit) =>
            Effect.gen(function* () {
              const result = outcome(exit, failure)
              // A success result set inside the operation (saved/duplicate/stopped/etc.) takes precedence.
              const parent = yield* Effect.serviceOption(Tracer.ParentSpan)
              const recorded =
                Option.isSome(parent) && parent.value._tag === "Span"
                  ? parent.value.attributes.get("echo.result")
                  : undefined
              const idleTimeout =
                name === "echo.pokes" &&
                result === "server-end" &&
                Option.isSome(parent) &&
                parent.value._tag === "Span" &&
                parent.value.attributes.get("echo.reason") === "idle-timeout"
              const value = idleTimeout
                ? "idle-timeout"
                : Exit.isSuccess(exit) && typeof recorded === "string" && outcomes.has(recorded)
                  ? recorded
                  : result
              yield* annotate({ "echo.outcome": value })
              yield* count(`${name}.total`, value)
              if (name === "echo.install" || name === "echo.rebase") {
                const elapsed = (yield* Clock.currentTimeMillis) - started
                yield* Effect.withFiber((fiber) => {
                  guard(() =>
                    Metric.timer(`${name}.duration`, {
                      boundaries: [0.5, 1, 5, 10, 50, 100, 500, 1000],
                      attributes: { "echo.outcome": value }
                    }).updateUnsafe(Duration.millis(elapsed), fiber.context)
                  )
                  return Effect.void
                })
              }
              if (Exit.isFailure(exit)) yield* diagnostic(exit.cause)
            })
          ),
          Effect.withSpan(name)
        )
      })
    })
  )
