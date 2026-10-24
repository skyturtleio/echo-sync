/** Host-owned recorder shared by the real HTTP and native worker observability checks. */
import { Cause, Context, type Exit, Layer, Logger, Metric, Option, References, Tracer } from "effect"

export interface RecordedSpan {
  readonly name: string
  readonly spanId: string
  readonly traceId: string
  readonly sampled: boolean
  readonly parentSpanId: string | undefined
  readonly parentTraceId: string | undefined
  readonly parent: string | undefined
  readonly attributes: ReadonlyArray<readonly [string, unknown]>
  readonly events: ReadonlyArray<unknown>
  readonly links: number
  readonly annotations: number
  readonly exit: Exit.Exit<unknown, unknown>
}

export const recorder = (
  throws: "none" | "create" | "attribute" | "end" | "logger" = "none",
  sampled?: boolean,
  received: Array<RecordedSpan> = []
) => {
  const spans: Array<RecordedSpan> = []
  const logs: Array<{ diagnostics: boolean; message: unknown; cause: string }> = []
  const registry: Metric.MetricRegistry = new Map()
  class Recorded extends Tracer.NativeSpan {
    override attribute(key: string, value: unknown) {
      if (throws === "attribute") throw new Error("host attribute sink failed")
      super.attribute(key, value)
    }
    override end(time: bigint, exit: Exit.Exit<unknown, unknown>) {
      super.end(time, exit)
      const recorded: RecordedSpan = {
        name: this.name,
        spanId: this.spanId,
        traceId: this.traceId,
        sampled: this.sampled,
        parentSpanId: Option.getOrUndefined(this.parent)?.spanId,
        parentTraceId: Option.getOrUndefined(this.parent)?.traceId,
        parent: Option.isSome(this.parent)
          ? this.parent.value._tag === "Span"
            ? this.parent.value.name
            : "external"
          : undefined,
        attributes: [...this.attributes],
        events: this.events,
        links: this.links.length,
        annotations: this.annotations.mapUnsafe.size,
        exit
      }
      spans.push(recorded)
      received.push(recorded)
      if (throws === "end") throw new Error("host end sink failed")
    }
  }
  const tracer = Tracer.make({
    span(options) {
      if (throws === "create") throw new Error("host create sink failed")
      return new Recorded(sampled === undefined ? options : { ...options, sampled })
    }
  })
  const logger = Logger.make<unknown, void>((options) => {
    logs.push({
      diagnostics: options.fiber.getRef(References.CurrentLogAnnotations)["echo.diagnostics"] === true,
      message: options.message,
      cause: Cause.pretty(options.cause)
    })
    if (throws === "logger") throw new Error("host log sink failed")
  })
  const context = Context.make(Tracer.Tracer, tracer).pipe(
    Context.add(Logger.CurrentLoggers, new Set([logger, Logger.tracerLogger])),
    Context.add(References.MinimumLogLevel, "Debug"),
    Context.add(Metric.MetricRegistry, registry),
    Context.add(Metric.CurrentMetricAttributes, { test_runtime: "host" })
  )
  return { spans, logs, registry, context, layer: Layer.succeedContext(context) }
}

export const serialized = (value: unknown): string =>
  JSON.stringify(value, (_key, item: unknown) => (typeof item === "bigint" ? item.toString() : item))
