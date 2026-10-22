# Host-owned observability

Echo Sync uses Effect 4.0.5 only. There is no exporter, SDK, queue, flush, sampling policy,
or required observability service. Unconfigured tracing does not export anything; the native
tracer still allocates spans, and metrics use Effect's in-memory registry. Instrumentation
does not change sync decisions, transaction boundaries, status values, retries, or wire data.

## Running a worker with the host's runtime

An app-owned dedicated-worker entry can run the exported program rather than the default runner:

```ts
import { Effect, Layer, Logger, Metric, References, Tracer } from "effect"
import { leaderWorker } from "echo-sync/client/worker"

// tracer and routineLogger are host-owned; see the mandatory diagnostics filter below.
const telemetry = Layer.mergeAll(
  Layer.succeed(Tracer.Tracer, tracer),
  Layer.succeed(Logger.CurrentLoggers, new Set([routineLogger])),
  Layer.succeed(References.MinimumLogLevel, "Info"),
  Layer.succeed(Metric.CurrentMetricAttributes, { component: "replica" })
)
Effect.runFork(leaderWorker(self, { registry, mutations, syncUrl: "/sync" }).pipe(
  Effect.provide(telemetry)
))
```

`router(self)` is the equivalent SharedWorker program. Both are lazy, long-lived, scoped Effects
with no required telemetry services. `runLeaderWorker` / `runRouter` fork the same programs with
the default runtime. Install telemetry in **each execution realm**: a tab's runtime does not cross
the browser's Worker boundary. Interrupting the program closes engine-owned ports, disables
callbacks, ends child fibers, releases locks and closes the replica. Hosts own worker termination.
Browser lock/OPFS release completes asynchronously after scope finalization.

Fiber ownership and reference inheritance:

| Execution | Lifetime / context |
| --- | --- |
| Leader OPFS server, sync loop, poke follower, per-tab RPC servers | `forkScoped` in the leader program; same host references |
| Sync pending-count observer | `forkChild` of the running loop |
| Presence track/delivery fibers and stream pull/timeout fibers | Subscription scopes; same references |
| Router message callbacks and serialized announcement acceptance | `runForkWith` captures context **after** the telemetry boundary; `Fiber.runIn` attaches to router scope |
| Router epoch watcher | `forkIn(routerScope)`, not a child of one short acceptance fiber |
| Router tab-lifetime watchers | Callback runner attached to router scope |
| Tab-side LeaderClient epoch watchers, connection setup, deferred connection close, serialized acceptance | Captured guarded context, attached to the client's long-lived scope |
| LeaderClient status publication | `runSyncWith` with that captured context |
| SQL/RPC/HTTP internal fibers | Inherit the boundary from the program / actual request execution |

No engine telemetry dispatcher or detached epoch watcher exists. Callback runners check closure
**before** `runForkWith`, since it evaluates synchronously; checking only `Fiber.runIn` is too late.
The boundary replaces tracer/loggers only. Minimum log level, metric attributes/registry, and other
host references retain their usual Effect inheritance. Streams carry the protected parent and
references into execution, not merely construction.

## Server and HTTP trace context

Compose telemetry around `Startup.layer`, which observes prepare → migrate → attach → verify as
`echo.server.startup`. Serving is not inside that startup span. Provide the host's references on
actual request fibers too. `Postgres.layerHttp` installs a **route-scoped** boundary before RPC's
automatic spans and handler-context merge. Unrelated routes retain the host's tracer and logs.
The shared notification supervisor and revocation watcher inherit protected layer context.
Standalone Postgres batch/ledger, pull, maintenance, schema/migration and row-store query
entrypoints also suppress dependency telemetry; they do not add new span names or metrics.
Host-authored SQL outside engine operations remains the host's responsibility.

Echo Sync never parses `traceparent`, baggage, or forwarded headers. The host decides which
incoming trace context to trust and installs its parent with Effect's tracing facilities.
Engine spans attach to the nearest exported engine ancestor, otherwise the parent outside the
boundary. Proxies retain the exported span's IDs and sampling; suppressed spans propagate the
nearest visible ancestor's identity. An explicit external span inside the boundary is terminal
and remains the parent, rather than being replaced by the boundary's captured parent.
Host parent metadata, host global metric
attributes and log annotations are host-owned: do not put credentials or identities in them.

## Privacy boundary and sink failures

Only the exact span names below are exported—not an `echo.*` prefix. SQL statements, HTTP URLs,
RPC payloads, dependency spans, arbitrary attributes, annotations, links and **all span events**
are suppressed inside engine execution. Routine log messages are fixed library literals;
unrecognized dependency log messages are dropped because they can contain an error object as
the message itself. Suppressing span events also prevents `Logger.tracerLogger` from copying
`Cause.pretty` into a trace, including on a host-owned parent between operations.

Each span is an engine-owned proxy. Its `end` forwards **`Exit.void`**, never the real return
value or cause. `observe` runs classification in `Effect.onExit` inside `Effect.withSpan`.
Pinned `internal/effect.ts` `useSpan` registers `endSpan` as an exit finalizer; `endSpan` passes
the original exit to `span.end`. Thus `Effect.exit` inside a span alone would not protect an
interruption delivered outside that capture. The proxy protects every exit, including defects
and interruption, while the original exit propagates unchanged. No extra interruptibility mask
is added; `onExit` already finalizes uninterruptibly.

Each host span creation/attribute/end/logger invocation is guarded independently. Creation
failure produces a local, non-exporting span; other failed deliveries are dropped. No recursive
sink-error logging occurs. Metric registry updates are guarded too. The engine does not await
export or flush. **Host callbacks must be bounded and nonblocking**; a blocked JavaScript event
loop cannot be isolated synchronously. The guarantee is unchanged commit/halt/retry decisions,
not wall-clock immunity to a blocked event loop.

Stream pulls and stream-scope closure both run in captured protected context. Effect's channel
service provision alone does not capture scope-finalizer context: an owned stream scope is
closed explicitly with those references, and an observed stream's outcome finalizer captures
its actual operation span. Cancelling downstream between pulls therefore cannot route cleanup
SQL/logs to the host tracer or lose the operation's `interrupted`/`defect` outcome.

### Mandatory diagnostics filter

Raw causes are sensitive. They exist only in separate Debug log entries annotated
`echo.diagnostics=true`, not in routine engine logs or traces. Routine exporters **must drop
these entries even if Debug is enabled**. A separate opt-in diagnostic logger may retain them
under the host's sensitive-data policy. The default Info minimum does not emit Debug diagnostics.

```ts
const routineLogger = Logger.make((options) => {
  if (options.fiber.getRef(References.CurrentLogAnnotations)["echo.diagnostics"] === true) return
  // Export options.message and the host-approved metadata here.
})
```

`Logger.Options` carries the fiber, not an annotations snapshot. Do not look for
`options.annotations`. Do not routinely export unfiltered diagnostics to a backend.

## Signal contract

All exported exits are `Success(undefined)`. `echo.outcome` carries the actual outcome.
Its closed vocabulary is `ok`, `credentials`, `unauthorized`, `unavailable`, `network`, `server`,
`outdated`, `server-outdated`, `malformed`, `resnapshot`, `interrupted`, `defect`, `rejected`,
`forbidden`, `halted`, `stopped`, `saved`, `duplicate`, `elected`, `lost`, `idle-timeout`,
`server-end`, `page`, `done`. Interruption takes precedence over defects, then recognized typed
failures, then `server`. Classification is descriptive; it never supplies retry/halt policy.
For push batches driven by `SyncLoop`, its actual whole-cause classification supplies the outcome:
`halted` when the host's `isFatalPush` halts, otherwise its closed error kind. That decision is
reused for status, not recomputed from tags; interruption and defect outcomes retain precedence.
`echo.result` carries the operation-specific success result from the same vocabulary;
`echo.reason` distinguishes first-element idle timeout from clean stream end.
Only nonnegative safe integer counts and `echo.resnapshot: boolean` are otherwise allowed.
Never identifiers, table/mutation names, args, rows, headers, URLs, free text, cursors or continuations.

| Exact name | Boundary and attributes |
| --- | --- |
| `echo.enqueue` | Local transaction through commit; `saved`, `duplicate`, `rejected` or failure kind. Duplicate does not count as saved. Interruption does not imply rollback of a masked commit. |
| `echo.push` | One attempted nonempty batch, before credential acquisition through response bookkeeping; `echo.batch_size`, `echo.processed`, `echo.applied`, `echo.rejected`; short prefix is `stopped`. Credential acquisition failure records `credentials` even though nothing was sent. Empty/halted cycles have no batch span. Each real retry is a new span. |
| `echo.pull` | One set attempt through install, including resumed staged pages. `echo.pages`, `echo.resnapshot`, `echo.resnapshot_count`. Internal resnapshot restarts stay inside the attempt and successful recovery is `resnapshot`. External backoff retries are new spans. On success pages includes the entire installed set and PullDone; on failure it is the last observed set size including staged pages, if a page arrived. |
| `echo.install` | Child of pull, after PullDone, wraps the storage transaction through commit/rollback. Empty PullDone still installs successfully. Push halt does not prevent install. |
| `echo.rebase` | Child of install or push's covered-outcome release; `echo.pending`, `echo.replayed`, `echo.released`, `echo.notices_rejected`, `echo.notices_unknown`. Empty replay counts are zero. Completion is inside the transaction; only install proves commit. |
| `echo.pokes` | Credential preparation and first pull through first protocol chunk or establishment failure; first-chunk `echo.pokes`, `echo.heartbeats`. Heartbeats never wake sync. Later termination records the reconnect counter, not a failure of an already established connection. |
| `echo.leader.election` | Wait for leader lock; ends `elected` before database open. Interrupted candidates do not count as elected. |
| `echo.leader.handover` | Surviving tab/router observes loss, performs unchanged recovery, then waits for replacement connection/announcement; `elected` on replacement, interruption on observer teardown. No tenure span: dead workers cannot finish one. |
| `echo.router.handover` | Accepted live announcement, `elected`; stale/duplicate announcements create no span. Loss also increments its `lost` counter. |
| `echo.server.startup` | Preparation through catalog verification; idempotent startup is still `ok`, Drift is `server`. |
| `echo.server.push` | Mapped authenticated batch, including internal ledger retries; same batch/result counts, short prefix `stopped`; empty batch `ok` with zeros. |
| `echo.server.pull` | One authenticated page request; result `page` or `done`, `echo.changes`, `echo.acks`. Empty PullDone is `done` with zero changes. |
| `echo.server.pokes` | Subscription; clean end `server-end`, scope teardown `interrupted`. |

Every failed/interrupted attempt records a classification, never a failure exit to the host tracer.
No-op installs/rebases remain real operations. There are no artificial retries or independent
install/rebase retry policies, no presence-operation spans, and no per-mutation server spans.

Metrics use the same host context:

- `<span-name>.total` counters tagged by `echo.outcome` from that closed set.
- `echo.leader.loss.total`, `lost`, per surviving observer (not a global exactly-once count).
- `echo.pokes.reconnect.total`, classified stream-termination reason; no reconnect on teardown.
- `echo.outbox.pending` gauge, from the existing reactive count, no extra query/timer. Without
  a host runtime attribute it is a last-writer gauge across workers, not an aggregate.
- `echo.push.batch_size` histogram: boundaries `[0,1,10,25,50,100]`, attempted nonempty batches,
  including those stopped by credential acquisition before sending.
- `echo.pull.pages` histogram: `[1,2,5,10,25,50,100,500]`, successful installed sets only.
- `echo.install.duration`, `echo.rebase.duration`: `Metric.timer`, milliseconds, including
  failed/interrupted attempts, tagged by outcome; Effect adds `time_unit=milliseconds`.

## Existing log inventory and reclassification

Every original `Effect.log*` site now uses fixed routine text and (where present) a separate
annotated diagnostic cause. Status/halt text returned to callers is unchanged.

| Original site | Routine level |
| --- | --- |
| Engine `applyChange`, unknown table | Debug; table/change interpolation removed |
| SyncLoop integrity halt | Error; no interpolated halt reason or attached cause |
| SyncLoop push retry; pull retry | Debug instead of Warn |
| SyncLoop pending-count restart | Warn |
| SyncLoop pokes reconnect | Debug; interpolated `Cause.pretty`/delay removed |
| Presence track retry | Debug instead of Warn |
| Leader candidate cannot serve | Error; existing Failed/close behavior retained |
| Router leader epoch watcher; tab watcher; announcement acceptance | Error |
| LeaderClient connected epoch watcher; connection setup; router acceptance; own epoch watcher | Error |
| Server presence revocation watcher restart | Warn |

Interruption-related routine entries are Debug, not Error/Warn. The only direct `Effect.log*`
calls remaining in `src` are the shared routine and diagnostics implementations.

## Verification

`test/observability.e2e.test.ts` exercises real SQLite and PostgreSQL HTTP RPC with
`Logger.tracerLogger`, typed failures and defect canaries. It checks commit/install, transient
retry at 999/1000 ms, halt, idempotency, diagnostics-only causes, safe hierarchy/metrics and
unrelated host routes. Existing suites run unmodified.
The subscription regression explicitly closes a pull's scope from the host context and checks
cleanup, parent isolation and both interruption/defect outcomes. Removing either context capture
makes it fail. A client regression closes transferred ports both before queued acceptance and
during its epoch-lock check. Direct maintenance/batch tests reject row-bearing dependency spans.

With the browser harness running, `bash examples/browser-spike/observability.sh` checks the
host-run leader with real OPFS, actual worker RPC and HTTP sync, typed/defect canaries, inherited
metric attributes, interruption, released locks, late callbacks, and scoped router watchers.
`scenarios.sh` and `slice.sh` remain unchanged and are separate full browser gates.
