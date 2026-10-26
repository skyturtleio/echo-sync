// oxlint-disable-next-line import/no-unassigned-import -- Node wasm fixture
import "../src/client/testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import type * as PgClient from "@effect/sql-pg/PgClient"
import { describe, expect, it } from "@effect/vitest"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Metric, Schema, Scope, Stream, Tracer } from "effect"
import { paginate } from "../src/client/sync/testing/Pull.ts"
import * as Reactivity from "effect/reactivity/Reactivity"
import { FetchHttpClient, Headers, HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http"
import { Rpc } from "effect/rpc"
import { RequestId } from "effect/rpc/RpcMessage"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Errors, Ids, Mutation, Protocol } from "echo-sync"
import { TestClock } from "effect/testing"
import * as Engine from "../src/client/sync/Engine.ts"
import * as SqliteSyncStorage from "../src/client/sync/SqliteSyncStorage.ts"
import * as SyncLoop from "../src/client/sync/SyncLoop.ts"
import * as Transport from "../src/client/sync/Transport.ts"
import { executeBatch, handlers, layerHttp, Notifications } from "../src/server/postgres/rpc.ts"
import { collectGarbage, expireLedger } from "../src/server/postgres/gc.ts"
import { pgAvailable, registry, TestDatabase } from "../src/server/postgres/test-support.ts"
import * as Telemetry from "../src/internal/Telemetry.ts"
import { recorder as makeRecorder, serialized, type RecordedSpan } from "./telemetry-recorder.ts"

// Keep every delivery, including spans cleared by individual scenarios and throwing end sinks.
const received: RecordedSpan[] = []
const recorder = (throws?: Parameters<typeof makeRecorder>[0], sampled?: boolean) =>
  makeRecorder(throws, sampled, received)

const secret = "canary-header-arg-principal-defect-721994"
const cursor = Ids.Position.make(819273645564738291n)
const actor = AccessScope.PrincipalId.make(secret)
const mutations = Mutation.makeRegistry([Mutation.make("accept", { args: Schema.Json, handler: () => Effect.void })])
const done: Protocol.PullResponse = {
  _tag: "PullDone",
  changes: [],
  acks: [],
  nextCursor: cursor,
  expiredThrough: Ids.SeqFloor.make(0)
}
const malformedEnvelope = () =>
  new Protocol.MalformedEnvelope({
    clientId: Ids.ClientId.make("device"),
    mutationId: Ids.MutationId.make("intent"),
    message: secret
  })
const device = Effect.fnUntraced(
  function* (transport: Engine.Transport) {
    const sqlite = yield* SqliteClient.makeMemory({})
    yield* SqliteSyncStorage.migrate(registry).pipe(Effect.provideService(SqlClient, sqlite))
    const storage = SqliteSyncStorage.make(sqlite, registry)
    const engine = yield* Engine.make({
      clientId: Ids.ClientId.make("device"),
      actor,
      storage,
      registry,
      mutations,
      transport
    })
    return { engine, storage, sqlite }
  },
  Effect.provide(Reactivity.layer),
  Effect.provideService(Tracer.Tracer, Tracer.nativeTracer)
)

it.effect("request failures append no notices and ledgered rejections keep successful batch outcomes", () => {
  const recording = recorder()
  const outcomes = (name: string) =>
    recording.spans.filter((span) => span.name === name).map((span) => new Map(span.attributes).get("echo.outcome"))
  return Effect.gen(function* () {
    let refused = true
    let results: Protocol.PushResponse["results"] = []
    const { engine, storage, sqlite } = yield* device({
      push: (request) => {
        if (refused) return Effect.fail(new Protocol.Unauthorized({ message: secret }))
        results = request.mutations.map((envelope, index) => ({
          mutationId: envelope.mutationId,
          position: cursor,
          outcome: {
            _tag: "Rejected",
            error:
              index === 0
                ? new Errors.ConstraintViolated({ kind: "unique", constraint: "unique-title", message: secret })
                : index === 1
                  ? new Mutation.ClockSkew({ createdAt: 40001, serverNow: 10000 })
                  : new Mutation.ClientExpired({ mutationId: envelope.mutationId })
          }
        }))
        return Effect.succeed({ results })
      },
      pull: () => Stream.succeed({ ...done, acks: results })
    })
    const local = yield* Effect.flip(engine.mutate(Ids.MutationId.make("local"), "missing", {}))
    expect(local).toBeInstanceOf(Engine.LocallyRejected)
    for (const id of ["constraint", "clock", "expired"]) yield* engine.mutate(Ids.MutationId.make(id), "accept", {})
    yield* Effect.exit(engine.push)
    // Inspection SQL is test-owned, not an engine operation; do not export its dependency spans.
    expect(yield* storage.transaction(SqliteSyncStorage.notices(sqlite)).pipe(Telemetry.boundary)).toEqual([])
    expect(yield* storage.transaction(storage.pending).pipe(Telemetry.boundary)).toHaveLength(3)
    refused = false
    yield* engine.push
    const immediate = yield* storage.transaction(SqliteSyncStorage.notices(sqlite)).pipe(Telemetry.boundary)
    expect(immediate.map(({ notice }) => notice._tag)).toEqual(["OutcomeUnknown"])
    yield* engine.pull
    const retained = yield* storage.transaction(SqliteSyncStorage.notices(sqlite)).pipe(Telemetry.boundary)
    expect(retained.map(({ notice }) => notice._tag)).toEqual(["OutcomeUnknown", "Rejected", "Rejected"])
    expect(retained.flatMap(({ notice }) => (notice._tag === "Rejected" ? [notice.error] : []))).toEqual([
      new Errors.ConstraintViolated({ kind: "unique", constraint: "unique-title", message: secret }),
      new Mutation.ClockSkew({ createdAt: 40001, serverNow: 10000 })
    ])
    expect(yield* storage.transaction(storage.pending).pipe(Telemetry.boundary)).toEqual([])
    expect(outcomes("echo.enqueue")).toEqual(["rejected", "saved", "saved", "saved"])
    expect(outcomes("echo.push")).toEqual(["unauthorized", "ok"])
    expect(outcomes("echo.pull")).toEqual(["ok"])
  }).pipe(Effect.scoped, Effect.provide(recording.layer))
})

it.live(
  "sync propagates exported IDs and sampling through suppressed spans and preserves an explicit external parent",
  () => {
    const recording = recorder("none", false)
    return Effect.gen(function* () {
      const external = Tracer.externalSpan({ spanId: "incoming-span", traceId: "incoming-trace", sampled: false })
      const current: Array<{ spanId: string; traceId: string; sampled: boolean }> = []
      const capture = Effect.gen(function* () {
        const span = yield* Effect.currentSpan
        current.push({ spanId: span.spanId, traceId: span.traceId, sampled: span.sampled })
      })
      const { engine } = yield* device({
        push: () => Effect.succeed({ results: [] }),
        // The nested enqueues are expected to succeed; a failure of one is the test's defect.
        pull: paginate((): Effect.Effect<Protocol.PullResponse> =>
          Effect.orDie(
            Effect.gen(function* () {
              yield* capture
              yield* capture.pipe(Effect.withSpan("http.dependency"))
              yield* engine
                .mutate(Ids.MutationId.make("nested"), "accept", {})
                .pipe(Effect.withSpan("sql.dependency"), Effect.provideService(Tracer.ParentSpan, external))
              yield* engine
                .mutate(Ids.MutationId.make("rooted"), "accept", {})
                .pipe(Effect.withSpan("dependency.root", { root: true, sampled: true }))
              yield* engine
                .mutate(Ids.MutationId.make("explicit-root-parent"), "accept", {})
                .pipe(Effect.withSpan("dependency.explicit-root-parent", { root: true, parent: external }))
              return done
            })
          )
        )
      })
      yield* engine.pull.pipe(Effect.withSpan("host.parent"))
      const pull = recording.spans.find((span) => span.name === "echo.pull")
      expect(pull).toBeDefined()
      expect(current).toEqual([0, 1].map(() => ({ spanId: pull?.spanId, traceId: pull?.traceId, sampled: false })))
      const enqueue = recording.spans.find((span) => span.name === "echo.enqueue")
      expect(enqueue?.parentSpanId).toBe(external.spanId)
      expect(enqueue?.parentTraceId).toBe(external.traceId)
      expect(enqueue?.traceId).toBe(external.traceId)
      expect(enqueue?.sampled).toBe(false)
      const install = recording.spans.find((span) => span.name === "echo.install")
      expect(install?.parentSpanId).toBe(pull?.spanId)
      expect(install?.parentTraceId).toBe(pull?.traceId)
      const root = recording.spans.filter((span) => span.name === "echo.enqueue")[1]
      const host = recording.spans.find((span) => span.name === "host.parent")
      expect(root).toBeDefined()
      expect(host).toBeDefined()
      expect(root?.parentSpanId).toBeUndefined()
      expect(root?.parentTraceId).toBeUndefined()
      expect(root?.traceId).not.toBe(host?.traceId)
      expect(root?.sampled).toBe(false)
      const explicitRootParent = recording.spans.filter((span) => span.name === "echo.enqueue")[2]
      expect(explicitRootParent?.parentSpanId).toBe(external.spanId)
      expect(explicitRootParent?.parentTraceId).toBe(external.traceId)
      expect(explicitRootParent?.traceId).toBe(external.traceId)
      expect(explicitRootParent?.sampled).toBe(false)
    }).pipe(Effect.scoped, Effect.provide(recording.layer))
  }
)

it.effect("batch telemetry agrees with the loop's whole-cause decision, not the first failure", () =>
  Effect.gen(function* () {
    const unauthorized = Cause.fail(new Protocol.Unauthorized({ reason: "authenticationRefused", message: secret }))
    const malformed = Cause.fail(malformedEnvelope())
    for (const { cause, expected } of [
      { cause: Cause.combine(unauthorized, malformed), expected: "halted" },
      { cause: Cause.combine(malformed, unauthorized), expected: "halted" },
      { cause: unauthorized, expected: "unauthorized" }
    ]) {
      const recording = recorder()
      yield* Effect.gen(function* () {
        let pushes = 0
        const { engine, storage } = yield* device({
          push: () =>
            Effect.suspend(() => {
              pushes++
              return Effect.failCause(cause)
            }),
          pull: () => Stream.succeed(done)
        })
        yield* engine.mutate(Ids.MutationId.make("intent"), "accept", {})
        const loop = yield* SyncLoop.make({ engine, pendingMutations: storage.pendingCount })
        const fiber = yield* Effect.forkChild(loop.run)
        yield* Effect.repeat(Effect.yieldNow, { times: 80 })
        expect((yield* loop.status).push._tag).toBe(expected === "halted" ? "Halted" : "Active")
        if (expected === "halted") yield* TestClock.adjust("30 seconds")
        expect(pushes).toBe(1)
        expect(
          recording.spans
            .filter((span) => span.name === "echo.push")
            .map((span) => new Map(span.attributes).get("echo.outcome"))
        ).toEqual([expected])
        expect(
          (yield* Metric.value(Metric.counter("echo.push.total", { attributes: { "echo.outcome": expected } }))).count
        ).toBe(1)
        if (expected === "halted") {
          expect(
            (yield* Metric.value(Metric.counter("echo.push.total", { attributes: { "echo.outcome": "unauthorized" } })))
              .count
          ).toBe(0)
        }
        yield* Fiber.interrupt(fiber)
      }).pipe(Effect.scoped, Effect.provide(recording.layer))
    }
  })
)

it.effect("the whole final push cause remains authoritative, including typed cleanup failures", () =>
  Effect.gen(function* () {
    for (const mode of ["defect", "interrupt", "typed"]) {
      const recording = recorder()
      yield* Effect.gen(function* () {
        let pulls = 0
        const failure = new Error(`cleanup-${secret}`)
        const { engine, storage } = yield* device({
          push: () => Effect.fail(new Protocol.Unauthorized({ reason: "authenticationRefused", message: secret })),
          pull: () =>
            Stream.sync(() => {
              pulls++
              return done
            })
        })
        yield* engine.mutate(Ids.MutationId.make("intent"), "accept", {})
        const push = engine.push.pipe(
          Effect.onExit(() =>
            mode === "typed"
              ? Effect.fail(malformedEnvelope())
              : mode === "interrupt"
                ? Effect.interrupt
                : Effect.die(failure)
          )
        )
        const loop = yield* SyncLoop.make({ engine: { ...engine, push }, pendingMutations: storage.pendingCount })
        const fiber = yield* Effect.forkChild(Effect.exit(loop.run))
        yield* Effect.repeat(Effect.yieldNow, { times: 80 })
        if (mode === "defect" || mode === "typed") {
          expect((yield* loop.status).push._tag).toBe("Halted")
          expect(pulls).toBe(1)
          yield* TestClock.adjust("30 seconds")
          expect((yield* loop.status).phase).toBe("idle")
          yield* Fiber.interrupt(fiber)
        } else {
          const exit = yield* Fiber.join(fiber)
          expect(Exit.isFailure(exit)).toBe(true)
          if (Exit.isFailure(exit)) {
            expect(Cause.hasInterrupts(exit.cause)).toBe(true)
          }
          expect(pulls).toBe(0)
        }
        if (mode === "typed") {
          // The batch saw only Unauthorized; the loop additionally saw the fatal cleanup error.
          expect(
            recording.spans
              .filter((span) => span.name === "echo.push")
              .map((span) => new Map(span.attributes).get("echo.outcome"))
          ).toEqual(["unauthorized"])
          expect(
            (yield* Metric.value(Metric.counter("echo.push.total", { attributes: { "echo.outcome": "unauthorized" } })))
              .count
          ).toBe(1)
          expect(
            (yield* Metric.value(Metric.counter("echo.push.total", { attributes: { "echo.outcome": "halted" } }))).count
          ).toBe(0)
        }
      }).pipe(Effect.scoped, Effect.provide(recording.layer))
    }
  })
)

it.effect("throwing host sinks cannot change saved intent, retry deadlines, halt or successful installs", () =>
  Effect.gen(function* () {
    for (const failure of ["none", "create", "attribute", "end", "logger"] satisfies ReadonlyArray<
      Parameters<typeof recorder>[0]
    >) {
      const recording = recorder(failure)
      const run = Effect.gen(function* () {
        let pushes = 0
        const { engine, storage } = yield* device({
          push: () =>
            Effect.suspend(() => {
              pushes++
              return Effect.fail(
                pushes === 1
                  ? new Protocol.Unauthorized({ reason: "authenticationRefused", message: secret })
                  : malformedEnvelope()
              )
            }),
          pull: () => Stream.succeed(done)
        })
        const saved = yield* engine.mutate(Ids.MutationId.make("intent"), "accept", { secret })
        expect((yield* engine.mutate(Ids.MutationId.make("intent"), "accept", { other: true })).seq).toBe(saved.seq)
        const loop = yield* SyncLoop.make({ engine, pendingMutations: storage.pendingCount })
        const fiber = yield* Effect.forkChild(loop.run)
        yield* Effect.repeat(Effect.yieldNow, { times: 80 })
        expect(pushes).toBe(1)
        expect((yield* loop.status).lastError?.kind).toBe("unauthorized")
        yield* TestClock.adjust("999 millis")
        expect(pushes).toBe(1)
        yield* TestClock.adjust("1 millis")
        yield* Effect.repeat(Effect.yieldNow, { times: 80 })
        expect(pushes).toBe(2)
        expect((yield* loop.status).push._tag).toBe("Halted")
        yield* loop.wake
        yield* Effect.repeat(Effect.yieldNow, { times: 80 })
        expect(pushes).toBe(2)
        expect(
          (yield* storage.transaction(storage.meta).pipe(Effect.provideService(Tracer.Tracer, Tracer.nativeTracer)))
            .cursor
        ).toBe(cursor)
        expect(
          (yield* storage.transaction(storage.pending).pipe(Effect.provideService(Tracer.Tracer, Tracer.nativeTracer)))
            .length
        ).toBe(1)
        yield* Fiber.interrupt(fiber)
      }).pipe(Effect.scoped, Effect.provide(recording.layer))
      yield* run
      if (failure === "none" || failure === "logger") {
        const installs = recording.spans.filter((span) => span.name === "echo.install")
        expect(installs.length).toBe(3)
        expect(
          installs.every((span) => span.parent === "echo.pull" && new Map(span.attributes).get("echo.outcome") === "ok")
        ).toBe(true)
      }
      expect(recording.spans.every((span) => Exit.isSuccess(span.exit) && span.exit.value === undefined)).toBe(true)
    }
  })
)

it.live("SQLite sync never exports a typed failure, defect, result or dependency span containing secret inputs", () => {
  const recording = recorder()
  return Effect.gen(function* () {
    let failing = false
    const { engine, storage } = yield* device({
      push: () => Effect.fail(new Protocol.Unauthorized({ reason: "authenticationRefused", message: secret })),
      pull: () => (failing ? Stream.die(new Error(secret)) : Stream.succeed(done))
    })
    yield* engine.mutate(Ids.MutationId.make("intent"), "accept", { secret })
    yield* Effect.exit(engine.push)
    yield* engine.pull
    failing = true
    const failed = yield* Effect.exit(engine.pull)
    expect(Exit.isFailure(failed) && Cause.pretty(failed.cause)).toContain(secret)
    const metrics = yield* Metric.snapshot
    expect(serialized(metrics.map((metric) => ({ name: metric.id, attributes: metric.attributes })))).not.toContain(
      secret
    )
    expect(
      (yield* storage.transaction(storage.meta).pipe(Effect.provideService(Tracer.Tracer, Tracer.nativeTracer))).cursor
    ).toBe(cursor)
    expect(recording.spans.map((span) => span.name)).toEqual(
      expect.arrayContaining(["echo.enqueue", "echo.push", "echo.pull", "echo.install", "echo.rebase"])
    )
    expect(
      recording.spans.every(
        (span) =>
          span.name.startsWith("echo.") &&
          span.events.length === 0 &&
          span.links === 0 &&
          span.annotations === 0 &&
          Exit.isSuccess(span.exit) &&
          span.exit.value === undefined
      )
    ).toBe(true)
    expect(serialized(recording.spans)).not.toContain(secret)
    expect(serialized(recording.spans)).not.toContain(cursor.toString())
    expect(serialized(recording.logs.filter((log) => !log.diagnostics))).not.toContain(secret)
    expect(serialized(recording.logs.filter((log) => log.diagnostics))).toContain(secret)
    expect(
      recording.spans
        .filter((span) => span.name === "echo.pull")
        .map((span) => new Map(span.attributes).get("echo.outcome"))
    ).toEqual(["ok", "defect"])
  }).pipe(Effect.scoped, Effect.provide(recording.layer))
})

describe.skipIf(!pgAvailable)("real HTTP route telemetry", () => {
  it.live(
    "closing a pulled subscription in the host context retains its own outcome and protects cleanup and the host parent",
    () => {
      const recording = recorder()
      const program = Effect.gen(function* () {
        const seen: Array<string> = []
        const source = Stream.concat(
          Stream.succeed<typeof Protocol.PokesElement.Type>({ _tag: "poke" }),
          Stream.never
        ).pipe(
          Stream.onExit((exit) =>
            Effect.gen(function* () {
              seen.push(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause) ? "interrupted" : "defect")
              yield* Effect.logError(secret)
            }).pipe(Effect.withSpan(`cleanup.${secret}`))
          )
        )
        const db = yield* Effect.context<SqlClient>()
        const serve = yield* Protocol.SyncRpcs.accessHandler("pokes").pipe(
          Effect.provide(
            handlers({ registry, mutations, principalFor: () => Effect.succeed(actor) }).pipe(
              Layer.provide([
                Layer.succeed(Notifications, source),
                Layer.succeedContext(db),
                Layer.succeed(
                  HttpServerRequest.HttpServerRequest,
                  HttpServerRequest.fromWeb(new Request("http://echo.test/sync"))
                )
              ])
            )
          )
        )
        for (const closing of [Exit.interrupt(), Exit.die(new Error(secret))]) {
          const scope = yield* Scope.make()
          const subscription = serve(
            { clientId: Ids.ClientId.make("subscription") },
            { client: new Rpc.ServerClient(1), requestId: RequestId(1), headers: Headers.empty }
          )
          if (!Stream.isStream(subscription)) return yield* Effect.die("expected streaming pokes handler")
          const pull = yield* Stream.toPull(subscription).pipe(Scope.provide(scope))
          expect(yield* pull).toEqual([{ _tag: "poke" }])
          yield* Scope.close(scope, closing)
        }
        expect(seen).toEqual(["interrupted", "defect"])
        expect(recording.spans.map((span) => span.name)).toEqual(["echo.server.pokes", "echo.server.pokes"])
        expect(recording.spans.map((span) => new Map(span.attributes).get("echo.outcome"))).toEqual([
          "interrupted",
          "defect"
        ])
        expect(
          recording.spans.every(
            (span) =>
              span.parent === "host.subscription" &&
              Exit.isSuccess(span.exit) &&
              span.exit.value === undefined &&
              span.events.length === 0
          )
        ).toBe(true)
        expect(serialized(recording.spans)).not.toContain(secret)
        expect(serialized(recording.logs.filter((log) => !log.diagnostics))).not.toContain(secret)
        expect(serialized(recording.logs.filter((log) => log.diagnostics))).toContain(secret)
      }).pipe(
        Effect.withSpan("host.subscription"),
        Effect.scoped,
        Effect.provide(recording.layer),
        Effect.provide(TestDatabase)
      )
      return program.pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            expect(recording.spans.find((span) => span.name === "host.subscription")?.events).toEqual([])
          })
        )
      )
    }
  )

  it.live(
    "direct server batches and maintenance cannot export returned identities or fail from a throwing tracer",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        for (const failure of ["none", "create"] satisfies ReadonlyArray<Parameters<typeof recorder>[0]>) {
          const recording = recorder(failure)
          const clientId = Ids.ClientId.make(`${secret}-${failure}`)
          yield* Effect.gen(function* () {
            const response = yield* executeBatch({ registry, mutations }, actor, {
              clientId,
              mutations: [
                { mutationId: Ids.MutationId.make(secret), seq: Ids.Seq.make(1), name: "accept", args: secret }
              ]
            })
            expect(response.results[0]?.outcome._tag).toBe("Applied")
            yield* sql`UPDATE echo_sync.mutations SET acked_at = now() - interval '10 days' WHERE client_id = ${clientId}`.pipe(
              Effect.provideService(Tracer.Tracer, Tracer.nativeTracer)
            )
            expect(yield* expireLedger()).toEqual({ clients: 1, expired: 1 })
            expect((yield* collectGarbage("7 days")).deleted).toBe(0)
            expect(recording.spans).toEqual([])
          }).pipe(Effect.provide(recording.layer))
        }
      }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live(
    "RPC request context cannot bypass sanitization for typed failures, defects, SQL spans or tracerLogger events",
    () => {
      const recording = recorder()
      return Effect.gen(function* () {
        const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
        const sync = layerHttp({
          registry,
          mutations,
          principalFor: (headers) =>
            headers["x-mode"] === "defect"
              ? Effect.die(new Error(secret))
              : headers["x-mode"] === "typed"
                ? Effect.fail(new Protocol.Unauthorized({ reason: "authenticationRefused", message: secret }))
                : Effect.succeed(actor)
        }).pipe(Layer.provide(Layer.succeedContext(db)))
        const other = HttpRouter.add(
          "GET",
          "/other",
          Effect.succeed(HttpServerResponse.text("host")).pipe(Effect.withSpan("host.route"))
        )
        const web = HttpRouter.toWebHandler(Layer.merge(sync, other), {
          disableLogger: true,
          middleware: (work) => Effect.provide(work, recording.layer)
        })
        yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
        const fetch: typeof globalThis.fetch = Object.assign(
          (input: RequestInfo | URL, init?: RequestInit) => web.handler(new Request(input, init)),
          { preconnect: globalThis.fetch.preconnect }
        )
        let mode = "ok"
        const rpc = yield* Transport.make.pipe(
          Effect.provide(
            Transport.layerProtocolHttp(
              "http://echo.test/sync",
              Effect.sync(() => ({ "x-mode": mode, authorization: secret }))
            )
          )
        )
        const transport: Engine.Transport = {
          push: (request) => rpc.push(request).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch)),
          pull: (request) => rpc.pull(request).pipe(Stream.provideService(FetchHttpClient.Fetch, fetch))
        }
        const { engine } = yield* device(transport)
        recording.spans.length = 0
        recording.logs.length = 0
        yield* engine.mutate(Ids.MutationId.make("intent"), "accept", { secret })
        yield* engine.push
        yield* engine.pull
        for (const refusal of ["typed", "defect"]) {
          mode = refusal
          const failed = yield* Effect.exit(engine.pull)
          expect(Exit.isFailure(failed)).toBe(true)
        }
        const exported = recording.spans
        expect(exported.map((span) => span.name)).toEqual(
          expect.arrayContaining(["echo.server.push", "echo.server.pull", "echo.install"])
        )
        expect(
          exported.every(
            (span) =>
              span.events.length === 0 &&
              span.links === 0 &&
              span.annotations === 0 &&
              Exit.isSuccess(span.exit) &&
              span.exit.value === undefined
          )
        ).toBe(true)
        expect(serialized(exported)).not.toContain(secret)
        expect(serialized(exported)).not.toContain(cursor.toString())
        expect(serialized(recording.logs.filter((log) => !log.diagnostics))).not.toContain(secret)
        expect(serialized(recording.logs.filter((log) => log.diagnostics))).toContain(secret)
        expect(
          exported
            .filter((span) => span.name === "echo.server.pull")
            .map((span) => new Map(span.attributes).get("echo.outcome"))
        ).toEqual(["done", "unauthorized", "defect"])
        const before = recording.spans.length
        yield* Effect.promise(() => web.handler(new Request("http://echo.test/other")))
        expect(recording.spans.slice(before).some((span) => span.name === "host.route")).toBe(true)
        mode = "ok"
        const first = yield* Deferred.make<void>()
        const beforeStream = recording.spans.length
        const follower = yield* Effect.forkChild(
          SyncLoop.followPokes({
            connect: rpc
              .pokes(Ids.ClientId.make("device"), actor)
              .pipe(Effect.map((stream) => stream.pipe(Stream.provideService(FetchHttpClient.Fetch, fetch)))),
            wake: Deferred.succeed(first, undefined)
          })
        )
        yield* Deferred.await(first)
        yield* Fiber.interrupt(follower)
        // Closing the real HTTP handler's scope must classify the subscription in its own span,
        // even though the closing request fiber no longer has that span as its ambient parent.
        yield* Effect.promise(web.dispose)
        const streamSpans = recording.spans.slice(beforeStream)
        const subscription = streamSpans.find((span) => span.name === "echo.server.pokes")
        expect(subscription).toBeDefined()
        expect(new Map(subscription?.attributes).get("echo.outcome")).toBe("interrupted")
        expect(
          streamSpans.every(
            (span) => Exit.isSuccess(span.exit) && span.exit.value === undefined && span.events.length === 0
          )
        ).toBe(true)
      }).pipe(Effect.scoped, Effect.provide(recording.layer), Effect.provide(TestDatabase))
    },
    30_000
  )
})

it("engine spans across the e2e run never export an unregistered name or a raw failure cause", () => {
  // These three spans are deliberately host-owned controls, not engine emissions.
  const hostNames = new Set(["host.parent", "host.subscription", "host.route"])
  const engineSpans = received.filter((span) => !hostNames.has(span.name))
  const allowed = new Set<string>(Telemetry.spanNames)
  expect(engineSpans.length).toBeGreaterThan(0)
  expect([...new Set(engineSpans.map((span) => span.name))].filter((name) => !allowed.has(name))).toEqual([])
  expect(engineSpans.filter((span) => !Exit.isSuccess(span.exit) || span.exit.value !== undefined)).toEqual([])
})
