import { expect, it } from "@effect/vitest"
import { Cause, Effect, Exit, Layer, Option, Schema, Stream } from "effect"
import { FetchHttpClient, HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http"
import { Rpc, RpcClient, RpcGroup, RpcSerialization, RpcServer } from "effect/rpc"
import { AccessScope, Ids, Mutation, Protocol, Registry } from "echo-sync"
import * as Engine from "./Engine.ts"
import * as MemorySyncStorage from "./testing/MemorySyncStorage.ts"
import * as SyncLoop from "./SyncLoop.ts"
import * as Transport from "./Transport.ts"

interface Handlers {
  readonly push?: (
    request: Protocol.PushRequest
  ) => Effect.Effect<Protocol.PushResponse, Protocol.MalformedEnvelope | Protocol.Unauthorized>
  readonly pull?: (
    request: Protocol.PullRequest
  ) => Effect.Effect<
    Protocol.PullResponse,
    Protocol.ResnapshotRequired | Protocol.ServerUnavailable | Protocol.Unauthorized
  >
}

/**
 * A real RpcServer (HTTP, NDJSON) behind a web handler, and the leader's Transport talking to it
 * through `fetch`. `served` counts requests that reached the server.
 */
const harness = <A extends Transport.TransportError | Protocol.ResnapshotRequired, B>(
  server: Layer.Layer<never, never, HttpRouter.HttpRouter> | "unreachable",
  body: (transport: Effect.Success<typeof Transport.make>) => Effect.Effect<B, A>
) =>
  Effect.gen(function* () {
    const served = { count: 0 }
    const web = server === "unreachable" ? undefined : HttpRouter.toWebHandler(server, { disableLogger: true })
    yield* Effect.addFinalizer(() => Effect.promise(() => web?.dispose() ?? Promise.resolve()))
    const fetch = Object.assign(
      (input: URL | RequestInfo, init?: RequestInit) => {
        if (web === undefined) return Promise.reject(new TypeError("Failed to fetch"))
        served.count++
        return web.handler(new Request(input, init))
      },
      { preconnect: globalThis.fetch.preconnect }
    )
    const client = RpcClient.layerProtocolHttp({ url: "http://echo.test/rpc" }).pipe(
      Layer.provide([RpcSerialization.layerNdjson, FetchHttpClient.layer])
    )
    // `FetchHttpClient` reads `Fetch` from the fiber that sends each request.
    const exit = yield* Effect.exit(Effect.flatMap(Transport.make.pipe(Effect.provide(client)), body)).pipe(
      Effect.provideService(FetchHttpClient.Fetch, fetch)
    )
    return { exit, served: served.count }
  })

const serve = (handlers: Handlers) =>
  RpcServer.layerHttp({ group: Protocol.SyncRpcs, path: "/rpc", protocol: "http" }).pipe(
    Layer.provide([
      Protocol.SyncRpcs.toLayer({
        push: handlers.push ?? (() => Effect.die("push not expected")),
        pull: handlers.pull ?? (() => Effect.die("pull not expected")),
        pokes: () => Stream.never
      }),
      RpcSerialization.layerNdjson
    ])
  )

/** A server on another protocol version: its push payload schema refuses this client's requests. */
const skewed = RpcServer.layerHttp({
  group: RpcGroup.make(
    Rpc.make("push", { payload: { clientId: Schema.Literal("someone-else") }, success: Protocol.PushResponse })
  ),
  path: "/rpc",
  protocol: "http"
}).pipe(
  Layer.provide([
    RpcGroup.make(
      Rpc.make("push", { payload: { clientId: Schema.Literal("someone-else") }, success: Protocol.PushResponse })
    ).toLayer({ push: () => Effect.die("handler must not run") }),
    RpcSerialization.layerNdjson
  ])
)

/** `n` mutations; the protocol caps a batch at 100, so `n = 101` is a request an honest engine must not send. */
const batch = (n: number): Protocol.PushRequest => {
  const mutations = Array.from({ length: n }, (_, i) => ({
    mutationId: Ids.MutationId.make(`m${i + 1}`),
    seq: Ids.Seq.make(i + 1),
    name: "todo.add",
    args: { title: "x" }
  }))
  const [first, ...rest] = mutations
  if (first === undefined) throw new Error("batch: n must be >= 1")
  // Not validated here, so the 101 case reaches the Transport's own check.
  return { clientId: Ids.ClientId.make("c"), mutations: [first, ...rest] }
}

/** A server on another protocol version whose pull response this client cannot decode. */
const skewedPull = (() => {
  const group = RpcGroup.make(
    Rpc.make("pull", { payload: Schema.Unknown, success: Schema.Struct({ _tag: Schema.Literal("Weird") }) })
  )
  return RpcServer.layerHttp({ group, path: "/rpc", protocol: "http" }).pipe(
    Layer.provide([
      group.toLayer({ pull: () => Effect.succeed({ _tag: "Weird" as const }) }),
      RpcSerialization.layerNdjson
    ])
  )
})()
const position = Schema.decodeUnknownSync(Ids.Position)("7")

/** The failure an exit ended with, or the string "defect"/"success" (a defect would kill the loop's fiber). */
const failureOf = <A, E>(exit: Exit.Exit<A, E>) =>
  Exit.isSuccess(exit) ? "success" : Option.getOrElse(Exit.findErrorOption(exit), () => "defect" as const)

it.effect("push failures are fatal exactly when §6 says so, classified by their real tags over a real RPC server", () =>
  Effect.gen(function* () {
    const cases = [
      {
        name: "server refuses the batch with MalformedEnvelope",
        server: serve({
          push: (req) =>
            Effect.fail(
              new Protocol.MalformedEnvelope({
                clientId: req.clientId,
                mutationId: req.mutations[0].mutationId,
                message: "seq"
              })
            )
        }),
        size: 1,
        tag: "MalformedEnvelope",
        fatal: true,
        served: 1
      },
      {
        name: "server cannot decode the push payload",
        server: skewed,
        size: 1,
        tag: "PushIntegrityError:serverDefect",
        fatal: true,
        served: 1
      },
      {
        name: "push handler dies",
        server: serve({ push: () => Effect.die(new Error("bug")) }),
        size: 1,
        tag: "PushIntegrityError:serverDefect",
        fatal: true,
        served: 1
      },
      {
        name: "the outbox batch is not a valid PushRequest (101 mutations)",
        server: serve({}),
        size: 101,
        tag: "PushIntegrityError:invalidPayload",
        fatal: true,
        served: 0
      },
      { name: "network down", server: "unreachable" as const, size: 1, tag: "RpcClientError", fatal: false, served: 0 },
      {
        name: "Unauthorized",
        server: serve({ push: () => Effect.fail(new Protocol.Unauthorized({ message: "expired" })) }),
        size: 1,
        tag: "Unauthorized",
        fatal: false,
        served: 1
      }
    ]
    for (const c of cases) {
      const { exit, served } = yield* harness(c.server, (t) => t.push(batch(c.size)))
      const failure = failureOf(exit)
      const tag =
        failure instanceof Transport.PushIntegrityError
          ? `${failure._tag}:${failure.reason}`
          : Schema.is(Schema.Struct({ _tag: Schema.String }))(failure)
            ? failure._tag
            : failure
      expect({ case: c.name, tag, fatal: Transport.isFatalPush(failure), served }).toEqual({
        case: c.name,
        tag: c.tag,
        fatal: c.fatal,
        served: c.served
      })
    }
  }).pipe(Effect.scoped)
)

it.effect("a processed push returns core's result; a rejection keeps its class and fields", () =>
  Effect.gen(function* () {
    const { exit } = yield* harness(
      serve({
        push: (req) =>
          Effect.succeed({
            results: req.mutations.map((m) => ({
              mutationId: m.mutationId,
              position,
              outcome: { _tag: "Rejected" as const, error: new Mutation.UnknownMutation({ name: m.name }) }
            }))
          })
      }),
      (t) => t.push(batch(1))
    )
    const results = Exit.isSuccess(exit) ? exit.value.results : []
    expect(results.map((r) => [r.mutationId, r.position, r.outcome._tag])).toEqual([["m1", 7n, "Rejected"]])
    const outcome = results[0]?.outcome
    expect(outcome?._tag === "Rejected" ? outcome.error : undefined).toEqual(
      new Mutation.UnknownMutation({ name: "todo.add" })
    )
  }).pipe(Effect.scoped)
)

it.effect(
  "pull: core's ResnapshotRequired and ServerUnavailable reach the engine typed; protocol faults are transient failures, not defects",
  () =>
    Effect.gen(function* () {
      const request: Protocol.PullRequest = {
        clientId: Ids.ClientId.make("c"),
        cursor: Schema.decodeUnknownSync(Ids.Position)("3"),
        continuation: Option.none()
      }
      const resnapshot = yield* harness(
        serve({ pull: () => Effect.fail(new Protocol.ResnapshotRequired({ reason: "gcHorizon" })) }),
        (t) => t.pull(request)
      )
      const error = failureOf(resnapshot.exit)
      expect(error).toEqual(new Protocol.ResnapshotRequired({ reason: "gcHorizon" }))

      // The server's store is down: the engine sees core's typed ServerUnavailable, a transient pull failure.
      const down = yield* harness(
        serve({ pull: () => Effect.fail(new Protocol.ServerUnavailable({ message: "db down" })) }),
        (t) => t.pull(request)
      )
      const unavailable = failureOf(down.exit)
      expect(unavailable).toBeInstanceOf(Protocol.ServerUnavailable)
      expect(unavailable).toEqual(new Protocol.ServerUnavailable({ message: "db down" }))
      expect(Transport.isFatalPush(unavailable)).toBe(false)

      const noId = yield* harness(skewedPull, (t) => t.pull(request))
      const dies = yield* harness(serve({ pull: () => Effect.die(new Error("bug")) }), (t) => t.pull(request))
      for (const exit of [noId.exit, dies.exit]) {
        const failure = failureOf(exit)
        expect(failure).toBeInstanceOf(Transport.PullProtocolError)
        expect(Transport.isFatalPush(failure)).toBe(false)
      }
    }).pipe(Effect.scoped)
)

it.effect("a transport built with headers sends them on push, pull and the pokes subscription", () =>
  Effect.gen(function* () {
    // What the server's handlers saw in the header, per RPC: a missing header on pokes would be an
    // unauthenticated subscription the client never notices.
    const seen: Record<string, string | undefined> = {}
    const app = RpcServer.layerHttp({ group: Protocol.SyncRpcs, path: "/rpc", protocol: "http" }).pipe(
      Layer.provide([
        Protocol.SyncRpcs.toLayer({
          push: (request, { headers }) =>
            Effect.sync(() => {
              seen["push"] = headers["x-echo-principal"]
              return {
                results: request.mutations.map((m) => ({
                  mutationId: m.mutationId,
                  position,
                  outcome: { _tag: "Applied" as const }
                }))
              }
            }),
          pull: (_, { headers }) =>
            Effect.sync(() => {
              seen["pull"] = headers["x-echo-principal"]
              return Schema.decodeUnknownSync(Protocol.PullDone)({
                _tag: "PullDone",
                changes: [],
                acks: [],
                nextCursor: "1",
                expiredThrough: 0
              })
            }),
          pokes: (_, { headers }) =>
            Stream.fromEffect(Effect.sync(() => (seen["pokes"] = headers["x-echo-principal"]))).pipe(
              Stream.as({ _tag: "poke" as const })
            )
        }),
        RpcSerialization.layerNdjson
      ])
    )
    const web = HttpRouter.toWebHandler(app, { disableLogger: true })
    yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
    const fetch = Object.assign(
      (input: URL | RequestInfo, init?: RequestInit) => web.handler(new Request(input, init)),
      {
        preconnect: globalThis.fetch.preconnect
      }
    )
    // `FetchHttpClient` reads `Fetch` from the fiber that sends each request.
    yield* Effect.gen(function* () {
      const t = yield* Transport.make.pipe(
        Effect.provide(
          Transport.layerProtocolHttp("http://echo.test/rpc", Effect.succeed({ "x-echo-principal": "alice" }))
        )
      )
      yield* t.push(batch(1))
      yield* t.pull({ clientId: Ids.ClientId.make("c"), cursor: null, continuation: Option.none() })
      yield* Stream.runHead(Stream.unwrap(t.pokes(Ids.ClientId.make("c"))))
    }).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
    expect(seen).toEqual({ push: "alice", pull: "alice", pokes: "alice" })
  }).pipe(Effect.scoped)
)

/**
 * A server that authenticates every request against the currently valid token (as an app's
 * `principalFor` would) and records which token each push, pull and pokes connect carried, plus a
 * transport whose `syncHeaders` Effect returns the client's current token and counts evaluations.
 */
const credentials = Effect.gen(function* () {
  // `failNext`: that many next `syncHeaders` evaluations die with `headerDefect` (the app's token store throws).
  // `pullUnavailable`: pulls fail `ServerUnavailable`; `stopPush`: pushes process nothing (a stopped batch).
  // `pullDefect` / `pushDefect`: the server's handler dies with that value.
  const state: {
    valid: string
    client: string
    evaluations: number
    failNext: number
    headerDefect: unknown
    fetches: number
    pullUnavailable: boolean
    stopPush: boolean
    pullDefect: unknown
    pushDefect: unknown
  } = {
    valid: "t1",
    client: "t1",
    evaluations: 0,
    failNext: 0,
    headerDefect: new Error("token store unavailable"),
    fetches: 0,
    pullUnavailable: false,
    stopPush: false,
    pullDefect: undefined,
    pushDefect: undefined
  }
  const seen: Array<string> = []
  const authenticate = (rpc: string, headers: Readonly<Record<string, string | undefined>>) =>
    Effect.suspend(() => {
      const token = headers["authorization"] ?? "none"
      const ok = token === state.valid
      seen.push(`${rpc}:${token}${ok ? "" : ":rejected"}`)
      return ok ? Effect.void : Effect.fail(new Protocol.Unauthorized({ message: `stale ${token}` }))
    })
  const app = RpcServer.layerHttp({ group: Protocol.SyncRpcs, path: "/rpc", protocol: "http" }).pipe(
    Layer.provide([
      Protocol.SyncRpcs.toLayer({
        push: (request, { headers }) =>
          Effect.as(
            Effect.andThen(
              Effect.suspend(() => (state.pushDefect === undefined ? Effect.void : Effect.die(state.pushDefect))),
              authenticate(`push(${request.mutations.map((m) => `${m.mutationId}#${m.seq}`).join(",")})`, headers)
            ),
            {
              results: state.stopPush
                ? []
                : request.mutations.map((m) => ({
                    mutationId: m.mutationId,
                    position,
                    outcome: { _tag: "Applied" as const }
                  }))
            }
          ),
        pull: (_, { headers }) =>
          Effect.as(
            Effect.andThen(
              authenticate("pull", headers),
              Effect.suspend(() =>
                state.pullDefect !== undefined
                  ? Effect.die(state.pullDefect)
                  : state.pullUnavailable
                    ? Effect.fail(new Protocol.ServerUnavailable({ message: "database is restarting" }))
                    : Effect.void
              )
            ),
            Schema.decodeUnknownSync(Protocol.PullDone)({
              _tag: "PullDone",
              changes: [],
              acks: [],
              nextCursor: "1",
              expiredThrough: 0
            })
          ),
        pokes: (_, { headers }) =>
          // The initial poke, then silence (no heartbeat): only the client's idleTimeout ends it.
          Stream.concat(
            Stream.fromEffect(authenticate("pokes", headers)).pipe(Stream.as({ _tag: "poke" as const })),
            Stream.never
          )
      }),
      RpcSerialization.layerNdjson
    ])
  )
  const web = HttpRouter.toWebHandler(app, { disableLogger: true })
  yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
  const fetch = Object.assign(
    (input: URL | RequestInfo, init?: RequestInit) => {
      state.fetches++
      return web.handler(new Request(input, init))
    },
    { preconnect: globalThis.fetch.preconnect }
  )
  // The leader's `syncHeaders(principal)`: the app's current credential, renewed by setting `state.client`.
  const syncHeaders = Effect.suspend(() => {
    state.evaluations++
    if (state.failNext > 0) {
      state.failNext--
      return Effect.die(state.headerDefect)
    }
    return Effect.succeed({ authorization: state.client })
  })
  const transport = yield* Transport.make.pipe(
    Effect.provide(Transport.layerProtocolHttp("http://echo.test/rpc", syncHeaders))
  )
  return { state, seen, transport, fetch }
})

const pullRequest = { clientId: Ids.ClientId.make("c"), cursor: null, continuation: Option.none() }

it.effect(
  "a token renewed after the transport was built is used by the next push, pull and pokes reconnect, and syncHeaders runs once per request",
  () =>
    Effect.gen(function* () {
      const { state, seen, transport: t, fetch } = yield* credentials
      yield* Effect.gen(function* () {
        // Built once, like the leader's `connect`; each run is one (re)connect.
        const pokes = Stream.unwrap(t.pokes(Ids.ClientId.make("c")))
        yield* t.push(batch(1))
        yield* t.pull(pullRequest)
        yield* Stream.runHead(pokes)
        // The app renews: the old token is no longer accepted, and the client now returns the new one.
        state.valid = "t2"
        state.client = "t2"
        yield* t.push(batch(1))
        yield* t.pull(pullRequest)
        yield* Stream.runHead(pokes)
        yield* t.pull(pullRequest)
      }).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
      expect(seen).toEqual(["push(m1#1):t1", "pull:t1", "pokes:t1", "push(m1#1):t2", "pull:t2", "pokes:t2", "pull:t2"])
      // Seven requests, seven evaluations: never cached for the leader's lifetime, never twice per request.
      expect(state.evaluations).toBe(7)
    }).pipe(Effect.scoped)
)

/** A real Engine (memory storage) holding one pending mutation, `m1#1`, and the real sync loop over `transport`. */
const loopOver = (transport: Effect.Success<typeof Transport.make>) =>
  Effect.gen(function* () {
    const storage = MemorySyncStorage.make(MemorySyncStorage.makeDisk())
    const engine = yield* Engine.make({
      clientId: Ids.ClientId.make("c"),
      actor: AccessScope.PrincipalId.make("p"),
      storage,
      registry: Registry.make([]),
      mutations: Mutation.makeRegistry([
        Mutation.make("noop", { args: Schema.Struct({}), handler: () => Effect.void })
      ]),
      transport
    })
    yield* engine.mutate(Ids.MutationId.make("m1"), "noop", {})
    const loop = yield* SyncLoop.make({
      engine,
      isFatalPush: Transport.isFatalPush,
      pendingMutations: Stream.concat(Stream.make(1), Stream.never),
      retryDelay: "10 millis",
      maxRetryDelay: "40 millis"
    })
    const statuses: Array<SyncLoop.SyncStatus> = []
    yield* Effect.forkChild(Stream.runForEach(loop.changes, (status) => Effect.sync(() => void statuses.push(status))))
    return { loop, statuses, pending: storage.transaction(storage.pending) }
  })

/** Real time: polls `check` every 10 ms for up to 5 s. */
const eventually = (check: () => boolean, what: string) =>
  Effect.gen(function* () {
    for (let i = 0; i < 500; i++) {
      if (check()) return
      yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 10)))
    }
    return yield* Effect.die(`timed out waiting for ${what}`)
  })

it.live(
  "an Unauthorized push followed by renewal succeeds on the loop's retry with the new header and the same outbox entry, never halting",
  () =>
    Effect.gen(function* () {
      const { state, seen, transport, fetch } = yield* credentials
      // The token expired on the server before the client renewed it.
      state.valid = "t2"
      const { loop, statuses } = yield* loopOver(transport)
      yield* Effect.forkChild(loop.run.pipe(Effect.provideService(FetchHttpClient.Fetch, fetch)))
      yield* eventually(() => seen.includes("push(m1#1):t1:rejected"), "a rejected push")
      // The app renews; the loop's next retry carries it.
      state.client = "t2"
      yield* eventually(() => seen.includes("push(m1#1):t2"), "the renewed push")
      const pushes = seen.filter((e) => e.startsWith("push"))
      const accepted = pushes.indexOf("push(m1#1):t2")
      expect(pushes.slice(0, accepted).every((e) => e === "push(m1#1):t1:rejected")).toBe(true)
      expect(accepted).toBeGreaterThan(0)
      expect(statuses.some((s) => s.lastError !== null)).toBe(true)
      expect(statuses.map((s) => s.push._tag)).not.toContain("Halted")
    }).pipe(Effect.scoped),
  15_000
)

it.live(
  "a syncHeaders Effect that dies is a transient local failure: nothing is sent, the loop retries the same entry, and push never halts",
  () =>
    Effect.gen(function* () {
      const { state, seen, transport, fetch } = yield* credentials
      state.failNext = 1
      const { loop, statuses } = yield* loopOver(transport)
      yield* Effect.forkChild(loop.run.pipe(Effect.provideService(FetchHttpClient.Fetch, fetch)))
      yield* eventually(() => seen.includes("push(m1#1):t1"), "the push after the failed acquisition")
      // The first push never reached the server; the retry sent the same entry once.
      expect(seen.filter((e) => e.startsWith("push"))).toEqual(["push(m1#1):t1"])
      expect(statuses.some((s) => s.lastError !== null)).toBe(true)
      expect(statuses.map((s) => s.push._tag)).not.toContain("Halted")
    }).pipe(Effect.scoped),
  15_000
)

it.effect(
  "a syncHeaders interruption whose cleanup also dies stays an interruption: it is never turned into a credentials failure, and nothing is sent",
  () =>
    Effect.gen(function* () {
      const fetches = { n: 0 }
      const fetch = Object.assign(
        (_input: URL | RequestInfo, _init?: RequestInit) => {
          fetches.n++
          return Promise.reject(new TypeError("must not be sent"))
        },
        { preconnect: globalThis.fetch.preconnect }
      )
      const t = yield* Transport.make.pipe(
        Effect.provide(
          Transport.layerProtocolHttp(
            "http://echo.test/rpc",
            Effect.interrupt.pipe(Effect.ensuring(Effect.die(new Error("cleanup bug"))))
          )
        )
      )
      const exit = yield* Effect.exit(t.push(batch(1)).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch)))
      expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true)
      expect(Exit.isFailure(exit) ? Cause.pretty(exit.cause) : "").not.toContain("CredentialsUnavailable")
      expect(fetches.n).toBe(0)
    }).pipe(Effect.scoped)
)

it.live(
  "a pokes reconnect whose syncHeaders takes longer than idleTimeout evaluates them once, then sends once; silence after the send still ends the stream",
  () =>
    Effect.gen(function* () {
      const { state, fetch } = yield* credentials
      // Slow headers (300 ms) on a short idleTimeout (100 ms): preparation is not stream silence.
      const slow = Transport.make.pipe(
        Effect.provide(
          Transport.layerProtocolHttp(
            "http://echo.test/rpc",
            Effect.andThen(
              Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 300))),
              Effect.sync(() => {
                state.evaluations++
                return { authorization: state.client }
              })
            )
          )
        )
      )
      const t = yield* slow
      const wakes = { n: 0 }
      yield* Effect.forkChild(
        SyncLoop.followPokes({
          connect: t.pokes(Ids.ClientId.make("c")),
          wake: Effect.sync(() => void wakes.n++),
          idleTimeout: "100 millis",
          retryDelay: "1 second"
        }).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
      )
      // The connect's request went out once, with the headers of its one evaluation, and delivered its poke.
      yield* eventually(() => wakes.n === 1, "the initial poke")
      expect([state.evaluations, state.fetches]).toEqual([1, 1])
      // The server then sends nothing: 100 ms later the stream ends, and after the 1 s backoff the
      // follower prepares and sends a second connect.
      yield* eventually(() => state.fetches === 2, "the reconnect after post-send silence")
      expect(state.evaluations).toBe(2)
    }).pipe(Effect.scoped),
  15_000
)

/** The first `lastError` the loop reported. */
const firstError = (statuses: ReadonlyArray<SyncLoop.SyncStatus>) =>
  statuses.find((s) => s.lastError !== null)?.lastError

/** Runs the real loop over the fixture until it reports an error, and returns that error. */
const errorOf = (fixture: Effect.Success<typeof credentials>, transport = fixture.transport) =>
  Effect.gen(function* () {
    const { loop, statuses } = yield* loopOver(transport)
    yield* Effect.forkChild(loop.run.pipe(Effect.provideService(FetchHttpClient.Fetch, fixture.fetch)))
    yield* eventually(() => firstError(statuses) !== undefined, "a lastError")
    return firstError(statuses)
  })

const secret = "Bearer secret-4f9c2e"

it.live("a syncHeaders that dies is reported as kind credentials", () =>
  Effect.gen(function* () {
    const fixture = yield* credentials
    fixture.state.failNext = 1
    expect((yield* errorOf(fixture))?.kind).toBe("credentials")
  }).pipe(Effect.scoped)
)

it.live(
  "a refused connection is reported as kind network, and the request's headers never reach the message",
  () =>
    Effect.gen(function* () {
      // Nothing listens on port 1: the request carries the headers and fails before any response. A
      // custom header name, since Effect's HTTP headers already redact `authorization`.
      const refused = yield* Transport.make.pipe(
        Effect.provide(
          Transport.layerProtocolHttp("http://127.0.0.1:1/rpc", Effect.succeed({ "x-app-session": secret }))
        )
      )
      const { loop, statuses } = yield* loopOver(refused)
      yield* Effect.forkChild(loop.run)
      yield* eventually(() => firstError(statuses) !== undefined, "a lastError")
      const error = firstError(statuses)
      expect(error?.kind).toBe("network")
      expect(error?.message.trim().length).toBeGreaterThan(0)
      expect(error?.message).not.toContain("secret-4f9c2e")
    }).pipe(Effect.scoped),
  15_000
)

it.live("a server Unauthorized is reported as kind unauthorized", () =>
  Effect.gen(function* () {
    const fixture = yield* credentials
    fixture.state.valid = "t2" // the client still sends t1
    expect((yield* errorOf(fixture))?.kind).toBe("unauthorized")
  }).pipe(Effect.scoped)
)

it.live(
  "HTTP auth refusals before RPC decoding report unauthorized, retry without halting, and keep the pending mutation; other statuses stay network",
  () =>
    Effect.gen(function* () {
      for (const status of [401, 403, 500]) {
        yield* Effect.gen(function* () {
          const received: Array<string | undefined> = []
          const refusal = HttpRouter.middleware((httpEffect) =>
            Effect.gen(function* () {
              const request = yield* HttpServerRequest.HttpServerRequest
              received.push(request.headers["authorization"])
              if (status !== 200) return HttpServerResponse.empty({ status })
              return yield* httpEffect
            })
          )
          const app = RpcServer.layerHttp({ group: Protocol.SyncRpcs, path: "/sync", protocol: "http" }).pipe(
            Layer.provide([
              Protocol.SyncRpcs.toLayer({
                push: () => Effect.die("middleware must refuse before RPC"),
                pull: () => Effect.die("middleware must refuse before RPC"),
                pokes: () => Stream.die("middleware must refuse before RPC")
              }),
              RpcSerialization.layerNdjson,
              refusal.layer
            ])
          )
          const web = HttpRouter.toWebHandler(app, { disableLogger: true })
          yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
          const fetch = Object.assign(
            (input: URL | RequestInfo, init?: RequestInit) => web.handler(new Request(input, init)),
            { preconnect: globalThis.fetch.preconnect }
          )
          const transport = yield* Transport.make.pipe(
            Effect.provide(
              Transport.layerProtocolHttp("http://echo.test/sync", Effect.succeed({ authorization: "valid-token" }))
            )
          )
          const { loop, statuses, pending } = yield* loopOver(transport)
          yield* Effect.forkChild(loop.run.pipe(Effect.provideService(FetchHttpClient.Fetch, fetch)))
          yield* eventually(
            () => statuses.filter((s) => s.phase === "backoff").length >= 2,
            "two failed cycles with backoff"
          )
          expect(firstError(statuses)).toEqual(
            status === 500
              ? { kind: "network", message: "the request produced no usable response" }
              : { kind: "unauthorized", message: "the server rejected the credential" }
          )
          expect(received.length).toBeGreaterThanOrEqual(4) // push and pull, then their retries
          expect(received.every((token) => token === "valid-token")).toBe(true)
          expect(statuses.map((s) => s.push._tag)).not.toContain("Halted")
          expect(statuses.filter((s) => s.phase === "backoff").every((s) => s.nextRetryAt !== null)).toBe(true)
          expect((yield* pending).map((entry) => [entry.envelope.mutationId, entry.envelope.seq])).toEqual([["m1", 1]])
        }).pipe(Effect.scoped)
      }
    }).pipe(Effect.scoped)
)

it.live("a server ServerUnavailable is reported as kind unavailable", () =>
  Effect.gen(function* () {
    const fixture = yield* credentials
    fixture.state.pullUnavailable = true
    expect((yield* errorOf(fixture))?.kind).toBe("unavailable")
  }).pipe(Effect.scoped)
)

it.live("a push batch the server stopped is reported as kind server", () =>
  Effect.gen(function* () {
    const fixture = yield* credentials
    fixture.state.stopPush = true
    expect((yield* errorOf(fixture))?.kind).toBe("server")
  }).pipe(Effect.scoped)
)

/** Present in each failure's own text, URL or body below; no emitted status may contain it (DESIGN §9.1). */
const sentinel = "SENTINEL-7d3a91"

/** Every status the real loop emitted until `done`, serialized; the assertions search all of it. */
const statusText = (
  fixture: Effect.Success<typeof credentials>,
  done: (statuses: ReadonlyArray<SyncLoop.SyncStatus>) => boolean,
  transport = fixture.transport
) =>
  Effect.gen(function* () {
    const { loop, statuses } = yield* loopOver(transport)
    yield* Effect.forkChild(loop.run.pipe(Effect.provideService(FetchHttpClient.Fetch, fixture.fetch)))
    yield* eventually(() => done(statuses), "the failure in status")
    return { text: JSON.stringify(statuses), statuses }
  })

const hasError = (statuses: ReadonlyArray<SyncLoop.SyncStatus>) => firstError(statuses) !== undefined

it.live("no syncStatus string carries a secret from the failure: syncHeaders dying with an Error or a string", () =>
  Effect.gen(function* () {
    for (const defect of [new Error(`refresh failed ${sentinel}`), sentinel]) {
      const fixture = yield* credentials
      fixture.state.failNext = 1
      fixture.state.headerDefect = defect
      const { text, statuses } = yield* statusText(fixture, hasError)
      expect(firstError(statuses)?.kind).toBe("credentials")
      expect(text).not.toContain(sentinel)
    }
  }).pipe(Effect.scoped)
)

it.live("no syncStatus string carries a secret from the failure: a server Unauthorized echoing the token", () =>
  Effect.gen(function* () {
    const fixture = yield* credentials
    // The client sends the sentinel as its token; the server's Unauthorized text echoes it ("stale <token>").
    fixture.state.client = sentinel
    const { text, statuses } = yield* statusText(fixture, hasError)
    expect(firstError(statuses)?.kind).toBe("unauthorized")
    expect(text).not.toContain(sentinel)
  }).pipe(Effect.scoped)
)

it.live("no syncStatus string carries a secret from the failure: a pull defect carrying a request body", () =>
  Effect.gen(function* () {
    const fixture = yield* credentials
    fixture.state.pullDefect = { body: sentinel }
    const { text, statuses } = yield* statusText(fixture, hasError)
    expect(firstError(statuses)?.kind).toBe("server")
    expect(text).not.toContain(sentinel)
  }).pipe(Effect.scoped)
)

it.live("no syncStatus string carries a secret from the failure: a token in the request URL", () =>
  Effect.gen(function* () {
    const fixture = yield* credentials
    // Nothing listens on port 1; the transport error's text names the URL.
    const refused = yield* Transport.make.pipe(
      Effect.provide(Transport.layerProtocolHttp(`http://127.0.0.1:1/${sentinel}/rpc`, Effect.succeed({})))
    )
    const { text, statuses } = yield* statusText(fixture, hasError, refused)
    expect(firstError(statuses)?.kind).toBe("network")
    expect(text).not.toContain(sentinel)
  }).pipe(Effect.scoped)
)

it.live("no syncStatus string carries a secret from the failure: the Halted reason of a server push defect", () =>
  Effect.gen(function* () {
    const fixture = yield* credentials
    fixture.state.pushDefect = new Error(`handler crashed on ${sentinel}`)
    const { text, statuses } = yield* statusText(fixture, (all) => all.some((s) => s.push._tag === "Halted"))
    expect(text).not.toContain(sentinel)
    expect(statuses.find((s) => s.push._tag === "Halted")?.push).toEqual({
      _tag: "Halted",
      message: "the server ended the push with a defect"
    })
  }).pipe(Effect.scoped)
)
