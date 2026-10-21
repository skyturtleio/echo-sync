import { expect, it } from "@effect/vitest"
import { Effect, Exit, Layer, Option, Schema, Stream } from "effect"
import { FetchHttpClient, HttpRouter } from "effect/http"
import { Rpc, RpcClient, RpcGroup, RpcSerialization, RpcServer } from "effect/rpc"
import { AccessScope, Ids, Mutation, Protocol, Registry } from "echo-sync"
import * as Engine from "./Engine.ts"
import * as MemorySyncStorage from "./testing/MemorySyncStorage.ts"
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
      yield* Stream.runHead(t.pokes(Ids.ClientId.make("c")))
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
  const state = { valid: "t1", client: "t1", evaluations: 0 }
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
            authenticate(`push(${request.mutations.map((m) => `${m.mutationId}#${m.seq}`).join(",")})`, headers),
            {
              results: request.mutations.map((m) => ({
                mutationId: m.mutationId,
                position,
                outcome: { _tag: "Applied" as const }
              }))
            }
          ),
        pull: (_, { headers }) =>
          Effect.as(
            authenticate("pull", headers),
            Schema.decodeUnknownSync(Protocol.PullDone)({
              _tag: "PullDone",
              changes: [],
              acks: [],
              nextCursor: "1",
              expiredThrough: 0
            })
          ),
        pokes: (_, { headers }) =>
          Stream.fromEffect(authenticate("pokes", headers)).pipe(Stream.as({ _tag: "poke" as const }))
      }),
      RpcSerialization.layerNdjson
    ])
  )
  const web = HttpRouter.toWebHandler(app, { disableLogger: true })
  yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
  const fetch = Object.assign((input: URL | RequestInfo, init?: RequestInit) => web.handler(new Request(input, init)), {
    preconnect: globalThis.fetch.preconnect
  })
  // The leader's `syncHeaders(principal)`: the app's current credential, renewed by setting `state.client`.
  const syncHeaders = Effect.sync(() => {
    state.evaluations++
    return { authorization: state.client }
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
        const pokes = t.pokes(Ids.ClientId.make("c"))
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

it.effect(
  "an Unauthorized push followed by renewal succeeds on the retry with the new header and the same outbox entry",
  () =>
    Effect.gen(function* () {
      const { state, seen, transport, fetch } = yield* credentials
      const disk = MemorySyncStorage.makeDisk()
      const engine = yield* Engine.make({
        clientId: Ids.ClientId.make("c"),
        actor: AccessScope.PrincipalId.make("p"),
        storage: MemorySyncStorage.make(disk),
        registry: Registry.make([]),
        mutations: Mutation.makeRegistry([
          Mutation.make("noop", { args: Schema.Struct({}), handler: () => Effect.void })
        ]),
        transport
      })
      yield* engine.mutate(Ids.MutationId.make("m1"), "noop", {})
      // The token expired on the server before the client renewed it.
      state.valid = "t2"
      const refused = yield* Effect.exit(engine.push.pipe(Effect.provideService(FetchHttpClient.Fetch, fetch)))
      const error = failureOf(refused)
      expect(error).toBeInstanceOf(Protocol.Unauthorized)
      if (error instanceof Protocol.Unauthorized) expect(Transport.isFatalPush(error)).toBe(false)
      expect(disk.outbox.map((p) => `${p.envelope.mutationId}#${p.envelope.seq}`)).toEqual(["m1#1"])
      // The app renews; the loop's retry pushes the same entry, now with the new token.
      state.client = "t2"
      const retried = yield* engine.push.pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
      expect(retried.processed).toBe(1)
      expect(seen).toEqual(["push(m1#1):t1:rejected", "push(m1#1):t2"])
    }).pipe(Effect.scoped)
)
