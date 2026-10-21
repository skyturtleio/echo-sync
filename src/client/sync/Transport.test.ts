import { expect, it } from "@effect/vitest"
import { Effect, Exit, Layer, Option, Schema, Stream } from "effect"
import { FetchHttpClient, HttpRouter } from "effect/http"
import { Rpc, RpcClient, RpcGroup, RpcSerialization, RpcServer } from "effect/rpc"
import { Ids, Mutation, Protocol } from "echo-sync"
import * as Transport from "./Transport.ts"
import * as Wire from "./Wire.ts"

interface Handlers {
  readonly push?: (
    request: Protocol.PushRequest
  ) => Effect.Effect<Protocol.PushResponse, Protocol.MalformedEnvelope | Protocol.Unauthorized>
  readonly pull?: (
    request: Protocol.PullRequest
  ) => Effect.Effect<Protocol.PullResponse, Protocol.ResnapshotRequired | Protocol.Unauthorized>
}

/**
 * A real RpcServer (HTTP, NDJSON) behind a web handler, and the leader's Transport talking to it
 * through `fetch`. `served` counts requests that reached the server.
 */
const harness = <A extends Transport.TransportError | Wire.ResnapshotRequired, B>(
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

const batch = (seq: number): Wire.PushRequest => ({
  clientId: "c",
  mutations: [{ mutationId: "m1", seq, name: "todo.add", args: { title: "x" } }]
})
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
        seq: 1,
        tag: "MalformedEnvelope",
        fatal: true,
        served: 1
      },
      {
        name: "server cannot decode the push payload",
        server: skewed,
        seq: 1,
        tag: "PushIntegrityError",
        fatal: true,
        served: 1
      },
      {
        name: "push handler dies",
        server: serve({ push: () => Effect.die(new Error("bug")) }),
        seq: 1,
        tag: "PushIntegrityError",
        fatal: true,
        served: 1
      },
      {
        name: "the outbox batch is not a valid PushRequest (seq 0)",
        server: serve({}),
        seq: 0,
        tag: "PushIntegrityError",
        fatal: true,
        served: 0
      },
      { name: "network down", server: "unreachable" as const, seq: 1, tag: "RpcClientError", fatal: false, served: 0 },
      {
        name: "Unauthorized",
        server: serve({ push: () => Effect.fail(new Protocol.Unauthorized({ message: "expired" })) }),
        seq: 1,
        tag: "Unauthorized",
        fatal: false,
        served: 1
      }
    ]
    for (const c of cases) {
      const { exit, served } = yield* harness(c.server, (t) => t.push(batch(c.seq)))
      const failure = failureOf(exit)
      const tag = Schema.is(Schema.Struct({ _tag: Schema.String }))(failure) ? failure._tag : failure
      expect({ case: c.name, tag, fatal: Transport.isFatalPush(failure), served }).toEqual({
        case: c.name,
        tag: c.tag,
        fatal: c.fatal,
        served: c.served
      })
    }
  }).pipe(Effect.scoped)
)

it.effect("a processed push returns the engine's shapes; a rejection keeps its tag and fields", () =>
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
    expect(exit).toEqual(
      Exit.succeed({
        results: [
          {
            mutationId: "m1",
            position: 7n,
            outcome: { _tag: "Rejected", error: { _tag: "UnknownMutation", name: "todo.add" } }
          }
        ]
      })
    )
  }).pipe(Effect.scoped)
)

it.effect(
  "pull: ResnapshotRequired reaches the engine as its own class; protocol faults are transient failures, not defects",
  () =>
    Effect.gen(function* () {
      const request: Wire.PullRequest = { clientId: "c", cursor: 3n, continuation: Option.none() }
      const resnapshot = yield* harness(
        serve({ pull: () => Effect.fail(new Protocol.ResnapshotRequired({ reason: "gcHorizon" })) }),
        (t) => t.pull(request)
      )
      const error = failureOf(resnapshot.exit)
      expect(error).toBeInstanceOf(Wire.ResnapshotRequired)

      const noId = yield* harness(
        serve({
          pull: () =>
            Effect.succeed({
              _tag: "PullDone" as const,
              changes: [{ _tag: "upsert" as const, table: "t", row: { access_scope_id: "s" } }],
              acks: [],
              nextCursor: position,
              expiredThrough: Ids.SeqFloor.make(0)
            })
        }),
        (t) => t.pull(request)
      )
      const dies = yield* harness(serve({ pull: () => Effect.die(new Error("bug")) }), (t) => t.pull(request))
      for (const exit of [noId.exit, dies.exit]) {
        const failure = failureOf(exit)
        expect(failure).toBeInstanceOf(Transport.PullProtocolError)
        expect(Transport.isFatalPush(failure)).toBe(false)
      }
    }).pipe(Effect.scoped)
)
