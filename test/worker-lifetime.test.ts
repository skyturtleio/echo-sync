// oxlint-disable-next-line import/no-unassigned-import -- Node wasm fixture
import "../src/client/testing/nodeWasm.ts"
import { it } from "@effect/vitest"
import { Deferred, Effect, Fiber } from "effect"
import { Mutation } from "echo-sync"
import { leaderWorker } from "../src/client/worker/Leader.ts"
import { router } from "../src/client/worker/Router.ts"
import { registry } from "../src/server/postgres/test-support.ts"

const transferred = () =>
  Effect.gen(function* () {
    const channel = new MessageChannel()
    const closed = yield* Deferred.make<void>()
    channel.port2.addEventListener("close", () => Deferred.doneUnsafe(closed, Effect.void))
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        channel.port1.close()
        channel.port2.close()
      })
    )
    return { port: channel.port1, closed: Deferred.await(closed).pipe(Effect.timeout("1 second")) }
  })

it.live(
  "leader teardown closes fresh transfers discarded before Init, still queued, or delivered to a saved late callback",
  () =>
    Effect.gen(function* () {
      for (const queued of [false, true]) {
        let listener: ((event: MessageEvent) => void) | undefined
        const fiber = yield* Effect.forkChild(
          leaderWorker(
            {
              addEventListener(_type, callback) {
                listener = callback
              },
              postMessage() {},
              close() {}
            },
            { registry, mutations: Mutation.makeRegistry([]) }
          )
        )
        yield* Effect.yieldNow
        const receive = listener
        if (receive === undefined) return yield* Effect.die("missing leader listener")
        const ports = yield* Effect.all([transferred(), transferred()])
        for (const [index, { port }] of ports.entries())
          receive(
            new MessageEvent("message", { data: { _tag: index === 0 ? "Connect" : "RouterPort", tabId: "tab", port } })
          )
        if (!queued) for (const port of ports) yield* port.closed
        yield* Fiber.interrupt(fiber)
        for (const port of ports) yield* port.closed
        for (const tag of ["Connect", "RouterPort"]) {
          const late = yield* transferred()
          receive(new MessageEvent("message", { data: { _tag: tag, tabId: "late", port: late.port } }))
          yield* late.closed
        }
      }
    }).pipe(Effect.scoped)
)

it.live(
  "router teardown and rejection close fresh connect and announcement peers, including saved late callbacks",
  () =>
    Effect.gen(function* () {
      let listener: ((event: MessageEvent) => void) | undefined
      const fiber = yield* Effect.forkChild(
        router({
          addEventListener(_type, callback) {
            listener = callback
          }
        })
      )
      yield* Effect.yieldNow
      const connect = listener
      if (connect === undefined) return yield* Effect.die("missing router listener")
      const control = yield* transferred()
      let messageListener: EventListenerOrEventListenerObject | undefined
      const add = control.port.addEventListener.bind(control.port)
      control.port.addEventListener = (
        type: string,
        callback: EventListenerOrEventListenerObject | null,
        options?: boolean | AddEventListenerOptions
      ) => {
        if (type === "message" && callback !== null) messageListener = callback
        if (callback !== null) add(type, callback, options)
      }
      connect(new MessageEvent("connect", { ports: [control.port] }))
      const receive = messageListener
      if (typeof receive !== "function") return yield* Effect.die("missing router port listener")
      const rejected = yield* transferred()
      receive(new MessageEvent("message", { data: { _tag: "Leader", leaderId: "early", port: rejected.port } }))
      yield* rejected.closed
      yield* Fiber.interrupt(fiber)
      yield* control.closed
      const lateConnect = yield* transferred()
      connect(new MessageEvent("connect", { ports: [lateConnect.port] }))
      yield* lateConnect.closed
      const lateAnnouncement = yield* transferred()
      receive(new MessageEvent("message", { data: { _tag: "Leader", leaderId: "late", port: lateAnnouncement.port } }))
      yield* lateAnnouncement.closed
    }).pipe(Effect.scoped)
)
