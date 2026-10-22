/** Driven by observability.sh, separate from the unchanged substrate/slice suites. */
import { Effect, Exit, Fiber, Layer } from "effect"
import * as BrowserWorker from "@effect/platform-browser/BrowserWorker"
import { RpcClient } from "effect/rpc"
import { Ids } from "../../src/core/index.ts"
import { LeaderRpcs } from "../../src/client/worker/LeaderRpcs.ts"
import type { FromLeader, ToLeader } from "../../src/client/worker/messages.ts"
import { router } from "../../src/client/worker/Router.ts"
import * as Platform from "../../src/client/worker/platform.ts"

const noop = () => {}

/** A completed announcement must leave its watcher alive; router teardown must then end it. */
const routerTeardown = () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const identity = { appId: `router-telemetry-${crypto.randomUUID()}`, profile: "scope" }
      const epoch = Platform.leaderEpochLockName(identity, "epoch")
      const tab = Platform.tabLockName(identity, "tab")
      yield* Platform.holdLock(epoch)
      yield* Platform.holdLock(tab)
      let listener: ((event: MessageEvent) => void) | undefined
      let removed = false
      const fiber = yield* Effect.forkChild(
        router({
          addEventListener(_type, callback) {
            listener = callback
          },
          removeEventListener() {
            removed = true
          }
        })
      )
      yield* Effect.yieldNow
      const connect = listener
      if (connect === undefined) return yield* Effect.die("router did not install listener")
      const channel = new MessageChannel()
      const leader = new MessageChannel()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          channel.port1.close()
          channel.port2.close()
          leader.port1.close()
          leader.port2.close()
        })
      )
      connect(new MessageEvent("connect", { ports: [channel.port1] }))
      channel.port2.postMessage({ _tag: "Hello", ...identity, tabId: "tab", connectedTo: null }, { transfer: [] })
      channel.port2.postMessage({ _tag: "Leader", leaderId: "epoch", port: leader.port1 }, [leader.port1])
      let before = yield* Effect.promise(() => navigator.locks.query())
      for (let i = 0; i < 100 && !before.pending?.some((lock) => lock.name === epoch); i++) {
        yield* Effect.sleep("20 millis")
        before = yield* Effect.promise(() => navigator.locks.query())
      }
      yield* Fiber.interrupt(fiber)
      // Exercise a saved callback directly: runForkWith executes synchronously, so scope checks after
      // launching the fiber would be too late to prevent callback work.
      connect(new MessageEvent("connect", { ports: [channel.port1] }))
      yield* Effect.sleep("50 millis")
      const after = yield* Effect.promise(() => navigator.locks.query())
      return {
        watched: before.pending?.some((lock) => lock.name === epoch) === true,
        removed,
        stopped: !after.pending?.some((lock) => lock.name === epoch || lock.name === tab)
      }
    }).pipe(Effect.scoped)
  )

export const run = async () => {
  const secret = "native-secret-arg-principal-defect-916582"
  const appId = `observability-${crypto.randomUUID()}`
  const worker = new Worker(new URL("./observability.worker.ts", import.meta.url), { type: "module" })
  const elected = new Promise<void>((resolve, reject) =>
    worker.addEventListener("message", (event: MessageEvent<FromLeader>) => {
      if (event.data._tag === "Elected") resolve()
      if (event.data._tag === "Failed") reject(new Error(event.data.message))
    })
  )
  const init: ToLeader = { _tag: "Init", appId, profile: "native", principal: secret }
  worker.postMessage(init, { transfer: [] })
  let release = noop
  try {
    await elected
    const tabId = crypto.randomUUID()
    const held = new Promise<void>((resolve) => {
      release = resolve
    })
    // A real tab lifetime lock, released after the engine program is interrupted.
    const lock = navigator.locks.request(`echo:${appId}:native:tab:${tabId}`, () => held)
    const channel = new MessageChannel()
    const connect: ToLeader = { _tag: "Connect", tabId, port: channel.port1 }
    worker.postMessage(connect, [channel.port1])
    const protocol = RpcClient.layerProtocolWorker({ size: 1, concurrency: 100 }).pipe(
      Layer.provide(BrowserWorker.layer(() => channel.port2))
    )
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* RpcClient.make(LeaderRpcs)
        yield* client.enqueue({ envelope: { mutationId: Ids.MutationId.make("saved"), name: "t", args: { secret } } })
        yield* Effect.exit(
          client.enqueue({ envelope: { mutationId: Ids.MutationId.make("typed"), name: secret, args: secret } })
        )
        // Wait until the actual sync loop has sent the pending batch and halted on its typed refusal.
        yield* Effect.sleep("1500 millis")
        const defect = yield* Effect.exit(
          client.enqueue({ envelope: { mutationId: Ids.MutationId.make("defect"), name: "die", args: secret } })
        )
        if (Exit.isSuccess(defect)) return yield* Effect.die("defect unexpectedly succeeded")
      }).pipe(Effect.provide(protocol), Effect.scoped)
    )
    const report = () =>
      new Promise<unknown>((resolve) => {
        const listener = (event: MessageEvent<unknown>) => {
          if (typeof event.data === "object" && event.data !== null && "telemetry" in event.data) {
            worker.removeEventListener("message", listener)
            resolve(event.data.telemetry)
          }
        }
        worker.addEventListener("message", listener)
        worker.postMessage("telemetry-stop", { transfer: [] })
      })
    const result = await report()
    // A late initialization cannot restart a stopped host-run program, even synchronously.
    worker.postMessage(init, { transfer: [] })
    const late = await report()
    release()
    await lock
    return { secret, appId, result, late, router: await routerTeardown() }
  } finally {
    release()
    worker.terminate()
  }
}
