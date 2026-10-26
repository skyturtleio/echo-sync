/** Native OPFS test entry: host-owned runtime, tracerLogger, and deliberate sensitive failures. */
import { Array as Arr, Effect, Fiber, Metric, Schema, String as Str } from "effect"
import { Mutation } from "../../src/core/index.ts"
import { leaderWorker } from "../../src/client/worker/index.ts"
import { recorder, serialized } from "../../test/telemetry-recorder.ts"
import { registry } from "./app.ts"

const recording = recorder()
let identity = ""
self.addEventListener("message", (event: MessageEvent<unknown>) => {
  if (
    typeof event.data === "object" &&
    event.data !== null &&
    "appId" in event.data &&
    typeof event.data.appId === "string"
  )
    identity = event.data.appId
})
const mutations = Mutation.makeRegistry([
  Mutation.make("t", { args: Schema.Json, handler: () => Effect.void }),
  Mutation.make("die", {
    args: Schema.String,
    handler: (_ctx, value) =>
      Effect.die(new Error(value)).pipe(
        Effect.withSpan(`echo.unapproved.${value}`, { attributes: { authorization: value } })
      )
  })
])
// The harness owns worker termination so it can inspect the program after scope closure.
const endpoint = {
  addEventListener: self.addEventListener.bind(self),
  removeEventListener: self.removeEventListener.bind(self),
  postMessage: self.postMessage.bind(self),
  close() {}
}
const fiber = Effect.runFork(
  leaderWorker(endpoint, {
    registry,
    mutations,
    syncUrl: `${self.location.origin}/fake-sync/rpc`,
    syncHeaders: (principal) => Effect.succeed({ authorization: principal })
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Worker-realm telemetry root owns this recording for the leader fiber's lifetime.
  }).pipe(Effect.provide(recording.layer))
)

self.addEventListener("message", (event: MessageEvent) => {
  if (event.data !== "telemetry-stop") return
  Effect.runFork(
    Effect.gen(function* () {
      yield* Fiber.interrupt(fiber)
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Worker-realm report runner reads the same recorder after the leader fiber closes.
      const metrics = yield* Metric.snapshot.pipe(Effect.provide(recording.layer))
      let held = yield* Effect.promise(() => navigator.locks.query())
      // Lock release resolves a browser promise; scope closure is not a browser-task completion barrier.
      for (
        let i = 0;
        i < 100 &&
        Arr.some(
          held.held ?? [],
          (lock) => lock.name !== undefined && Str.startsWith(`echo:${identity}:native:leader`)(lock.name)
        );
        i++
      ) {
        yield* Effect.sleep("20 millis")
        held = yield* Effect.promise(() => navigator.locks.query())
      }
      const telemetry = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(
        serialized({ spans: recording.spans, logs: recording.logs, metrics, held })
      )
      self.postMessage({ telemetry }, { transfer: [] })
    })
  )
})
