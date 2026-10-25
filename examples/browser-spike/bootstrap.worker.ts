/** App-owned runtime: keep only aggregate install timings, never rows or span histories. */
import { Effect, Tracer } from "effect"
import { leaderWorker } from "../../src/client/worker/index.ts"
import { clientMutations, principalHeader, registry } from "./app.ts"

let longestInstallMs = 0
let installs = 0
const tracer = Tracer.make({
  span(options) {
    const span = new Tracer.NativeSpan(options)
    const end = span.end.bind(span)
    span.end = (time, exit) => {
      end(time, exit)
      if (span.name !== "echo.install") return
      longestInstallMs = Math.max(longestInstallMs, Number(time - span.startTime) / 1_000_000)
      installs++
    }
    return span
  }
})
self.addEventListener("message", (event: MessageEvent<unknown>) => {
  if (event.data === "bootstrap-metrics") {
    self.postMessage({ bootstrapMetrics: { longestInstallMs, installs } }, { transfer: [] })
  }
})
Effect.runFork(
  leaderWorker(self, {
    registry,
    mutations: clientMutations,
    allowRawExec: true,
    syncUrl: `${self.location.origin}/sync`,
    syncHeaders: (principal) => Effect.succeed({ [principalHeader]: principal })
  }).pipe(Effect.provideService(Tracer.Tracer, tracer))
)
