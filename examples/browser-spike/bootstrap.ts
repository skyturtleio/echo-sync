import { Effect, Exit, Option, Scope, Stream } from "effect"
import { LeaderClient, ResetReplica } from "../../src/client/index.ts"
import type { Rows } from "../../src/client/worker/LeaderRpcs.ts"

const validate = (rows: Rows, scope: string, start: number, count: number) => {
  if (rows.length !== count) throw new Error(`expected ${count} rows, got ${rows.length}`)
  rows.forEach((row, offset) => {
    const i = start + offset
    if (
      row["id"] !== `${scope}-${String(i).padStart(6, "0")}` ||
      row["access_scope_id"] !== scope ||
      row["title"] !== `Todo ${i}` ||
      row["note"] !== (i % 3 === 0 ? null : `Note ${i} café`)
    )
      throw new Error(`row ${i} differs: ${JSON.stringify(row)}`)
  })
}
const select = "SELECT id, access_scope_id, title, note FROM todos ORDER BY id"

const trial = async (count: number) => {
  const response = await fetch(`/admin/bootstrap-seed?count=${count}`, { method: "POST" })
  if (!response.ok) throw new Error(await response.text())
  const seed: unknown = await response.json()
  if (
    typeof seed !== "object" ||
    seed === null ||
    !("scope" in seed) ||
    typeof seed.scope !== "string" ||
    !("principal" in seed) ||
    typeof seed.principal !== "string"
  )
    throw new Error("invalid seed response")
  const identity = { appId: `bootstrap-${crypto.randomUUID()}`, profile: "trial" }
  const lifetime = Scope.makeUnsafe()
  let worker: Worker | undefined
  const spawnLeader = (name: string) => {
    worker = new Worker(new URL("./bootstrap.worker.ts", import.meta.url), { type: "module", name })
    return worker
  }
  const started = performance.now()
  try {
    const client = await Effect.runPromise(
      LeaderClient.make({
        ...identity,
        principal: seed.principal,
        spawnLeader,
        spawnRouter: (name) =>
          new SharedWorker(new URL("./router.worker.ts", import.meta.url), { type: "module", name })
      }).pipe(Scope.provide(lifetime))
    )
    const status = await Effect.runPromise(Stream.runHead(client.status))
    if (Option.isNone(status) || status.value.mode !== "shared-worker") throw new Error("SharedWorker mode required")
    const first = await Effect.runPromise(
      client
        .subscribeRaw({
          tables: ["todos"],
          statements: [
            { sql: "SELECT count(*) AS n FROM todos", params: [] },
            { sql: `${select} LIMIT 10`, params: [] }
          ]
        })
        .pipe(
          Stream.filter((value) => value.results[0]?.[0]?.["n"] === count),
          Stream.runHead,
          Effect.timeout("30 minutes")
        )
    )
    if (Option.isNone(first)) throw new Error("query ended before full install")
    validate(first.value.results[1] ?? [], seed.scope, 1, Math.min(10, count))
    const firstUsableQueryMs = performance.now() - started
    const leader = worker
    if (leader === undefined) throw new Error("leader not spawned")
    // FIFO message to this leader samples completed spans after the first usable query.
    const metrics = await new Promise<{ longestInstallMs: number; installs: number }>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("metrics timeout")), 10_000)
      leader.addEventListener("message", function receive(event: MessageEvent<unknown>) {
        const data = event.data
        if (typeof data !== "object" || data === null || !("bootstrapMetrics" in data)) return
        const m = data.bootstrapMetrics
        if (
          typeof m !== "object" ||
          m === null ||
          !("longestInstallMs" in m) ||
          typeof m.longestInstallMs !== "number" ||
          !("installs" in m) ||
          typeof m.installs !== "number"
        )
          return
        clearTimeout(timer)
        leader.removeEventListener("message", receive)
        resolve({ longestInstallMs: m.longestInstallMs, installs: m.installs })
      })
      leader.postMessage("bootstrap-metrics", { transfer: [] })
    })
    if (metrics.installs < 1) throw new Error("no install span observed")
    // Full transfer and deterministic field validation are deliberately outside the timing window.
    const validationStarted = performance.now()
    for (let offset = 0; offset < count; offset += 1000) {
      const size = Math.min(1000, count - offset)
      const rows = await Effect.runPromise(client.exec(`${select} LIMIT ? OFFSET ?`, [size, offset]))
      validate(rows, seed.scope, offset + 1, size)
    }
    return {
      count,
      ...identity,
      scope: seed.scope,
      principal: seed.principal,
      firstUsableQueryMs,
      ...metrics,
      validatedRows: count,
      validationMs: performance.now() - validationStarted
    }
  } finally {
    await Effect.runPromise(Scope.close(lifetime, Exit.void))
    await Effect.runPromise(ResetReplica.resetReplica({ ...identity, spawnLeader }, { discardPending: true }))
  }
}

const results: Array<Awaited<ReturnType<typeof trial>>> = []
const benchmark = {
  state: "ready",
  results,
  error: "",
  start(sizes: Array<number> = [10_000, 100_000], trials = 3) {
    if (benchmark.state !== "ready") throw new Error("already started")
    benchmark.state = "running"
    void (async () => {
      try {
        if (
          !Number.isSafeInteger(trials) ||
          trials < 1 ||
          sizes.some((n) => !Number.isSafeInteger(n) || n < 1 || n > 100_000)
        ) {
          throw new Error("invalid sizes or trials")
        }
        for (const count of sizes) for (let i = 0; i < trials; i++) benchmark.results.push(await trial(count))
        benchmark.state = "done"
      } catch (error) {
        benchmark.error = String(error)
        benchmark.state = "failed"
      }
      const output = document.getElementById("result")
      if (output !== null) output.textContent = JSON.stringify(benchmark, null, 2)
    })()
  }
}
declare global {
  interface Window {
    bootstrapBenchmark: typeof benchmark
  }
}
window.bootstrapBenchmark = benchmark
