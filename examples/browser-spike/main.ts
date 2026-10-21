/**
 * Dev harness page. Exposes `window.echo` so agent-browser (or a human in devtools) can drive
 * scenarios across tabs; see docs/browser-substrate.md "Running the browser scenarios".
 */
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as LeaderClientModule from "../../src/client/worker/LeaderClient.ts"
import type { OutboxEnvelope, QueryResult, SqlParams } from "../../src/client/worker/LeaderRpcs.ts"
import type { ToRouter } from "../../src/client/worker/messages.ts"
import type { SyncStatus } from "../../src/client/sync/SyncLoop.ts"

const params = new URLSearchParams(location.search)
const appId = params.get("app") ?? "spike"
const profile = params.get("profile") ?? "default"
const noSharedWorker = params.has("noSharedWorker")

let releaseEpochWatch = () => {}
const epochWatchGate = new Promise<void>((resolve) => {
  releaseEpochWatch = resolve
})
const isEpoch = (name: string) => /^echo:[^:]+:[^:]+:leader:./.test(name)
if (params.has("holdEpochWatch")) {
  // Fault injection: this tab's shared-mode waits on epoch locks (`awaitLockReleased`) get their grant but
  // run their callback only after `echo.releaseEpochWatch()`, so a leader epoch's end reaches this tab late.
  const locks = navigator.locks
  const request = locks.request.bind(locks)
  Object.defineProperty(locks, "request", {
    value: (name: string, options: LockOptions, callback: LockGrantedCallback<unknown>) =>
      options.mode === "shared" && isEpoch(name)
        ? request(name, options, async (lock) => {
            await epochWatchGate
            return callback(lock)
          })
        : request(name, options, callback)
  })
}
if (noSharedWorker) {
  // Simulates a browser without SharedWorker (e.g. Chrome Android) to exercise the fail-closed fallback.
  Reflect.deleteProperty(globalThis, "SharedWorker")
}

const $ = (id: string) => document.getElementById(id)
const log = (line: string) => {
  const el = $("log")
  if (el) el.textContent = `${new Date().toISOString().slice(11, 23)} ${line}\n${el.textContent ?? ""}`
}

const spawnRouter = (name: string) =>
  new SharedWorker(new URL("./router.worker.ts", import.meta.url), { type: "module", name })

const scope = Effect.runSync(Scope.make())
const client = await Effect.runPromise(
  LeaderClientModule.make({
    appId,
    profile,
    principal: params.get("principal") ?? "spike-user",
    // Fault-injection flags (`crashAfterCommit`, `failOpen`) are forwarded to this tab's leader candidate only.
    spawnLeader: (name) => {
      const url = new URL("./leader.worker.ts", import.meta.url)
      for (const flag of ["crashAfterCommit", "failOpen"]) if (params.has(flag)) url.searchParams.set(flag, "")
      const syncUrl = params.get("syncUrl")
      if (syncUrl !== null) url.searchParams.set("syncUrl", syncUrl)
      return new Worker(url, { type: "module", name })
    },
    spawnRouter
  }).pipe(Scope.provide(scope))
)
addEventListener("pagehide", () => Effect.runFork(Scope.close(scope, Exit.void)))

/** Errors are returned as values so the driving script can assert on `_tag`/`reason`. */
type Outcome<A> = { readonly ok: true; readonly value: A } | { readonly ok: false; readonly error: unknown }
const run = <A, E>(effect: Effect.Effect<A, E>): Promise<Outcome<A>> =>
  Effect.runPromise(
    effect.pipe(
      Effect.match({
        onSuccess: (value): Outcome<A> => ({ ok: true, value }),
        onFailure: (error): Outcome<A> => ({ ok: false, error: JSON.parse(JSON.stringify(error)) })
      })
    )
  )

interface Subscription {
  readonly emissions: Array<QueryResult>
  ended: unknown
  readonly cancel: () => void
}

// `status` is a stream (current value first); the scenarios poll it.
const currentStatus = Stream.runHead(client.status).pipe(
  Effect.flatMap((head) => (Option.isSome(head) ? Effect.succeed(head.value) : Effect.die(new Error("no status"))))
)

/** Every `status` emission since `watchStatus()`, in order (the stream is continuous, not polled). */
const statusLog: Array<LeaderClientModule.Status> = []
/**
 * Every `syncStatus` emission since `watchSyncStatus()`, each with the `statusLog` length when it arrived
 * (to order it against leader changes); `syncStatusEnded` is how the stream ended, if it did.
 */
const syncStatusLog: Array<{ readonly afterStatus: number; readonly value: SyncStatus }> = []
const syncWatch: { ended: unknown } = { ended: undefined }

const echo = {
  statusLog,
  /** Lets the epoch-lock callbacks held by `?holdEpochWatch` run. */
  releaseEpochWatch: () => {
    releaseEpochWatch()
    return true
  },
  syncStatusLog,
  syncWatch,
  watchSyncStatus: () => {
    Effect.runFork(
      Stream.runForEach(client.syncStatus, (value) =>
        Effect.sync(() => void syncStatusLog.push({ afterStatus: statusLog.length, value }))
      ).pipe(
        Effect.exit,
        Effect.map((exit) => {
          syncWatch.ended = Exit.isFailure(exit) ? JSON.parse(JSON.stringify(exit.cause)) : "completed"
        })
      )
    )
    return true
  },
  requestSync: () => run(client.requestSync),
  watchStatus: () => {
    Effect.runFork(Stream.runForEach(client.status, (s) => Effect.sync(() => void statusLog.push(s))))
    return true
  },
  status: () => Effect.runPromise(currentStatus),
  /** Status once a leader is connected, or the current status after `timeoutMs`. */
  ready: async (timeoutMs = 10_000) => {
    const deadline = Date.now() + timeoutMs
    let status = await Effect.runPromise(currentStatus)
    while (status.leaderId === undefined && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 50))
      status = await Effect.runPromise(currentStatus)
    }
    return status
  },
  info: () => run(client.leaderInfo),
  /** The current notice list (first emission of the notices stream). */
  notices: () => run(Stream.runHead(client.notices).pipe(Effect.map((head) => Option.getOrElse(head, () => [])))),
  dismissNotices: (ids: ReadonlyArray<number>) => run(client.dismissNotices(ids)),
  exportPending: () => run(client.exportPending),
  /** First sync status emission (or the `SyncDisabled` error on a local-only leader). */
  syncStatus: () => run(Stream.runHead(client.syncStatus)),
  exec: (sql: string, sqlParams: SqlParams = []) => run(client.exec(sql, sqlParams)),
  enqueue: (envelope: OutboxEnvelope) => run(client.enqueue(envelope)),
  /** Retry on LeaderUnavailable{LeaderLost} — safe because enqueue is idempotent on mutationId. */
  /** Resolves `{ ok, value, attempts }`; `attempts > 1` means at least one try ended in `LeaderLost`. */
  enqueueRetrying: async (envelope: OutboxEnvelope) => {
    let attempts = 0
    const outcome = await run(
      Effect.suspend(() => {
        attempts++
        return client.enqueue(envelope)
      }).pipe(
        Effect.retry({
          times: 50,
          while: (e) => e._tag === "LeaderUnavailable" && e.reason === "LeaderLost"
        })
      )
    )
    return { ...outcome, attempts }
  },
  /** Poll `condition` until it is truthy; returns its value, or `undefined` after `timeoutMs`. */
  until: async <A>(condition: () => A | Promise<A>, timeoutMs = 15_000): Promise<A | undefined> => {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      const value = await condition()
      if (value) return value
      await new Promise((resolve) => setTimeout(resolve, 20))
    }
    return undefined
  },
  /**
   * Cross-tab start barrier: resolves when any other tab calls `release(name)`. BroadcastChannel delivery
   * is not subject to background-tab timer throttling, unlike a shared wall-clock start time.
   */
  barrier: (name: string): Promise<void> =>
    new Promise((resolve) => {
      const channel = new BroadcastChannel(`echo-test:${name}`)
      channel.addEventListener("message", () => {
        channel.close()
        resolve()
      })
    }),
  /** Fault injection: replay a leader announcement for `leaderId` to the router, as a delayed relay would. */
  forgeLeaderAnnouncement: (leaderId: string) => {
    const router = spawnRouter(`echo:${appId}:${profile}:router`)
    const channel = new MessageChannel()
    const announcement: ToRouter = { _tag: "Leader", leaderId, port: channel.port2 }
    router.port.postMessage(announcement, { transfer: [channel.port2] })
  },
  release: (name: string) => {
    const channel = new BroadcastChannel(`echo-test:${name}`)
    // oxlint-disable-next-line unicorn/require-post-message-target-origin -- BroadcastChannel has no target origin
    channel.postMessage("go")
    channel.close()
  },
  /** Live query over `statements` (all read in one transaction per emission); collects emissions. */
  subscribe: (statements: ReadonlyArray<string>, tables: ReadonlyArray<string>): Subscription => {
    const sub: Subscription = { emissions: [], ended: undefined, cancel: () => Effect.runFork(Fiber.interrupt(fiber)) }
    const fiber = Effect.runFork(
      client.subscribeRaw({ statements: statements.map((sql) => ({ sql, params: [] })), tables }).pipe(
        Stream.runForEach((result) => Effect.sync(() => void sub.emissions.push(result))),
        Effect.exit,
        Effect.map((exit) => {
          sub.ended = Exit.isFailure(exit) ? JSON.parse(JSON.stringify(exit.cause)) : "completed"
        })
      )
    )
    return sub
  }
}

declare global {
  interface Window {
    echo: typeof echo
  }
}
window.echo = echo

// Live UI: status badge and a live count, re-subscribing on handover.
const render = async () => {
  const status = await echo.status()
  const info = await Effect.runPromise(client.leaderInfo.pipe(Effect.option, Effect.timeoutOption("50 millis")))
  const tab = $("tab")
  if (tab) tab.textContent = status.tabId.slice(0, 8)
  const mode = $("mode")
  if (mode) mode.textContent = status.mode
  const leader = $("leader")
  if (leader) leader.textContent = status.leaderId?.slice(0, 8) ?? "—"
  const db = $("db")
  if (db) db.textContent = info._tag === "Some" && info.value._tag === "Some" ? info.value.value.dbName : "—"
  const badge = $("badge")
  if (badge) {
    badge.className = `badge ${status.leaderId === undefined ? "down" : status.isLeaderTab ? "leader" : "follower"}`
    badge.textContent = status.leaderId === undefined ? "no leader" : status.isLeaderTab ? "LEADER TAB" : "follower"
  }
}
setInterval(() => void render(), 250)

const liveCount = Effect.gen(function* () {
  yield* client.exec("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
  yield* client
    .subscribeRaw({ statements: [{ sql: "SELECT count(*) AS n FROM items", params: [] }], tables: ["items"] })
    .pipe(
      Stream.runForEach((r) =>
        Effect.sync(() => {
          const el = $("live")
          const n = r.results[0]?.[0]?.["n"]
          if (el) el.textContent = String(n)
          log(`live count = ${String(n)}`)
        })
      )
    )
}).pipe(
  Effect.tapError((e) => Effect.sync(() => log(`subscription ended: ${e._tag} ${"reason" in e ? e.reason : ""}`))),
  Effect.retry({ while: (e) => e._tag === "LeaderUnavailable" && e.reason === "LeaderLost" }),
  Effect.catch((e) => Effect.sync(() => log(`live query stopped: ${JSON.stringify(e)}`)))
)
Effect.runFork(liveCount)
