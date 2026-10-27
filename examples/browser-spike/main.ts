/**
 * Dev harness page. Exposes `window.echo` so agent-browser (or a human in devtools) can drive
 * scenarios across tabs; see AGENTS.md "Browser scenarios".
 */
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { Array as Arr, Clock, DateTime, Record as Rec, String as Str } from "effect"
import { randomUUIDv4 } from "../../src/client/Crypto.ts"
import * as LeaderClientModule from "../../src/client/worker/LeaderClient.ts"
import type { OutboxEnvelope, QueryResult, SqlParams } from "../../src/client/worker/LeaderRpcs.ts"
import type { ToRouter } from "../../src/client/worker/messages.ts"
import type { SyncStatus } from "../../src/client/sync/SyncLoop.ts"
import { resetReplica } from "../../src/client/worker/ResetReplica.ts"
import * as Platform from "../../src/client/worker/platform.ts"
import * as Schema from "effect/Schema"
import { AccessScope, Mutation } from "../../src/core/index.ts"

const params = new URLSearchParams(location.search)
const appId = params.get("app") ?? "spike"
const profile = params.get("profile") ?? "default"
const noSharedWorker = params.has("noSharedWorker")

let releaseEpochWatch = () => {}
// oxlint-disable-next-line effecttsgo/new-promise -- Fault injection must return the real browser lock callback's shared, manually released Promise.
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
        ? request(
            name,
            options,
            // oxlint-disable-next-line effecttsgo/async-function -- Browser lock replacement preserves the Promise callback contract and delayed grant timing.
            async (lock) => {
              await epochWatchGate
              return callback(lock)
            }
          )
        : request(name, options, callback)
  })
}
if (params.has("ignoreReset")) {
  // Fault injection: this page never hears the engine's reset signal (INV-BRW-7), as an old build or a
  // frozen tab would not; its client keeps its leader. Only the engine's `echo:*:reset` channels are muted.
  const add = BroadcastChannel.prototype.addEventListener
  Object.defineProperty(BroadcastChannel.prototype, "addEventListener", {
    value: function (this: BroadcastChannel, ...args: Parameters<typeof add>): unknown {
      if (Str.startsWith("echo:")(this.name) && Str.endsWith(":reset")(this.name)) return
      return Reflect.apply(add, this, args)
    }
  })
}
/** Set once the client exists: what `?closeOnReset` runs right after the client handled the reset signal. */
let afterResetDelivered: (() => void) | undefined
if (params.has("closeOnReset")) {
  // Fault injection: right after the client's own reset listener ran (same task, so its reset close of the
  // client's topology has only been forked), close the client's scope one scheduler step later, which lands
  // while that topology close is still in progress.
  const add = BroadcastChannel.prototype.addEventListener
  Object.defineProperty(BroadcastChannel.prototype, "addEventListener", {
    value: function (this: BroadcastChannel, type: string, listener: EventListener, ...rest: Array<unknown>): unknown {
      if (!(Str.startsWith("echo:")(this.name) && Str.endsWith(":reset")(this.name))) {
        return Reflect.apply(add, this, [type, listener, ...rest])
      }
      const wrapped = (event: Event) => {
        listener(event)
        afterResetDelivered?.()
      }
      return Reflect.apply(add, this, [type, wrapped, ...rest])
    }
  })
}
if (noSharedWorker) {
  // Simulates a browser without SharedWorker (e.g. Chrome Android) to exercise the fail-closed fallback.
  Reflect.deleteProperty(globalThis, "SharedWorker")
}

const $ = (id: string) => document.getElementById(id)
const log = (line: string) => {
  const el = $("log")
  if (el)
    el.textContent = `${Str.slice(11, 23)(DateTime.formatIso(DateTime.nowUnsafe()))} ${line}\n${el.textContent ?? ""}`
}

const netKey = `${appId}/${profile}`

// Fault injection on this tab's *first* router SharedWorker only (later reconnects are untouched):
// `holdRouterOut` queues what the tab posts to it, `holdRouterIn` buffers what it receives from it, both
// until `echo.flushRouter()`. They model a router message delayed across a router death.
let routerSpawns = 0
const heldRouterOut: Array<() => void> = []
const heldRouterIn: Array<() => void> = []
const spawnRouter = (name: string) => {
  const router = new SharedWorker(new URL("./router.worker.ts", import.meta.url), { type: "module", name })
  routerSpawns++
  if (routerSpawns === 1 && params.has("holdRouterOut")) {
    const post = router.port.postMessage.bind(router.port)
    Object.defineProperty(router.port, "postMessage", {
      value: (message: unknown, options: StructuredSerializeOptions) => heldRouterOut.push(() => post(message, options))
    })
  }
  if (routerSpawns === 1 && params.has("holdRouterIn")) {
    const add = router.port.addEventListener.bind(router.port)
    Object.defineProperty(router.port, "addEventListener", {
      value: (type: string, listener: EventListener) => add(type, (event) => heldRouterIn.push(() => listener(event)))
    })
  }
  return router
}

// Fault-injection flags (`crashAfterCommit`, `failOpen`) are forwarded to this tab's leader candidate only;
// `extra` adds worker flags for one spawn (a reset's crash point, see `echo.resetReplica`).
const spawnLeaderWith = (extra: Readonly<Record<string, string>>) => (name: string) => {
  const url = new URL("./leader.worker.ts", import.meta.url)
  for (const flag of ["crashAfterCommit", "failOpen"]) if (params.has(flag)) url.searchParams.set(flag, "")
  for (const [flag, value] of Rec.toEntries(extra)) url.searchParams.set(flag, value)
  const syncUrl = params.get("syncUrl")
  if (syncUrl !== null) url.searchParams.set("syncUrl", syncUrl)
  // The harness network gate (`echo.setOffline`), per identity; the state outlives a reload.
  url.searchParams.set("net", netKey)
  if (localStorage.getItem(`echo-test:net:${netKey}`) === "offline") url.searchParams.set("offline", "")
  return new Worker(url, { type: "module", name })
}
const spawnLeader = spawnLeaderWith({})

const scope = Scope.makeUnsafe()
const client = await Effect.runPromise(
  LeaderClientModule.make({
    appId,
    profile,
    principal: params.get("principal") ?? "spike-user",
    spawnLeader,
    spawnRouter
  }).pipe(Scope.provide(scope))
)
addEventListener("pagehide", () => Effect.runFork(Scope.close(scope, Exit.void)))
/** With `?closeOnReset`: the client's scope close forked as the reset reached the client, then this page's locks. */
let closedOnReset: Promise<{ readonly held: Array<string>; readonly pending: Array<string> }> | undefined
afterResetDelivered = () => {
  closedOnReset ??= Effect.runPromise(Effect.andThen(Effect.yieldNow, Scope.close(scope, Exit.void))).then(() =>
    echo.pageLocks()
  )
}

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
const statusWatch: { ended: unknown } = { ended: undefined }
/** The harness app's no-op mutation `t` (app.ts), for `client.mutate`: only its name and args schema matter. */
const t = Mutation.make("t", { args: Schema.Json, handler: () => Effect.void })
/**
 * Every `syncStatus` emission since `watchSyncStatus()`, each with the `statusLog` length when it arrived
 * (to order it against leader changes); `syncStatusEnded` is how the stream ended, if it did.
 */
const syncStatusLog: Array<{ readonly afterStatus: number; readonly value: SyncStatus }> = []
const syncWatch: { ended: unknown } = { ended: undefined }

/** Open `presence` streams by room: every snapshot's entries (as `tabId=state`), how it ended, and a closer. */
interface PresenceWatch {
  readonly snapshots: Array<ReadonlyArray<string>>
  ended: unknown
  readonly close: () => void
}
const presenceWatches = new Map<string, PresenceWatch>()

const echo = {
  statusLog,
  statusWatch,
  /** `client.mutate` (the retrying write API) with mutation `t`; resolves with how long it took. */
  // oxlint-disable-next-line effecttsgo/async-function -- window.echo is the Promise-based browser automation boundary, not engine control flow.
  mutate: async (args: Schema.Json = {}) => {
    const started = performance.now()
    const outcome = await run(client.mutate(t, args))
    return { ...outcome, ms: Math.round(performance.now() - started) }
  },
  /** Every Web Lock, held or pending, of this page's identity (or `target`), by name and mode. */
  // oxlint-disable-next-line effecttsgo/async-function -- Browser automation directly inspects the Promise-based Web Locks API.
  locksFor: async (target: { readonly appId: string; readonly profile: string } = { appId, profile }) => {
    const prefix = `echo:${target.appId}:${target.profile}:`
    const snapshot = await navigator.locks.query()
    const pick = (locks: ReadonlyArray<LockInfo> | undefined) =>
      Arr.map(
        Arr.filter(locks ?? [], (l) => l.name !== undefined && Str.startsWith(prefix)(l.name)),
        (l) => `${l.mode}:${l.name}`
      )
    return { held: pick(snapshot.held), pending: pick(snapshot.pending) }
  },
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
  /**
   * Harness network gate for this identity (every tab and leader of `app`/`profile`): offline makes the
   * leader's requests fail as an unreachable server does and aborts its open streams. Persists across
   * reloads (localStorage) until set back.
   */
  setOffline: (offline: boolean) => {
    const state = offline ? "offline" : "online"
    localStorage.setItem(`echo-test:net:${netKey}`, state)
    const channel = new BroadcastChannel(`echo-test:net:${netKey}`)
    // oxlint-disable-next-line unicorn/require-post-message-target-origin -- BroadcastChannel has no target origin
    channel.postMessage(state)
    channel.close()
    return true
  },
  presence: presenceWatches,
  /** Open this tab's presence in `room` with `state`; snapshots go to `echo.presence.get(room)`. */
  presenceOpen: (room: string, state: string) => {
    const watch: PresenceWatch = {
      snapshots: [],
      ended: undefined,
      close: () => Effect.runFork(Fiber.interrupt(fiber))
    }
    const fiber = Effect.runFork(
      Stream.runForEach(client.presence(AccessScope.AccessScopeId.make(room), state), (snapshot) =>
        Effect.sync(
          () => void watch.snapshots.push(Arr.map(snapshot.entries, (e) => `${e.tabId}=${JSON.stringify(e.state)}`))
        )
      ).pipe(
        Effect.exit,
        Effect.map((exit) => {
          watch.ended = Exit.isFailure(exit) ? JSON.parse(JSON.stringify(exit.cause)) : "completed"
        })
      )
    )
    presenceWatches.set(room, watch)
    return true
  },
  track: (room: string, state: string) => run(client.track(AccessScope.AccessScopeId.make(room), state)),
  /** Post the engine's reset signal for this page's identity directly, as `resetReplica` does (no reset worker). */
  signalReset: () => {
    const channel = new BroadcastChannel(`echo:${appId}:${profile}:reset`)
    // oxlint-disable-next-line unicorn/require-post-message-target-origin -- BroadcastChannel has no target origin
    channel.postMessage({ _tag: "ResetSignal", appId, profile })
    channel.close()
    return true
  },
  /** With `?closeOnReset`: resolves once the scope close forked by the reset completed, with this page's locks. */
  // oxlint-disable-next-line effecttsgo/async-function -- Preserve the automation surface's Promise result, including native Promise flattening.
  closedOnReset: async () => {
    const settled = await echo.until(() => closedOnReset)
    return settled === undefined ? null : settled
  },
  /** This page's own (not its workers') Web Locks of its identity, held or pending, as `mode:name`. */
  // oxlint-disable-next-line effecttsgo/async-function -- Web Locks inspection is an external Promise interface.
  pageLocks: async () => {
    const probe = `echo-test:probe:${await Effect.runPromise(randomUUIDv4)}`
    // oxlint-disable-next-line effecttsgo/async-function -- The lock remains held until this browser callback's Promise settles.
    return navigator.locks.request(probe, async () => {
      const snapshot = await navigator.locks.query()
      const self = Option.getOrUndefined(Arr.findFirst(snapshot.held ?? [], (l) => l.name === probe))?.clientId
      const prefix = `echo:${appId}:${profile}:`
      const pick = (locks: ReadonlyArray<LockInfo> | undefined) =>
        Arr.map(
          Arr.filter(locks ?? [], (l) => l.clientId === self && l.name !== undefined && Str.startsWith(prefix)(l.name)),
          (l) => `${l.mode}:${l.name}`
        )
      return { held: pick(snapshot.held), pending: pick(snapshot.pending) }
    })
  },
  /**
   * `resetReplica` for `target` (this page's own identity by default); resolves with how long it took. With a
   * crash-point `fault` (`beforeTombstone`, `midRemoval`; see leader.worker.ts) the reset is interrupted, which
   * terminates its worker, as soon as its worker reaches that point (`interrupted: true`); a reset that never
   * reaches it completes as usual (`interrupted: false`).
   */
  // oxlint-disable-next-line effecttsgo/async-function -- Automation combines the engine's result with wall-clock timing at its Promise boundary.
  resetReplica: async (
    target: { readonly appId: string; readonly profile: string } = { appId, profile },
    discardPending = false,
    fault: "spawnThrows" | "postThrows" | "beforeTombstone" | "midRemoval" | undefined = undefined,
    closeTimeoutMs: number | undefined = undefined
  ) => {
    const started = performance.now()
    const crash = fault === "beforeTombstone" || fault === "midRemoval" ? fault : undefined
    const faultChannel = `echo-test:reset-fault:${await Effect.runPromise(randomUUIDv4)}`
    // oxlint-disable-next-line effecttsgo/new-promise -- The fault must be armed before the reset worker starts, and stay pending when no crash point is requested.
    const hit = new Promise<void>((resolve) => {
      if (crash === undefined) return
      const channel = new BroadcastChannel(faultChannel)
      channel.addEventListener("message", () => {
        channel.close()
        resolve()
      })
    })
    const reset = resetReplica(
      {
        ...target,
        spawnLeader:
          crash !== undefined
            ? spawnLeaderWith({ resetFault: crash, faultChannel })
            : fault === "spawnThrows"
              ? () => {
                  throw new TypeError("injected spawnLeader failure")
                }
              : fault === "postThrows"
                ? (name) => {
                    const worker = spawnLeader(name)
                    Object.defineProperty(worker, "postMessage", {
                      value: () => {
                        throw new DOMException("injected postMessage failure", "DataCloneError")
                      }
                    })
                    return worker
                  }
                : spawnLeader
      },
      {
        ...(discardPending ? { discardPending: true } : {}),
        ...(closeTimeoutMs === undefined ? {} : { closeTimeout: closeTimeoutMs })
      }
    )
    const outcome = await run(
      Effect.raceFirst(
        Effect.as(reset, { interrupted: false }),
        Effect.as(
          Effect.promise(() => hit),
          { interrupted: true }
        )
      )
    )
    return {
      ...outcome,
      interrupted: outcome.ok && outcome.value.interrupted,
      ms: Math.round(performance.now() - started)
    }
  },
  /** The engine's reset tombstone name for `target` (INV-BRW-8), an entry of `echo-sync/<appId>`. */
  tombstoneName: (target: { readonly appId: string; readonly profile: string } = { appId, profile }) =>
    Effect.runPromise(Platform.tombstoneName(target)),
  /**
   * Fault injection: overwrite the SQLite header of `<appId>/<profile>`'s database file in its pool (the
   * pool keeps its own 4096-byte header per file, path first), so the database no longer opens.
   */
  // oxlint-disable-next-line effecttsgo/async-function -- Fault injection intentionally drives raw OPFS Promise/async-iterator APIs outside the engine.
  corruptReplica: async (targetApp: string, targetProfile: string): Promise<boolean> => {
    let dir = await navigator.storage.getDirectory()
    for (const part of ["echo-sync", targetApp, targetProfile]) dir = await dir.getDirectoryHandle(part)
    // oxlint-disable-next-line typescript/consistent-type-assertions -- platform edge: `values()` is not in TS's DOM lib
    const iterable = dir as unknown as { values(): AsyncIterable<FileSystemHandle> }
    for await (const handle of iterable.values()) {
      if (!(handle instanceof FileSystemFileHandle)) continue
      const header = new Uint8Array(await (await handle.getFile()).slice(0, 512).arrayBuffer())
      const path = new TextDecoder().decode(header.subarray(0, Math.max(0, header.indexOf(0))))
      if (!Str.endsWith(`${targetProfile}.db`)(path)) continue
      const writable = await handle.createWritable({ keepExistingData: true })
      await writable.write({ type: "write", position: 4096, data: new Uint8Array(100).fill(0x5a) })
      await writable.close()
      return true
    }
    return false
  },
  /** Entry names in an OPFS directory (`a/b/c`), or `null` if it does not exist. */
  // oxlint-disable-next-line effecttsgo/async-function -- Browser automation reads the raw OPFS async iterator independently of engine behavior.
  opfsList: async (path: string): Promise<Array<string> | null> => {
    let dir = await navigator.storage.getDirectory()
    for (const part of Str.split(path, "/")) {
      const next = await dir.getDirectoryHandle(part).catch(() => undefined)
      if (next === undefined) return null
      dir = next
    }
    const names: Array<string> = []
    // `values()` is in the WHATWG spec and every engine with OPFS, but not in TS's DOM lib yet.
    // oxlint-disable-next-line typescript/consistent-type-assertions -- platform edge, see above
    const iterable = dir as unknown as { values(): AsyncIterable<FileSystemHandle> }
    for await (const handle of iterable.values()) names.push(handle.name)
    return Arr.sort(names, Str.Order)
  },
  watchStatus: () => {
    Effect.runFork(
      Stream.runForEach(client.status, (s) => Effect.sync(() => void statusLog.push(s))).pipe(
        Effect.exit,
        Effect.map((exit) => {
          statusWatch.ended = Exit.isFailure(exit) ? JSON.parse(JSON.stringify(exit.cause)) : "completed"
        })
      )
    )
    return true
  },
  status: () => Effect.runPromise(currentStatus),
  /** Status once a leader is connected, or the current status after `timeoutMs`. */
  ready: (timeoutMs = 10_000) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const deadline = (yield* Clock.currentTimeMillis) + timeoutMs
        let status = yield* currentStatus
        while (status.leaderId === undefined && (yield* Clock.currentTimeMillis) < deadline) {
          yield* Effect.sleep("50 millis")
          status = yield* currentStatus
        }
        return status
      })
    ),
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
  // oxlint-disable-next-line effecttsgo/async-function -- The automation Promise wraps the engine's typed retry program with a test observation count.
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
  until: <A>(condition: () => A | Promise<A>, timeoutMs = 15_000): Promise<A | undefined> =>
    Effect.runPromise(
      Effect.gen(function* () {
        const deadline = (yield* Clock.currentTimeMillis) + timeoutMs
        while ((yield* Clock.currentTimeMillis) < deadline) {
          const value = yield* Effect.promise(() => Promise.resolve(condition()))
          if (value) return value
          yield* Effect.sleep("20 millis")
        }
        return undefined
      })
    ),
  /**
   * Cross-tab start barrier: resolves when any other tab calls `release(name)`. BroadcastChannel delivery
   * is not subject to background-tab timer throttling, unlike a shared wall-clock start time.
   */
  barrier: (name: string): Promise<void> =>
    // oxlint-disable-next-line effecttsgo/new-promise -- Cross-tab driver API resolves on an actual BroadcastChannel event; no engine fiber owns this barrier.
    new Promise((resolve) => {
      const channel = new BroadcastChannel(`echo-test:${name}`)
      channel.addEventListener("message", () => {
        channel.close()
        resolve()
      })
    }),
  routerSpawns: () => routerSpawns,
  routerHeld: () => ({ out: heldRouterOut.length, in: heldRouterIn.length }),
  /** Deliver everything `holdRouterOut` / `holdRouterIn` held, in order (synchronously). */
  flushRouter: () => {
    for (const deliver of heldRouterOut.splice(0)) deliver()
    for (const deliver of heldRouterIn.splice(0)) deliver()
  },
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
  /** Runs the real OPFS catalog installer in an isolated worker for timing and hard-kill probes. */
  catalogBenchmark: (version: string, mode = "bench", id = String(Effect.runSync(Clock.currentTimeMillis))) => {
    const url = new URL("./catalog-benchmark.worker.ts", import.meta.url)
    url.searchParams.set("version", version)
    url.searchParams.set("id", id)
    url.searchParams.set("mode", mode)
    const worker = new Worker(url, { type: "module" })
    // oxlint-disable-next-line effecttsgo/new-promise -- Browser host receives the benchmark worker's single terminal message.
    return new Promise<unknown>((resolve) => {
      worker.addEventListener("message", (event: MessageEvent<unknown>) => {
        worker.terminate()
        resolve(event.data)
      })
      worker.addEventListener("error", (event) => {
        worker.terminate()
        resolve({ ok: false, error: event.message })
      })
    })
  },
  /** Live query over `statements` (all read in one transaction per emission); collects emissions. */
  subscribe: (statements: ReadonlyArray<string>, tables: ReadonlyArray<string>): Subscription => {
    const sub: Subscription = { emissions: [], ended: undefined, cancel: () => Effect.runFork(Fiber.interrupt(fiber)) }
    const fiber = Effect.runFork(
      client.subscribeRaw({ statements: Arr.map(statements, (sql) => ({ sql, params: [] })), tables }).pipe(
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
// oxlint-disable-next-line effecttsgo/async-function -- DOM rendering is the non-Effect host boundary consuming the tab API.
const render = async () => {
  const status = await echo.status()
  const info = await Effect.runPromise(client.leaderInfo.pipe(Effect.option, Effect.timeoutOption("50 millis")))
  const tab = $("tab")
  if (tab) tab.textContent = Str.slice(0, 8)(status.tabId)
  const mode = $("mode")
  if (mode) mode.textContent = status.mode
  const leader = $("leader")
  if (leader) leader.textContent = status.leaderId === undefined ? "—" : Str.slice(0, 8)(status.leaderId)
  const db = $("db")
  if (db) db.textContent = info._tag === "Some" && info.value._tag === "Some" ? info.value.value.dbName : "—"
  const badge = $("badge")
  if (badge) {
    badge.className = `badge ${status.leaderId === undefined ? "down" : status.isLeaderTab ? "leader" : "follower"}`
    badge.textContent = status.leaderId === undefined ? "no leader" : status.isLeaderTab ? "LEADER TAB" : "follower"
  }
}
// oxlint-disable-next-line effecttsgo/global-timers -- Host DOM refresh keeps its fixed browser cadence independently of the leader's Effect clock and lifetime.
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
  // oxlint-disable-next-line effecttsgo/prefer-schema-over-json -- Dev-only log intentionally formats the raw error's JavaScript enumerable fields, not a wire schema.
  Effect.catch((e) => Effect.sync(() => log(`live query stopped: ${JSON.stringify(e)}`)))
)
Effect.runFork(liveCount)
