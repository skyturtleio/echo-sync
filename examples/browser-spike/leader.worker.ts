import { Effect } from "effect"
import { runLeaderWorker } from "../../src/client/worker/index.ts"
import { clientMutations, principalHeader, registry } from "./app.ts"

// Harness-only fault injection, enabled per tab through the worker URL (see main.ts / scenarios.sh).
const flags = new URLSearchParams(self.location.search)

const isCrashResponse = (message: unknown): boolean =>
  JSON.stringify(message ?? null).includes('"mutationId":"crash-after-commit') &&
  JSON.stringify(message).includes('"_tag":"Exit"')

if (flags.has("crashAfterCommit")) {
  // Commit-before-lost-response crash point: an `enqueue` response exists only after its transaction
  // committed. For mutation ids starting with "crash-after-commit", end this worker instead of sending it.
  const post = MessagePort.prototype.postMessage
  Object.defineProperty(MessagePort.prototype, "postMessage", {
    value: function (this: MessagePort, ...args: Array<unknown>) {
      if (isCrashResponse(args[0])) return self.close()
      return Reflect.apply(post, this, args)
    }
  })
}

if (flags.has("failOpen")) {
  // OPFS startup failure on demand: the first sync access handle request blocks until another tab posts on
  // the "echo-test:fail-open" channel, then fails, so the candidate holds the leader lock while it waits.
  const gate = new Promise<void>((resolve) => {
    new BroadcastChannel("echo-test:fail-open").addEventListener("message", () => resolve())
  })
  Object.defineProperty(FileSystemFileHandle.prototype, "createSyncAccessHandle", {
    value: async () => {
      await gate
      throw new DOMException("injected OPFS startup failure", "NotReadableError")
    }
  })
}

const resetFault = flags.get("resetFault")
if (resetFault !== null) {
  // Crash points inside a reset worker (DECISIONS 26), set by `echo.resetReplica(…, fault)` on the reset's
  // worker only. At the point, the worker posts on `faultChannel` and stops making progress; the tab then
  // interrupts the reset, which terminates this worker there. `beforeTombstone`: the creation of an engine-owned
  // "~" entry (the tombstone) never starts. `midRemoval`: the recursive removal of a profile directory deletes
  // one of the pool's non-database files and never finishes, leaving a partially removed directory.
  const announce = () => {
    const channel = new BroadcastChannel(flags.get("faultChannel") ?? "")
    // oxlint-disable-next-line unicorn/require-post-message-target-origin -- BroadcastChannel has no target origin
    channel.postMessage("hit")
    channel.close()
  }
  const stall = () => {
    announce()
    return new Promise<never>(() => {})
  }
  const proto = FileSystemDirectoryHandle.prototype
  if (resetFault === "beforeTombstone") {
    const getFileHandle = proto.getFileHandle
    Object.defineProperty(proto, "getFileHandle", {
      value: function (this: FileSystemDirectoryHandle, name: string, options?: FileSystemGetFileOptions) {
        if (options?.create === true && name.startsWith("~")) return stall()
        return Reflect.apply(getFileHandle, this, [name, options])
      }
    })
  }
  if (resetFault === "midRemoval") {
    const removeEntry = proto.removeEntry
    Object.defineProperty(proto, "removeEntry", {
      value: async function (this: FileSystemDirectoryHandle, name: string, options?: FileSystemRemoveOptions) {
        if (options?.recursive !== true) return Reflect.apply(removeEntry, this, [name, options])
        const dir = await this.getDirectoryHandle(name)
        // oxlint-disable-next-line typescript/consistent-type-assertions -- platform edge: `values()` is not in TS's DOM lib
        const iterable = dir as unknown as { values(): AsyncIterable<FileSystemHandle> }
        for await (const handle of iterable.values()) {
          if (!(handle instanceof FileSystemFileHandle)) continue
          // The pool keeps each file's SQLite path at the start of its header; skip the database itself.
          const header = new Uint8Array(await (await handle.getFile()).slice(0, 512).arrayBuffer())
          const path = new TextDecoder().decode(header.subarray(0, Math.max(0, header.indexOf(0))))
          if (path.endsWith(".db")) continue
          await Reflect.apply(removeEntry, dir, [handle.name])
          break
        }
        return stall()
      }
    })
  }
}

const syncUrl = flags.get("syncUrl")
if (flags.has("net")) {
  // Harness network gate (slice.sh): while this replica's identity is "offline", every request to the sync
  // server fails as an unreachable server does (fetch rejects with a TypeError) and responses already
  // streaming (the pokes stream) are aborted. Only the sync URL: the worker's own assets (the SQLite wasm)
  // still load, as they would from cache when a real device is offline. The tab sets the initial state in
  // the worker URL and flips it on the "echo-test:net:<identity>" channel, so a leader started by a reload
  // starts in the same state. Requests made while online share one abort signal per online period.
  let offline = flags.has("offline")
  let period = new AbortController()
  new BroadcastChannel(`echo-test:net:${flags.get("net") ?? ""}`).addEventListener("message", (event) => {
    offline = event.data === "offline"
    if (!offline) return
    period.abort(new TypeError("harness: network offline"))
    period = new AbortController()
  })
  const fetch = globalThis.fetch.bind(globalThis)
  Object.defineProperty(globalThis, "fetch", {
    value: (input: RequestInfo | URL, init?: RequestInit) => {
      const url = input instanceof Request ? input.url : String(input)
      if (syncUrl === null || !url.startsWith(syncUrl)) return fetch(input, init)
      if (offline) return Promise.reject(new TypeError("Failed to fetch (harness: network offline)"))
      const signal = init?.signal ? AbortSignal.any([init.signal, period.signal]) : period.signal
      return fetch(input, { ...init, signal })
    }
  })
}

// The harness app (app.ts): the `todos` table and its mutations. `?syncUrl=` turns the sync loop on; the
// tab's principal goes to the server in the dev-only principal header.
runLeaderWorker(self, {
  registry,
  mutations: clientMutations,
  // The harness's scenarios use raw `exec`; apps never set this (INV-BRW-6).
  allowRawExec: true,
  ...(syncUrl === null ? {} : { syncUrl, syncHeaders: (principal) => Effect.succeed({ [principalHeader]: principal }) })
})
