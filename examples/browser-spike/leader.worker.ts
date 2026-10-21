import { runLeaderWorker } from "../../src/client/worker/index.ts"

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

runLeaderWorker(self)
