/**
 * Chromium only: end the router SharedWorker named `$2` through the DevTools protocol at `$1` (the
 * agent-browser session's `get cdp-url`), as a browser that kills or relaunches a SharedWorker would.
 * Prints `{"killed":n}` once no such target is left; exits 1 if none was found or it did not go away.
 *
 *   bun cdp/kill-router.ts "$(agent-browser get cdp-url)" echo:<appId>:<profile>:router
 */
import { Array as Arr, Console, Effect } from "effect"

const [url, name] = process.argv.slice(2)
if (url === undefined || name === undefined) throw new Error("usage: kill-router.ts <cdp-url> <router name>")

const socket = new WebSocket(url)
const replies = new Map<number, (result: unknown) => void>()
let nextId = 0
socket.addEventListener("message", (event) => {
  const reply: unknown = JSON.parse(String(event.data))
  if (typeof reply === "object" && reply !== null && "id" in reply && typeof reply.id === "number") {
    replies.get(reply.id)?.("result" in reply ? reply.result : reply)
  }
})
const call = (method: string, params: object): Promise<unknown> =>
  // oxlint-disable-next-line effecttsgo/new-promise -- CDP correlation table resolves external WebSocket replies by request id.
  new Promise((resolve) => {
    nextId++
    replies.set(nextId, resolve)
    socket.send(JSON.stringify({ id: nextId, method, params }))
  })
await Effect.runPromise(Effect.callback<void>((resume) => socket.addEventListener("open", () => resume(Effect.void))))

// oxlint-disable-next-line effecttsgo/async-function -- This browser driver consumes the external CDP Promise protocol, not the sync engine.
const routers = async (): Promise<Array<string>> => {
  const result = await call("Target.getTargets", {})
  const infos = typeof result === "object" && result !== null && "targetInfos" in result ? result.targetInfos : []
  if (!Arr.isArray(infos)) return []
  return Arr.flatMap(infos, (info: unknown) =>
    typeof info === "object" &&
    info !== null &&
    "type" in info &&
    info.type === "shared_worker" &&
    "title" in info &&
    info.title === name &&
    "targetId" in info &&
    typeof info.targetId === "string"
      ? [info.targetId]
      : []
  )
}

const found = await routers()
for (const targetId of found) await call("Target.closeTarget", { targetId })
let left = found.length
for (let i = 0; i < 100 && left > 0; i++) {
  await Effect.runPromise(Effect.sleep("50 millis"))
  left = (await routers()).length
}
socket.close()
Effect.runSync(Console.log(JSON.stringify({ killed: found.length, left })))
process.exit(found.length > 0 && left === 0 ? 0 : 1)
