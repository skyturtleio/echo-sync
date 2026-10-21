/**
 * Harness-only fake sync server (Vite dev middleware): a real `RpcServer` for core's `SyncRpcs` over HTTP
 * with NDJSON, as DESIGN §12 mounts it. Its push refuses every batch with `MalformedEnvelope`, pull answers
 * an empty `PullDone`, and pokes sends one poke and then ends cleanly. Per-client counters are served at
 * `/fake-sync/stats?clientId=…`, so a scenario can watch the leader's sync loop from outside.
 */
import type { IncomingMessage, ServerResponse } from "node:http"
import { Effect, Layer, Schema, Stream } from "effect"
import { HttpRouter } from "effect/http"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { Ids, Protocol } from "../../src/core/index.ts"

interface Stats {
  pushes: number
  pulls: number
  pokes: number
}
const stats = new Map<string, Stats>()
const statsFor = (clientId: string): Stats => {
  const existing = stats.get(clientId)
  if (existing !== undefined) return existing
  const fresh = { pushes: 0, pulls: 0, pokes: 0 }
  stats.set(clientId, fresh)
  return fresh
}

const handlers = Protocol.SyncRpcs.toLayer({
  push: (request) => {
    statsFor(request.clientId).pushes++
    return Effect.fail(
      new Protocol.MalformedEnvelope({
        clientId: request.clientId,
        mutationId: request.mutations[0].mutationId,
        message: "fake server refuses every batch"
      })
    )
  },
  pull: (request) => {
    statsFor(request.clientId).pulls++
    return Effect.succeed({
      _tag: "PullDone" as const,
      changes: [],
      acks: [],
      nextCursor: Schema.decodeUnknownSync(Ids.Position)("1"),
      expiredThrough: Ids.SeqFloor.make(0)
    })
  },
  pokes: ({ clientId }) => {
    statsFor(clientId).pokes++
    return Stream.make({ _tag: "poke" as const })
  }
})

const web = HttpRouter.toWebHandler(
  RpcServer.layerHttp({ group: Protocol.SyncRpcs, path: "/fake-sync/rpc", protocol: "http" }).pipe(
    Layer.provide([handlers, RpcSerialization.layerNdjson])
  ),
  { disableLogger: true }
)

const body = (req: IncomingMessage): Promise<Buffer> =>
  new Promise((resolve, reject) => {
    const chunks: Array<Buffer> = []
    req.on("data", (chunk: Buffer) => chunks.push(chunk))
    req.on("end", () => resolve(Buffer.concat(chunks)))
    req.on("error", reject)
  })

export const middleware = (req: IncomingMessage, res: ServerResponse, next: () => void): void => {
  const url = new URL(req.url ?? "/", "http://harness")
  // `RpcClient.layerProtocolHttp({ url })` posts to `<url>/` (it prepends `url` to the path "/").
  if (url.pathname.endsWith("/")) url.pathname = url.pathname.slice(0, -1)
  if (url.pathname === "/fake-sync/stats") {
    res.setHeader("content-type", "application/json")
    res.end(JSON.stringify(statsFor(url.searchParams.get("clientId") ?? "")))
    return
  }
  if (url.pathname !== "/fake-sync/rpc") return next()
  void (async () => {
    const response = await web.handler(
      new Request(url, { method: req.method ?? "POST", body: new Uint8Array(await body(req)) })
    )
    res.statusCode = response.status
    response.headers.forEach((value, key) => res.setHeader(key, value))
    res.end(Buffer.from(await response.arrayBuffer()))
  })().catch((error: unknown) => {
    res.statusCode = 500
    res.end(String(error))
  })
}
