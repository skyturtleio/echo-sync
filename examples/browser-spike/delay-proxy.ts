// Test-only HTTP proxy: one artificial RTT per request, never per body chunk/page.
// Run with Node >=22.18: node examples/browser-spike/delay-proxy.ts
// Bun's node:http compatibility did not propagate downstream disconnect in the smoke test.
// ECHO_PROXY_RTT_MS=0|75, ECHO_PROXY_PORT=5201, ECHO_PROXY_UPSTREAM=http://127.0.0.1:5200
// Route Vite's ECHO_SLICE_SERVER through this proxy for BOTH benchmark variants.
import { createHash } from "node:crypto"
import { readFileSync } from "node:fs"
import { createServer, request, type IncomingHttpHeaders, type IncomingMessage } from "node:http"

if (process.versions["bun"] !== undefined) throw new Error("Run this proxy with Node, not Bun (disconnect propagation)")
const rtt = Number(process.env["ECHO_PROXY_RTT_MS"] ?? "0")
const port = Number(process.env["ECHO_PROXY_PORT"] ?? "5201")
const target = new URL(process.env["ECHO_PROXY_UPSTREAM"] ?? "http://127.0.0.1:5200")
if (rtt !== 0 && rtt !== 75) throw new Error("ECHO_PROXY_RTT_MS must be 0 or 75")
if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error("invalid ECHO_PROXY_PORT")
if (
  target.protocol !== "http:" ||
  target.pathname !== "/" ||
  target.search ||
  target.hash ||
  target.username ||
  target.password
)
  throw new Error("ECHO_PROXY_UPSTREAM must be an HTTP origin")
const sourceSha256 = createHash("sha256")
  .update(readFileSync(new URL(import.meta.url)))
  .digest("hex")

const headers = (source: IncomingHttpHeaders) => {
  const result = { ...source }
  for (const name of [
    ...(source.connection ?? "").split(",").map((token) => token.trim().toLowerCase()),
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade"
  ])
    delete result[name]
  return result
}

const server = createServer((downstream, response) => {
  let upstream: ReturnType<typeof request> | undefined
  let body: IncomingMessage | undefined
  let timer: ReturnType<typeof setTimeout> | undefined
  const cancel = () => {
    clearTimeout(timer)
    body?.destroy()
    upstream?.destroy()
  }
  const fail = () => {
    cancel()
    if (response.destroyed) return
    if (response.headersSent) response.destroy()
    else {
      response.writeHead(502)
      response.end("benchmark upstream failed")
    }
  }
  downstream.on("aborted", cancel)
  downstream.on("error", fail)
  response.on("close", cancel)
  response.on("error", cancel)
  const afterHalfRtt = (action: () => void) => {
    if (rtt === 0) action()
    else timer = setTimeout(action, rtt / 2)
  }
  afterHalfRtt(() => {
    if (response.destroyed || downstream.destroyed) return
    upstream = request(
      target,
      {
        method: downstream.method,
        path: downstream.url ?? "/",
        headers: { ...headers(downstream.headers), host: target.host }
      },
      (incoming) => {
        body = incoming
        incoming.on("error", fail)
        incoming.on("aborted", fail)
        // Leave the readable paused during the header delay; pipe then provides backpressure.
        // Only bounded socket/stream buffers are used, never response.text()/arrayBuffer().
        afterHalfRtt(() => {
          if (response.destroyed) return cancel()
          response.writeHead(incoming.statusCode ?? 502, {
            ...headers(incoming.headers),
            "x-echo-benchmark-rtt-ms": String(rtt),
            "x-echo-benchmark-proxy-sha256": sourceSha256
          })
          response.flushHeaders()
          incoming.pipe(response)
        })
      }
    )
    upstream.on("error", fail)
    downstream.pipe(upstream)
  })
})
server.requestTimeout = 0
server.listen(port, "127.0.0.1", () => {
  console.log(JSON.stringify({ port, upstream: target.origin, addedRttMs: rtt, sourceSha256 }))
})
