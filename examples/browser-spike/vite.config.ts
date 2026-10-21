import { fileURLToPath } from "node:url"
import { defineConfig, type ProxyOptions } from "vite"
import { middleware } from "./fakeSync.ts"

// The vertical-slice server (examples/server/main.ts): the real SyncRpcs at /sync, scenario hooks at /admin.
const sliceProxy: ProxyOptions = {
  target: process.env["ECHO_SLICE_SERVER"] ?? "http://127.0.0.1:5200",
  // When the server dies mid-response (a live pokes stream), Vite's proxy keeps the browser's response open
  // forever, so the client never sees the stream end and never reconnects. Break it, as a direct connection would.
  configure: (proxy) =>
    void proxy.on("proxyRes", (upstream, _req, res) => {
      upstream.on("close", () => {
        if (!upstream.complete) res.destroy()
      })
    })
}
export default defineConfig({
  root: fileURLToPath(new URL(".", import.meta.url)),
  server: {
    port: 5199,
    strictPort: true,
    host: "127.0.0.1",
    fs: { allow: [fileURLToPath(new URL("../..", import.meta.url))] },
    proxy: { "/sync": sliceProxy, "/admin": sliceProxy }
  },
  // wa-sqlite resolves its .wasm relative to its own module URL; pre-bundling would break that.
  // Scan the workers too: otherwise deps first seen when a worker loads trigger a mid-run full page reload.
  optimizeDeps: {
    entries: ["index.html", "leader.worker.ts", "router.worker.ts"],
    exclude: ["@effect/wa-sqlite", "@effect/sql-sqlite-wasm"]
  },
  worker: { format: "es" },
  plugins: [{ name: "echo-fake-sync", configureServer: (server) => void server.middlewares.use(middleware) }]
})
