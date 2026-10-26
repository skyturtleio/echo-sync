import { fileURLToPath } from "node:url"
import { Config, Effect } from "effect"
import { defineConfig, type ProxyOptions } from "vite"
import { middleware } from "./fakeSync.ts"

// The vertical-slice server (examples/server/main.ts): the real SyncRpcs at /sync, scenario hooks at /admin.
const sliceProxy: ProxyOptions = {
  target: Effect.runSync(Config.String("ECHO_SLICE_SERVER").pipe(Config.withDefault("http://127.0.0.1:5200")))
  // No proxy workaround for a server that dies mid-stream: Vite's proxy keeps the browser's pokes response
  // open, and the client's idleTimeout (INV-LOOP-7) ends it, as it would behind any such proxy.
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
    entries: ["index.html", "bootstrap.html", "bootstrap.worker.ts", "leader.worker.ts", "router.worker.ts"],
    exclude: ["@effect/wa-sqlite", "@effect/sql-sqlite-wasm"]
  },
  worker: { format: "es" },
  plugins: [{ name: "echo-fake-sync", configureServer: (server) => void server.middlewares.use(middleware) }]
})
