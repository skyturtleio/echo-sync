import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"
import { middleware } from "./fakeSync.ts"

export default defineConfig({
  root: fileURLToPath(new URL(".", import.meta.url)),
  server: {
    port: 5199,
    strictPort: true,
    host: "127.0.0.1",
    fs: { allow: [fileURLToPath(new URL("../..", import.meta.url))] },
    // The vertical-slice server (examples/server/main.ts): the real SyncRpcs at /sync, scenario hooks at /admin.
    proxy: Object.fromEntries(
      ["/sync", "/admin"].map((path) => [path, process.env["ECHO_SLICE_SERVER"] ?? "http://127.0.0.1:5200"])
    )
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
