import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"

export default defineConfig({
  root: fileURLToPath(new URL(".", import.meta.url)),
  server: {
    port: 5199,
    strictPort: true,
    host: "127.0.0.1",
    fs: { allow: [fileURLToPath(new URL("../..", import.meta.url))] }
  },
  // wa-sqlite resolves its .wasm relative to its own module URL; pre-bundling would break that.
  optimizeDeps: { exclude: ["@effect/wa-sqlite", "@effect/sql-sqlite-wasm"] },
  worker: { format: "es" }
})
