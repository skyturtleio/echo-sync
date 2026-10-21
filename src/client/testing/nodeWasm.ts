// Test-only: wa-sqlite loads its .wasm with fetch(file:...), which Node's fetch does not implement.
import { readFile } from "node:fs/promises"

const nodeFetch = globalThis.fetch
globalThis.fetch = Object.assign(
  async (input: URL | RequestInfo, init?: RequestInit) => {
    const url = input instanceof URL ? input : new URL(typeof input === "string" ? input : input.url)
    return url.protocol === "file:"
      ? new Response(await readFile(url), { headers: { "content-type": "application/wasm" } })
      : nodeFetch(input, init)
  },
  { preconnect: nodeFetch.preconnect }
)
