// Test-only: wa-sqlite loads its .wasm with fetch(file:...), which Node's fetch does not implement.
// oxlint-disable-next-line effecttsgo/node-builtin-import -- Vitest runs on Node; this fetch compatibility shim reads Node file: URLs, not application I/O.
import { readFile } from "node:fs/promises"

const nodeFetch = globalThis.fetch
globalThis.fetch = Object.assign(
  // oxlint-disable-next-line effecttsgo/async-function -- Implements the native fetch Promise signature consumed by wa-sqlite's WASM loader.
  async (input: URL | RequestInfo, init?: RequestInit) => {
    const url = input instanceof URL ? input : new URL(typeof input === "string" ? input : input.url)
    return url.protocol === "file:"
      ? new Response(await readFile(url), { headers: { "content-type": "application/wasm" } })
      : // oxlint-disable-next-line effecttsgo/global-fetch -- Forward non-file requests to the original native fetch; an Effect HTTP client would recurse through this shim.
        nodeFetch(input, init)
  },
  { preconnect: nodeFetch.preconnect }
)
