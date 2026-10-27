// oxlint-disable-next-line import/no-unassigned-import -- Run the real wasm loader under Node.
import "../testing/nodeWasm.ts"
import { Effect, Fiber } from "effect"
import { afterEach, expect, it, vi } from "vitest"
import * as OpfsSqlite from "./OpfsSqlite.ts"
import { AccessHandlePoolVFS } from "./vendor/AccessHandlePoolVFS.js"

// These fixtures implement native Promise-based OPFS completions, not Effect services: the
// tests must deliver a handle after the consuming Effect has already finished interruption.
/* oxlint-disable effecttsgo/async-function -- Native OPFS boundary fixtures and their Promise-driving tests. */

const accessHandle = () => {
  let closes = 0
  let writes = 0
  return {
    get closes() {
      return closes
    },
    get writes() {
      return writes
    },
    close() {
      closes++
    },
    read(buffer: Uint8Array | Uint32Array) {
      if (closes) throw new Error("read after close")
      if (buffer instanceof Uint32Array) buffer.set([0xfecc5f80, 0xaccec037])
      return buffer.byteLength
    },
    write(buffer: Uint8Array | Uint32Array) {
      if (closes) throw new Error("write after close")
      writes++
      return buffer.byteLength
    },
    truncate() {
      if (closes) throw new Error("truncate after close")
    },
    flush() {
      if (closes) throw new Error("flush after close")
    }
  }
}
type AccessHandle = ReturnType<typeof accessHandle>
const file = (acquire: () => Promise<AccessHandle>) => ({ kind: "file", createSyncAccessHandle: acquire })
const install = (files: Array<readonly [string, ReturnType<typeof file>]>, added?: () => Promise<AccessHandle>) => {
  const directory = {
    async getDirectoryHandle() {
      return directory
    },
    async getFileHandle() {
      if (added === undefined) throw new Error("unexpected pool growth")
      return file(added)
    },
    async *[Symbol.asyncIterator]() {
      yield* files
    }
  }
  vi.stubGlobal("navigator", { storage: { getDirectory: () => Promise.resolve(directory) } })
}
afterEach(() => vi.unstubAllGlobals())

it("a failed OPFS pool open releases partial handles and late siblings without waiting for them", async () => {
  const first = accessHandle()
  const late = accessHandle()
  const failure = Promise.withResolvers<AccessHandle>()
  const pending = Promise.withResolvers<AccessHandle>()
  const started = Promise.withResolvers<void>()
  const closedLate = Promise.withResolvers<void>()
  const close = late.close
  late.close = () => {
    close()
    closedLate.resolve()
  }
  install([
    ["first", file(() => Promise.resolve(first))],
    ["failure", file(() => failure.promise)],
    [
      "late",
      file(() => {
        started.resolve()
        return pending.promise
      })
    ]
  ])
  const opening = AccessHandlePoolVFS.create("replica", {})
  const refused = opening.catch((error: unknown) => error)
  await started.promise
  failure.reject(new DOMException("busy", "NoModificationAllowedError"))
  expect(await refused).toMatchObject({ name: "NoModificationAllowedError", message: "busy" })
  expect(first.closes).toBe(1)
  expect(late.closes).toBe(0)
  pending.resolve(late)
  await closedLate.promise
  expect(late.closes).toBe(1)
  expect(late.writes).toBe(0)
})

it("interrupting OPFS startup finishes before native acquisition and closes its late handle", async () => {
  const pending = Promise.withResolvers<AccessHandle>()
  const started = Promise.withResolvers<void>()
  const closed = Promise.withResolvers<void>()
  const handle = accessHandle()
  const close = handle.close
  handle.close = () => {
    close()
    closed.resolve()
  }
  install([
    [
      "pending",
      file(() => {
        started.resolve()
        return pending.promise
      })
    ]
  ])
  const fiber = Effect.runFork(Effect.scoped(OpfsSqlite.open({ poolDirectory: "replica", dbName: "test.db" })))
  await started.promise
  // This cannot await the pending native result; the old masked acquisition hangs here.
  await Effect.runPromise(Fiber.interrupt(fiber))
  expect(handle.closes).toBe(0)
  pending.resolve(handle)
  await closed.promise
  expect(handle.closes).toBe(1)
  expect(handle.writes).toBe(0)
})

it("closing an OPFS pool during growth closes late handles and never grows the closed pool", async () => {
  const first = accessHandle()
  const late = accessHandle()
  const pending = Promise.withResolvers<AccessHandle>()
  const started = Promise.withResolvers<void>()
  install([["first", file(() => Promise.resolve(first))]], () => {
    started.resolve()
    return pending.promise
  })
  const pool = await AccessHandlePoolVFS.create("replica", {})
  const growth = pool.addCapacity(2)
  const refused = growth.catch((error: unknown) => error)
  await started.promise
  await pool.close()
  expect(first.closes).toBe(1)
  pending.resolve(late)
  expect(await refused).toMatchObject({ name: "InvalidStateError" })
  expect(late.closes).toBe(1)
  expect(late.writes).toBe(0)
  expect(pool.getCapacity()).toBe(0)
  await expect(pool.addCapacity(1)).rejects.toThrow("closed")
  await expect(pool.isReady()).rejects.toThrow("closed")
  await pool.close()
  expect(first.closes).toBe(1)
  expect(late.closes).toBe(1)
})

it("an OPFS header initialization failure closes its handle and earlier pool files", async () => {
  const first = accessHandle()
  const failed = accessHandle()
  failed.write = () => {
    throw new Error("header write failed")
  }
  install([["first", file(() => Promise.resolve(first))]], () => Promise.resolve(failed))
  const pool = await AccessHandlePoolVFS.create("replica", {})
  await expect(pool.addCapacity(1)).rejects.toThrow("header write failed")
  expect(first.closes).toBe(1)
  expect(failed.closes).toBe(1)
  expect(pool.getCapacity()).toBe(0)
})
