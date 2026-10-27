// Real OPFS ownership controls. Run with opfs-lifetime.sh; no server/database is required.
import { Effect, Fiber } from "effect"
import * as OpfsSqlite from "../../src/client/worker/OpfsSqlite.ts"
import { AccessHandlePoolVFS } from "../../src/client/worker/vendor/AccessHandlePoolVFS.js"

const assert = (condition, message) => {
  if (!condition) throw new Error(message)
}
const nativeAcquire = FileSystemFileHandle.prototype.createSyncAccessHandle
const acquire = (file) => nativeAcquire.call(file)
const reopen = async (file) => {
  const handle = await acquire(file)
  handle.close()
}
const failed = (promise) =>
  promise.then(
    () => {
      throw new Error("expected rejection")
    },
    (error) => error
  )

async function run() {
  const root = await navigator.storage.getDirectory()
  const name = `echo-opfs-lifetime-${crypto.randomUUID()}`
  const parent = await root.getDirectoryHandle(name, { create: true })
  const results = []
  try {
    // Healthy pools still allocate six files and independent identities can own them concurrently.
    const a = await AccessHandlePoolVFS.create(`${name}/a`, {})
    try {
      const b = await AccessHandlePoolVFS.create(`${name}/b`, {})
      try {
        assert(a.getCapacity() === 6 && b.getCapacity() === 6, "six-file isolated pools")
        const conflict = await failed(AccessHandlePoolVFS.create(`${name}/a`, {}))
        assert(
          conflict.name === "NoModificationAllowedError" || conflict.name === "InvalidStateError",
          "exclusive pool"
        )
        const ownedDirectory = await parent.getDirectoryHandle("a")
        for await (const handle of ownedDirectory.values()) {
          const blocked = await failed(acquire(handle))
          assert(
            blocked.name === "NoModificationAllowedError" || blocked.name === "InvalidStateError",
            "conflict must not release another pool's handles"
          )
        }
      } finally {
        await b.close()
      }
    } finally {
      await a.close()
    }
    const again = await AccessHandlePoolVFS.create(`${name}/a`, {})
    await again.close()
    results.push("six-file pools, isolation and reopening")

    const directory = await parent.getDirectoryHandle("partial", { create: true })
    const files = await Promise.all(
      ["first", "failure", "late"].map((file) => directory.getFileHandle(file, { create: true }))
    )
    const firstAcquired = Promise.withResolvers()
    const lateAcquired = Promise.withResolvers()
    const deliverLate = Promise.withResolvers()
    let lateNative
    FileSystemFileHandle.prototype.createSyncAccessHandle = async function () {
      if (this.name === "failure") {
        await Promise.all([firstAcquired.promise, lateAcquired.promise])
        throw new DOMException("deliberate failure", "NoModificationAllowedError")
      }
      const handle = await acquire(this)
      if (this.name === "first") firstAcquired.resolve()
      if (this.name === "late") {
        lateNative = handle
        lateAcquired.resolve()
        await deliverLate.promise
      }
      return handle
    }
    try {
      const error = await failed(AccessHandlePoolVFS.create(`${name}/partial`, {}))
      assert(error.message === "deliberate failure", "initialization failure propagation")
      await reopen(files[0])
      // Late sibling is deliberately still held by the native completion gate.
      const blocked = await failed(acquire(files[2]))
      assert(blocked instanceof DOMException, "late handle really holds an OPFS lock")
      deliverLate.resolve()
      // The delivery and VFS close happen in Promise reactions, before a fresh task.
      await new Promise((resolve) => setTimeout(resolve, 0))
      await reopen(files[2])
      results.push("partial failure releases early and late native handles")
    } finally {
      FileSystemFileHandle.prototype.createSyncAccessHandle = nativeAcquire
      deliverLate.resolve()
      lateNative?.close()
    }

    const delayed = await parent.getDirectoryHandle("delayed", { create: true })
    const delayedFile = await delayed.getFileHandle("pending", { create: true })
    const acquired = Promise.withResolvers()
    const deliver = Promise.withResolvers()
    let pendingNative
    FileSystemFileHandle.prototype.createSyncAccessHandle = async function () {
      pendingNative = await acquire(this)
      acquired.resolve()
      await deliver.promise
      return pendingNative
    }
    let fiber
    try {
      fiber = Effect.runFork(Effect.scoped(OpfsSqlite.open({ poolDirectory: `${name}/delayed`, dbName: "test.db" })))
      await acquired.promise
      await Effect.runPromise(Fiber.interrupt(fiber).pipe(Effect.timeout("1 second")))
      deliver.resolve()
      await new Promise((resolve) => setTimeout(resolve, 0))
      await reopen(delayedFile)
      results.push("Effect startup interruption finishes before native delivery")
    } finally {
      FileSystemFileHandle.prototype.createSyncAccessHandle = nativeAcquire
      deliver.resolve()
      pendingNative?.close()
      fiber?.interruptUnsafe()
    }

    const pool = await AccessHandlePoolVFS.create(`${name}/growth`, {})
    const growing = await parent.getDirectoryHandle("growth")
    const growthAcquired = Promise.withResolvers()
    const deliverGrowth = Promise.withResolvers()
    let growthFile
    let growthHandle
    FileSystemFileHandle.prototype.createSyncAccessHandle = async function () {
      growthFile = this
      growthHandle = await acquire(this)
      growthAcquired.resolve()
      await deliverGrowth.promise
      return growthHandle
    }
    try {
      const outcome = failed(pool.addCapacity(2))
      await growthAcquired.promise
      await pool.close()
      deliverGrowth.resolve()
      const error = await outcome
      assert(error.name === "InvalidStateError" && pool.getCapacity() === 0, "closed pool cannot regrow")
      await reopen(growthFile)
      let count = 0
      for await (const _entry of growing.values()) count++
      assert(count === 7, "no second growth file created after close")
      results.push("post-close growth releases late handle without further files")
    } finally {
      FileSystemFileHandle.prototype.createSyncAccessHandle = nativeAcquire
      deliverGrowth.resolve()
      growthHandle?.close()
      await pool.close()
    }
    return results
  } finally {
    FileSystemFileHandle.prototype.createSyncAccessHandle = nativeAcquire
    await root.removeEntry(name, { recursive: true })
  }
}

run().then(
  (checks) => self.postMessage({ ok: true, checks }, { transfer: [] }),
  (error) => self.postMessage({ ok: false, error: String(error), stack: error.stack }, { transfer: [] })
)
