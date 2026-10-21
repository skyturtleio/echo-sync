import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import * as Conformance from "./Conformance.ts"
import * as MemoryStore from "./MemoryStore.ts"
import * as MemorySyncStorage from "./MemorySyncStorage.ts"

const register = (checks: ReadonlyArray<Conformance.Check<never>>) => {
  for (const check of checks) {
    it.effect(check.name, () => Effect.map(check.run, (cases) => assert.isAbove(cases, 0)), 60_000)
  }
}

describe("MemoryStore conforms to ReplicaRowStore (the reference semantics)", () => {
  register(
    Conformance.replicaRowStore(
      Effect.sync(() => {
        const store = MemoryStore.make()
        return { rows: store, transaction: store.transaction }
      })
    )
  )
})

describe("MemorySyncStorage conforms to SyncStorage", () => {
  register(
    Conformance.syncStorage(
      Effect.sync(() => {
        const disk = MemorySyncStorage.makeDisk()
        return {
          storage: MemorySyncStorage.make(disk),
          reopen: Effect.sync(() => MemorySyncStorage.make(disk)),
          pruneReleased: Effect.sync(() => MemorySyncStorage.pruneReleased(disk))
        }
      })
    )
  )
})
