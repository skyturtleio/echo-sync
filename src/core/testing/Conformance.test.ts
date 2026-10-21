import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import * as Engine from "../../client/sync/Engine.ts"
import { Ids } from "../index.ts"
import type { Store } from "../index.ts"
import * as Conformance from "./Conformance.ts"
import * as MemoryStore from "./MemoryStore.ts"
import * as MemorySyncStorage from "./MemorySyncStorage.ts"

const register = (checks: ReadonlyArray<Conformance.Check<never>>) => {
  for (const check of checks) {
    it.effect(check.name, () => Effect.map(check.run, (cases) => assert.isAbove(cases, 0)), 60_000)
  }
}

/** The check must fail with a ConformanceViolation (not pass, not crash with something else). */
const mustCatch = (label: string, checks: ReadonlyArray<Conformance.Check<never>>) => {
  for (const check of checks) {
    it.effect(`${label} fails: ${check.name}`, () =>
      Effect.map(Effect.flip(check.run), (error) => assert.instanceOf(error, Conformance.ConformanceViolation))
    )
  }
}

const memorySubject = Effect.sync(() => {
  const disk = MemorySyncStorage.makeDisk()
  return {
    disk,
    subject: {
      storage: MemorySyncStorage.make(disk),
      reopen: Effect.sync(() => MemorySyncStorage.make(disk)),
      pruneReleased: Effect.sync(() => MemorySyncStorage.pruneReleased(disk))
    }
  }
})

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
  register(Conformance.syncStorage(Effect.map(memorySubject, ({ subject }) => subject)))
})

/**
 * Like a real database handle: a reopen closes every earlier handle, and a closed handle dies on
 * use. The suite must only reopen after the transaction it is judging has finished.
 */
const closeOnReopen = Effect.sync(() => {
  const disk = MemorySyncStorage.makeDisk()
  const handles = { current: 0 }
  const open = (): Engine.SyncStorage => {
    const mine = ++handles.current
    const inner = MemorySyncStorage.make(disk)
    const live = <A, E, R>(self: Effect.Effect<A, E, R>) =>
      Effect.suspend(() => (mine === handles.current ? self : Effect.die(new Error("use of a closed storage handle"))))
    return {
      transaction: (self) => live(inner.transaction(self)),
      rows: {
        read: (table, id) => live(inner.rows.read(table, id)),
        write: (table, id, row) => live(inner.rows.write(table, id, row)),
        range: (table, plan) => live(inner.rows.range(table, plan)),
        scan: (table) => live(inner.rows.scan(table)),
        purgeScope: (scopeId) => live(inner.rows.purgeScope(scopeId)),
        clearAll: live(inner.rows.clearAll)
      },
      meta: live(inner.meta),
      setMeta: (meta) => live(inner.setMeta(meta)),
      pending: live(inner.pending),
      lookup: (mutationId) => live(inner.lookup(mutationId)),
      nextSeq: live(inner.nextSeq),
      appendPending: (entry) => live(inner.appendPending(entry)),
      setUndo: (seq, undo) => live(inner.setUndo(seq, undo)),
      release: (seqs) => live(inner.release(seqs)),
      staged: live(inner.staged),
      stage: (page) => live(inner.stage(page)),
      clearStaged: live(inner.clearStaged),
      addNotices: (notices) => live(inner.addNotices(notices))
    }
  }
  return {
    storage: open(),
    reopen: Effect.sync(open),
    pruneReleased: Effect.sync(() => MemorySyncStorage.pruneReleased(disk))
  }
})

describe("a SyncStorage whose reopen closes earlier handles conforms (the suite reopens only after the transaction under test)", () => {
  register(Conformance.syncStorage(closeOnReopen))
})

describe("the suite's expectations come from fixtures, not from the subject (DESIGN §14)", () => {
  const noopRows: Store.ReplicaRowStore = {
    read: () => Effect.succeedNone,
    write: () => Effect.void,
    range: () => Effect.succeed([]),
    scan: () => Effect.succeed([]),
    purgeScope: () => Effect.void,
    clearAll: Effect.void
  }
  const noop: Engine.SyncStorage = {
    transaction: (self) => self,
    rows: noopRows,
    meta: Effect.succeed(Engine.initialMeta),
    setMeta: () => Effect.void,
    pending: Effect.succeed([]),
    lookup: () => Effect.succeedNone,
    nextSeq: Effect.succeed(Ids.Seq.make(1)),
    appendPending: () => Effect.void,
    setUndo: () => Effect.void,
    release: () => Effect.void,
    staged: Effect.succeed([]),
    stage: () => Effect.void,
    clearStaged: Effect.void,
    addNotices: () => Effect.void
  }
  mustCatch(
    "a subject whose every write is a no-op",
    Conformance.syncStorage(Effect.succeed({ storage: noop, reopen: Effect.succeed(noop), pruneReleased: Effect.void }))
  )

  // The reviewer's stub: the reference storage with staging and meta writes dropped.
  const dropsMetaAndStaging = Effect.map(memorySubject, ({ disk }) => {
    const broken = (): Engine.SyncStorage => ({
      ...MemorySyncStorage.make(disk),
      setMeta: () => Effect.void,
      stage: () => Effect.void,
      staged: Effect.succeed([])
    })
    return { storage: broken(), reopen: Effect.sync(broken), pruneReleased: Effect.void }
  })
  mustCatch(
    "a storage that drops meta and staging writes",
    Conformance.syncStorage(dropsMetaAndStaging).filter((check) =>
      ["nextSeq allocates", "clearAll", "a storage transaction"].some((prefix) => check.name.startsWith(prefix))
    )
  )
})

// Every row operation awaits a promise (like a real driver), and rollback is skipped when an
// interruption lands after any I/O has resumed: only a sweep whose interruption requests reach
// the steps following a resumption can see the partial commit.
const asyncStore = (broken: boolean) =>
  Effect.sync((): Conformance.RowStoreSubject => {
    const store = MemoryStore.make()
    const state = { resumed: false }
    const io = <A, E>(self: Effect.Effect<A, E>) =>
      Effect.gen(function* () {
        yield* Effect.promise(() => Promise.resolve())
        state.resumed = true
        return yield* self
      })
    return {
      rows: {
        read: (table, id) => io(store.read(table, id)),
        write: (table, id, row) => io(store.write(table, id, row)),
        range: (table, plan) => io(store.range(table, plan)),
        scan: (table) => io(store.scan(table))
      },
      transaction: (self) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.suspend(() => {
            const before = MemoryStore.cloneTables(store.tables())
            state.resumed = false
            return restore(self).pipe(
              Effect.onExit((exit) =>
                Effect.sync(() => {
                  if (exit._tag === "Success") return
                  const interrupted = exit.cause.reasons.some((reason) => reason._tag === "Interrupt")
                  if (broken && interrupted && state.resumed) return // the bug
                  store.replace(before)
                })
              )
            )
          })
        )
    }
  })

describe("the interrupt sweep reaches checkpoints after asynchronous I/O resumes", () => {
  mustCatch(
    "a store whose rollback is skipped after an asynchronous resumption",
    Conformance.rowStore(asyncStore(true)).filter((check) => check.name.startsWith("a transaction that fails"))
  )
  // Control: the same asynchronous store with a correct rollback passes.
  register(Conformance.rowStore(asyncStore(false)).filter((check) => check.name.startsWith("a transaction that fails")))
})
