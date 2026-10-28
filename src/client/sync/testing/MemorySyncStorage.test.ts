import { assert, describe, it } from "@effect/vitest"
import { Array as Arr, Effect, Exit, Option, Record as Rec, String as Str } from "effect"
import { Ids } from "../../../core/index.ts"
import type { Store } from "../../../core/index.ts"
import * as Conformance from "../../../core/testing/Conformance.ts"
import * as Engine from "../Engine.ts"
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

describe("MemorySyncStorage conforms to SyncStorage", () => {
  register(Conformance.syncStorage(Effect.map(memorySubject, ({ subject }) => subject)))
})

it.effect("conformance retains typed reopen and pruning failures rather than disguising them as violations", () =>
  Effect.gen(function* () {
    for (const operation of ["reopen", "pruneReleased"]) {
      const failure = new Error(`${operation} refused`)
      const checks = Conformance.syncStorage(
        Effect.map(memorySubject, ({ subject }) => ({
          ...subject,
          reopen: operation === "reopen" ? Effect.fail(failure) : subject.reopen,
          pruneReleased: operation === "pruneReleased" ? Effect.fail(failure) : subject.pruneReleased
        }))
      )
      const check = Option.getOrUndefined(Arr.findFirst(checks, (c) => Str.startsWith("seq is never reused:")(c.name)))
      if (check === undefined) return assert.fail("missing seq durability check")
      assert.deepStrictEqual(yield* Effect.exit(check.run), Exit.fail(failure))
    }
  })
)

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
        writeMany: (table, rows) => live(inner.rows.writeMany(table, rows)),
        range: (table, plan, columns) => live(inner.rows.range(table, plan, columns)),
        aggregate: (table, plan, sums) => live(inner.rows.aggregate(table, plan, sums)),
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
      stagedProgress: live(inner.stagedProgress),
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

describe("the suite's expectations come from fixtures, not from the subject (SPEC §17)", () => {
  const noopRows: Store.ReplicaRowStore = {
    read: () => Effect.succeedNone,
    write: () => Effect.void,
    writeMany: () => Effect.void,
    range: () => Effect.succeed([]),
    aggregate: (_, __, sums) => Effect.succeed({ count: 0, sum: Rec.fromIterableWith(sums, (c) => [c.field, null]) }),
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
    stagedProgress: Effect.succeed({ continuation: Option.none(), count: 0 }),
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
    Arr.filter(Conformance.syncStorage(dropsMetaAndStaging), (check) =>
      Arr.some(["nextSeq allocates", "clearAll", "a storage transaction"], (prefix) =>
        Str.startsWith(prefix)(check.name)
      )
    )
  )
})
