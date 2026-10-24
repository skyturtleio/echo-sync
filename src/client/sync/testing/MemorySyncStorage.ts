/**
 * In-memory reference implementation of the engine's client storage port (`SyncStorage`):
 * replica rows (a `MemoryStore`), sync meta, the outbox with durable `seq` allocation and
 * released-row markers (INV-CLI-4), staged pull pages, and notices.
 *
 * `Disk` is what survives a reload; the engine and everything in memory may be rebuilt over it.
 * Transactions are serialized: the snapshot of the whole disk, the body and the rollback run
 * under one permit, so a failed or interrupted transaction restores exactly its own writes.
 * Rows, outbox, seq counter, meta, staging and notices share ONE commit/rollback decision taken
 * from the body's exit (like SQLite's BEGIN/COMMIT); the replica's own `transaction` is not
 * nested, because nested finalizers can observe different exits and split the commit.
 */
import { Effect, Option, Schema, Semaphore } from "effect"
import * as Engine from "../Engine.ts"
import { Ids } from "../../../core/index.ts"
import type { Mutation, Protocol } from "../../../core/index.ts"
import * as MemoryStore from "../../../core/testing/MemoryStore.ts"

export interface Disk {
  readonly replica: MemoryStore.MemoryStore
  meta: Engine.SyncMeta
  /** Last allocated outbox `seq` (0 = none yet). */
  lastSeq: number
  /** Unreleased entries, ascending `seq`. */
  outbox: Array<Engine.PendingMutation>
  /** Released-row markers by `mutationId`, until pruned. */
  released: Map<string, Mutation.MutationEnvelope>
  staged: Array<typeof Protocol.PullPage.Type>
  notices: Array<Engine.Notice>
  /** Fault injection: the Nth replica write from now dies (a crash mid-transaction). */
  crashAfterWrites: Option.Option<number>
}

/** A simulated process crash in the middle of a storage transaction. */
export class SimulatedCrash extends Schema.TaggedError<SimulatedCrash>()("SimulatedCrash", {}) {}

export const makeDisk = (): Disk => ({
  replica: MemoryStore.make(),
  meta: Engine.initialMeta,
  lastSeq: 0,
  outbox: [],
  released: new Map(),
  staged: [],
  notices: [],
  crashAfterWrites: Option.none()
})

/** The released-row retention window has passed (INV-CLI-8: default 24 h). */
export const pruneReleased = (disk: Disk) => {
  disk.released = new Map()
}

export const make = (disk: Disk): Engine.SyncStorage => {
  const lock = Semaphore.makeUnsafe(1)
  const crashPoint = Effect.suspend(() => {
    if (Option.isNone(disk.crashAfterWrites)) return Effect.void
    const left = disk.crashAfterWrites.value
    if (left > 0) {
      disk.crashAfterWrites = Option.some(left - 1)
      return Effect.void
    }
    disk.crashAfterWrites = Option.none()
    return Effect.die(new SimulatedCrash())
  })
  return {
    transaction: (self) =>
      lock.withPermit(
        Effect.uninterruptibleMask((restore) =>
          Effect.suspend(() => {
            const rows = MemoryStore.cloneTables(disk.replica.tables())
            const saved = {
              meta: disk.meta,
              lastSeq: disk.lastSeq,
              outbox: disk.outbox,
              released: new Map(disk.released),
              staged: disk.staged,
              notices: disk.notices
            }
            return restore(self).pipe(
              Effect.onExit((exit) =>
                Effect.sync(() => {
                  if (exit._tag === "Success") return
                  disk.replica.replace(rows)
                  Object.assign(disk, saved)
                })
              )
            )
          })
        )
      ),
    rows: {
      read: disk.replica.read,
      range: disk.replica.range,
      scan: disk.replica.scan,
      write: (table, id, row) => Effect.andThen(crashPoint, disk.replica.write(table, id, row)),
      purgeScope: (scopeId) => Effect.andThen(crashPoint, disk.replica.purgeScope(scopeId)),
      clearAll: Effect.andThen(crashPoint, disk.replica.clearAll)
    },
    meta: Effect.sync(() => disk.meta),
    setMeta: (meta) =>
      Effect.sync(() => {
        disk.meta = meta
      }),
    pending: Effect.sync(() => disk.outbox),
    lookup: (mutationId) =>
      Effect.sync(() =>
        Option.fromNullishOr(
          disk.outbox.find((entry) => entry.envelope.mutationId === mutationId)?.envelope ??
            disk.released.get(mutationId)
        )
      ),
    nextSeq: Effect.sync(() => {
      disk.lastSeq += 1
      return Ids.Seq.make(disk.lastSeq)
    }),
    appendPending: (entry) =>
      Effect.sync(() => {
        disk.outbox = [...disk.outbox, entry]
      }),
    setUndo: (seq, undo) =>
      Effect.sync(() => {
        disk.outbox = disk.outbox.map((e) => (e.envelope.seq === seq ? { ...e, undo } : e))
      }),
    release: (seqs) =>
      Effect.sync(() => {
        const gone = new Set(seqs)
        for (const entry of disk.outbox) {
          if (gone.has(entry.envelope.seq)) disk.released.set(entry.envelope.mutationId, entry.envelope)
        }
        disk.outbox = disk.outbox.filter((e) => !gone.has(e.envelope.seq))
      }),
    staged: Effect.sync(() => disk.staged),
    stage: (page) =>
      Effect.sync(() => {
        disk.staged = [...disk.staged, page]
      }),
    clearStaged: Effect.sync(() => {
      disk.staged = []
    }),
    addNotices: (notices) =>
      Effect.sync(() => {
        disk.notices = [...disk.notices, ...notices]
      })
  }
}
