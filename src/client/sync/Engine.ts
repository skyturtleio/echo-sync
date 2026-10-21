/**
 * Client reconciliation engine (DESIGN §6, §8, §9).
 *
 * Pure Effect code over three ports: a transactional `SyncStorage` (replica rows, outbox,
 * sync metadata, staged pull pages, notices), an `Executor` that runs a mutation handler
 * optimistically against the replica, and a `Transport` for push/pull.
 *
 * Replay-set invariant (§8): a pending mutation leaves the outbox only inside the transaction
 * that installs a complete pull page set (`more: false`) carrying its ack. Pages with
 * `more: true` are staged durably and never applied on their own: a page's payloads are read at
 * that page's snapshot, which can already contain effects whose log entries (and acks) arrive
 * in later pages, so applying early would either expose a torn transaction or replay a pending
 * mutation over a server row that already contains it.
 */
import { Array as Arr, Clock, Effect, Option, Schema, Semaphore } from "effect"
import { ResnapshotRequired } from "./Wire.ts"
import type {
  Cursor,
  MutationEnvelope,
  MutationRejection,
  PullDone,
  PullPage,
  PullRequest,
  PullResponse,
  PushRequest,
  PushResponse,
  RowWriter,
  UndoImage
} from "./Wire.ts"

/** An outbox entry. `createdAt` is the fixed `ctx.now` for every optimistic run of it. */
export interface PendingMutation {
  readonly envelope: MutationEnvelope
  readonly createdAt: number
  readonly undo: ReadonlyArray<UndoImage>
}

export interface SyncMeta {
  /** Position through which the installed replica is authoritative. */
  readonly cursor: Cursor
  /** Last mutation id ever assigned (survives removal of acked entries). */
  readonly lastMutationId: number
  /** Highest mutation id the server has reported as processed; never resent. */
  readonly pushedThrough: number
  /**
   * The server demanded a resnapshot: the next page set starts from `null` and replaces the
   * replica. `cursor` keeps describing the installed replica until then.
   */
  readonly resnapshot: boolean
}

export const initialMeta: SyncMeta = { cursor: null, lastMutationId: 0, pushedThrough: 0, resnapshot: false }

/** The cursor the in-progress page set started from. */
const pullFrom = (meta: SyncMeta): Cursor => (meta.resnapshot ? null : meta.cursor)

/** A server-rejected mutation, retained for inspection after leaving the outbox (§9). */
export interface Notice {
  readonly envelope: MutationEnvelope
  readonly error: MutationRejection
}

/**
 * Durable client state. Every method other than `transaction` must be called inside
 * `transaction`; transactions are serializable and roll back every write on failure.
 */
export interface SyncStorage<E> {
  readonly transaction: <A, E2, R>(self: Effect.Effect<A, E2, R>) => Effect.Effect<A, E | E2, R>
  readonly rows: RowWriter<E>
  readonly meta: Effect.Effect<SyncMeta, E>
  readonly setMeta: (meta: SyncMeta) => Effect.Effect<void, E>
  /** Outbox entries in ascending `mutationId`. */
  readonly pending: Effect.Effect<ReadonlyArray<PendingMutation>, E>
  readonly appendPending: (entry: PendingMutation) => Effect.Effect<void, E>
  readonly setUndo: (mutationId: number, undo: ReadonlyArray<UndoImage>) => Effect.Effect<void, E>
  readonly removePending: (mutationIds: ReadonlyArray<number>) => Effect.Effect<void, E>
  /**
   * Pages of the in-progress set, in arrival order. Durable: after a reload the engine resumes
   * the set with `meta.cursor` and the last staged page's continuation.
   */
  readonly staged: Effect.Effect<ReadonlyArray<PullPage>, E>
  readonly stage: (page: PullPage) => Effect.Effect<void, E>
  readonly clearStaged: Effect.Effect<void, E>
  readonly addNotices: (notices: ReadonlyArray<Notice>) => Effect.Effect<void, E>
}

export interface RunResult {
  /** First-touch pre-images of every row the handler wrote, in write order. */
  readonly undo: ReadonlyArray<UndoImage>
  readonly rejection: Option.Option<MutationRejection>
}

export interface Executor<E> {
  /**
   * Runs the handler for `envelope` against the replica with `ctx.now = createdAt` and
   * `ctx.random` seeded from the mutation identity, so every rebase reproduces the same
   * optimistic result. Reports pre-images even when the handler rejects.
   */
  readonly run: (envelope: MutationEnvelope, createdAt: number) => Effect.Effect<RunResult, E>
}

export interface Transport<E> {
  readonly push: (request: PushRequest) => Effect.Effect<PushResponse, E>
  readonly pull: (request: PullRequest) => Effect.Effect<PullResponse, E | ResnapshotRequired>
}

/** The handler rejected the mutation against the local replica; nothing was saved. */
export class LocallyRejected extends Schema.TaggedError<LocallyRejected>()("LocallyRejected", {
  name: Schema.String,
  error: Schema.Unknown
}) {}

const restore = <E>(rows: RowWriter<E>, undo: ReadonlyArray<UndoImage>) =>
  Effect.forEach(Arr.reverse(undo), (image) => rows.write(image.table, image.id, image.before), {
    discard: true
  })

/** Re-runs `entry` optimistically and records its fresh undo images. */
const replay = <E>(storage: SyncStorage<E>, executor: Executor<E>, entry: PendingMutation) =>
  Effect.gen(function* () {
    const run = yield* executor.run(entry.envelope, entry.createdAt)
    // A pending mutation that no longer applies locally has no optimistic effect; it stays
    // queued because only the server's verdict removes it.
    if (Option.isSome(run.rejection)) {
      yield* restore(storage.rows, run.undo)
      yield* storage.setUndo(entry.envelope.mutationId, [])
    } else {
      yield* storage.setUndo(entry.envelope.mutationId, run.undo)
    }
  })

/**
 * Installs a complete page set: the staged `PullPage`s followed by their `PullDone`. Must run
 * inside one storage transaction. A set that started from cursor `null` is a snapshot of every
 * readable scope, so it replaces the replica instead of patching it (rows of scopes revoked
 * while the client could not see the log would otherwise survive a resnapshot).
 * Exported so tests can build a deliberately wrong page-at-a-time client.
 */
export const applyPageSet = <E>(
  storage: SyncStorage<E>,
  executor: Executor<E>,
  pages: ReadonlyArray<PullPage>,
  done: PullDone
) =>
  Effect.gen(function* () {
    const meta = yield* storage.meta
    const pending = yield* storage.pending
    for (const entry of Arr.reverse(pending)) yield* restore(storage.rows, entry.undo)
    if (pullFrom(meta) === null) yield* storage.rows.clearAll

    const set: ReadonlyArray<PullResponse> = [...pages, done]
    for (const page of set) {
      for (const change of page.changes) {
        switch (change._tag) {
          case "Upsert":
            yield* storage.rows.write(change.table, change.row.id, Option.some(change.row))
            break
          case "Tombstone":
            yield* storage.rows.write(change.table, change.id, Option.none())
            break
          case "ScopePurged":
            yield* storage.rows.purgeScope(change.scopeId)
            break
        }
      }
    }

    const outcomes = new Map(set.flatMap((page) => page.acks.map((ack) => [ack.mutationId, ack.outcome])))
    const acked: Array<number> = []
    const notices: Array<Notice> = []
    for (const entry of pending) {
      const outcome = outcomes.get(entry.envelope.mutationId)
      if (outcome === undefined) {
        yield* replay(storage, executor, entry)
        continue
      }
      acked.push(entry.envelope.mutationId)
      if (outcome._tag === "Rejected") notices.push({ envelope: entry.envelope, error: outcome.error })
    }
    yield* storage.removePending(acked)
    yield* storage.addNotices(notices)

    // The server processes a client's mutations in order, so an ack for id k means every id
    // up to k has been processed.
    const pushedThrough = Math.max(meta.pushedThrough, ...outcomes.keys())
    yield* storage.setMeta({ ...meta, cursor: done.nextCursor, pushedThrough, resnapshot: false })
    yield* storage.clearStaged
  })

export interface PushResult {
  /** Number of mutations the server reported as processed in this cycle. */
  readonly processed: number
  readonly stopped: Option.Option<{ readonly reason: string }>
}

export interface Engine<E, TE> {
  /** Appends to the outbox and applies optimistically in one transaction ("saved" on success). */
  readonly mutate: (name: string, args: unknown) => Effect.Effect<MutationEnvelope, E | LocallyRejected>
  /** Sends unprocessed outbox entries in order until none remain or the server stops. */
  readonly push: Effect.Effect<PushResult, E | TE>
  /** Pulls until `more: false`, staging intermediate pages, then installs the set atomically. */
  readonly pull: Effect.Effect<void, E | TE | ResnapshotRequired>
}

export const make = <E, TE>(options: {
  readonly clientId: string
  readonly storage: SyncStorage<E>
  readonly executor: Executor<E>
  readonly transport: Transport<TE>
  readonly pushBatchSize?: number
}): Effect.Effect<Engine<E, TE>> =>
  Effect.gen(function* () {
    const { clientId, storage, executor, transport } = options
    const batchSize = options.pushBatchSize ?? 50
    const pushLock = yield* Semaphore.make(1)
    const pullLock = yield* Semaphore.make(1)

    const mutate = (name: string, args: unknown) =>
      storage.transaction(
        Effect.gen(function* () {
          const meta = yield* storage.meta
          const envelope: MutationEnvelope = { clientId, mutationId: meta.lastMutationId + 1, name, args }
          const createdAt = yield* Clock.currentTimeMillis
          const run = yield* executor.run(envelope, createdAt)
          if (Option.isSome(run.rejection)) {
            return yield* new LocallyRejected({ name, error: run.rejection.value })
          }
          yield* storage.appendPending({ envelope, createdAt, undo: run.undo })
          yield* storage.setMeta({ ...meta, lastMutationId: envelope.mutationId })
          return envelope
        })
      )

    const push = pushLock.withPermit(
      Effect.gen(function* () {
        let processed = 0
        while (true) {
          const { meta, pending } = yield* storage.transaction(
            Effect.all({ meta: storage.meta, pending: storage.pending })
          )
          const batch = pending.filter((entry) => entry.envelope.mutationId > meta.pushedThrough).slice(0, batchSize)
          if (batch.length === 0) return { processed, stopped: Option.none() }
          const response = yield* transport.push({
            clientId,
            mutations: batch.map(({ envelope }) => ({
              mutationId: envelope.mutationId,
              name: envelope.name,
              args: envelope.args
            }))
          })
          const through = response.results.at(-1)?.mutationId
          if (through !== undefined) {
            yield* storage.transaction(
              Effect.gen(function* () {
                const current = yield* storage.meta
                yield* storage.setMeta({ ...current, pushedThrough: Math.max(current.pushedThrough, through) })
              })
            )
          }
          processed += response.results.length
          if (Option.isSome(response.stopped) || response.results.length < batch.length) {
            return { processed, stopped: response.stopped }
          }
        }
      })
    )

    const pull = pullLock.withPermit(
      Effect.gen(function* () {
        while (true) {
          const { meta, staged } = yield* storage.transaction(
            Effect.all({ meta: storage.meta, staged: storage.staged })
          )
          const continuation = Option.fromNullishOr(staged.at(-1)?.continuation)
          const request: PullRequest = { clientId, cursor: pullFrom(meta), continuation }
          const page = yield* transport.pull(request).pipe(
            Effect.map(Option.some<PullResponse>),
            // Generic `TE` defeats `catchTag`'s tag extraction, so narrow by class.
            Effect.catch((error) =>
              Effect.gen(function* () {
                if (!(error instanceof ResnapshotRequired)) return yield* Effect.fail(error)
                if (request.cursor === null && Option.isNone(continuation)) return yield* error
                // Never apply a partial set: drop it and start a snapshot from null. The
                // replica (and its cursor) keeps serving reads until that snapshot is installed.
                yield* storage.transaction(
                  Effect.andThen(storage.clearStaged, storage.setMeta({ ...meta, resnapshot: true }))
                )
                return Option.none<PullResponse>()
              })
            )
          )
          if (Option.isNone(page)) continue
          const response = page.value
          if (response._tag === "PullPage") {
            yield* storage.transaction(storage.stage(response))
            continue
          }
          return yield* storage.transaction(
            Effect.gen(function* () {
              const set = yield* storage.staged
              yield* applyPageSet(storage, executor, set, response)
            })
          )
        }
      })
    )

    return { mutate, push, pull }
  })
