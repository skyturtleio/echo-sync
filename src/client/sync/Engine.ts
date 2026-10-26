/**
 * Client reconciliation engine (SPEC §5, §7, §9).
 *
 * Pure Effect code over two ports: a transactional `SyncStorage` (replica rows, outbox, sync
 * metadata, staged pull pages, notices) and a `Transport` for push/pull. Handlers run through
 * core's typed store (`Store.make`) and mutation registry (`Mutation.run`), the same code the
 * server runs authoritatively.
 *
 * Replay-set invariant (INV-CLI-6): a pending mutation leaves the outbox only inside the transaction
 * that installs a complete pull page set (`PullDone`) carrying its ack, or when a push response
 * reports an outcome the installed replica already covers, or bounded transient answers exhaust
 * it (INV-MUT-15). `PullPage`s are staged durably and
 * never applied on their own: a page's payloads are read at that page's snapshot, which can
 * already contain effects whose log entries (and acks) arrive in later pages, so applying early
 * would either expose a torn transaction or replay a pending mutation over a server row that
 * already contains it.
 */
import { Array as Arr, Clock, Effect, Match, Option, Schema, Semaphore, Stream } from "effect"
import { Mutation, Protocol, Store } from "../../core/index.ts"
import type { AccessScope, Errors, Ids, Registry, SyncTable } from "../../core/index.ts"
import * as Telemetry from "../../internal/Telemetry.ts"
import { failureOutcome } from "../../internal/ErrorCatalog.ts"
import type { LocalChange } from "../Provenance.ts"
import * as SyncFailure from "./SyncFailure.ts"
import { MalformedChange, type PullReason, type PushReason, ServerOutdated } from "./SyncFailure.ts"

type PullPage = typeof Protocol.PullPage.Type
type PullDone = typeof Protocol.PullDone.Type

/** An outbox entry. `createdAt` (epoch ms) is the fixed `ctx.now` for every optimistic run of it. */
export interface PendingMutation {
  readonly envelope: Mutation.MutationEnvelope
  readonly createdAt: number
  readonly undo: ReadonlyArray<Store.UndoImage>
}

export interface SyncMeta {
  /** Absent in pre-epoch storage; null means no timeline has been negotiated. */
  readonly epoch?: string | null
  /** Position through which the installed replica is authoritative. */
  readonly cursor: Ids.Cursor
  /** Highest `seq` the server has reported as processed (0 = none); never resent. */
  readonly pushedThrough: number
  /**
   * The server demanded a resnapshot: the next page set starts from `null` and replaces the
   * replica. `cursor` keeps describing the installed replica until then.
   */
  readonly resnapshot: boolean
}

export const initialMeta: SyncMeta = { cursor: null, pushedThrough: 0, resnapshot: false }

/** The cursor the in-progress page set started from. */
const pullFrom = (meta: SyncMeta): Ids.Cursor => (meta.resnapshot ? null : meta.cursor)

/** A mutation that left the outbox without being applied as intended, retained for inspection (SPEC §9.5). */
export type Notice = Mutation.Notice

type StoreUnavailable = Errors.StoreUnavailable

/**
 * Durable client state. Every method other than `transaction` and `localCommit` must be called inside
 * one of those transaction boundaries. Transactions are serialized, and each one commits or rolls back rows, outbox,
 * seq counter, meta, staging and notices together, from one decision.
 */
export interface SyncStorage {
  readonly transaction: <A, E, R>(self: Effect.Effect<A, E, R>) => Effect.Effect<A, E | StoreUnavailable, R>
  /**
   * Optional enqueue transaction boundary, with the same admission/rollback semantics as `transaction`.
   * Certify only a newly appended optimistic mutation, publishing its interval with COMMIT; duplicates
   * return null. Other engine operations must use `transaction` and cannot produce a local certificate.
   */
  readonly localCommit?: <A, E, R>(
    self: Effect.Effect<A, E, R>
  ) => Effect.Effect<{ readonly value: A; readonly change: LocalChange | null }, E | StoreUnavailable, R>
  readonly rows: Store.ReplicaRowStore
  readonly meta: Effect.Effect<SyncMeta, StoreUnavailable>
  readonly setMeta: (meta: SyncMeta) => Effect.Effect<void, StoreUnavailable>
  /** Unreleased outbox entries in ascending `seq`. */
  readonly pending: Effect.Effect<ReadonlyArray<PendingMutation>, StoreUnavailable>
  /**
   * The outbox entry for a caller-generated `mutationId`, pending or released (INV-CLI-4:
   * release marks the row; released rows are retained for a window, then pruned).
   */
  readonly lookup: (
    mutationId: Ids.MutationId
  ) => Effect.Effect<Option.Option<Mutation.MutationEnvelope>, StoreUnavailable>
  /**
   * Allocates the next outbox `seq` from durable state (INV-CLI-5): starts at 1, monotone,
   * never reused by a committed entry; rolled back with the transaction, so also gap-free.
   */
  readonly nextSeq: Effect.Effect<Ids.Seq, StoreUnavailable>
  readonly appendPending: (entry: PendingMutation) => Effect.Effect<void, StoreUnavailable>
  readonly setUndo: (seq: Ids.Seq, undo: ReadonlyArray<Store.UndoImage>) => Effect.Effect<void, StoreUnavailable>
  /** Marks entries released: gone from `pending`, still found by `lookup` until pruned. */
  readonly release: (seqs: ReadonlyArray<Ids.Seq>) => Effect.Effect<void, StoreUnavailable>
  /**
   * Pages of the in-progress set, in arrival order. Durable: after a reload the engine resumes
   * the set with `meta.cursor` and the last staged page's continuation.
   */
  readonly staged: Effect.Effect<ReadonlyArray<PullPage>, StoreUnavailable>
  /** Durable resume metadata without reading/decoding the accumulated page payloads. */
  readonly stagedProgress: Effect.Effect<
    {
      readonly continuation: Option.Option<Protocol.PullContinuation>
      readonly count: number
    },
    StoreUnavailable
  >
  readonly stage: (page: PullPage) => Effect.Effect<void, StoreUnavailable>
  readonly clearStaged: Effect.Effect<void, StoreUnavailable>
  readonly addNotices: (notices: ReadonlyArray<Notice>) => Effect.Effect<void, StoreUnavailable>
}

/** The network port. Every failure is classified once, by the transport (see `SyncFailure`). */
export interface Transport {
  readonly push: (request: Protocol.PushRequest) => Effect.Effect<Protocol.PushResponse, PushReason>
  readonly pull: (request: Protocol.PullRequest) => Stream.Stream<Protocol.PullResponse, PullReason>
}

/** The handler rejected the mutation against the local replica; nothing was saved. */
export class LocallyRejected extends Schema.TaggedError<LocallyRejected>()("LocallyRejected", {
  name: Schema.String,
  error: Mutation.MutationRejection
}) {}

// ---------------------------------------------------------------------------------------------
// Running a handler (shared with the reference server)
// ---------------------------------------------------------------------------------------------

// The handler runner, role lookup and restore live in core (SPEC §17: the reference server
// and the core harnesses use them without importing client code); re-exported here.
export const roleFromGrants = Store.roleFromGrants
export const claimFromGrants = Store.claimFromGrants
export type RunResult = Mutation.RunResult
export const runHandler = Mutation.runHandler
export const restore = Store.restore

// ---------------------------------------------------------------------------------------------
// Rebase and install
// ---------------------------------------------------------------------------------------------

interface Context {
  readonly storage: SyncStorage
  readonly mutations: Mutation.MutationRegistry
  readonly actor: AccessScope.PrincipalId
}

/** Re-runs `entry` optimistically and records its fresh undo images. */
const replay = (context: Context, entry: PendingMutation) =>
  Effect.gen(function* () {
    const { storage } = context
    const run = yield* runHandler(storage.rows, context.mutations, context.actor, entry.envelope, entry.createdAt)
    // A pending mutation that no longer applies locally has no optimistic effect; it stays
    // queued until the server's verdict or bounded transient exhaustion removes it.
    if (Option.isSome(run.rejection)) {
      yield* restore(storage.rows, run.undo)
      yield* storage.setUndo(entry.envelope.seq, [])
    } else {
      yield* storage.setUndo(entry.envelope.seq, run.undo)
    }
  })

/** The notice a released mutation leaves behind, if any. */
const noticeFor = (
  envelope: Mutation.MutationEnvelope,
  outcome: typeof Mutation.Applied.Type | Omit<Extract<Notice, { readonly _tag: "Rejected" }>, "envelope">
): Option.Option<Notice> => {
  if (outcome._tag === "Applied") return Option.none()
  const rejected = (error: typeof outcome.error): Notice => ({ _tag: "Rejected", envelope, error })
  return Option.some(
    Match.valueTags(outcome.error, {
      UnknownMutation: rejected,
      ArgsDecodeFailed: rejected,
      RowNotFound: rejected,
      RowAlreadyExists: rejected,
      ScopeForbidden: rejected,
      InvalidRow: rejected,
      DomainError: rejected,
      ConstraintViolated: rejected,
      ClockSkew: rejected,
      TransientExhausted: rejected,
      // Expired ledger rows were processed before, but no longer have a known outcome.
      ClientExpired: (): Notice => ({ _tag: "OutcomeUnknown", envelope })
    })
  )
}

/**
 * The rebase every replay-set change goes through, inside the caller's transaction: undo all
 * pending optimistic effects (reverse order), run `install` against the authoritative base,
 * release the entries `settle` says are covered by that base (with their notices), and re-run
 * the rest with fresh undo images.
 */
const rebase = <E>(
  context: Context,
  install: Effect.Effect<void, E | StoreUnavailable>,
  settle: (envelope: Mutation.MutationEnvelope) => Option.Option<Option.Option<Notice>>
) =>
  Effect.gen(function* () {
    const { storage } = context
    const pending = yield* storage.pending
    for (const entry of Arr.reverse(pending)) yield* restore(storage.rows, entry.undo)
    yield* install
    const released: Array<Ids.Seq> = []
    const notices: Array<Notice> = []
    for (const entry of pending) {
      const settled = settle(entry.envelope)
      if (Option.isNone(settled)) {
        yield* replay(context, entry)
        continue
      }
      released.push(entry.envelope.seq)
      if (Option.isSome(settled.value)) notices.push(settled.value.value)
    }
    yield* storage.release(released)
    yield* storage.addNotices(notices)
    yield* Telemetry.annotate({
      "echo.pending": pending.length,
      "echo.replayed": pending.length - released.length,
      "echo.released": released.length,
      "echo.notices_rejected": notices.filter((n) => n._tag === "Rejected").length,
      "echo.notices_unknown": notices.filter((n) => n._tag === "OutcomeUnknown").length
    })
  }).pipe((work) => Telemetry.observe(work, "echo.rebase", Telemetry.serverFailure))

/** The same wire decode gates staging and install; never persist a page that retries must re-fetch. */
const decodeRow = (table: SyncTable.AnySyncTable, row: SyncTable.EncodedRow) =>
  Schema.decodeUnknownEffect(table.wire)(row).pipe(
    Effect.mapError((error) =>
      table.columns.some((column) => !Object.hasOwn(row, column.name))
        ? new ServerOutdated({ table: table.name })
        : new MalformedChange({ table: table.name, message: error.message })
    )
  )

const canonicalRow = Effect.fnUntraced(function* (table: SyncTable.AnySyncTable, wire: SyncTable.EncodedRow) {
  // Protocol's row is a generic JSON record; only the table's codec knows its fields. Store the
  // canonical re-encoding (excess columns dropped, values normalized), as every writer does.
  const malformed = (error: Schema.SchemaError) => new MalformedChange({ table: table.name, message: error.message })
  const decoded = yield* decodeRow(table, wire)
  const row = yield* Schema.encodeEffect(table.wire)(decoded).pipe(Effect.mapError(malformed))
  return { ...row, id: decoded.id }
})

const applyChanges = Effect.fnUntraced(function* (
  storage: SyncStorage,
  registry: Registry.SyncRegistry,
  changes: ReadonlyArray<Protocol.Change>
) {
  for (let i = 0; i < changes.length;) {
    const change = changes[i++]
    if (change === undefined) break
    if (change._tag === "scopePurged") {
      yield* storage.rows.purgeScope(change.scopeId)
      continue
    }
    const table = registry.get(change.table)
    if (Option.isNone(table)) {
      // A later registry update rebuilds and receives these previously unknown rows (§7).
      yield* Telemetry.log("Debug", "echo-sync: unknown table change skipped")
      continue
    }
    if (change._tag === "tombstone") {
      yield* storage.rows.write(table.value, change.id, Option.none())
      continue
    }
    const rows = [yield* canonicalRow(table.value, change.row)]
    // Only consecutive upserts in this page: never reorder across tables, deletes or purges.
    while (true) {
      const next = changes[i]
      if (next?._tag !== "upsert" || next.table !== change.table) break
      rows.push(yield* canonicalRow(table.value, next.row))
      i++
    }
    yield* storage.rows.writeMany(table.value, rows)
  }
})

/**
 * Installs a complete page set: the staged `PullPage`s followed by their `PullDone`. Must run
 * inside one storage transaction. A set that started from cursor `null` is a snapshot of every
 * readable scope, so it replaces the replica instead of patching it (rows of scopes revoked
 * while the client could not see the log would otherwise survive a resnapshot).
 * Exported so tests can build a deliberately wrong page-at-a-time client.
 */
export const applyPageSet = (
  context: Context & { readonly registry: Registry.SyncRegistry },
  pages: ReadonlyArray<PullPage>,
  done: PullDone
) =>
  Effect.gen(function* () {
    const { storage } = context
    const meta = yield* storage.meta
    const set: ReadonlyArray<Protocol.PullResponse> = [...pages, done]
    const install = Effect.gen(function* () {
      if (pullFrom(meta) === null) yield* storage.rows.clearAll
      for (const page of set) yield* applyChanges(storage, context.registry, page.changes)
    })
    const outcomes = new Map(set.flatMap((page) => page.acks.map((ack) => [ack.mutationId, ack.outcome])))
    yield* rebase(context, install, (envelope) => {
      const outcome = outcomes.get(envelope.mutationId)
      if (outcome !== undefined) return Option.some(noticeFor(envelope, outcome))
      // Every seq ≤ the floor was processed (clients push in seq order) and the ledger row
      // that would carry its ack is gone; the installed state already contains its effect.
      if (envelope.seq <= done.expiredThrough) return Option.some(Option.some({ _tag: "OutcomeUnknown", envelope }))
      return Option.none()
    })
    // `pushedThrough` is deliberately not raised here (SPEC §9: it advances only on push responses):
    // a pending entry below a released one was processed, but if its outcome is at a position
    // ≤ the old cursor no pull will ever ack it (a duplicate enqueued after its original was
    // released). It must be resent so the push response's position releases it.
    yield* storage.setMeta({
      ...meta,
      ...(done.epoch === undefined ? {} : { epoch: done.epoch }),
      cursor: done.nextCursor,
      resnapshot: false
    })
    yield* storage.clearStaged
  })

// ---------------------------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------------------------

export interface PushResult {
  /** Number of mutations the server reported as processed in this cycle. */
  readonly processed: number
  /** Any batch stopped transiently, even if exhaustion let this push continue afterward. */
  readonly stopped: boolean
}

export interface Engine {
  /**
   * Appends to the outbox and applies optimistically in one transaction ("saved" on success).
   * `mutationId` is the caller's idempotency key: enqueuing an id the outbox knows (pending or
   * released, INV-CLI-4) returns the original envelope without running the handler again.
   */
  readonly mutate: (
    mutationId: Ids.MutationId,
    name: string,
    args: Schema.Json
  ) => Effect.Effect<
    Mutation.MutationEnvelope & { readonly change?: LocalChange | null },
    StoreUnavailable | LocallyRejected
  >
  /** Sends unprocessed outbox entries in seq order until none remain or the server stops. */
  readonly push: Effect.Effect<PushResult, SyncFailure.PushFailure>
  /** Installs a complete set. `retryPush` asks the loop to deliver intent made eligible by this install. */
  readonly pull: Effect.Effect<void | { readonly retryPush: boolean }, SyncFailure.PullFailure>
}

export const make = (options: {
  readonly clientId: Ids.ClientId
  /**
   * The principal this client acts as; optimistic role checks read its replica grants. Every push and
   * pull asserts it to the server (SPEC §5).
   */
  readonly actor: AccessScope.PrincipalId
  readonly storage: SyncStorage
  /** The client's tables; pull changes for tables it does not know are skipped. */
  readonly registry: Registry.SyncRegistry
  readonly mutations: Mutation.MutationRegistry
  readonly transport: Transport
  /** Mutations per push request (default 50; the protocol caps a request at 100). */
  readonly pushBatchSize?: number
}): Effect.Effect<Engine> =>
  Effect.gen(function* () {
    const { clientId, storage, transport } = options
    const context = { storage, mutations: options.mutations, actor: options.actor, registry: options.registry }
    const clientVersion: Protocol.ClientVersion = {
      dataHash: options.registry.dataHash,
      wireVersion: Protocol.wireVersion
    }
    const batchSize = Math.min(options.pushBatchSize ?? 50, 100)
    const pushLock = yield* Semaphore.make(1)
    const pullLock = yield* Semaphore.make(1)
    // Per-leader only, like the loop's integrity halt. Network failures never touch this map.
    const transientAnswers = new Map<Ids.MutationId, number>()

    // A history fence invalidates positions, including successful-push bookkeeping, but never intent.
    // Ignore an old in-flight failure after another request already changed our timeline.
    const resetEpoch = (epoch: string, expected: string | null | undefined) =>
      storage.transaction(
        Effect.gen(function* () {
          const current = yield* storage.meta
          if ((current.epoch ?? null) !== (expected ?? null)) return
          yield* rebase(context, storage.rows.clearAll, () => Option.none())
          yield* storage.clearStaged
          yield* storage.setMeta({ ...current, epoch, cursor: null, pushedThrough: 0, resnapshot: true })
        })
      )

    const mutate = (mutationId: Ids.MutationId, name: string, args: Schema.Json) =>
      Telemetry.observe(
        (
          storage.localCommit ?? ((self) => Effect.map(storage.transaction(self), (value) => ({ value, change: null })))
        )(
          Effect.gen(function* () {
            const known = yield* storage.lookup(mutationId)
            if (Option.isSome(known)) {
              yield* Telemetry.annotate({ "echo.result": "duplicate" })
              return known.value
            }
            const envelope: Mutation.MutationEnvelope = {
              clientId,
              mutationId,
              seq: yield* storage.nextSeq,
              name,
              args
            }
            const createdAt = yield* Clock.currentTimeMillis
            const run = yield* runHandler(storage.rows, options.mutations, options.actor, envelope, createdAt)
            if (Option.isSome(run.rejection)) {
              return yield* new LocallyRejected({ name, error: run.rejection.value })
            }
            yield* storage.appendPending({ envelope, createdAt, undo: run.undo })
            yield* Telemetry.annotate({ "echo.result": "saved" })
            return envelope
          })
        ).pipe(Effect.map(({ value, change }) => ({ ...value, change }))),
        "echo.enqueue",
        Telemetry.byFailure(failureOutcome)
      )

    const push = pushLock.withPermit(
      Effect.gen(function* () {
        let processed = 0
        let sawTransient = false
        while (true) {
          const { meta, pending } = yield* storage.transaction(
            Effect.all({ meta: storage.meta, pending: storage.pending })
          )
          const pendingIds = new Set(pending.map((entry) => entry.envelope.mutationId))
          for (const id of transientAnswers.keys()) if (!pendingIds.has(id)) transientAnswers.delete(id)
          const batch = pending.filter((entry) => entry.envelope.seq > meta.pushedThrough).slice(0, batchSize)
          if (!Arr.isReadonlyArrayNonEmpty(batch)) return { processed, stopped: sawTransient }
          // Defer until snapshot install, but keep the cycle retrying without an external wake.
          if (meta.resnapshot) return { processed, stopped: true }
          const stopped = yield* Telemetry.observe(
            Effect.gen(function* () {
              yield* Telemetry.annotate({ "echo.batch_size": batch.length })
              yield* Telemetry.histogram("echo.push.batch_size", batch.length)
              const response = yield* transport
                .push({
                  clientId,
                  epoch: meta.epoch ?? null,
                  clientVersion,
                  principal: options.actor,
                  mutations: Arr.map(batch, ({ envelope, createdAt }) => ({
                    mutationId: envelope.mutationId,
                    seq: envelope.seq,
                    createdAt,
                    name: envelope.name,
                    args: envelope.args
                  }))
                })
                .pipe(
                  Effect.catchTag("ResnapshotRequired", (error) =>
                    Effect.gen(function* () {
                      if (error.epoch !== undefined) yield* resetEpoch(error.epoch, meta.epoch)
                      return undefined
                    })
                  )
                )
              // A history refusal is not a usable short response and must never exhaust intent.
              if (response === undefined) {
                yield* Telemetry.annotate({ "echo.result": "stopped" })
                return true
              }
              // `results` is the processed prefix of the request, in request (= seq) order.
              const firstMissing = batch[response.results.length]
              const attempts =
                firstMissing === undefined ? 0 : (transientAnswers.get(firstMissing.envelope.mutationId) ?? 0) + 1
              const exhausted = attempts >= 8
              if (firstMissing !== undefined) sawTransient = true
              const through = batch[response.results.length - 1]?.envelope.seq
              const accepted = yield* storage.transaction(
                Effect.gen(function* () {
                  const current = yield* storage.meta
                  if ((current.epoch ?? null) !== (meta.epoch ?? null) || current.resnapshot) return false
                  // INV-PUSH-6 corollary: an outcome at a position ≤ the installed cursor is already in
                  // the replica, and no later pull will ack it (acks cover (cursor, nextCursor]).
                  // This is how a duplicate enqueued after its original was released terminates.
                  // Second corollary: `Rejected{ClientExpired}` answers an expired identity, which
                  // no pull ever acks, so it is released on receipt whatever its position (after a
                  // rebuild the cursor is null); its effect, if any, arrives with the next pull.
                  const cursor = current.cursor
                  const covered = new Map(
                    response.results
                      .filter(
                        (result) =>
                          (cursor !== null && result.position <= cursor) ||
                          (result.outcome._tag === "Rejected" && result.outcome.error._tag === "ClientExpired")
                      )
                      .map((result) => [result.mutationId, result.outcome])
                  )
                  if (covered.size > 0 || exhausted) {
                    yield* rebase(context, Effect.void, (envelope) => {
                      const outcome = covered.get(envelope.mutationId)
                      if (outcome !== undefined) return Option.some(noticeFor(envelope, outcome))
                      if (exhausted && envelope.mutationId === firstMissing?.envelope.mutationId) {
                        return Option.some(
                          noticeFor(envelope, {
                            _tag: "Rejected",
                            error: new Mutation.TransientExhausted({
                              attempts,
                              message: "the server stopped the push batch at a transient failure"
                            })
                          })
                        )
                      }
                      return Option.none()
                    })
                  }
                  if (through !== undefined) {
                    yield* storage.setMeta({ ...current, pushedThrough: Math.max(current.pushedThrough, through) })
                  }
                  return true
                })
              )
              if (!accepted) return true
              // Publish counts only after bookkeeping commits: a local rollback must not count.
              for (const result of response.results) transientAnswers.delete(result.mutationId)
              if (firstMissing !== undefined) {
                if (exhausted) transientAnswers.delete(firstMissing.envelope.mutationId)
                else transientAnswers.set(firstMissing.envelope.mutationId, attempts)
              }
              yield* Telemetry.annotate({
                "echo.processed": response.results.length,
                "echo.applied": response.results.filter((r) => r.outcome._tag === "Applied").length,
                "echo.rejected": response.results.filter((r) => r.outcome._tag === "Rejected").length,
                "echo.result": response.results.length < batch.length ? "stopped" : "ok"
              })
              processed += response.results.length
              return firstMissing !== undefined && !exhausted
            }),
            "echo.push",
            (cause) => SyncFailure.classifyPush(cause).outcome
          )
          if (stopped) return { processed, stopped: true }
        }
      })
    )

    const pull = pullLock.withPermit(
      Telemetry.observe(
        Effect.gen(function* () {
          let resnapshots = 0
          while (true) {
            const { meta, staged } = yield* storage.transaction(
              Effect.all({ meta: storage.meta, staged: storage.stagedProgress })
            )
            const continuation = staged.continuation
            const request: Protocol.PullRequest = {
              clientId,
              epoch: meta.epoch ?? null,
              clientVersion,
              principal: options.actor,
              cursor: pullFrom(meta),
              continuation
            }
            let receivedPages = 0
            let result: { readonly retryPush: boolean } | undefined
            const completed = yield* transport.pull(request).pipe(
              Stream.takeUntil((response) => response._tag === "PullDone"),
              Stream.runForEach((response) =>
                Effect.gen(function* () {
                  receivedPages++
                  yield* Telemetry.annotate({
                    "echo.pages": staged.count + receivedPages,
                    "echo.resnapshot": pullFrom(meta) === null
                  })
                  if (response._tag === "PullPage") {
                    // A page becomes resumable only after registry validation and durable commit.
                    for (const change of response.changes) {
                      if (change._tag !== "upsert") continue
                      const table = options.registry.get(change.table)
                      if (Option.isSome(table)) yield* decodeRow(table.value, change.row)
                    }
                    yield* storage.transaction(
                      Effect.gen(function* () {
                        const current = yield* storage.meta
                        if ((current.epoch ?? null) === (meta.epoch ?? null)) yield* storage.stage(response)
                      })
                    )
                    return
                  }
                  const installedSet = yield* Telemetry.observe(
                    storage.transaction(
                      Effect.gen(function* () {
                        const set = yield* storage.staged
                        const current = yield* storage.meta
                        if ((current.epoch ?? null) !== (meta.epoch ?? null)) return undefined
                        yield* applyPageSet(context, set, response)
                        const installed = yield* storage.meta
                        const retryPush = (yield* storage.pending).some(
                          (entry) => entry.envelope.seq > installed.pushedThrough
                        )
                        return { retryPush, set }
                      })
                    ),
                    "echo.install",
                    (cause) => SyncFailure.classifyPull(cause).outcome
                  )
                  if (installedSet === undefined) return
                  for (const installed of [...installedSet.set, response]) {
                    for (const ack of installed.acks) transientAnswers.delete(ack.mutationId)
                  }
                  yield* Telemetry.histogram("echo.pull.pages", installedSet.set.length + 1)
                  yield* Telemetry.annotate({
                    "echo.pages": installedSet.set.length + 1,
                    "echo.result": resnapshots > 0 ? "resnapshot" : "ok"
                  })
                  result = { retryPush: installedSet.retryPush }
                })
              ),
              Effect.as(true),
              Effect.catchTag("ResnapshotRequired", (error) =>
                Effect.gen(function* () {
                  if (error.epoch !== undefined) {
                    yield* resetEpoch(error.epoch, meta.epoch)
                    resnapshots++
                    return false
                  }
                  if (request.cursor === null && Option.isNone(continuation) && receivedPages === 0) return yield* error
                  // Never apply a partial set: drop it and start a snapshot from null. The
                  // replica (and its cursor) keeps serving reads until that snapshot is installed.
                  // Re-read meta: the request was built before the network wait, and push responses
                  // may have advanced `pushedThrough` meanwhile; a stale write would resend them.
                  yield* storage.transaction(
                    Effect.gen(function* () {
                      const current = yield* storage.meta
                      yield* storage.clearStaged
                      yield* storage.setMeta({ ...current, resnapshot: true })
                    })
                  )
                  resnapshots++
                  yield* Telemetry.annotate({ "echo.resnapshot_count": resnapshots })
                  return false
                })
              )
            )
            if (!completed) continue
            if (result !== undefined) return result
            const current = yield* storage.transaction(storage.meta)
            if ((current.epoch ?? null) !== (meta.epoch ?? null)) continue
            // A clean RPC end alone cannot certify a complete page set.
            return yield* new SyncFailure.PullProtocolError({ message: "pull stream ended before PullDone" })
          }
        }),
        "echo.pull",
        (cause) => SyncFailure.classifyPull(cause).outcome
      )
    )

    return { mutate, push: Telemetry.boundary(push), pull }
  })
