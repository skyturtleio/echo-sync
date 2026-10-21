/**
 * Reference sync server for the simulation (DESIGN §4, §6–§8), no Postgres.
 *
 * - One store of current rows; a log of change identities (position, table, id, scope) and a
 *   per-client ledger, sharing one commit-ordered position counter (§7).
 * - Readability is computed from `scope_grants` rows (grants are a system SyncTable, §4).
 * - Pull pages are served at their own snapshot: payloads are the row state at the moment the
 *   page is served, not when the entry was logged (§8). Membership is a state diff against the
 *   per-client held-scope record (`client_scopes`); a null cursor holds nothing, so it walks
 *   every readable scope and then continues with the log.
 * - Log and ledger are independently GC-able; ledger GC keeps a per-client floor so
 *   resubmissions are answered `ClientExpired` instead of re-executed (§16.5, planned).
 *
 * `history` (state after every commit) and `executions` are observer-only, for invariants.
 */
import { Array as Arr, Effect, Option, Order } from "effect"
import type {
  Ack,
  Change,
  Cursor,
  EncodedRow,
  MutationEnvelope,
  MutationOutcome,
  Position,
  PullRequest,
  PullResponse,
  PushRequest,
  PushResponse
} from "../../client/sync/Wire.ts"
import { ResnapshotRequired } from "../../client/sync/Wire.ts"
import * as MemoryStore from "./MemoryStore.ts"
import { ANYONE, GRANTS, restore, runMutation } from "./SimApp.ts"

interface LogEntry {
  readonly position: Position
  readonly table: string
  readonly id: string
  readonly scope: string
}

interface LedgerEntry {
  readonly position: Position
  readonly seq: number
  readonly outcome: MutationOutcome
}

/** Server-side state of an in-progress page set (the client only sees an opaque token). */
interface Continuation {
  /** Cursor the set started from: acks are delivered for `(start, nextCursor]`. */
  readonly start: Cursor
  /** Log entries `≤ through` are covered; the next log read starts after it. */
  readonly through: Position
  /** Scopes the client will hold once the set is installed (walked or walking). */
  readonly held: ReadonlySet<string>
  /** Row walks in progress, keyed by scope; value = last delivered `(table, id)` key. */
  readonly walks: ReadonlyMap<string, Option.Option<string>>
}

export interface DirectWrite {
  readonly table: string
  readonly id: string
  readonly row: Option.Option<EncodedRow>
}

const rowKey = (table: string, id: string) => `${table}\u0000${id}`

export const make = (options: { readonly pageSize: number }) => {
  const store = MemoryStore.make()
  let head: Position = 0n
  let logFloor: Position = 0n
  let log: Array<LogEntry> = []
  const ledger = new Map<string, Map<string, LedgerEntry>>()
  const principals = new Map<string, string>()
  /** `client_scopes`: scopes held by a client as of each completed pull, by `nextCursor`. */
  const clientScopes = new Map<string, Map<Position, ReadonlySet<string>>>()
  /** Highest `seq` per client whose ledger row was GC'd (DESIGN §16.5 floor). */
  const ledgerFloor = new Map<string, { readonly seq: number; readonly position: Position }>()
  const continuations = new Map<string, Continuation>()
  const history = new Map<Position, MemoryStore.Tables>([[0n, new Map()]])
  const executions = new Map<string, number>()

  const ledgerOf = (clientId: string) => {
    const existing = ledger.get(clientId)
    if (existing !== undefined) return existing
    const created = new Map<string, LedgerEntry>()
    ledger.set(clientId, created)
    return created
  }

  /** Allocates log positions for a committed transaction's writes (before → current state). */
  const logWrites = (writes: ReadonlyArray<{ table: string; id: string; before: Option.Option<EncodedRow> }>) =>
    Effect.gen(function* () {
      for (const { table, id, before } of writes) {
        const after = yield* store.read(table, id)
        const scopes = new Set<string>()
        if (Option.isSome(before)) scopes.add(before.value.access_scope_id)
        if (Option.isSome(after)) scopes.add(after.value.access_scope_id)
        for (const scope of scopes) {
          head += 1n
          log.push({ position: head, table, id, scope })
        }
      }
    })

  const commit = (position: Position) => history.set(position, MemoryStore.cloneTables(store.tables()))

  /** Out-of-band writer (admin, import, script): logged like any writer, no ledger. */
  const writeDirect = (writes: ReadonlyArray<DirectWrite>) =>
    store.transaction(
      Effect.gen(function* () {
        const images = []
        for (const write of writes) {
          images.push({ table: write.table, id: write.id, before: yield* store.read(write.table, write.id) })
          yield* store.write(write.table, write.id, write.row)
        }
        yield* logWrites(images)
        commit(head)
      })
    )

  const execute = (envelope: MutationEnvelope, now: number) =>
    store.transaction(
      Effect.gen(function* () {
        const principal = principals.get(envelope.clientId) ?? ""
        const run = yield* runMutation(store, principal, envelope, now)
        let outcome: MutationOutcome
        if (Option.isSome(run.rejection)) {
          yield* restore(store, run.undo)
          outcome = { _tag: "Rejected", error: run.rejection.value }
        } else {
          yield* logWrites(run.undo)
          outcome = { _tag: "Applied" }
        }
        head += 1n
        ledgerOf(envelope.clientId).set(envelope.mutationId, { position: head, seq: envelope.seq, outcome })
        const key = `${envelope.clientId}#${envelope.mutationId}`
        executions.set(key, (executions.get(key) ?? 0) + 1)
        commit(head)
        return { outcome, position: head }
      })
    )

  /** Processes in order; `stopAt` injects a transient failure before that index. */
  const push = (request: PushRequest, stopAt: Option.Option<number>) =>
    Effect.gen(function* () {
      const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis)
      const results: Array<PushResponse["results"][number]> = []
      for (const [index, item] of request.mutations.entries()) {
        if (Option.isSome(stopAt) && index === stopAt.value) {
          return { results, stopped: Option.some({ reason: "SerializationFailure" }) }
        }
        const recorded = ledgerOf(request.clientId).get(item.mutationId)
        const floor = ledgerFloor.get(request.clientId)
        // Clients push in seq order and every processed mutation is in the ledger, so a seq at
        // or below the floor was processed and its row is gone: answer terminally, never re-execute.
        const result: { outcome: MutationOutcome; position: Position } =
          recorded ??
          (floor !== undefined && item.seq <= floor.seq
            ? {
                outcome: { _tag: "Rejected", error: { _tag: "ClientExpired", mutationId: item.mutationId } },
                position: floor.position
              }
            : yield* execute({ clientId: request.clientId, ...item }, now))
        results.push({ mutationId: item.mutationId, ...result })
      }
      const response: PushResponse = { results, stopped: Option.none() }
      return response
    })

  /**
   * One page, read at the current state (= this page's snapshot). Membership is a state diff
   * (DESIGN §8): held-but-unreadable ⇒ `ScopePurged`; readable-but-not-held ⇒ row walk;
   * walks run before any log read; log entries in held scopes become upserts if the row's
   * current scope is readable, else tombstones.
   */
  const pull = (request: PullRequest): Effect.Effect<PullResponse, ResnapshotRequired> =>
    Effect.gen(function* () {
      const clientId = request.clientId
      const principal = principals.get(clientId) ?? ""
      let state: Continuation
      if (Option.isSome(request.continuation)) {
        const found = continuations.get(request.continuation.value)
        if (found === undefined) return yield* new ResnapshotRequired({ reason: "staleContinuation" })
        state = found
      } else if (request.cursor === null) {
        state = { start: null, through: head, held: new Set(), walks: new Map() }
      } else {
        const held = clientScopes.get(clientId)?.get(request.cursor)
        if (held === undefined) return yield* new ResnapshotRequired({ reason: "unknownCursor" })
        state = { start: request.cursor, through: request.cursor, held, walks: new Map() }
      }
      if (state.through < logFloor) return yield* new ResnapshotRequired({ reason: "gcHorizon" })

      const readable = new Set<string>()
      for (const row of store.tables().get(GRANTS)?.values() ?? []) {
        if (row.principal_id === principal || row.principal_id === ANYONE) readable.add(row.access_scope_id)
      }
      const changes: Array<Change> = []
      const held = new Set(state.held)
      const walks = new Map(state.walks)
      for (const scope of state.held) {
        if (readable.has(scope)) continue
        changes.push({ _tag: "ScopePurged", scopeId: scope })
        held.delete(scope)
        walks.delete(scope)
      }
      for (const scope of readable) {
        if (held.has(scope)) continue
        held.add(scope)
        walks.set(scope, Option.none())
      }
      const issue = (next: Continuation): PullResponse => {
        const token = `k${continuations.size}`
        continuations.set(token, next)
        return { _tag: "PullPage", changes, acks: [], continuation: token }
      }

      if (walks.size > 0) {
        const rows: Array<{ key: string; scope: string; table: string; row: EncodedRow }> = []
        for (const [table, tableRows] of store.tables()) {
          for (const row of tableRows.values()) {
            const after = walks.get(row.access_scope_id)
            if (after === undefined) continue
            const key = rowKey(table, row.id)
            if (Option.isSome(after) && MemoryStore.compareValues(key, after.value) <= 0) continue
            rows.push({ key, scope: row.access_scope_id, table, row })
          }
        }
        const ordered = Arr.sort(
          rows,
          Order.mapInput(Order.make(MemoryStore.compareValues), (r: (typeof rows)[number]) => r.key)
        )
        for (const { key, scope, table, row } of ordered.slice(0, options.pageSize)) {
          changes.push({ _tag: "Upsert", table, row })
          walks.set(scope, Option.some(key))
        }
        // Scopes with nothing left after this page are complete.
        const remaining = new Set(ordered.slice(options.pageSize).map((r) => r.scope))
        for (const scope of walks.keys()) if (!remaining.has(scope)) walks.delete(scope)
        return issue({ ...state, held, walks })
      }

      const entries = log.filter((entry) => entry.position > state.through && held.has(entry.scope))
      const page = entries.slice(0, options.pageSize)
      for (const entry of page) {
        const row = yield* store.read(entry.table, entry.id)
        if (Option.isSome(row) && readable.has(row.value.access_scope_id)) {
          changes.push({ _tag: "Upsert", table: entry.table, row: row.value })
        } else {
          changes.push({ _tag: "Tombstone", table: entry.table, id: entry.id })
        }
      }
      const last = page.at(-1)
      if (entries.length > options.pageSize && last !== undefined) {
        return issue({ ...state, through: last.position, held, walks })
      }

      const acks: Array<Ack> = []
      for (const [mutationId, entry] of ledgerOf(clientId)) {
        if ((state.start === null || entry.position > state.start) && entry.position <= head) {
          acks.push({ mutationId, position: entry.position, outcome: entry.outcome })
        }
      }
      const records = clientScopes.get(clientId) ?? new Map<Position, ReadonlySet<string>>()
      if (!records.has(head)) records.set(head, held)
      for (const position of records.keys())
        if (state.start !== null && position < state.start) records.delete(position)
      clientScopes.set(clientId, records)
      const done: PullResponse = {
        _tag: "PullDone",
        changes,
        acks,
        nextCursor: head,
        expiredThrough: ledgerFloor.get(clientId)?.seq ?? 0
      }
      return done
    })

  return {
    store,
    history,
    executions,
    head: () => head,
    ledgerEntry: (clientId: string, mutationId: string) => Option.fromNullishOr(ledger.get(clientId)?.get(mutationId)),
    register: (clientId: string, principal: string) => principals.set(clientId, principal),
    writeDirect,
    push,
    pull,
    /** Drops log entries with position ≤ `through`; older cursors must resnapshot. */
    gcLog: (through: Position) => {
      log = log.filter((entry) => entry.position > through)
      if (through > logFloor) logFloor = through
    },
    /** Drops ledger entries with position ≤ `through`. */
    gcLedger: (through: Position) => {
      for (const [clientId, entries] of ledger) {
        for (const [id, entry] of entries) {
          if (entry.position > through) continue
          entries.delete(id)
          const floor = ledgerFloor.get(clientId)
          ledgerFloor.set(clientId, {
            seq: Math.max(floor?.seq ?? 0, entry.seq),
            position: floor !== undefined && floor.position > entry.position ? floor.position : entry.position
          })
        }
      }
    }
  }
}

export type RefServer = ReturnType<typeof make>
