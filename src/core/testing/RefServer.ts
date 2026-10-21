/**
 * Reference sync server for the simulation (DESIGN §4, §6–§8), no Postgres.
 *
 * - One store of current rows; a log of change identities (position, table, id, scope) and a
 *   per-client ledger, sharing one commit-ordered position counter (§7).
 * - Readability is computed from `scope_grants` rows (grants are a system SyncTable, §4).
 * - Pull pages are served at their own snapshot: payloads are the row state at the moment the
 *   page is served, not when the entry was logged (§8). A null cursor walks current rows of
 *   readable scopes by (table, id), then continues with the log from the walk's start.
 * - A log entry for a grant concerning the puller emits `ScopePurged` and, if the scope is
 *   readable at that page's snapshot, a walk of the scope's current rows.
 * - Log and ledger are independently GC-able so retention policies can be tested.
 *
 * `history` (state after every commit) and `executions` are observer-only, for invariants.
 */
import { Effect, Option } from "effect"
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
import { ANYONE, GRANTS, restore, roleOf, runMutation } from "./SimApp.ts"

interface LogEntry {
  readonly position: Position
  readonly table: string
  readonly id: string
  readonly scope: string
}

interface LedgerEntry {
  readonly position: Position
  readonly outcome: MutationOutcome
}

type Continuation =
  | { readonly _tag: "Walk"; readonly head: Position; readonly after: Option.Option<string> }
  | { readonly _tag: "Log"; readonly through: Position }

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
  const ledger = new Map<string, Map<number, LedgerEntry>>()
  const principals = new Map<string, string>()
  const continuations = new Map<string, Continuation>()
  const history = new Map<Position, MemoryStore.Tables>([[0n, new Map()]])
  const executions = new Map<string, number>()

  const ledgerOf = (clientId: string) => {
    const existing = ledger.get(clientId)
    if (existing !== undefined) return existing
    const created = new Map<number, LedgerEntry>()
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
        ledgerOf(envelope.clientId).set(envelope.mutationId, { position: head, outcome })
        const key = `${envelope.clientId}#${envelope.mutationId}`
        executions.set(key, (executions.get(key) ?? 0) + 1)
        commit(head)
        return outcome
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
        const outcome = recorded?.outcome ?? (yield* execute({ clientId: request.clientId, ...item }, now))
        results.push({ mutationId: item.mutationId, outcome })
      }
      const response: PushResponse = { results, stopped: Option.none() }
      return response
    })

  const pull = (request: PullRequest): Effect.Effect<PullResponse, ResnapshotRequired> =>
    Effect.gen(function* () {
      const principal = principals.get(request.clientId) ?? ""
      const readable = (scope: string) => Effect.map(roleOf(store, principal, scope), Option.isSome)
      const changes: Array<Change> = []
      const acks: Array<Ack> = []
      const ackRange = (from: Cursor, through: Position) => {
        for (const [mutationId, entry] of ledgerOf(request.clientId)) {
          if ((from === null || entry.position > from) && entry.position <= through) {
            acks.push({ mutationId, position: entry.position, outcome: entry.outcome })
          }
        }
      }
      const walkScope = (scope: string) => {
        for (const [table, rows] of store.tables()) {
          for (const row of rows.values())
            if (row.access_scope_id === scope) changes.push({ _tag: "Upsert", table, row })
        }
      }
      const issue = (continuation: Continuation): PullResponse => {
        const token = `c${continuations.size}`
        continuations.set(token, continuation)
        return { _tag: "PullPage", changes, acks, continuation: token }
      }

      let state: Continuation
      if (Option.isSome(request.continuation)) {
        const found = continuations.get(request.continuation.value)
        if (found === undefined) return yield* new ResnapshotRequired()
        state = found
      } else if (request.cursor === null) {
        state = { _tag: "Walk", head, after: Option.none() }
        ackRange(null, head)
      } else {
        state = { _tag: "Log", through: request.cursor }
      }
      const resumeFrom = state._tag === "Walk" ? state.head : state.through
      if (resumeFrom < logFloor) return yield* new ResnapshotRequired()

      if (state._tag === "Walk") {
        const all: Array<{ key: string; table: string; row: EncodedRow }> = []
        for (const [table, rows] of store.tables()) {
          for (const row of rows.values()) {
            const key = rowKey(table, row.id)
            if (Option.isSome(state.after) && MemoryStore.compareValues(key, state.after.value) <= 0) continue
            if (yield* readable(row.access_scope_id)) all.push({ key, table, row })
          }
        }
        all.sort((a, b) => MemoryStore.compareValues(a.key, b.key))
        const page = all.slice(0, options.pageSize)
        for (const { table, row } of page) changes.push({ _tag: "Upsert", table, row })
        const last = page.at(-1)
        if (all.length > options.pageSize && last !== undefined) {
          return issue({ _tag: "Walk", head: state.head, after: Option.some(last.key) })
        }
        state = { _tag: "Log", through: state.head }
      }

      const from = state.through
      const pending = log.filter((entry) => entry.position > from)
      const page = pending.slice(0, options.pageSize)
      for (const entry of page) {
        const [scope, who] = entry.table === GRANTS ? entry.id.split("|") : []
        if (scope !== undefined && (who === principal || who === ANYONE)) {
          changes.push({ _tag: "ScopePurged", scopeId: scope })
          if (yield* readable(scope)) walkScope(scope)
          continue
        }
        if (!(yield* readable(entry.scope))) continue
        const row = yield* store.read(entry.table, entry.id)
        if (Option.isSome(row) && (yield* readable(row.value.access_scope_id))) {
          changes.push({ _tag: "Upsert", table: entry.table, row: row.value })
        } else {
          changes.push({ _tag: "Tombstone", table: entry.table, id: entry.id })
        }
      }
      const last = page.at(-1)
      if (pending.length > options.pageSize && last !== undefined) {
        ackRange(from, last.position)
        return issue({ _tag: "Log", through: last.position })
      }
      ackRange(from, head)
      const done: PullResponse = { _tag: "PullDone", changes, acks, nextCursor: head }
      return done
    })

  return {
    store,
    history,
    executions,
    head: () => head,
    ledgerEntry: (clientId: string, mutationId: number) => Option.fromNullishOr(ledger.get(clientId)?.get(mutationId)),
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
      for (const entries of ledger.values()) {
        for (const [id, entry] of entries) if (entry.position <= through) entries.delete(id)
      }
    }
  }
}

export type RefServer = ReturnType<typeof make>
