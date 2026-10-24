/**
 * Reference sync server for the simulation (SPEC §4–§7), no Postgres.
 *
 * - One store of current rows; a log of change identities (position, table, id, scope) and a
 *   per-client ledger, sharing one commit-ordered position counter (SPEC §6).
 * - Readability is computed from `scope_grants` rows (grants are a system SyncTable, SPEC §4).
 * - Pull pages are served at their own snapshot: payloads are the row state at the moment the
 *   page is served, not when the entry was logged (SPEC §7). Membership is a state diff against the
 *   per-client held-scope record (`client_scopes`); a null cursor holds nothing, so it walks
 *   every readable scope and then continues with the log.
 * - Log and ledger are independently GC-able; ledger GC clears an entry's outcome but keeps its
 *   identity, and keeps a per-client floor, so resubmissions (under any seq) are answered
 *   `ClientExpired` instead of re-executed (DECISIONS 5).
 * - Mutations run through the same `Mutation.runHandler` (core `Store.make` + `Mutation.run`)
 *   the client runs optimistically, against the server's rows and grants.
 *
 * `history` (state after every commit) and `executions` are observer-only, for invariants.
 */
import { Array as Arr, Effect, Option, Order } from "effect"
import { AccessScope, Ids, Mutation, Protocol, ScopeGrants, Store } from "../index.ts"
import type { Errors, SyncTable } from "../index.ts"
import * as MemoryStore from "./MemoryStore.ts"
import { mutations, serverRegistry } from "./SimApp.ts"

type Position = bigint
type Cursor = Position | null
type EncodedRow = SyncTable.EncodedRow
type MutationOutcome = Mutation.MutationOutcome

const scopeOf = (row: EncodedRow) => String(row["access_scope_id"])
const asPosition = (p: Position) => Ids.Position.make(p)
/** The in-memory store has neither SQL constraints nor transient failures. */
const inMemory = <A, E, R>(self: Effect.Effect<A, E | Errors.StoreUnavailable | Errors.ConstraintViolated, R>) =>
  Effect.catchTag(self, ["StoreUnavailable", "ConstraintViolated"], (error) => Effect.die(error))

interface LogEntry {
  readonly position: Position
  readonly table: SyncTable.AnySyncTable
  readonly id: string
  readonly scope: string
}

interface LedgerEntry {
  readonly position: Position
  readonly seq: number
  /** `null` once expired: the identity stays, the outcome is reclaimed (DECISIONS 5). */
  readonly outcome: MutationOutcome | null
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
  readonly table: SyncTable.AnySyncTable
  readonly id: string
  readonly row: Option.Option<EncodedRow>
}

const rowKey = (table: string, id: string) => `${table}\u0000${id}`

export const make = (options: { readonly pageSize: number }) => {
  const store = MemoryStore.make()
  let head: Position = 0n
  let logFloor: Position = 0n
  let log: Array<LogEntry> = []
  const ledger = new Map<string, Map<Ids.MutationId, LedgerEntry>>()
  const principals = new Map<string, AccessScope.PrincipalId>()
  /** `client_scopes`: scopes held by a client as of each completed pull, by `nextCursor`. */
  const clientScopes = new Map<string, Map<Position, ReadonlySet<string>>>()
  /** Highest `seq` per client whose ledger row was GC'd (DECISIONS 5 floor). */
  const ledgerFloor = new Map<string, { readonly seq: number; readonly position: Position }>()
  const continuations = new Map<string, Continuation>()
  const history = new Map<Position, MemoryStore.Tables>([[0n, new Map()]])
  const executions = new Map<string, number>()
  /** `access_scopes`: every scope ever created or granted in; kept when its grants go (SPEC §4). */
  const accessScopes = new Set<string>()

  const ledgerOf = (clientId: string) => {
    const existing = ledger.get(clientId)
    if (existing !== undefined) return existing
    const created = new Map<Ids.MutationId, LedgerEntry>()
    ledger.set(clientId, created)
    return created
  }

  /** Allocates log positions for a committed transaction's writes (before → current state). */
  const logWrites = (
    writes: ReadonlyArray<{ table: SyncTable.AnySyncTable; id: string; before: Option.Option<EncodedRow> }>
  ) =>
    Effect.gen(function* () {
      for (const { table, id, before } of writes) {
        const after = yield* store.read(table, id)
        const scopes = new Set<string>()
        if (Option.isSome(before)) scopes.add(scopeOf(before.value))
        if (Option.isSome(after)) scopes.add(scopeOf(after.value))
        for (const scope of scopes) {
          head += 1n
          log.push({ position: head, table, id, scope })
        }
      }
    })

  const commit = (position: Position) => history.set(position, MemoryStore.cloneTables(store.tables()))

  /** Out-of-band writer (admin, import, script): logged like any writer, no ledger. */
  const writeDirect = (writes: ReadonlyArray<DirectWrite>): Effect.Effect<void> =>
    inMemory(
      store.transaction(
        Effect.gen(function* () {
          const images = []
          for (const write of writes) {
            images.push({ table: write.table, id: write.id, before: yield* store.read(write.table, write.id) })
            yield* store.write(write.table, write.id, write.row)
            // A grant needs its scope's access_scopes row (the Postgres FK).
            if (write.table === ScopeGrants.ScopeGrants && Option.isSome(write.row)) {
              accessScopes.add(scopeOf(write.row.value))
            }
          }
          yield* logWrites(images)
          commit(head)
        })
      )
    )

  const execute = (envelope: Mutation.MutationEnvelope, now: number) =>
    store.transaction(
      Effect.gen(function* () {
        const principal = principals.get(envelope.clientId) ?? AccessScope.PrincipalId.make("nobody")
        // The authoritative claim (SPEC §4): fresh iff no access_scopes row; it is added only
        // if the mutation applies, like the Postgres insert that rolls back with the savepoint.
        const claimed = new Set<string>()
        const claimScope = (scopeId: AccessScope.AccessScopeId) =>
          Effect.sync(() => {
            if (accessScopes.has(scopeId) || claimed.has(scopeId)) return false
            claimed.add(scopeId)
            return true
          })
        // The in-memory store has no transient failures.
        const run = yield* Effect.orDie(Mutation.runHandler(store, mutations, principal, envelope, now, claimScope))
        let outcome: MutationOutcome
        if (Option.isSome(run.rejection)) {
          yield* Store.restore(store, run.undo)
          outcome = { _tag: "Rejected", error: run.rejection.value }
        } else {
          yield* logWrites(run.undo)
          for (const scope of claimed) accessScopes.add(scope)
          outcome = { _tag: "Applied" }
        }
        head += 1n
        ledgerOf(envelope.clientId).set(envelope.mutationId, { position: head, seq: envelope.seq, outcome })
        const key = `${envelope.clientId}#${envelope.mutationId}`
        executions.set(key, (executions.get(key) ?? 0) + 1)
        commit(head)
        return { outcome, position: asPosition(head) }
      })
    )

  /** Processes in order; `stopAt` injects a transient failure before that index. */
  const push = (request: Protocol.PushRequest, stopAt: Option.Option<number>): Effect.Effect<Protocol.PushResponse> =>
    inMemory(
      Effect.gen(function* () {
        const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis)
        const results: Array<Protocol.PushResult> = []
        for (const [index, item] of request.mutations.entries()) {
          if (Option.isSome(stopAt) && index === stopAt.value) {
            // Transient stop (e.g. serialization failure): only the processed prefix is returned.
            return { results }
          }
          const recorded = ledgerOf(request.clientId).get(item.mutationId)
          const floor = ledgerFloor.get(request.clientId)
          // Clients push in seq order and every processed mutation is in the ledger, so a seq at
          // or below the floor was processed and its row is gone: answer terminally, never re-execute.
          const expired: MutationOutcome = {
            _tag: "Rejected",
            error: new Mutation.ClientExpired({ mutationId: item.mutationId })
          }
          const result: { outcome: MutationOutcome; position: Ids.Position } =
            (recorded && { outcome: recorded.outcome ?? expired, position: asPosition(recorded.position) }) ??
            (floor !== undefined && item.seq <= floor.seq
              ? { outcome: expired, position: asPosition(floor.position) }
              : yield* execute({ clientId: request.clientId, ...item }, now))
          results.push({ mutationId: item.mutationId, ...result })
        }
        const response: Protocol.PushResponse = { results }
        return response
      })
    )

  /**
   * One page, read at the current state (= this page's snapshot). Membership is a state diff
   * (SPEC §7): held-but-unreadable ⇒ `ScopePurged`; readable-but-not-held ⇒ row walk;
   * walks run before any log read; log entries in held scopes become upserts if the row's
   * current scope is readable, else tombstones.
   */
  const pull = (request: Protocol.PullRequest): Effect.Effect<Protocol.PullResponse, Protocol.ResnapshotRequired> =>
    inMemory(
      Effect.gen(function* () {
        const clientId = request.clientId
        const principal = principals.get(clientId)
        let state: Continuation
        if (Option.isSome(request.continuation)) {
          const found = continuations.get(request.continuation.value)
          if (found === undefined) return yield* new Protocol.ResnapshotRequired({ reason: "staleContinuation" })
          state = found
        } else if (request.cursor === null) {
          state = { start: null, through: head, held: new Set(), walks: new Map() }
        } else {
          const held = clientScopes.get(clientId)?.get(request.cursor)
          if (held === undefined) return yield* new Protocol.ResnapshotRequired({ reason: "unknownCursor" })
          state = { start: request.cursor, through: request.cursor, held, walks: new Map() }
        }
        if (state.through < logFloor) return yield* new Protocol.ResnapshotRequired({ reason: "gcHorizon" })

        const readable = new Set<string>()
        for (const row of store.tables().get(ScopeGrants.ScopeGrants.name)?.values() ?? []) {
          const who = row["principal_id"]
          if (who === principal || who === AccessScope.Everyone) readable.add(scopeOf(row))
        }
        const changes: Array<Protocol.Change> = []
        const held = new Set(state.held)
        const walks = new Map(state.walks)
        for (const scope of state.held) {
          if (readable.has(scope)) continue
          changes.push({ _tag: "scopePurged", scopeId: AccessScope.AccessScopeId.make(scope) })
          held.delete(scope)
          walks.delete(scope)
        }
        for (const scope of readable) {
          if (held.has(scope)) continue
          held.add(scope)
          walks.set(scope, Option.none())
        }
        const issue = (next: Continuation): Protocol.PullResponse => {
          const token = `k${continuations.size}`
          continuations.set(token, next)
          return { _tag: "PullPage", changes, acks: [], continuation: Protocol.PullContinuation.make(token) }
        }

        if (walks.size > 0) {
          const rows: Array<{ key: string; scope: string; table: string; row: EncodedRow }> = []
          for (const [table, tableRows] of store.tables()) {
            for (const row of tableRows.values()) {
              const after = walks.get(scopeOf(row))
              if (after === undefined) continue
              const key = rowKey(table, String(row["id"]))
              if (Option.isSome(after) && Store.compareText(key, after.value) <= 0) continue
              rows.push({ key, scope: scopeOf(row), table, row })
            }
          }
          const ordered = Arr.sort(
            rows,
            Order.mapInput(
              Order.make((a: string, b: string) => (Store.compareText(a, b) < 0 ? -1 : 1)),
              (r: (typeof rows)[number]) => r.key
            )
          )
          for (const { key, scope, table, row } of ordered.slice(0, options.pageSize)) {
            changes.push({ _tag: "upsert", table, row })
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
          if (Option.isSome(row) && readable.has(scopeOf(row.value))) {
            changes.push({ _tag: "upsert", table: entry.table.name, row: row.value })
          } else {
            changes.push({ _tag: "tombstone", table: entry.table.name, id: entry.id })
          }
        }
        const last = page.at(-1)
        if (entries.length > options.pageSize && last !== undefined) {
          return issue({ ...state, through: last.position, held, walks })
        }

        const acks: Array<Protocol.Ack> = []
        for (const [mutationId, entry] of ledgerOf(clientId)) {
          if (entry.outcome === null) continue
          if ((state.start === null || entry.position > state.start) && entry.position <= head) {
            acks.push({ mutationId, position: asPosition(entry.position), outcome: entry.outcome })
          }
        }
        const records = clientScopes.get(clientId) ?? new Map<Position, ReadonlySet<string>>()
        if (!records.has(head)) records.set(head, held)
        for (const position of records.keys())
          if (state.start !== null && position < state.start) records.delete(position)
        clientScopes.set(clientId, records)
        const done: Protocol.PullResponse = {
          _tag: "PullDone",
          changes,
          acks,
          nextCursor: asPosition(head),
          expiredThrough: Ids.SeqFloor.make(ledgerFloor.get(clientId)?.seq ?? 0)
        }
        return done
      })
    )

  return {
    store,
    history,
    executions,
    head: () => head,
    /** The entry while it still holds its outcome; none once expired (or never processed). */
    ledgerEntry: (clientId: string, mutationId: Ids.MutationId) => {
      const entry = ledger.get(clientId)?.get(mutationId)
      return entry === undefined || entry.outcome === null
        ? Option.none()
        : Option.some({ position: entry.position, seq: entry.seq, outcome: entry.outcome })
    },
    register: (clientId: string, principal: AccessScope.PrincipalId) => principals.set(clientId, principal),
    /** The server's table object for a sync name. */
    table: (name: string) => Option.getOrThrow(serverRegistry.get(name)),
    writeDirect,
    push,
    pull,
    /** Drops log entries with position ≤ `through`; older cursors must resnapshot. */
    gcLog: (through: Position) => {
      log = log.filter((entry) => entry.position > through)
      if (through > logFloor) logFloor = through
    },
    /** Expires ledger entries with position ≤ `through`: outcome cleared, identity kept (DECISIONS 5). */
    gcLedger: (through: Position) => {
      for (const [clientId, entries] of ledger) {
        for (const [id, entry] of entries) {
          if (entry.position > through || entry.outcome === null) continue
          entries.set(id, { ...entry, outcome: null })
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
