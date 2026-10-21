import { Array as Arr, Effect, Option, Order, Schema } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { MutationOutcome, registerClient } from "./ledger.ts"
import { type SyncedTable, syncedTables } from "./schema.ts"

/** Keyset position of a scope row walk: rows are walked in `(table, id)` code-point order. */
const RowKey = Schema.Struct({ table: Schema.String, rowId: Schema.String })

/**
 * Server-side progress of an unfinished page set (DESIGN.md §8). Private to the server: on the
 * wire it is an opaque string (core's branded `PullContinuation`), the JSON encoding of this
 * struct, which the client resends verbatim.
 *
 * Invariant: the pages so far deliver every log change with position ≤ `position` in every scope
 * of `scopes`, plus the current rows of the first entry of `walking` up to its `after`. The log is
 * not read past `position` while a walk is outstanding.
 *
 * It is untrusted input. Forging it can only hurt the forger: payloads are always filtered by the
 * request principal's current grants, and identity fields are checked against the request.
 */
const Continuation = Schema.Struct({
  clientId: Schema.String,
  principalId: Schema.String,
  /** The request cursor this page set started from; `null` = initial sync. */
  cursor: Schema.NullOr(Schema.BigIntFromString),
  /** Log read through here (for an initial sync, starts at `head` of the first page). */
  position: Schema.BigIntFromString,
  /** Scopes fully delivered in this set. */
  scopes: Schema.Array(Schema.String),
  /** Scopes whose current rows are being delivered, in order; `after` = last row delivered. */
  walking: Schema.Array(Schema.Struct({ scopeId: Schema.String, after: Schema.NullOr(RowKey) }))
})
type Continuation = typeof Continuation.Type
const ContinuationJson = Schema.fromJsonString(Continuation)
const encodeContinuation = Schema.encodeSync(ContinuationJson)
const decodeContinuation = Schema.decodeUnknownEffect(ContinuationJson)

export type Change =
  | {
      readonly _tag: "Upsert"
      readonly table: string
      readonly rowId: string
      readonly scopeId: string
      /** Current row as JSON (`to_jsonb`), snake_case columns, as of this page's snapshot. */
      readonly row: unknown
    }
  | { readonly _tag: "Delete"; readonly table: string; readonly rowId: string }
  | { readonly _tag: "ScopePurged"; readonly scopeId: string }

export interface Ack {
  readonly mutationId: number
  readonly outcome: MutationOutcome
}

/** Stage `changes` (in order) and `acks`; the set is consistent only at `PullDone`. */
export type PullResponse =
  | {
      readonly _tag: "PullPage"
      readonly changes: ReadonlyArray<Change>
      readonly acks: ReadonlyArray<Ack>
      /** Opaque; send back verbatim with the same cursor. */
      readonly continuation: string
    }
  | {
      readonly _tag: "PullDone"
      readonly changes: ReadonlyArray<Change>
      readonly acks: ReadonlyArray<Ack>
      readonly nextCursor: bigint
    }

/** The client must discard staged pages and pull from a `null` cursor. */
export class ResnapshotRequired extends Schema.TaggedError<ResnapshotRequired>()("ResnapshotRequired", {
  reason: Schema.Literals([
    /** Log GC passed the cursor or continuation. */
    "gcHorizon",
    /** The server has no record of the scopes held at this cursor. */
    "unknownCursor",
    /** The continuation belongs to another client, principal, or cursor. */
    "staleContinuation"
  ])
}) {}

export interface PullRequest {
  /** Authenticated by the app (`principalFor`), never taken from the wire. */
  readonly principalId: string
  readonly clientId: string
  readonly cursor: bigint | null
  readonly continuation: Option.Option<string>
  /** Upper bound on upserts and tombstones per page (≥ 1). Purges are not counted. */
  readonly limit: number
}

const decodeOutcome = Schema.decodeUnknownEffect(MutationOutcome)

interface LogEntry {
  readonly position: bigint
  readonly table_name: string
  readonly row_id: string
  readonly op: "upsert" | "delete"
}

interface RowPayload {
  readonly row_id: string
  readonly access_scope_id: string
  readonly row: unknown
}

/**
 * Pull one page (DESIGN.md §8) in its own `REPEATABLE READ` snapshot:
 *
 * 1. purge every held scope that is no longer readable;
 * 2. start a row walk for every readable scope not held (initial sync is the same thing from an
 *    empty set, with the log position = `head` at the first page);
 * 3. continue outstanding walks;
 * 4. once no walk is outstanding, read log entries > position in readable scopes.
 *
 * Membership is a diff of states (held vs. readable now), not a replay of grant log entries.
 * At `PullDone` the held set is recorded under `nextCursor`, so the next page set starts from it.
 * See docs/server-substrate.md.
 */
export const pull = Effect.fnUntraced(function* (request: PullRequest) {
  const sql = yield* SqlClient
  yield* registerClient(request.principalId, request.clientId)
  const stale = new ResnapshotRequired({ reason: "staleContinuation" })
  const resumed: Continuation | undefined = Option.isSome(request.continuation)
    ? yield* decodeContinuation(request.continuation.value).pipe(Effect.mapError(() => stale))
    : undefined
  if (
    resumed !== undefined &&
    (resumed.clientId !== request.clientId ||
      resumed.principalId !== request.principalId ||
      resumed.cursor !== request.cursor)
  ) {
    return yield* stale
  }

  const response = yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`

      const stateRows = yield* sql<{ gc_horizon: bigint; head: bigint }>`
        SELECT gc_horizon,
               GREATEST(gc_horizon,
                        (SELECT max(position) FROM echo_sync.log),
                        (SELECT max(position) FROM echo_sync.mutations)) AS head
        FROM echo_sync.engine_state`
      const state = stateRows[0]
      if (state === undefined) return yield* Effect.die("echo_sync.engine_state is empty; run migrate")
      const base = resumed?.position ?? request.cursor
      if (base !== null && base < state.gc_horizon) {
        return yield* new ResnapshotRequired({ reason: "gcHorizon" })
      }

      // What the client holds at the start of this page.
      let held: ReadonlyArray<string> = []
      let walking: Array<{ scopeId: string; after: typeof RowKey.Type | null }> = []
      if (resumed !== undefined) {
        held = resumed.scopes
        walking = [...resumed.walking]
      } else if (request.cursor !== null) {
        const recorded = yield* sql<{ scope_ids: ReadonlyArray<string> }>`
          SELECT scope_ids FROM echo_sync.client_scopes
          WHERE client_id = ${request.clientId} AND position = ${request.cursor}`
        const row = recorded[0]
        if (row === undefined) return yield* new ResnapshotRequired({ reason: "unknownCursor" })
        held = row.scope_ids
      }

      const readable = new Set(
        (yield* sql<{ access_scope_id: string }>`
          SELECT DISTINCT access_scope_id FROM echo_sync.scope_grants
          WHERE principal_id IN (${request.principalId}, '*')`).map((r) => r.access_scope_id)
      )
      const tables = Arr.sort(
        yield* syncedTables,
        Order.mapInput(compare, (t: SyncedTable) => t.name)
      )
      const position = base ?? state.head
      const changes: Array<Change> = []

      // 1 + 2. Membership diff.
      const scopes: Array<string> = []
      for (const scopeId of held) {
        if (readable.has(scopeId)) scopes.push(scopeId)
        else changes.push({ _tag: "ScopePurged", scopeId })
      }
      walking = walking.filter((walk) => {
        if (readable.has(walk.scopeId)) return true
        changes.push({ _tag: "ScopePurged", scopeId: walk.scopeId })
        return false
      })
      const known = new Set([...scopes, ...walking.map((w) => w.scopeId)])
      for (const scopeId of Arr.sort(readable, compare)) {
        if (!known.has(scopeId)) walking.push({ scopeId, after: null })
      }

      // 3. Row walks.
      let budget = request.limit
      while (budget > 0) {
        const walk = walking[0]
        if (walk === undefined) break
        const page = yield* walkPage(tables, walk.scopeId, walk.after, budget)
        changes.push(...page.changes)
        budget -= page.changes.length
        if (page.after === null) {
          walking.shift()
          scopes.push(walk.scopeId)
        } else walking[0] = { scopeId: walk.scopeId, after: page.after }
      }

      // 4. The log, only once no walk is outstanding.
      let nextPosition = position
      let logDone = false
      if (walking.length === 0 && budget > 0) {
        const fetched = yield* sql<LogEntry>`
          SELECT position, table_name, row_id, op FROM echo_sync.log
          WHERE position > ${position} AND access_scope_id = ANY(${[...readable]}::text[])
          ORDER BY position
          LIMIT ${budget + 1}`
        logDone = fetched.length <= budget
        const entries = logDone ? fetched : fetched.slice(0, budget)
        const last = entries.at(-1)
        nextPosition = !logDone && last !== undefined ? last.position : bigMax(state.head, position)

        const payloads = yield* fetchPayloads(tables, entries)
        for (const entry of entries) {
          const current = entry.op === "upsert" ? payloads.get(payloadKey(entry.table_name, entry.row_id)) : undefined
          changes.push(
            current !== undefined && readable.has(current.access_scope_id)
              ? {
                  _tag: "Upsert",
                  table: entry.table_name,
                  rowId: entry.row_id,
                  scopeId: current.access_scope_id,
                  row: current.row
                }
              : // Deleted, or its current scope is not readable: never leak a payload.
                { _tag: "Delete", table: entry.table_name, rowId: entry.row_id }
          )
        }
      }

      // Acks: ledger rows in (cursor, nextCursor] across the set, each page covering the log
      // range it advanced. An initial sync covers every committed mutation of this client.
      const ackFloor = resumed?.position ?? request.cursor ?? -1n
      const ackRows = yield* sql<{ mutation_id: bigint; outcome: unknown }>`
        SELECT mutation_id, outcome FROM echo_sync.mutations
        WHERE client_id = ${request.clientId} AND position > ${ackFloor} AND position <= ${nextPosition}
        ORDER BY position`
      const acks: Array<Ack> = []
      for (const row of ackRows) {
        acks.push({ mutationId: Number(row.mutation_id), outcome: yield* Effect.orDie(decodeOutcome(row.outcome)) })
      }

      const result: PullResponse = logDone
        ? { _tag: "PullDone", changes, acks, nextCursor: nextPosition }
        : {
            _tag: "PullPage",
            changes,
            acks,
            continuation: encodeContinuation({
              clientId: request.clientId,
              principalId: request.principalId,
              cursor: request.cursor,
              position: nextPosition,
              scopes,
              walking
            })
          }
      return { result, scopes }
    })
  )

  // Outside the read snapshot. Each statement is idempotent, so a crash between them, or before
  // the response reaches the client, leaves state any retry converges from: the cursor the
  // client still holds keeps its record until the client presents a newer one.
  if (request.cursor !== null) {
    yield* sql`
      DELETE FROM echo_sync.client_scopes WHERE client_id = ${request.clientId} AND position < ${request.cursor}`
    yield* sql`UPDATE echo_sync.clients SET cursor = ${request.cursor} WHERE client_id = ${request.clientId}`
  }
  if (response.result._tag === "PullDone") {
    yield* sql`
      INSERT INTO echo_sync.client_scopes (client_id, position, scope_ids)
      VALUES (${request.clientId}, ${response.result.nextCursor}, ${Arr.sort(response.scopes, compare)}::text[])
      ON CONFLICT DO NOTHING`
  }
  return response.result
})

/** Code-point order, matching `COLLATE "C"` (DESIGN.md §10). Table names are ASCII identifiers. */
const compare: Order.Order<string> = (a, b) => (a < b ? -1 : a > b ? 1 : 0)
const bigMax = (a: bigint, b: bigint) => (a > b ? a : b)
const payloadKey = (table: string, rowId: string) => `${table}\u0000${rowId}`

const fetchPayloads = Effect.fnUntraced(function* (
  tables: ReadonlyArray<SyncedTable>,
  entries: ReadonlyArray<LogEntry>
) {
  const sql = yield* SqlClient
  const payloads = new Map<string, RowPayload>()
  for (const table of tables) {
    const ids = [
      ...new Set(entries.filter((e) => e.op === "upsert" && e.table_name === table.name).map((e) => e.row_id))
    ]
    if (ids.length === 0) continue
    const rows = yield* sql<RowPayload>`
      SELECT t.id::text AS row_id, t.access_scope_id, to_jsonb(t) AS row
      FROM ${sql(table.schema)}.${sql(table.name)} t
      WHERE t.id::text = ANY(${ids}::text[])`
    for (const row of rows) payloads.set(payloadKey(table.name, row.row_id), row)
  }
  // Entries of tables detached since they were written have no payload: delivered as deletes.
  return payloads
})

const walkPage = Effect.fnUntraced(function* (
  tables: ReadonlyArray<SyncedTable>,
  scopeId: string,
  after: typeof RowKey.Type | null,
  budget: number
) {
  const sql = yield* SqlClient
  const changes: Array<Change> = []
  let reached = after
  for (const table of tables) {
    if (after !== null && table.name < after.table) continue
    const remaining = budget - changes.length
    // Budget spent exactly at a table boundary: resume after the last delivered row.
    if (remaining === 0) return { changes, after: reached }
    const afterRowId = after !== null && after.table === table.name ? after.rowId : null
    const rows = yield* sql<RowPayload>`
      SELECT t.id::text AS row_id, t.access_scope_id, to_jsonb(t) AS row
      FROM ${sql(table.schema)}.${sql(table.name)} t
      WHERE t.access_scope_id = ${scopeId}
        AND (${afterRowId}::text IS NULL OR t.id::text COLLATE "C" > ${afterRowId}::text COLLATE "C")
      ORDER BY t.id::text COLLATE "C"
      LIMIT ${remaining + 1}`
    for (const row of rows.slice(0, remaining)) {
      changes.push({ _tag: "Upsert", table: table.name, rowId: row.row_id, scopeId, row: row.row })
      reached = { table: table.name, rowId: row.row_id }
    }
    if (rows.length > remaining) return { changes, after: reached }
  }
  return { changes, after: null }
})
