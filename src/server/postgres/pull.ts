import { Array as Arr, Effect, Option, type Order, Schema } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Protocol, type Registry, Store, type SyncTable } from "echo-sync"
import { assertPrincipal, registerClient } from "./ledger.ts"
import { checkEpoch } from "./epoch.ts"
import { decodeRow, q, selectList } from "./rowStore.ts"
import { type Resolved, resolveTables } from "./schema.ts"
import * as Telemetry from "../../internal/Telemetry.ts"

/** Keyset position of a scope row walk: rows are walked in `(table, id)` code-point order. */
const RowKey = Schema.Struct({ table: Schema.String, rowId: Schema.String })

/**
 * Server-side progress of an unfinished page set (SPEC §7). Private to the server: on the
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
  /** Acks can lag changes when their byte/count budget fills. Absent on pre-stream continuations. */
  ackPosition: Schema.optionalKey(Schema.BigIntFromString),
  /** Scopes fully delivered in this set. */
  scopes: Schema.Array(Schema.String),
  /** Scopes whose current rows are being delivered, in order; `after` = last row delivered. */
  walking: Schema.Array(Schema.Struct({ scopeId: Schema.String, after: Schema.NullOr(RowKey) }))
})
type Continuation = typeof Continuation.Type
const ContinuationJson = Schema.fromJsonString(Continuation)
const encodeContinuation = Schema.encodeSync(ContinuationJson)
const decodeContinuation = Schema.decodeUnknownEffect(ContinuationJson)

/** Untrusted compatibility advice, never authorization. Omission serves every client. */
export type ClientPolicy = (version: Protocol.ClientVersion | undefined) => Effect.Effect<void, Protocol.ClientOutdated>

/** The server-side request: the principal is authenticated by the app, never taken from the wire. */
export interface PullRequest {
  readonly principalId: string
  readonly clientId: string
  readonly epoch?: string | null | undefined
  readonly clientVersion?: Protocol.ClientVersion | undefined
  /** The wire's replica principal (SPEC §5): checked against `principalId`, never used as it. */
  readonly principal?: string | undefined
  readonly cursor: bigint | null
  readonly continuation: Option.Option<string>
  /** Upper bound on upserts and tombstones per page (≥ 1). Purges are not counted. */
  readonly limit: number
}

const MutationOutcomeJson = Schema.toCodecJson(Mutation.MutationOutcome)

/** Target UTF-8 bytes of encoded PullResponse JSON; indivisible candidates may exceed it. */
export const maxPullPageBytes = 1_048_576
// A configured row budget is an upper bound, not a target. Bound candidate construction even
// for very large limits; this does not bound individual rows, acks or membership metadata.
const maxPullPageRows = 500
const encodeResponse = Schema.encodeSync(Schema.toCodecJson(Protocol.PullResponse))
const utf8 = new TextEncoder()

interface LogEntry {
  readonly position: bigint
  readonly table_name: string
  readonly row_id: string
  readonly op: "upsert" | "delete"
}

/** A current row as the wire row (declared columns only, decoded through `table.postgres.row`). */
interface Payload {
  readonly id: string
  readonly scopeId: string
  readonly row: SyncTable.EncodedRow
}

const payloadOf = Effect.fnUntraced(function* (table: SyncTable.AnySyncTable, raw: unknown) {
  const row = yield* decodeRow(table, raw)
  const id = row["id"]
  const scopeId = row["access_scope_id"]
  if (typeof id !== "string" || typeof scopeId !== "string") {
    return yield* Effect.die(`${table.name}: decoded row lacks a string id or access_scope_id`)
  }
  const payload: Payload = { id, scopeId, row }
  return payload
})

/**
 * Pull one page (SPEC §7) in its own `REPEATABLE READ` snapshot:
 *
 * 1. purge every held scope that is no longer readable;
 * 2. start a row walk for every readable scope not held (initial sync is the same thing from an
 *    empty set, with the log position = `head` at the first page);
 * 3. continue outstanding walks;
 * 4. once no walk is outstanding, read log entries > position in readable scopes.
 *
 * Membership is a diff of states (held vs. readable now), not a replay of grant log entries.
 * At `PullDone` the held set is recorded under `nextCursor`, so the next page set starts from it.
 * See SPEC §7.
 */
export const pull = Effect.fnUntraced(function* (
  registry: Registry.SyncRegistry,
  request: PullRequest,
  clientPolicy?: ClientPolicy
) {
  yield* assertPrincipal(request.principalId, request.principal)
  if (clientPolicy !== undefined) yield* clientPolicy(request.clientVersion)
  const epoch = yield* checkEpoch(request.epoch)
  const sql = yield* SqlClient
  yield* registerClient(request.principalId, request.clientId)
  const stale = new Protocol.ResnapshotRequired({ reason: "staleContinuation" })
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

      let limit = Math.min(request.limit, maxPullPageRows)
      let deferAcks = false
      const candidate = Effect.gen(function* () {
        const stateRows = yield* sql<{ gc_horizon: bigint; head: bigint; expired_through: bigint }>`
        SELECT gc_horizon,
               GREATEST(gc_horizon,
                        (SELECT max(position) FROM echo_sync.log),
                        (SELECT max(position) FROM echo_sync.mutations),
                        -- An expired ledger row's position outlives it here, so expiry never
                        -- moves the head (and with it a cursor or continuation) back.
                        (SELECT max(expired_position) FROM echo_sync.clients)) AS head,
               (SELECT expired_through FROM echo_sync.clients WHERE client_id = ${request.clientId}) AS expired_through
        FROM echo_sync.engine_state`
        const state = stateRows[0]
        if (state === undefined) return yield* Effect.die("echo_sync.engine_state is empty; run migrate")
        if (request.cursor !== null && request.cursor > state.head) {
          return yield* new Protocol.ResnapshotRequired({ reason: "unknownCursor", epoch })
        }
        // Resumed progress must lie in [originating cursor, head of this snapshot]. A position the
        // server never reached would let PullDone record a nextCursor no snapshot produced.
        if (
          resumed !== undefined &&
          (resumed.position < 0n ||
            resumed.position > state.head ||
            (resumed.ackPosition !== undefined &&
              (resumed.ackPosition < (request.cursor ?? -1n) || resumed.ackPosition > resumed.position)) ||
            (request.cursor !== null && resumed.position < request.cursor))
        ) {
          return yield* stale
        }
        const base = resumed?.position ?? request.cursor
        if (base !== null && base < state.gc_horizon) {
          return yield* new Protocol.ResnapshotRequired({ reason: "gcHorizon" })
        }

        // The originating cursor must be one the server completed a set at, resumed or not.
        let held: ReadonlyArray<string> = []
        if (request.cursor !== null) {
          const recorded = yield* sql<{ scope_ids: ReadonlyArray<string> }>`
          SELECT scope_ids FROM echo_sync.client_scopes
          WHERE client_id = ${request.clientId} AND position = ${request.cursor}`
          const row = recorded[0]
          if (row === undefined) return yield* new Protocol.ResnapshotRequired({ reason: "unknownCursor" })
          held = row.scope_ids
        }
        // What the client holds at the start of this page.
        let walking: Array<{ scopeId: string; after: typeof RowKey.Type | null }> = []
        if (resumed !== undefined) {
          held = resumed.scopes
          walking = [...resumed.walking]
        }

        const readable = new Set(
          (yield* sql<{ access_scope_id: string }>`
          SELECT DISTINCT access_scope_id FROM echo_sync.scope_grants
          WHERE principal_id IN (${request.principalId}, '*')`).map((r) => r.access_scope_id)
        )
        const tables = yield* resolveTables(registry)
        const position = base ?? state.head
        const changes: Array<Protocol.Change> = []

        // 1 + 2. Membership diff.
        const scopes: Array<string> = []
        for (const scopeId of held) {
          if (readable.has(scopeId)) scopes.push(scopeId)
          else changes.push(purged(scopeId))
        }
        walking = walking.filter((walk) => {
          if (readable.has(walk.scopeId)) return true
          changes.push(purged(walk.scopeId))
          return false
        })
        const known = new Set([...scopes, ...walking.map((w) => w.scopeId)])
        for (const scopeId of Arr.sort(readable, compare)) {
          if (!known.has(scopeId)) walking.push({ scopeId, after: null })
        }

        // 3. Row walks.
        let budget = limit
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
          // position ≤ head is enforced above, so a completed set ends exactly at this snapshot's head.
          nextPosition = !logDone && last !== undefined ? last.position : state.head

          const payloads = yield* fetchPayloads(tables, entries)
          for (const entry of entries) {
            const current = entry.op === "upsert" ? payloads.get(payloadKey(entry.table_name, entry.row_id)) : undefined
            changes.push(
              current !== undefined && readable.has(current.scopeId)
                ? { _tag: "upsert", table: entry.table_name, row: current.row }
                : // Deleted, or its current scope is not readable: never leak a payload.
                  { _tag: "tombstone", table: entry.table_name, id: entry.row_id }
            )
          }
        }

        // Acks: ledger rows in (cursor, nextCursor] across the set, each page covering the log
        // range it advanced. An initial sync covers every committed mutation of this client.
        const ackFloor = resumed?.ackPosition ?? resumed?.position ?? request.cursor ?? -1n
        const ackLimit = deferAcks ? 0 : limit
        const ackRows = yield* sql<{ mutation_id: string; position: bigint; outcome: unknown }>`
        SELECT mutation_id, position, outcome FROM echo_sync.mutations
        WHERE client_id = ${request.clientId} AND position > ${ackFloor} AND position <= ${nextPosition}
          AND outcome IS NOT NULL
        ORDER BY position LIMIT ${ackLimit + 1}`
        const acksDone = ackRows.length <= ackLimit
        const acks: Array<Protocol.Ack> = []
        for (const row of ackRows.slice(0, ackLimit)) {
          acks.push({
            mutationId: Ids.MutationId.make(row.mutation_id),
            position: Ids.Position.make(row.position),
            outcome: yield* Store.decodeStored(MutationOutcomeJson, "echo_sync.mutations", row.outcome, row.mutation_id)
          })
        }

        const result: Protocol.PullResponse =
          logDone && acksDone
            ? {
                _tag: "PullDone",
                epoch,
                changes,
                acks,
                nextCursor: Ids.Position.make(nextPosition),
                // Number() is exact only up to Number.MAX_SAFE_INTEGER; that matches SeqFloor being an
                // Int (SPEC §11). Raised only by `expireLedger`.
                expiredThrough: Ids.SeqFloor.make(Number(state.expired_through))
              }
            : {
                _tag: "PullPage",
                epoch,
                changes,
                acks,
                continuation: Protocol.PullContinuation.make(
                  encodeContinuation({
                    clientId: request.clientId,
                    principalId: request.principalId,
                    cursor: request.cursor,
                    position: nextPosition,
                    ackPosition: acksDone ? nextPosition : (acks.at(-1)?.position ?? ackFloor),
                    scopes,
                    walking
                  })
                )
              }
        // At PullDone the delivered set is the readable set of this snapshot (every readable scope
        // was kept or walked, every other purged). Record that directly, not what the request claimed.
        return { result, held: Arr.sort(readable, compare) }
      })
      // Rebuild from the original continuation in the SAME snapshot. Truncating changes alone
      // would skip rows/acks because its continuation has already advanced past them. Persisted
      // atomic data has no matching write-size limit, so the minimum candidate must make progress.
      while (true) {
        const page = yield* candidate
        if (utf8.encode(JSON.stringify(encodeResponse(page.result))).byteLength <= maxPullPageBytes) return page
        if (limit <= 1) {
          // A row and an ack may each fit separately but not together. Preserve ack progress
          // in the continuation and deliver it on a later page rather than refusing the pair.
          if (!deferAcks && page.result.changes.length > 0 && page.result.acks.length > 0) {
            deferAcks = true
            continue
          }
          return page
        }
        limit = Math.max(1, Math.floor(limit / 2))
      }
    })
  )

  // Outside the read snapshot. Each statement is idempotent, so a crash between them, or before
  // the response reaches the client, leaves state any retry converges from: the cursor the
  // client still holds keeps its record until the client presents a newer one.
  if (request.cursor !== null) {
    yield* sql`
      DELETE FROM echo_sync.client_scopes WHERE client_id = ${request.clientId} AND position < ${request.cursor}`
    yield* sql`UPDATE echo_sync.clients SET cursor = ${request.cursor} WHERE client_id = ${request.clientId}`
    // Ledger expiry (SPEC §11): presenting a cursor the server completed a set at proves
    // the client installed every ack at or below it, so those rows are acked from now. Only the
    // first stamp counts; a row is never expired before it has one.
    yield* sql`
      UPDATE echo_sync.mutations SET acked_at = now()
      WHERE client_id = ${request.clientId} AND position <= ${request.cursor} AND acked_at IS NULL
        AND outcome IS NOT NULL`
  }
  if (response.result._tag === "PullDone") {
    yield* sql`
      INSERT INTO echo_sync.client_scopes (client_id, position, scope_ids)
      VALUES (${request.clientId}, ${response.result.nextCursor}, ${response.held}::text[])
      ON CONFLICT DO NOTHING`
  }
  return response.result
}, Telemetry.boundary)

/** Code-point order, matching `COLLATE "C"` (INV-TBL-2). Table names are ASCII identifiers. */
const compare: Order.Order<string> = (a, b) => (a < b ? -1 : a > b ? 1 : 0)
const payloadKey = (table: string, rowId: string) => `${table}\u0000${rowId}`
const purged = (scopeId: string): Protocol.Change => ({
  _tag: "scopePurged",
  scopeId: AccessScope.AccessScopeId.make(scopeId)
})

const fetchPayloads = Effect.fnUntraced(function* (tables: ReadonlyArray<Resolved>, entries: ReadonlyArray<LogEntry>) {
  const sql = yield* SqlClient
  const payloads = new Map<string, Payload>()
  for (const { table, schema } of tables) {
    const ids = [
      ...new Set(entries.filter((e) => e.op === "upsert" && e.table_name === table.name).map((e) => e.row_id))
    ]
    if (ids.length === 0) continue
    // Declared columns only (INV-TBL-4): an undeclared column never leaves the database.
    const rows = yield* sql.unsafe<Record<string, unknown>>(
      `SELECT ${selectList(table, "t")} FROM ${q(schema)}.${q(table.name)} t WHERE t.id::text = ANY($1::text[])`,
      [ids]
    )
    for (const raw of rows) {
      const payload = yield* payloadOf(table, raw)
      payloads.set(payloadKey(table.name, payload.id), payload)
    }
  }
  // Entries of tables detached or unregistered since they were written have no payload: tombstones.
  return payloads
})

const walkPage = Effect.fnUntraced(function* (
  tables: ReadonlyArray<Resolved>,
  scopeId: string,
  after: typeof RowKey.Type | null,
  budget: number
) {
  const sql = yield* SqlClient
  const changes: Array<Protocol.Change> = []
  let reached = after
  for (const { table, schema } of tables) {
    if (after !== null && table.name < after.table) continue
    const remaining = budget - changes.length
    // Budget spent exactly at a table boundary: resume after the last delivered row.
    if (remaining === 0) return { changes, after: reached }
    const afterRowId = after !== null && after.table === table.name ? after.rowId : null
    const rows = yield* sql.unsafe<Record<string, unknown>>(
      `SELECT ${selectList(table, "t")} FROM ${q(schema)}.${q(table.name)} t
       WHERE t.access_scope_id = $1
         ${afterRowId === null ? "" : 'AND t.id COLLATE "C" > $2::text COLLATE "C"'}
       ORDER BY t.id::text COLLATE "C"
       LIMIT $${afterRowId === null ? 2 : 3}`,
      afterRowId === null ? [scopeId, remaining + 1] : [scopeId, afterRowId, remaining + 1]
    )
    for (const raw of rows.slice(0, remaining)) {
      const payload = yield* payloadOf(table, raw)
      changes.push({ _tag: "upsert", table: table.name, row: payload.row })
      reached = { table: table.name, rowId: payload.id }
    }
    if (rows.length > remaining) return { changes, after: reached }
  }
  return { changes, after: null }
})
