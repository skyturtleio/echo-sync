/**
 * Durable outbox primitive (DESIGN §9). The outbox is the only client state that is not disposable:
 * the replica can be rebuilt from the server, unsent intents cannot.
 *
 * Deliberately minimal: append inside the caller's transaction, read pending in order, mark released,
 * prune. The reconciliation loop (`src/client/sync`) builds on these operations.
 *
 * Release marks a row (`released_at`) instead of deleting it (DESIGN §9): an `enqueue` retried after
 * `LeaderLost` can arrive after the original was pushed, acked and released. Because the row is still
 * there, the retry returns the original `seq` and inserts nothing; a fresh row would replay the effect
 * locally a second time and get a server position no pull window ever acks. Released rows are pruned
 * after a retention window.
 *
 * Ordering: `seq` is `INTEGER PRIMARY KEY AUTOINCREMENT`, allocated inside the appending transaction on
 * the single leader connection, so local order is total and gap-free (a rolled-back append also rolls
 * back its `sqlite_sequence` bump, and AUTOINCREMENT never reuses a removed seq).
 */
import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"

export const OutboxEntry = Schema.Struct({
  seq: Schema.Number,
  clientId: Schema.String,
  mutationId: Schema.String,
  name: Schema.String,
  args: Schema.Json,
  createdAt: Schema.Number,
  /** When the reconcile engine released this entry; `null` while it is pending. */
  releasedAt: Schema.NullOr(Schema.Number)
})
export type OutboxEntry = typeof OutboxEntry.Type

const EntryRow = Schema.Struct({
  seq: Schema.Number,
  client_id: Schema.String,
  mutation_id: Schema.String,
  name: Schema.String,
  args: Schema.fromJsonString(Schema.Json),
  created_at: Schema.Number,
  released_at: Schema.NullOr(Schema.Number)
})
const decodeRows = Schema.decodeUnknownEffect(Schema.Array(EntryRow))

const toEntries = (rows: ReadonlyArray<unknown>): Effect.Effect<ReadonlyArray<OutboxEntry>> =>
  decodeRows(rows).pipe(
    // A row that does not decode was written by a different engine version or corrupted; that is a
    // defect, never something a caller can recover from by retrying.
    Effect.orDie,
    Effect.map((decoded) =>
      decoded.map((r) => ({
        seq: r.seq,
        clientId: r.client_id,
        mutationId: r.mutation_id,
        name: r.name,
        args: r.args,
        createdAt: r.created_at,
        releasedAt: r.released_at
      }))
    )
  )

/** Idempotent DDL for the engine-owned local tables. */
export const migrate: Effect.Effect<void, SqlError, SqlClient.SqlClient> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`CREATE TABLE IF NOT EXISTS echo_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT`
      yield* sql`CREATE TABLE IF NOT EXISTS echo_outbox (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id TEXT NOT NULL,
        mutation_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        args TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        released_at INTEGER,
        -- First-touch pre-images of the last optimistic run (JSON); owned by the reconcile storage.
        undo TEXT NOT NULL DEFAULT '[]'
      )`
    })
  )
})

/** This replica's client id (DESIGN §6 mutation identity), created on first use and persisted. */
export const clientId: Effect.Effect<string, SqlError, SqlClient.SqlClient> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const fresh = crypto.randomUUID()
      yield* sql`INSERT INTO echo_meta (key, value) VALUES ('client_id', ${fresh}) ON CONFLICT (key) DO NOTHING`
      const rows = yield* sql<{ readonly value: string }>`SELECT value FROM echo_meta WHERE key = 'client_id'`
      return rows[0]?.value ?? fresh
    })
  )
})

export interface AppendInput {
  readonly clientId: string
  readonly mutationId: string
  readonly name: string
  readonly args: Schema.Json
}

/**
 * Append an intent. Joins the caller's transaction when there is one (so the outbox entry commits
 * atomically with the optimistic apply); otherwise runs in its own. Idempotent on `mutationId`:
 * re-appending a known id, pending or released, returns the existing entry and allocates no new seq.
 */
export const append = (input: AppendInput): Effect.Effect<OutboxEntry, SqlError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const existing = yield* sql`SELECT * FROM echo_outbox WHERE mutation_id = ${input.mutationId}`
        if (existing.length === 0) {
          const now = yield* Clock.currentTimeMillis
          yield* sql`INSERT INTO echo_outbox (client_id, mutation_id, name, args, created_at)
            VALUES (${input.clientId}, ${input.mutationId}, ${input.name}, ${JSON.stringify(input.args)}, ${now})`
        }
        const rows = yield* sql`SELECT * FROM echo_outbox WHERE mutation_id = ${input.mutationId}`
        const [entry] = yield* toEntries(rows)
        if (entry === undefined) return yield* Effect.die(new Error("echo_outbox: appended row not readable"))
        return entry
      })
    )
  })

/**
 * The seq the next append will get: one past AUTOINCREMENT's high-water mark in `sqlite_sequence`, not
 * `MAX(seq)`, so a seq whose row was pruned is never handed out again. Call inside the appending
 * transaction; a rollback rolls `sqlite_sequence` back with it, so the number is not lost.
 */
export const nextSeq: Effect.Effect<number, SqlError, SqlClient.SqlClient> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  const rows = yield* sql<{ readonly seq: number }>`SELECT seq FROM sqlite_sequence WHERE name = 'echo_outbox'`
  return (rows[0]?.seq ?? 0) + 1
})

/**
 * Insert an entry at a seq allocated by `nextSeq` in the same transaction (the reconcile engine's path;
 * it checks `lookup` first, so `mutationId` is new).
 */
export const insert = (input: {
  readonly seq: number
  readonly clientId: string
  readonly mutationId: string
  readonly name: string
  readonly args: unknown
  readonly createdAt: number
  readonly undo: string
}): Effect.Effect<void, SqlError, SqlClient.SqlClient> =>
  Effect.flatMap(SqlClient.SqlClient, (sql) =>
    Effect.asVoid(sql`INSERT INTO echo_outbox (seq, client_id, mutation_id, name, args, created_at, undo)
      VALUES (${input.seq}, ${input.clientId}, ${input.mutationId}, ${input.name}, ${JSON.stringify(input.args)},
        ${input.createdAt}, ${input.undo})`)
  )

/** The entry for `mutationId`, pending or released (until pruned). */
export const lookup = (mutationId: string): Effect.Effect<Option.Option<OutboxEntry>, SqlError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const [entry] = yield* toEntries(yield* sql`SELECT * FROM echo_outbox WHERE mutation_id = ${mutationId}`)
    return Option.fromNullishOr(entry)
  })

/** Every not-yet-released entry in local (`seq`) order. */
export const pending: Effect.Effect<ReadonlyArray<OutboxEntry>, SqlError, SqlClient.SqlClient> = Effect.gen(
  function* () {
    const sql = yield* SqlClient.SqlClient
    return yield* toEntries(yield* sql`SELECT * FROM echo_outbox WHERE released_at IS NULL ORDER BY seq`)
  }
)

/**
 * Mark entries released once their outcome is installed in the replica (DESIGN §8). Joins the caller's
 * transaction. Re-marking keeps the first release time.
 */
export const markReleased = (seqs: ReadonlyArray<number>): Effect.Effect<void, SqlError, SqlClient.SqlClient> =>
  seqs.length === 0
    ? Effect.void
    : Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        const now = yield* Clock.currentTimeMillis
        yield* sql`UPDATE echo_outbox SET released_at = ${now} WHERE released_at IS NULL AND ${sql.in("seq", seqs)}`
      })

/** Default retention of released rows: long enough to absorb any realistic retry of a lost `enqueue`. */
export const defaultReleasedRetentionMillis = 24 * 60 * 60 * 1000

/**
 * Delete rows released more than `retentionMillis` ago. AUTOINCREMENT keeps its high-water mark in
 * `sqlite_sequence`, so pruning never lets a seq be reused.
 */
export const pruneReleased = (
  retentionMillis: number = defaultReleasedRetentionMillis
): Effect.Effect<void, SqlError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const now = yield* Clock.currentTimeMillis
    yield* sql`DELETE FROM echo_outbox WHERE released_at IS NOT NULL AND released_at < ${now - retentionMillis}`
  })
