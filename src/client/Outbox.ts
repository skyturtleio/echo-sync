/**
 * Durable outbox primitive (DESIGN §9). The outbox is the only client state that is not disposable:
 * the replica can be rebuilt from the server, unsent intents cannot.
 *
 * Deliberately minimal: append inside the caller's transaction, read pending in order, remove by
 * sequence. The reconciliation loop (`src/client/sync`) builds on these three operations.
 *
 * Ordering: `seq` is `INTEGER PRIMARY KEY AUTOINCREMENT`, allocated inside the appending transaction on
 * the single leader connection, so local order is total and gap-free (a rolled-back append also rolls
 * back its `sqlite_sequence` bump, and AUTOINCREMENT never reuses a removed seq).
 */
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"

export const OutboxEntry = Schema.Struct({
  seq: Schema.Number,
  clientId: Schema.String,
  mutationId: Schema.String,
  name: Schema.String,
  args: Schema.Json,
  createdAt: Schema.Number
})
export type OutboxEntry = typeof OutboxEntry.Type

const EntryRow = Schema.Struct({
  seq: Schema.Number,
  client_id: Schema.String,
  mutation_id: Schema.String,
  name: Schema.String,
  args: Schema.fromJsonString(Schema.Json),
  created_at: Schema.Number
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
        createdAt: r.created_at
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
        created_at INTEGER NOT NULL
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
 * re-appending returns the existing entry and allocates no new seq.
 */
export const append = (input: AppendInput): Effect.Effect<OutboxEntry, SqlError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const existing = yield* sql`SELECT * FROM echo_outbox WHERE mutation_id = ${input.mutationId}`
        if (existing.length === 0) {
          yield* sql`INSERT INTO echo_outbox (client_id, mutation_id, name, args, created_at)
            VALUES (${input.clientId}, ${input.mutationId}, ${input.name}, ${JSON.stringify(input.args)}, ${Date.now()})`
        }
        const rows = yield* sql`SELECT * FROM echo_outbox WHERE mutation_id = ${input.mutationId}`
        const [entry] = yield* toEntries(rows)
        if (entry === undefined) return yield* Effect.die(new Error("echo_outbox: appended row not readable"))
        return entry
      })
    )
  })

/** Every not-yet-removed entry in local order. */
export const pending: Effect.Effect<ReadonlyArray<OutboxEntry>, SqlError, SqlClient.SqlClient> = Effect.gen(
  function* () {
    const sql = yield* SqlClient.SqlClient
    return yield* toEntries(yield* sql`SELECT * FROM echo_outbox ORDER BY seq`)
  }
)

/** Remove entries (after their outcome is installed in the replica, DESIGN §8). Joins the caller's transaction. */
export const remove = (seqs: ReadonlyArray<number>): Effect.Effect<void, SqlError, SqlClient.SqlClient> =>
  seqs.length === 0
    ? Effect.void
    : Effect.flatMap(SqlClient.SqlClient, (sql) =>
        sql`DELETE FROM echo_outbox WHERE ${sql.in("seq", seqs)}`.pipe(Effect.asVoid)
      )
