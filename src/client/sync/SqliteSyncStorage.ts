/**
 * The engine's `SyncStorage` port on the leader's SQLite connection (DESIGN §8, §9).
 *
 * Everything the engine persists (replica rows, outbox, sync meta, staged pull pages, notices) lives in
 * one database and commits or rolls back in one SQLite transaction. `transaction` is
 * `SqlClient.withTransaction`, which leases the single connection for the whole BEGIN..COMMIT/ROLLBACK,
 * so concurrent engine fibers (mutate, push, pull) never interleave statements inside another fiber's
 * transaction.
 *
 * Stored forms are parsed with Schema when read. They mirror `Wire.ts` and are replaced by core's
 * Protocol codecs in the same change that swaps `Wire.ts` for `src/core`.
 */
import { Effect, Option, Schema } from "effect"
import * as SqlClient from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import * as Outbox from "../Outbox.ts"
import type * as Engine from "./Engine.ts"
import type { MutationEnvelope, PullPage, RowWriter, UndoImage } from "./Wire.ts"

const Row = Schema.StructWithRest(Schema.Struct({ id: Schema.String, access_scope_id: Schema.String }), [
  Schema.Record(Schema.String, Schema.Unknown)
])
const Tagged = Schema.StructWithRest(Schema.Struct({ _tag: Schema.String }), [
  Schema.Record(Schema.String, Schema.Unknown)
])
const Outcome = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Applied") }),
  Schema.Struct({ _tag: Schema.Literal("Rejected"), error: Tagged })
])
const Change = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("upsert"), table: Schema.String, row: Row }),
  Schema.Struct({ _tag: Schema.Literal("tombstone"), table: Schema.String, id: Schema.String }),
  Schema.Struct({ _tag: Schema.Literal("scopePurged"), scopeId: Schema.String })
])
/** A staged page; positions are decimal strings in storage (DESIGN §8: bigint in memory). */
const StoredPage = Schema.fromJsonString(
  Schema.Struct({
    _tag: Schema.Literal("PullPage"),
    changes: Schema.Array(Change),
    acks: Schema.Array(
      Schema.Struct({ mutationId: Schema.String, position: Schema.BigIntFromString, outcome: Outcome })
    ),
    continuation: Schema.String
  })
)
/** Undo images: `before` is `null` when the row did not exist. */
const StoredUndo = Schema.fromJsonString(
  Schema.Array(Schema.Struct({ table: Schema.String, id: Schema.String, before: Schema.OptionFromNullOr(Row) }))
)
const Envelope = Schema.Struct({
  clientId: Schema.String,
  mutationId: Schema.String,
  seq: Schema.Number,
  name: Schema.String,
  args: Schema.Unknown
})
const StoredNotice = Schema.fromJsonString(
  Schema.Union([
    Schema.Struct({ _tag: Schema.Literal("Rejected"), envelope: Envelope, error: Tagged }),
    Schema.Struct({ _tag: Schema.Literal("OutcomeUnknown"), envelope: Envelope })
  ])
)
const StoredMeta = Schema.fromJsonString(
  Schema.Struct({
    cursor: Schema.NullOr(Schema.BigIntFromString),
    pushedThrough: Schema.Number,
    resnapshot: Schema.Boolean
  })
)

// Our own rows failing to decode means corruption or a version skew: a defect, never retryable.
const decode = <S extends Schema.Codec<unknown, string>>(schema: S) => {
  const run = Schema.decodeEffect(schema)
  return (text: string) => Effect.orDie(run(text))
}
const encode = <S extends Schema.Codec<unknown, string>>(schema: S) => {
  const run = Schema.encodeEffect(schema)
  return (value: S["Type"]) => Effect.orDie(run(value))
}
const decodePage = decode(StoredPage)
const encodePage = encode(StoredPage)
const decodeUndo = decode(StoredUndo)
const encodeUndo = encode(StoredUndo)
const encodeNotice = encode(StoredNotice)
const decodeMeta = decode(StoredMeta)
const encodeMeta = encode(StoredMeta)

const initialMeta: Engine.SyncMeta = { cursor: null, pushedThrough: 0, resnapshot: false }

/** Idempotent DDL for the engine's tables beside `echo_outbox` (run after `Outbox.migrate`). */
export const migrate: Effect.Effect<void, SqlError, SqlClient.SqlClient> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`CREATE TABLE IF NOT EXISTS echo_staged (ord INTEGER PRIMARY KEY AUTOINCREMENT, page TEXT NOT NULL) STRICT`
      yield* sql`CREATE TABLE IF NOT EXISTS echo_notices (id INTEGER PRIMARY KEY AUTOINCREMENT, notice TEXT NOT NULL) STRICT`
    })
  )
})

const toEnvelope = (entry: Outbox.OutboxEntry): MutationEnvelope => ({
  clientId: entry.clientId,
  mutationId: entry.mutationId,
  seq: entry.seq,
  name: entry.name,
  args: entry.args
})

/**
 * `rows` is the replica's row store (the SQLite `ReplicaRowStore` once core's ports are on main); it
 * must run its statements on the same `SqlClient`, so they join this storage's transactions.
 */
export const make = (sql: SqlClient.SqlClient, rows: RowWriter<SqlError>): Engine.SyncStorage<SqlError> => {
  const provide = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
    Effect.provideService(effect, SqlClient.SqlClient, sql)

  const pending = Effect.gen(function* () {
    const result: Array<Engine.PendingMutation> = []
    const undo = yield* sql<{ readonly seq: number; readonly undo: string }>`
      SELECT seq, undo FROM echo_outbox WHERE released_at IS NULL ORDER BY seq`
    const undoBySeq = new Map(undo.map((r) => [r.seq, r.undo]))
    for (const entry of yield* provide(Outbox.pending)) {
      result.push({
        envelope: toEnvelope(entry),
        createdAt: entry.createdAt,
        undo: yield* decodeUndo(undoBySeq.get(entry.seq) ?? "[]")
      })
    }
    return result
  })

  return {
    transaction: (self) => sql.withTransaction(self),
    rows,
    meta: Effect.gen(function* () {
      const stored = yield* sql<{ readonly value: string }>`SELECT value FROM echo_meta WHERE key = 'sync_meta'`
      const text = stored[0]?.value
      return text === undefined ? initialMeta : yield* decodeMeta(text)
    }),
    setMeta: (meta) =>
      Effect.gen(function* () {
        const value = yield* encodeMeta(meta)
        yield* sql`INSERT INTO echo_meta (key, value) VALUES ('sync_meta', ${value})
          ON CONFLICT (key) DO UPDATE SET value = excluded.value`
      }),
    pending,
    lookup: (mutationId) => provide(Outbox.lookup(mutationId)).pipe(Effect.map(Option.map(toEnvelope))),
    nextSeq: provide(Outbox.nextSeq),
    appendPending: (entry) =>
      Effect.gen(function* () {
        const undo = yield* encodeUndo(entry.undo)
        yield* provide(
          Outbox.insert({
            seq: entry.envelope.seq,
            clientId: entry.envelope.clientId,
            mutationId: entry.envelope.mutationId,
            name: entry.envelope.name,
            args: entry.envelope.args,
            createdAt: entry.createdAt,
            undo
          })
        )
      }),
    setUndo: (seq, undo: ReadonlyArray<UndoImage>) =>
      Effect.gen(function* () {
        const text = yield* encodeUndo(undo)
        yield* sql`UPDATE echo_outbox SET undo = ${text} WHERE seq = ${seq}`
      }),
    release: (seqs) => provide(Outbox.markReleased(seqs)),
    staged: Effect.gen(function* () {
      const stored = yield* sql<{ readonly page: string }>`SELECT page FROM echo_staged ORDER BY ord`
      return yield* Effect.forEach(stored, (r): Effect.Effect<PullPage> => decodePage(r.page))
    }),
    stage: (page) =>
      Effect.gen(function* () {
        const text = yield* encodePage(page)
        yield* sql`INSERT INTO echo_staged (page) VALUES (${text})`
      }),
    clearStaged: Effect.asVoid(sql`DELETE FROM echo_staged`),
    addNotices: (notices) =>
      Effect.forEach(
        notices,
        (notice) =>
          Effect.gen(function* () {
            const text = yield* encodeNotice(notice)
            yield* sql`INSERT INTO echo_notices (notice) VALUES (${text})`
          }),
        { discard: true }
      )
  }
}
