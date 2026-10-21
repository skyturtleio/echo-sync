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
import { AccessScope, type Errors, type Registry, SyncTable } from "echo-sync"
import * as SqliteRowStore from "../SqliteRowStore.ts"
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

const StoredTables = Schema.fromJsonString(Schema.Array(Schema.String))
const decodeTables = decode(StoredTables)
const encodeTables = encode(StoredTables)
const q = (name: string) => `"${name.replaceAll('"', '""')}"`

/**
 * Brings the leader's database in line with `registry` (DESIGN §9, §11) in ONE SQLite transaction:
 * the outbox, the engine's tables, the replica tables, and the bookkeeping that describes them.
 *
 * The registry's manifest hashes live in `echo_meta` under `replica_tables`, `replica_data_hash`,
 * `replica_index_hash` and `replica_table_ddl` (a hash of the tables' SQLite DDL); this function is
 * their only writer.
 * - First run: create every replica table and index.
 * - `dataHash` or the table DDL changed (core hashes a `unique` constraint as index shape, but it
 *   lives in CREATE TABLE, which SQLite cannot alter): drop the previous replica tables, recreate
 *   them empty, and in the same transaction reset what described the old replica: `cursor` to `null`, `resnapshot` off, staged
 *   pages deleted, pending undo images emptied (they pre-image rows that no longer exist). The outbox,
 *   `pushedThrough` and notices are kept. A crash at any point therefore leaves either the old tables
 *   with the old cursor or the new tables with a `null` cursor, never a mix. Returns `rebuilt: true`.
 * - Otherwise, if `indexHash` changed: drop and recreate the declared indexes; rows and cursor stay.
 */
export const migrate = (
  registry: Registry.SyncRegistry
): Effect.Effect<{ readonly rebuilt: boolean }, SqlError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* Outbox.migrate
        yield* sql`CREATE TABLE IF NOT EXISTS echo_staged (ord INTEGER PRIMARY KEY AUTOINCREMENT, page TEXT NOT NULL) STRICT`
        yield* sql`CREATE TABLE IF NOT EXISTS echo_notices (id INTEGER PRIMARY KEY AUTOINCREMENT, notice TEXT NOT NULL) STRICT`
        const meta = new Map(
          (yield* sql<{ readonly key: string; readonly value: string }>`
            SELECT key, value FROM echo_meta WHERE key IN ('replica_tables', 'replica_data_hash', 'replica_index_hash', 'replica_table_ddl')`).map(
            (r) => [r.key, r.value]
          )
        )
        const put = (key: string, value: string) =>
          sql`INSERT INTO echo_meta (key, value) VALUES (${key}, ${value})
            ON CONFLICT (key) DO UPDATE SET value = excluded.value`
        const createIndexes = Effect.forEach(
          registry.tables.flatMap((t) => t.sqlite.createIndexes),
          (ddl) => sql.unsafe(ddl)
        )
        const previous = meta.get("replica_tables")
        // Table DDL also carries constraints the manifest hashes as index shape (`unique`); SQLite
        // cannot alter a table's constraints, so any change to the CREATE TABLE text rebuilds too.
        const tableDdl = SyncTable.hash(registry.tables.map((t) => t.sqlite.createTable).join(";\n"))
        const rebuilt =
          previous !== undefined &&
          (meta.get("replica_data_hash") !== registry.dataHash || meta.get("replica_table_ddl") !== tableDdl)

        if (previous === undefined || rebuilt) {
          for (const name of previous === undefined ? [] : yield* decodeTables(previous)) {
            yield* sql.unsafe(`DROP TABLE IF EXISTS ${q(name)}`)
          }
          for (const table of registry.tables) yield* sql.unsafe(table.sqlite.createTable)
          yield* createIndexes
          // Nothing may describe rows that are not in the new tables.
          const stored = yield* sql<{ readonly value: string }>`SELECT value FROM echo_meta WHERE key = 'sync_meta'`
          const current = stored[0] === undefined ? initialMeta : yield* decodeMeta(stored[0].value)
          yield* put("sync_meta", yield* encodeMeta({ ...current, cursor: null, resnapshot: false }))
          yield* sql`DELETE FROM echo_staged`
          yield* sql`UPDATE echo_outbox SET undo = '[]' WHERE released_at IS NULL`
        } else if (meta.get("replica_index_hash") !== registry.indexHash) {
          const names = registry.tables.map((t) => t.name)
          const indexes = yield* sql.unsafe<{ readonly name: string }>(
            `SELECT name FROM sqlite_schema WHERE type = 'index' AND sql IS NOT NULL AND tbl_name IN (${names.map(() => "?").join(", ")})`,
            names
          )
          for (const { name } of indexes) yield* sql.unsafe(`DROP INDEX ${q(name)}`)
          yield* createIndexes
        }
        yield* put("replica_tables", yield* encodeTables(registry.tables.map((t) => t.name)))
        yield* put("replica_data_hash", registry.dataHash)
        yield* put("replica_table_ddl", tableDdl)
        yield* put("replica_index_hash", registry.indexHash)
        return { rebuilt }
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

const toCoreRow = Schema.decodeUnknownEffect(SyncTable.EncodedRow)

/**
 * `rows` is the SQLite `ReplicaRowStore` for `registry`, on the same connection, so its statements join
 * this storage's transactions. Until the engine takes core's ports (reconcile's type swap), the engine's
 * name-keyed `RowWriter` is adapted here: a name the registry does not know is a defect, because the
 * engine skips unknown tables before writing (DESIGN §8) and handlers only write registered tables.
 */
export const make = (
  sql: SqlClient.SqlClient,
  registry: Registry.SyncRegistry
): Engine.SyncStorage<SqlError | Errors.StoreUnavailable> => {
  const store = SqliteRowStore.make(sql, registry)
  const tableNamed = (name: string) =>
    Option.match(registry.get(name), {
      onNone: () => Effect.die(new Error(`echo-sync: write to unregistered table ${name}`)),
      onSome: Effect.succeed
    })
  const rows: RowWriter<Errors.StoreUnavailable> = {
    write: (name, id, row) =>
      Effect.gen(function* () {
        const table = yield* tableNamed(name)
        const value = Option.isNone(row) ? Option.none() : Option.some(yield* Effect.orDie(toCoreRow(row.value)))
        yield* store.write(table, id, value)
      }),
    purgeScope: (scopeId) => store.purgeScope(AccessScope.AccessScopeId.make(scopeId)),
    clearAll: store.clearAll
  }
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
