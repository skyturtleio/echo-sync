/**
 * The engine's `SyncStorage` port on the leader's SQLite connection (SPEC §7, §9).
 *
 * Everything the engine persists (replica rows, outbox, sync meta, staged pull pages, notices) lives in
 * one database and commits or rolls back in one SQLite transaction. `transaction` is
 * `SqlClient.withTransaction`, which leases the single connection for the whole BEGIN..COMMIT/ROLLBACK,
 * so concurrent engine fibers (mutate, push, pull) never interleave statements inside another fiber's
 * transaction.
 *
 * Every method fails only with `StoreUnavailable` (retryable SQL errors); any other SQL error, or a
 * stored form that does not decode, is corruption or a bug, so a defect. Stored forms use core's
 * schemas: staged pages are `Protocol.PullPage` JSON, notices carry `Mutation.MutationEnvelope` and
 * `Mutation.MutationRejection`, and undo images store their table by sync name, resolved through the
 * registry when read (a rebuild empties them, so a name is always registered).
 */
import { Effect, Option, Schema, Semaphore, type Stream } from "effect"
import * as SqlClient from "effect/sql/SqlClient"
import { isSqlError, type SqlError } from "effect/sql/SqlError"
import { type Errors, Ids, Mutation, Protocol, type Registry, Store, SyncTable } from "echo-sync"
import * as Outbox from "../Outbox.ts"
import * as SqliteRowStore from "../SqliteRowStore.ts"
import type * as Engine from "./Engine.ts"

const StoredPage = Schema.fromJsonString(Protocol.PullPage)
const StoredUndo = Schema.fromJsonString(
  Schema.Array(
    Schema.Struct({
      table: Schema.String,
      id: Schema.String,
      before: Schema.OptionFromNullOr(SyncTable.EncodedRow)
    })
  )
)
/** A stored notice (SPEC §9): core's `Mutation.Notice`, so it can also cross the tab RPC. */
export const Notice = Mutation.Notice
const StoredNotice = Schema.fromJsonString(Notice)
const StoredMeta = Schema.fromJsonString(
  Schema.Struct({
    cursor: Ids.Cursor,
    pushedThrough: Schema.Int,
    resnapshot: Schema.Boolean,
    epoch: Schema.optionalKey(Schema.NullOr(Schema.String))
  })
)

// Our own rows failing to decode means corruption or a version skew: the `CorruptStorage` defect, never retryable.
const decode =
  <A>(schema: Schema.Decoder<A>, table: string) =>
  (text: string): Effect.Effect<A> =>
    Store.decodeStored(schema, table, text)
const encode = <S extends Schema.Codec<unknown, string>>(schema: S) => {
  const run = Schema.encodeEffect(schema)
  return (value: S["Type"]) => Effect.orDie(run(value))
}
const decodePage = decode(StoredPage, "echo_staged")
const encodePage = encode(StoredPage)
const decodeUndo = decode(StoredUndo, "echo_outbox")
const encodeUndo = encode(StoredUndo)
const encodeNotice = encode(StoredNotice)
const decodeNotice = decode(StoredNotice, "echo_notices")
const decodeMeta = decode(StoredMeta, "echo_meta")
const encodeMeta = encode(StoredMeta)

const initialMeta: Engine.SyncMeta = { cursor: null, pushedThrough: 0, resnapshot: false }

const StoredTables = Schema.fromJsonString(Schema.Array(Schema.String))
const decodeTables = decode(StoredTables, "echo_meta")
const encodeTables = encode(StoredTables)
const q = (name: string) => `"${name.replaceAll('"', '""')}"`

/**
 * Brings the leader's database in line with `registry` (SPEC §9.6, §13.4) in ONE SQLite transaction:
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
        // Engine tables first (versioned forward steps), in this same transaction.
        yield* Outbox.migrate
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

const encodeUndoImages = (undo: ReadonlyArray<Store.UndoImage>) =>
  encodeUndo(undo.map((image) => ({ table: image.table.name, id: image.id, before: image.before })))

const toEnvelope = (entry: Outbox.OutboxEntry): Effect.Effect<Mutation.MutationEnvelope> =>
  Store.decodeStored(
    Mutation.MutationEnvelope,
    "echo_outbox",
    { clientId: entry.clientId, mutationId: entry.mutationId, seq: entry.seq, name: entry.name, args: entry.args },
    entry.mutationId
  )

/**
 * The storage port over `sql`. `rows` is the SQLite `ReplicaRowStore` for `registry` on the same
 * connection, so its statements join this storage's transactions.
 */
/** The engine's storage port plus admission for SQL the leader runs outside the engine. */
export interface SqliteSyncStorage extends Engine.SyncStorage {
  /**
   * Run `self` holding the storage permit but with no transaction of its own (statements autocommit),
   * so SQL outside the engine (the tab `exec`/`subscribe` RPCs) waits as cancellably as `transaction`.
   */
  readonly admit: <A, E, R>(self: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  /**
   * The number of unreleased outbox entries, then the number after every write to `echo_outbox` (an
   * append or a release, through the connection's reactivity hook). Each count is read under the permit,
   * so it sees committed state only: a recount, never bookkeeping that can drift (INV-LOOP-9).
   */
  readonly pendingCount: Stream.Stream<number, Errors.StoreUnavailable>
}

export const make = (sql: SqlClient.SqlClient, registry: Registry.SyncRegistry): SqliteSyncStorage => {
  const rows = SqliteRowStore.make(sql, registry)
  const permit = Semaphore.makeUnsafe(1)
  const run = <A>(effect: Effect.Effect<A, SqlError, SqlClient.SqlClient>) =>
    SqliteRowStore.unavailable(Effect.provideService(effect, SqlClient.SqlClient, sql))

  const decodeUndoImages = (text: string) =>
    Effect.flatMap(decodeUndo(text), (images) =>
      Effect.forEach(images, (image) =>
        Option.match(registry.get(image.table), {
          onNone: () => Effect.die(new Error(`echo_outbox: undo image for unregistered table ${image.table}`)),
          onSome: (table): Effect.Effect<Store.UndoImage> => Effect.succeed({ ...image, table })
        })
      )
    )

  const pending = Effect.gen(function* () {
    const result: Array<Engine.PendingMutation> = []
    const undo = yield* run(sql<{ readonly seq: number; readonly undo: string }>`
      SELECT seq, undo FROM echo_outbox WHERE released_at IS NULL ORDER BY seq`)
    const undoBySeq = new Map(undo.map((r) => [r.seq, r.undo]))
    for (const entry of yield* run(Outbox.pending)) {
      result.push({
        envelope: yield* toEnvelope(entry),
        createdAt: entry.createdAt,
        undo: yield* decodeUndoImages(undoBySeq.get(entry.seq) ?? "[]")
      })
    }
    return result
  })

  return {
    admit: (self) => permit.withPermit(self),
    pendingCount: sql.reactive(
      ["echo_outbox"],
      permit.withPermit(
        Effect.map(
          run(sql<{ readonly n: number }>`SELECT count(*) AS n FROM echo_outbox WHERE released_at IS NULL`),
          (counted) => counted[0]?.n ?? 0
        )
      )
    ),
    // BEGIN/COMMIT failures map like any statement's; the body's own errors pass through unchanged.
    // `withTransaction` waits for the connection lease uninterruptibly; waiting for `permit` first makes
    // a transaction queued behind a long one (e.g. a pull install) cancellable while it waits. Admission
    // is serialized, not fair (SPEC Appendix C.1): a fiber that just released may take the permit again
    // ahead of a waiter. Order between mutations comes from the caller sequencing them, never from here.
    // The leader runs its own SQL through `admit` or `transaction`, so nothing it serves bypasses the permit.
    transaction: (self) =>
      permit
        .withPermit(sql.withTransaction(self))
        .pipe(Effect.catchIf(isSqlError, (error) => SqliteRowStore.unavailable(Effect.fail(error)))),
    rows,
    meta: Effect.gen(function* () {
      const stored = yield* run(sql<{ readonly value: string }>`SELECT value FROM echo_meta WHERE key = 'sync_meta'`)
      const text = stored[0]?.value
      return text === undefined ? initialMeta : yield* decodeMeta(text)
    }),
    setMeta: (meta) =>
      Effect.gen(function* () {
        const value = yield* encodeMeta(meta)
        yield* run(sql`INSERT INTO echo_meta (key, value) VALUES ('sync_meta', ${value})
          ON CONFLICT (key) DO UPDATE SET value = excluded.value`)
      }),
    pending,
    lookup: (mutationId) =>
      Effect.flatMap(run(Outbox.lookup(mutationId)), (entry) =>
        Option.isNone(entry) ? Effect.succeedNone : Effect.asSome(toEnvelope(entry.value))
      ),
    nextSeq: Effect.flatMap(run(Outbox.nextSeq), (seq) => Store.decodeStored(Ids.Seq, "sqlite_sequence", seq)),
    appendPending: (entry) =>
      Effect.gen(function* () {
        const undo = yield* encodeUndoImages(entry.undo)
        yield* run(
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
    setUndo: (seq, undo) =>
      Effect.gen(function* () {
        const text = yield* encodeUndoImages(undo)
        yield* run(sql`UPDATE echo_outbox SET undo = ${text} WHERE seq = ${seq}`)
      }),
    release: (seqs) => run(Outbox.markReleased(seqs)),
    staged: Effect.gen(function* () {
      const stored = yield* run(sql<{ readonly page: string }>`SELECT page FROM echo_staged ORDER BY ord`)
      return yield* Effect.forEach(stored, (r) => decodePage(r.page))
    }),
    stagedProgress: Effect.gen(function* () {
      // stage only appends and clearStaged deletes the whole set: committed ords are contiguous
      // within a set (including after rollback), but AUTOINCREMENT does not restart on clear.
      // Two primary-key end seeks avoid a count(*) scan and returning accumulated page JSON.
      const stored = yield* run(sql<{ readonly continuation: string | null; readonly count: number }>`
        SELECT CASE WHEN json_valid(page) THEN
                 CASE WHEN json_type(page, '$.continuation') = 'text'
                   THEN json_extract(page, '$.continuation') END
               END AS continuation,
               ord - (SELECT min(ord) FROM echo_staged) + 1 AS count
        FROM echo_staged ORDER BY ord DESC LIMIT 1`)
      const last = stored[0]
      return last === undefined
        ? { continuation: Option.none(), count: 0 }
        : {
            continuation: Option.some(
              yield* Store.decodeStored(Protocol.PullContinuation, "echo_staged", last.continuation)
            ),
            count: last.count
          }
    }),
    stage: (page) =>
      Effect.gen(function* () {
        const text = yield* encodePage(page)
        yield* run(sql`INSERT INTO echo_staged (page) VALUES (${text})`)
      }),
    clearStaged: Effect.asVoid(run(sql`DELETE FROM echo_staged`)),
    addNotices: (notices) =>
      Effect.forEach(
        notices,
        (notice) =>
          Effect.gen(function* () {
            const text = yield* encodeNotice(notice)
            yield* run(sql`INSERT INTO echo_notices (notice) VALUES (${text})`)
          }),
        { discard: true }
      )
  }
}

/** Delete the notices with these ids; the only way a notice is ever removed (SPEC §9.5). */
export const dismissNotices = (
  sql: SqlClient.SqlClient,
  ids: ReadonlyArray<number>
): Effect.Effect<void, Errors.StoreUnavailable> =>
  ids.length === 0
    ? Effect.void
    : Effect.asVoid(SqliteRowStore.unavailable(sql`DELETE FROM echo_notices WHERE ${sql.in("id", ids)}`))

/** Every retained notice, oldest first, with its id (SPEC §9.5: retained for inspection). */
export const notices = (
  sql: SqlClient.SqlClient
): Effect.Effect<
  ReadonlyArray<{ readonly id: number; readonly notice: typeof Notice.Type }>,
  Errors.StoreUnavailable
> =>
  SqliteRowStore.unavailable(sql<{ readonly id: number; readonly notice: string }>`
    SELECT id, notice FROM echo_notices ORDER BY id`).pipe(
    Effect.flatMap((rows) =>
      Effect.forEach(rows, (row) => Effect.map(decodeNotice(row.notice), (notice) => ({ id: row.id, notice })))
    )
  )
