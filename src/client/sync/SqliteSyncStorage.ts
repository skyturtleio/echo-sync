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
import { Array as Arr, Deferred, Effect, Option, Schema, String as Str, type Stream } from "effect"
import * as SqlClient from "effect/sql/SqlClient"
import { isSqlError, type SqlError } from "effect/sql/SqlError"
import { type Errors, Ids, Mutation, Protocol, type Registry, Store, SyncTable } from "echo-sync"
import * as Outbox from "../Outbox.ts"
import * as SqliteRowStore from "../SqliteRowStore.ts"
import * as Catalogs from "./Catalogs.ts"
import type { LocalChange, Revision } from "../Provenance.ts"
import type * as Engine from "./Engine.ts"
import { randomUUIDv4 } from "../Crypto.ts"

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
const q = (name: string) => `"${Str.replaceAll('"', '""')(name)}"`

const StoredManifests = Schema.fromJsonString(
  Schema.Array(
    Schema.Struct({
      name: Schema.String,
      columns: Schema.Array(
        Schema.Struct({
          name: Schema.String,
          kind: Schema.Literals(["text", "integer", "double", "boolean", "timestamp", "json"]),
          nullable: Schema.Boolean
        })
      ),
      unique: Schema.Array(Schema.Array(Schema.String)),
      ddl: Schema.String
    })
  )
)
const decodeManifests = decode(StoredManifests, "echo_meta")
const encodeManifests = encode(StoredManifests)
const encodeJson = encode(Schema.fromJsonString(Schema.Json))

/**
 * Brings the leader's database in line with `registry` (SPEC §9.6, §13.4) in ONE SQLite transaction:
 * the outbox, the engine's tables, the replica tables, and the bookkeeping that describes them.
 *
 * Stored per-table manifests let compatible tables retain their base while an ordinary resnapshot
 * downloads replacements. Undo uses the old physical shape before constraints change; `replay`
 * runs the new handlers and recaptures undo before commit. Leaders always supply it; storage-only
 * callers may omit it only when no pending intent needs replay. No callbacks escape the transaction.
 * Legacy replicas without manifests retain the one-time full-rebuild fallback. Index-only changes
 * keep rows/cursor and do not refresh. `rebuilt` reports a data/DDL change, including local migration.
 */
export const migrate = <E = never>(
  registry: Registry.SyncRegistry,
  replay?: (incompatibleCatalogs: ReadonlySet<string>) => Effect.Effect<void, E>
): Effect.Effect<{ readonly rebuilt: boolean }, SqlError | E, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        // Engine tables first (versioned forward steps), in this same transaction.
        yield* Outbox.migrate
        // Replayed handlers may read newly declared catalogs before downloads start.
        const incompatibleCatalogs = yield* Catalogs.open(sql, registry)
        const meta = new Map(
          Arr.map(
            yield* sql<{ readonly key: string; readonly value: string }>`
            SELECT key, value FROM echo_meta WHERE key IN ('replica_tables', 'replica_data_hash', 'replica_index_hash', 'replica_table_ddl', 'replica_manifests')`,
            (r) => [r.key, r.value] as const
          )
        )
        const put = (key: string, value: string) =>
          sql`INSERT INTO echo_meta (key, value) VALUES (${key}, ${value})
            ON CONFLICT (key) DO UPDATE SET value = excluded.value`
        const createIndexes = Effect.forEach(
          Arr.flatMap(registry.tables, (t) => t.sqlite.createIndexes),
          (ddl) => sql.unsafe(ddl)
        )
        const previous = meta.get("replica_tables")
        // Table DDL also carries constraints the manifest hashes as index shape (`unique`); SQLite
        // cannot alter a table's constraints, so any change to the CREATE TABLE text rebuilds too.
        const tableDdl = SyncTable.hash(
          Arr.join(
            Arr.map(registry.tables, (t) => t.sqlite.createTable),
            ";\n"
          )
        )
        const rebuilt =
          previous !== undefined &&
          (meta.get("replica_data_hash") !== registry.dataHash || meta.get("replica_table_ddl") !== tableDdl)

        const manifestText = meta.get("replica_manifests")
        if (rebuilt && manifestText !== undefined) {
          const manifests = yield* decodeManifests(manifestText)
          const old = new Map(Arr.map(manifests, (m) => [m.name, m] as const))
          const recreated = new Set<string>()
          const pending = yield* sql<{
            readonly undo: string
          }>`SELECT undo FROM echo_outbox WHERE released_at IS NULL ORDER BY seq DESC`
          // Restore before changing constraints. A new unique set may accept the optimistic
          // rows but reject their base, or vice versa. Never undo through the new constraints.
          for (const entry of pending) {
            const images = yield* decodeUndo(entry.undo)
            // A valid pending swap can have preimages that conflict with its current rows.
            // Remove the whole write set before restoring its valid baseline.
            for (const image of images) yield* sql.unsafe(`DELETE FROM ${q(image.table)} WHERE id = ?`, [image.id])
            for (const image of Arr.reverse(images)) {
              if (Option.isSome(image.before)) {
                const manifest = old.get(image.table)
                if (manifest === undefined) return yield* Effect.die(new Error(`unknown undo table ${image.table}`))
                const before = image.before.value
                const values: Array<SyncTable.SqliteValue> = []
                for (const column of manifest.columns) {
                  const value = before[column.name]
                  if (value === null) values.push(null)
                  else if (column.kind === "json" && value !== undefined) values.push(yield* encodeJson(value))
                  else if (typeof value === "boolean") values.push(value ? 1 : 0)
                  else if (typeof value === "number" || typeof value === "string") values.push(value)
                  else return yield* Effect.die(new Error(`invalid undo column ${image.table}.${column.name}`))
                }
                const names = Arr.map(manifest.columns, (c) => c.name)
                yield* sql.unsafe(
                  `INSERT INTO ${q(image.table)} (${Arr.join(Arr.map(names, q), ",")}) VALUES (${Arr.join(
                    Arr.map(values, () => "?"),
                    ","
                  )})`,
                  values
                )
              }
            }
          }
          for (const manifest of manifests) {
            if (Option.isNone(registry.get(manifest.name))) yield* sql.unsafe(`DROP TABLE ${q(manifest.name)}`)
          }
          for (const table of registry.tables) {
            const was = old.get(table.name)
            if (was?.ddl === table.sqlite.createTable) continue
            const oldColumns = new Map(Arr.map(was?.columns ?? [], (c) => [c.name, c] as const))
            const added = Arr.filter(table.columns, (c) => !oldColumns.has(c.name))
            const removed = Arr.some(was?.columns ?? [], (c) => !Arr.some(table.columns, (n) => n.name === c.name))
            const compatible =
              was !== undefined &&
              !(removed && added.length > 0) &&
              Arr.every(table.columns, (c) => {
                const before = oldColumns.get(c.name)
                return before === undefined ? c.nullable : before.kind === c.kind && (!before.nullable || c.nullable)
              })
            const additive =
              compatible &&
              !removed &&
              added.length > 0 &&
              SyncTable.canonicalJson(was.unique) === SyncTable.canonicalJson(table.manifest.unique) &&
              Arr.every(was.columns, (c) =>
                Arr.some(table.columns, (n) => n.name === c.name && n.nullable === c.nullable)
              )
            if (additive) {
              for (const column of added) {
                const type =
                  column.kind === "double"
                    ? "REAL"
                    : column.kind === "text" || column.kind === "json"
                      ? "TEXT"
                      : "INTEGER"
                yield* sql.unsafe(
                  `ALTER TABLE ${q(table.name)} ADD COLUMN ${q(column.name)} ${type}${column.kind === "boolean" ? ` CHECK (${q(column.name)} IN (0, 1))` : ""}`
                )
              }
              continue
            }
            // SQLite copy-rebuilds constraints and dropped columns without decoding old rows
            // through the new application codec. Only a constraint failure discards this table.
            recreated.add(table.name)
            if (was !== undefined) yield* sql.unsafe(`ALTER TABLE ${q(table.name)} RENAME TO __echo_migrating`)
            yield* sql.unsafe(table.sqlite.createTable)
            if (compatible) {
              yield* sql
                .unsafe(
                  `INSERT INTO ${q(table.name)} (${Arr.join(
                    Arr.map(table.columns, (c) => q(c.name)),
                    ","
                  )}) SELECT ${Arr.join(
                    Arr.map(table.columns, (c) => (oldColumns.has(c.name) ? q(c.name) : "NULL")),
                    ","
                  )} FROM __echo_migrating`
                )
                .pipe(
                  Effect.catchIf(
                    (error) => error.reason._tag === "ConstraintError" || error.reason._tag === "UniqueViolation",
                    () => Effect.void
                  )
                )
            }
            if (was !== undefined) yield* sql`DROP TABLE __echo_migrating`
          }
          // Pure nullable additions need no index rebuild. If indexes changed in the same
          // deploy, refresh those on unchanged tables as well as copy-rebuilt tables.
          const indexed = Arr.filter(
            registry.tables,
            (t) => recreated.has(t.name) || meta.get("replica_index_hash") !== registry.indexHash
          )
          const indexes = yield* sql<{
            readonly name: string
          }>`SELECT name FROM sqlite_schema WHERE type = 'index' AND sql IS NOT NULL AND ${sql.in(
            "tbl_name",
            Arr.map(indexed, (t) => t.name)
          )}`
          for (const { name } of indexes) yield* sql.unsafe(`DROP INDEX ${q(name)}`)
          for (const table of indexed) for (const ddl of table.sqlite.createIndexes) yield* sql.unsafe(ddl)
          const stored = yield* sql<{ readonly value: string }>`SELECT value FROM echo_meta WHERE key = 'sync_meta'`
          const current = stored[0] === undefined ? initialMeta : yield* decodeMeta(stored[0].value)
          yield* put("sync_meta", yield* encodeMeta({ ...current, resnapshot: true }))
          yield* sql`DELETE FROM echo_staged`
          yield* sql`UPDATE echo_outbox SET undo = '[]' WHERE released_at IS NULL`
          if (pending.length > 0) {
            if (replay === undefined)
              return yield* Effect.die(new Error("schema migration with pending intent requires replay"))
            yield* replay(incompatibleCatalogs)
          }
        } else if (previous === undefined || rebuilt) {
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
          const names = Arr.map(registry.tables, (t) => t.name)
          const indexes = yield* sql.unsafe<{ readonly name: string }>(
            `SELECT name FROM sqlite_schema WHERE type = 'index' AND sql IS NOT NULL AND tbl_name IN (${Arr.join(
              Arr.map(names, () => "?"),
              ", "
            )})`,
            names
          )
          for (const { name } of indexes) yield* sql.unsafe(`DROP INDEX ${q(name)}`)
          yield* createIndexes
        }
        yield* put("replica_tables", yield* encodeTables(Arr.map(registry.tables, (t) => t.name)))
        yield* put("replica_data_hash", registry.dataHash)
        yield* put("replica_table_ddl", tableDdl)
        yield* put("replica_index_hash", registry.indexHash)
        yield* put(
          "replica_manifests",
          yield* encodeManifests(
            Arr.map(registry.tables, (t) => ({
              name: t.name,
              columns: t.manifest.columns,
              unique: t.manifest.unique,
              ddl: t.sqlite.createTable
            }))
          )
        )
        return { rebuilt }
      })
    )
  })

const encodeUndoImages = (undo: ReadonlyArray<Store.UndoImage>) =>
  encodeUndo(Arr.map(undo, (image) => ({ table: image.table.name, id: image.id, before: image.before })))

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
  readonly localCommit: NonNullable<Engine.SyncStorage["localCommit"]>
  /** Read only while admitted, alongside the query's rows. */
  readonly revision: Effect.Effect<Revision>
  /** Raw SQL is outside the tracked row store. Call under admission before executing it. */
  readonly invalidateRevision: Effect.Effect<void>
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

export const make = (sql: SqlClient.SqlClient, registry: Registry.SyncRegistry): Effect.Effect<SqliteSyncStorage> =>
  Effect.gen(function* () {
    const rows = SqliteRowStore.make(sql, registry)
    // The head owns admission even before its fiber resumes. Keeping the owner in the
    // insertion-ordered set prevents a releasing fiber from barging back in.
    const admissions = new Set<Deferred.Deferred<void>>()
    const admit: SqliteSyncStorage["admit"] = (self) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.suspend(() => {
          const ready = Deferred.makeUnsafe<void>()
          admissions.add(ready)
          if (admissions.size === 1) Deferred.doneUnsafe(ready, Effect.void)
          // Install cleanup before restoring interruption, including while awaiting a
          // grant. A cancelled head passes ownership on; a cancelled waiter just leaves.
          return restore(Effect.andThen(Deferred.await(ready), self)).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                const wasHead = admissions.values().next().value === ready
                admissions.delete(ready)
                const next = admissions.values().next().value
                if (wasHead && next !== undefined) Deferred.doneUnsafe(next, Effect.void)
              })
            )
          )
        })
      )
    let revision: Revision = { epoch: yield* randomUUIDv4, offset: 0 }
    let changed = false
    let appended = false
    const advance = Effect.gen(function* () {
      revision =
        revision.offset === Number.MAX_SAFE_INTEGER
          ? { epoch: yield* randomUUIDv4, offset: 0 }
          : { ...revision, offset: revision.offset + 1 }
    })
    const touch = <A, E, R>(self: Effect.Effect<A, E, R>) =>
      Effect.tap(self, () =>
        Effect.sync(() => {
          changed = true
        })
      )
    const commit = <A, E, R>(self: Effect.Effect<A, E, R>, local: boolean) =>
      admit(
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            changed = false
            appended = false
            const before = revision
            // Keep publication masked with COMMIT, but the body (and admission above) cancellable.
            const value = yield* sql.withTransaction(restore(self))
            if (changed) yield* advance
            const change: LocalChange | null =
              local && appended && before.epoch === revision.epoch ? { before, after: revision } : null
            return { value, change }
          })
        )
      ).pipe(Effect.catchIf(isSqlError, (error) => SqliteRowStore.unavailable(Effect.fail(error))))
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
      const undoBySeq = new Map(Arr.map(undo, (r) => [r.seq, r.undo] as const))
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
      revision: Effect.sync(() => revision),
      invalidateRevision: advance,
      admit,
      pendingCount: sql.reactive(
        ["echo_outbox"],
        admit(
          Effect.map(
            run(sql<{ readonly n: number }>`SELECT count(*) AS n FROM echo_outbox WHERE released_at IS NULL`),
            (counted) => counted[0]?.n ?? 0
          )
        )
      ),
      // BEGIN/COMMIT failures map like any statement's; the body's own errors pass through unchanged.
      // `withTransaction` waits for the connection lease uninterruptibly; waiting for admission first makes
      // a transaction queued behind a long one (e.g. a pull install) cancellable while it waits. Admission
      // is FIFO (INV-CLI-15) and held through commit/rollback and revision publication. Both transaction
      // paths share this gate with `admit` and pending counts, so local receipts cannot bypass waiters.
      transaction: (self) => Effect.map(commit(self, false), ({ value }) => value),
      localCommit: (self) => commit(self, true),
      rows: {
        ...rows,
        write: (table, id, row) => touch(rows.write(table, id, row)),
        writeMany: (table, batch) => touch(rows.writeMany(table, batch)),
        purgeScope: (scope) => touch(rows.purgeScope(scope)),
        clearAll: touch(rows.clearAll)
      },
      meta: Effect.gen(function* () {
        const stored = yield* run(sql<{ readonly value: string }>`SELECT value FROM echo_meta WHERE key = 'sync_meta'`)
        const text = stored[0]?.value
        return text === undefined ? initialMeta : yield* decodeMeta(text)
      }),
      setMeta: (meta) =>
        touch(
          Effect.gen(function* () {
            const value = yield* encodeMeta(meta)
            yield* run(sql`INSERT INTO echo_meta (key, value) VALUES ('sync_meta', ${value})
          ON CONFLICT (key) DO UPDATE SET value = excluded.value`)
          })
        ),
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
          changed = true
          appended = true
        }),
      setUndo: (seq, undo) =>
        Effect.gen(function* () {
          const text = yield* encodeUndoImages(undo)
          yield* run(sql`UPDATE echo_outbox SET undo = ${text} WHERE seq = ${seq}`)
        }),
      release: (seqs) => touch(run(Outbox.markReleased(seqs))),
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
  })

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
