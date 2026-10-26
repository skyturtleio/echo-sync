import { Clock, Config, Effect, Layer, Option, Random, Redacted, Schema } from "effect"
import * as PgClient from "@effect/sql-pg/PgClient"
import { SqlClient } from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import { AccessScope, type Protocol, Registry, ScopeGrants, SyncTable } from "echo-sync"
import type { ClientForbidden } from "./ledger.ts"
import { pull } from "./pull.ts"
import { decodeRow, q, selectList } from "./rowStore.ts"
import { attach, migrate, resolveTables } from "./schema.ts"

export const RowId = Schema.String.pipe(Schema.brand("test/RowId"))
const fields = { id: RowId, accessScopeId: AccessScope.AccessScopeId, body: Schema.String }
export const Items = SyncTable.make("items", fields)
export const Notes = SyncTable.make("notes", fields)
/** The app's tables in the tests (scope_grants is added by `Registry.make`). */
export const registry = Registry.make([Items, Notes])

/** A SyncTable with only the required columns, for tables a test creates itself. */
export const bareTable = (name: string) => SyncTable.make(name, { id: RowId, accessScopeId: AccessScope.AccessScopeId })

/** Creates a synced table from its SyncTable DDL, as an app migration would, and attaches it. */
export const createSynced = (table: SyncTable.AnySyncTable) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* sql.unsafe(table.postgres.createTable)
    for (const ddl of table.postgres.createIndexes) yield* sql.unsafe(ddl)
    yield* attach(table)
  })

/**
 * Postgres tests run only when `ECHO_PG_URL` is set (see AGENTS.md "Local environment"), so
 * `bun run check` stays usable without a database. `bun run test:pg` sets it.
 */
const adminUrl = Effect.runSync(Config.String("ECHO_PG_URL").pipe(Config.withDefault("")))
export const pgAvailable = adminUrl !== ""

const admin = (statement: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* sql.unsafe(statement)
  }).pipe(Effect.provide(PgClient.layer({ url: Redacted.make(adminUrl), maxConnections: 1 })))

/** A fresh, empty database (no engine schema), dropped afterwards; `with` is appended to CREATE DATABASE. */
export const emptyDatabase = (with_ = "") =>
  Layer.unwrap(
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis
      const random = yield* Random.next
      const name = `echo_test_${now.toString(36)}_${random.toString(36).slice(2, 8)}`
      yield* admin(`CREATE DATABASE ${name} ${with_}`)
      yield* Effect.addFinalizer(() => Effect.orDie(admin(`DROP DATABASE ${name} WITH (FORCE)`)))
      const url = new URL(adminUrl)
      url.pathname = `/${name}`
      return PgClient.layer({ url: Redacted.make(url.toString()), maxConnections: 80 })
    })
  )

/** A fresh, empty database per test (no engine schema), dropped afterwards. */
export const EmptyDatabase = emptyDatabase()

/**
 * A fresh, migrated database per test, dropped afterwards, with one synced table `items` (and
 * `notes`, so table-agnostic paths see more than one table).
 */
export const TestDatabase = Layer.provideMerge(
  Layer.effectDiscard(
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* migrate
      for (const table of [Items, Notes]) {
        yield* createSynced(table)
        // Lets tests write raw SQL without naming every column; not part of the SyncTable.
        yield* sql`ALTER TABLE ${sql(table.name)} ALTER COLUMN body SET DEFAULT ''`
      }
    })
  ),
  EmptyDatabase
)

/** Creates scopes and grants `principal` read access to each. Grant ids are derived from the pair. */
export const grant = (principalId: string, ...scopeIds: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    for (const scopeId of scopeIds) {
      yield* sql`INSERT INTO echo_sync.access_scopes (id) VALUES (${scopeId}) ON CONFLICT DO NOTHING`
      const id = ScopeGrants.grantId(AccessScope.AccessScopeId.make(scopeId), AccessScope.PrincipalId.make(principalId))
      yield* sql`INSERT INTO echo_sync.scope_grants (id, access_scope_id, principal_id, role, granted_at)
                 VALUES (${id}, ${scopeId}, ${principalId}, 'reader', now())
                 ON CONFLICT DO NOTHING`
    }
  })

export const revoke = (principalId: string, scopeId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* sql`DELETE FROM echo_sync.scope_grants WHERE access_scope_id = ${scopeId} AND principal_id = ${principalId}`
  })

/**
 * A minimal stand-in for a client replica (SPEC §7, §9): stages pages and applies the whole
 * set, acks included, only at `PullDone`.
 */
export class Replica {
  readonly rows = new Map<string, SyncTable.EncodedRow>()
  readonly acked = new Set<string>()
  cursor: bigint | null = null
  continuation: Option.Option<string> = Option.none()
  /** Every change ever received, for leak checks. */
  readonly received: Array<Protocol.Change> = []
  private staged: Array<Protocol.Change> = []
  private stagedAcks: Array<string> = []
  readonly principalId: string
  readonly clientId: string

  readonly registry: Registry.SyncRegistry

  constructor(principalId: string, clientId: string, tables: Registry.SyncRegistry = registry) {
    this.principalId = principalId
    this.clientId = clientId
    this.registry = tables
  }

  private stage(response: Protocol.PullResponse) {
    this.received.push(...response.changes)
    this.staged.push(...response.changes)
    this.stagedAcks.push(...response.acks.map((a) => a.mutationId))
    if (response._tag === "PullPage") {
      this.continuation = Option.some(response.continuation)
      return
    }
    // A set started from cursor null replaces the replica (INV-CLI-2): nothing older survives.
    if (this.cursor === null) this.rows.clear()
    for (const change of this.staged) {
      if (change._tag === "upsert") this.rows.set(`${change.table}/${String(change.row["id"])}`, change.row)
      else if (change._tag === "tombstone") this.rows.delete(`${change.table}/${change.id}`)
      else for (const [key, row] of this.rows) if (row["access_scope_id"] === change.scopeId) this.rows.delete(key)
    }
    for (const id of this.stagedAcks) this.acked.add(id)
    this.staged = []
    this.stagedAcks = []
    this.cursor = response.nextCursor
    this.continuation = Option.none()
  }

  /** One pull request, staged (and applied if it completes the set). */
  pullOnce(limit: number) {
    return Effect.map(
      pull(this.registry, {
        principalId: this.principalId,
        clientId: this.clientId,
        cursor: this.cursor,
        continuation: this.continuation,
        limit
      }),
      (response) => {
        this.stage(response)
        return response
      }
    )
  }

  /** Drops the cursor and staged pages, as a client does on `ResnapshotRequired`. */
  resnapshot() {
    this.cursor = null
    this.continuation = Option.none()
    this.staged = []
    this.stagedAcks = []
  }

  /** Pulls until a page set completes. */
  sync(limit = 1000) {
    const loop = (
      remaining: number
    ): Effect.Effect<
      void,
      | Protocol.ResnapshotRequired
      | Protocol.ServerUnavailable
      | Protocol.ClientOutdated
      | Protocol.Unauthorized
      | ClientForbidden
      | SqlError,
      SqlClient
    > =>
      remaining === 0
        ? Effect.die(`pull did not reach PullDone from cursor ${String(this.cursor)}`)
        : Effect.flatMap(this.pullOnce(limit), (response) =>
            response._tag === "PullPage" ? loop(remaining - 1) : Effect.void
          )
    return loop(1000)
  }

  /** Applied replica contents in a comparable form: `table/id` → row JSON. */
  view() {
    return new Map([...this.rows].map(([key, row]) => [key, JSON.stringify(row)]))
  }
}

/** What the principal should hold right now: every synced row (grants included) in a readable scope. */
export const serverView = (principalId: string, tables: Registry.SyncRegistry = registry) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    const view = new Map<string, string>()
    for (const { table, schema } of yield* resolveTables(tables)) {
      const rows = yield* sql.unsafe<Record<string, unknown>>(
        `SELECT ${selectList(table, "t")} FROM ${q(schema)}.${q(table.name)} t
         WHERE t.access_scope_id IN (
           SELECT access_scope_id FROM echo_sync.scope_grants WHERE principal_id IN ($1, '*'))`,
        [principalId]
      )
      for (const raw of rows) {
        const row = yield* decodeRow(table, raw)
        view.set(`${table.name}/${String(row["id"])}`, JSON.stringify(row))
      }
    }
    return view
  })
