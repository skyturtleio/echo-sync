import { Effect, Layer, Option, Redacted } from "effect"
import * as PgClient from "@effect/sql-pg/PgClient"
import { SqlClient } from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import type { ClientForbidden } from "./ledger.ts"
import { type Change, pull, type PullResponse, type ResnapshotRequired } from "./pull.ts"
import { attach, migrate, syncedTables } from "./schema.ts"

/**
 * Postgres tests run only when `ECHO_PG_URL` is set (see docs/server-substrate.md), so
 * `bun run check` stays usable without a database. `bun run test:pg` sets it.
 */
const adminUrl = process.env["ECHO_PG_URL"]
export const pgAvailable = adminUrl !== undefined && adminUrl !== ""

const admin = (statement: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* sql.unsafe(statement)
  }).pipe(Effect.provide(PgClient.layer({ url: Redacted.make(adminUrl ?? ""), maxConnections: 1 })))

/**
 * A fresh, migrated database per test, dropped afterwards, with one synced table `items` (and
 * `notes`, so table-agnostic paths see more than one table).
 */
export const TestDatabase = Layer.unwrap(
  Effect.gen(function* () {
    const name = `echo_test_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
    yield* admin(`CREATE DATABASE ${name}`)
    yield* Effect.addFinalizer(() => Effect.orDie(admin(`DROP DATABASE ${name} WITH (FORCE)`)))
    const url = new URL(adminUrl ?? "")
    url.pathname = `/${name}`
    const client = PgClient.layer({ url: Redacted.make(url.toString()), maxConnections: 80 })
    const setup = Layer.effectDiscard(
      Effect.gen(function* () {
        const sql = yield* SqlClient
        yield* migrate
        for (const table of ["items", "notes"]) {
          yield* sql`CREATE TABLE ${sql(table)} (
            id text PRIMARY KEY,
            access_scope_id text NOT NULL,
            body text NOT NULL DEFAULT ''
          )`
          yield* attach(table)
        }
      })
    )
    return Layer.provideMerge(setup, client)
  })
)

/** Creates scopes and grants `principal` read access to each. Grant ids are derived from the pair. */
export const grant = (principalId: string, ...scopeIds: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    for (const scopeId of scopeIds) {
      yield* sql`INSERT INTO echo_sync.access_scopes (id) VALUES (${scopeId}) ON CONFLICT DO NOTHING`
      yield* sql`INSERT INTO echo_sync.scope_grants (id, access_scope_id, principal_id, role)
                 VALUES (${`${scopeId}:${principalId}`}, ${scopeId}, ${principalId}, 'reader')
                 ON CONFLICT DO NOTHING`
    }
  })

export const revoke = (principalId: string, scopeId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* sql`DELETE FROM echo_sync.scope_grants WHERE access_scope_id = ${scopeId} AND principal_id = ${principalId}`
  })

/**
 * A minimal stand-in for a client replica (DESIGN.md §8, §9): stages pages and applies the whole
 * set, acks included, only at `PullDone`.
 */
export class Replica {
  readonly rows = new Map<string, { readonly scopeId: string; readonly row: unknown }>()
  readonly acked = new Set<number>()
  cursor: bigint | null = null
  continuation: Option.Option<string> = Option.none()
  /** Every change ever received, for leak checks. */
  readonly received: Array<Change> = []
  private staged: Array<Change> = []
  private stagedAcks: Array<number> = []

  constructor(
    readonly principalId: string,
    readonly clientId: string
  ) {}

  private stage(response: PullResponse) {
    this.received.push(...response.changes)
    this.staged.push(...response.changes)
    this.stagedAcks.push(...response.acks.map((a) => a.mutationId))
    if (response._tag === "PullPage") {
      this.continuation = Option.some(response.continuation)
      return
    }
    // A set started from cursor null replaces the replica (§8): nothing older survives.
    if (this.cursor === null) this.rows.clear()
    for (const change of this.staged) {
      if (change._tag === "Upsert") this.rows.set(`${change.table}/${change.rowId}`, change)
      else if (change._tag === "Delete") this.rows.delete(`${change.table}/${change.rowId}`)
      else for (const [key, row] of this.rows) if (row.scopeId === change.scopeId) this.rows.delete(key)
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
      pull({
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
    ): Effect.Effect<void, ResnapshotRequired | ClientForbidden | SqlError, SqlClient> =>
      remaining === 0
        ? Effect.die(`pull did not reach PullDone from cursor ${String(this.cursor)}`)
        : Effect.flatMap(this.pullOnce(limit), (response) =>
            response._tag === "PullPage" ? loop(remaining - 1) : Effect.void
          )
    return loop(1000)
  }

  /** Applied replica contents in a comparable form: `table/id` → row JSON. */
  view() {
    return new Map([...this.rows].map(([key, value]) => [key, JSON.stringify(value.row)]))
  }
}

/** What the principal should hold right now: every synced row (grants included) in a readable scope. */
export const serverView = (principalId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    const view = new Map<string, string>()
    for (const table of yield* syncedTables) {
      const rows = yield* sql<{ id: string; row: unknown }>`
        SELECT t.id, to_jsonb(t) AS row FROM ${sql(table.schema)}.${sql(table.name)} t
        WHERE t.access_scope_id IN (
          SELECT access_scope_id FROM echo_sync.scope_grants WHERE principal_id IN (${principalId}, '*'))`
      for (const row of rows) view.set(`${table.name}/${row.id}`, JSON.stringify(row.row))
    }
    return view
  })
