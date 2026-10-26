import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer, Redacted, Schema } from "effect"
import * as PgClient from "@effect/sql-pg/PgClient"
import { SqlClient } from "effect/sql/SqlClient"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { join } from "node:path"
import { AccessScope, Migrate, Registry, SyncTable } from "echo-sync"
import { attach, migrate } from "../postgres/schema.ts"
import { migrateApp, verify } from "./index.ts"
import type { Drift } from "./index.ts"

const adminUrl = process.env["ECHO_PG_URL"] ?? ""

/** A fresh, empty database (engine schema not yet migrated), dropped afterwards. */
const EmptyDatabase = Layer.unwrap(
  Effect.gen(function* () {
    const admin = (statement: string) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        yield* sql.unsafe(statement)
      }).pipe(Effect.provide(PgClient.layer({ url: Redacted.make(adminUrl), maxConnections: 1 })))
    const name = `echo_migrate_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
    yield* admin(`CREATE DATABASE ${name}`)
    yield* Effect.addFinalizer(() => Effect.orDie(admin(`DROP DATABASE ${name} WITH (FORCE)`)))
    const url = new URL(adminUrl)
    url.pathname = `/${name}`
    return PgClient.layer({ url: Redacted.make(url.toString()), maxConnections: 4 })
  })
)

const Id = Schema.String.pipe(Schema.brand("verify/Id"))
const base = { id: Id, accessScopeId: AccessScope.AccessScopeId }
const docsFields = {
  ...base,
  title: Schema.String,
  label: Schema.NullOr(Schema.String),
  rank: Schema.Int,
  score: Schema.Finite,
  done: Schema.Boolean,
  at: Schema.NullOr(Schema.Date),
  meta: Schema.Struct({ tags: Schema.Array(Schema.String) })
}
const DocsV1 = SyncTable.make("docs", docsFields, {
  indexes: { by_label: ["label", "rank"], by_at: ["at"] },
  unique: [["title", "rank"]]
})
const Tags = SyncTable.make("tags", { ...base, name: Schema.String })
const v1 = Registry.make([DocsV1, Tags])
// v2: a nullable column, a NOT NULL column with a default, a changed index, a new index.
const DocsV2 = SyncTable.make(
  "docs",
  { ...docsFields, note: Schema.NullOr(Schema.String), pinned: Schema.Boolean },
  { indexes: { by_label: ["rank", "label"], by_at: ["at"], by_done: ["done"] }, unique: [["title", "rank"]] }
)
const v2 = Registry.make([DocsV2, Tags])

/** Writes generated sources as real modules and imports them, as an app's migrations folder is. */
const load = (generated: ReadonlyArray<Migrate.Generated>) =>
  Effect.acquireRelease(
    Effect.sync(() => mkdtempSync(join(import.meta.dirname, ".generated-"))),
    (dir) => Effect.sync(() => rmSync(dir, { recursive: true, force: true }))
  ).pipe(
    Effect.flatMap((dir) =>
      Effect.forEach(generated, (g) =>
        Effect.promise(async () => {
          const file = join(dir, `${g.id}.ts`)
          writeFileSync(file, g.source)
          const module: { readonly default: Effect.Effect<void, unknown, SqlClient> } = await import(file)
          return [g.id, module.default] as const
        })
      )
    ),
    Effect.map((entries) => Object.fromEntries(entries))
  )

/** Engine migrate, the app migrations, attach every table, as server startup does. */
const startUp = (
  registry: Registry.SyncRegistry,
  migrations: Record<string, Effect.Effect<void, unknown, SqlClient>>
) =>
  Effect.gen(function* () {
    yield* migrate
    yield* migrateApp(migrations)
    for (const table of registry.tables) if (table.name !== "scope_grants") yield* attach(table)
  })

const driftOf = (registry: Registry.SyncRegistry) =>
  verify(registry).pipe(
    Effect.as<ReadonlyArray<string>>([]),
    Effect.catchTag("Drift", (d: Drift) => Effect.succeed(d.mismatches.map((m) => `${m.table}: ${m.problem}`)))
  )

const first = Migrate.generate({ previous: Migrate.emptySnapshot, registry: v1, existing: [], name: "init" })
const second = Migrate.generate({
  previous: Migrate.decodeSnapshot(first.snapshot),
  registry: v2,
  intent: { defaults: { "docs.pinned": "false" } },
  existing: [first.id],
  name: "more"
})

describe.skipIf(adminUrl === "")("generated migrations and the startup verifier (SPEC §13, real Postgres)", () => {
  it.live(
    "a legacy app gets only scope-walk indexes, reports missing-index Drift until migrated, and keeps client hashes",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        // Recreate the committed pre-index snapshot and DDL, not a second current declaration.
        const legacy = Migrate.decodeSnapshot(
          JSON.stringify({
            version: 1,
            tables: Migrate.snapshotOf(v1).tables.map((table) => ({ ...table, scopeWalkIndex: undefined }))
          })
        )
        yield* migrate
        for (const table of [DocsV1, Tags]) {
          yield* sql.unsafe(table.postgres.createTable)
          for (const ddl of table.postgres.createIndexes) {
            if (!ddl.includes("__echo-scope")) yield* sql.unsafe(ddl)
          }
          yield* attach(table)
        }
        const missing = yield* driftOf(v1)
        assert.strictEqual(missing.length, 2)
        assert.isTrue(missing.every((m) => m.includes("__echo-scope") && m.includes("is missing")))
        const upgrade = Migrate.generate({ previous: legacy, registry: v1, existing: ["001_init"], name: "scope_walk" })
        assert.strictEqual(upgrade.steps.length, 2)
        const migrations = yield* load([upgrade])
        yield* migrateApp(migrations)
        assert.deepStrictEqual(yield* driftOf(v1), [])
        const current = Migrate.decodeSnapshot(upgrade.snapshot)
        assert.deepStrictEqual(
          current.tables.map((t) => [t.dataHash, t.indexHash]),
          legacy.tables.map((t) => [t.dataHash, t.indexHash])
        )
        assert.deepStrictEqual(Migrate.plan(current, v1), [])
        // Existing engine databases also need the grants index; 001 cannot be edited/replayed.
        yield* sql`DROP INDEX echo_sync."scope_grants__echo-scope"`
        yield* sql`DELETE FROM echo_sync.migrations WHERE migration_id = 5`
        yield* migrate
        assert.deepStrictEqual(yield* driftOf(v1), [])
        // These names were already legal. The implicit index must not take either name.
        const sibling = SyncTable.make("docs__echo_scope", base, { indexes: { echo_scope: ["accessScopeId"] } })
        const expanded = Registry.make([DocsV1, Tags, sibling])
        const addition = Migrate.generate({
          previous: current,
          registry: expanded,
          existing: [upgrade.id],
          name: "sibling"
        })
        yield* startUp(expanded, yield* load([addition]))
        assert.deepStrictEqual(yield* driftOf(expanded), [])
        // Baseline hashes pinned from the pre-change example's committed manifest, not computed
        // by removing the marker from a new manifest (which could hide a hash regression).
        const todos = SyncTable.make("todos", { ...base, title: Schema.String, note: Schema.NullOr(Schema.String) })
        assert.strictEqual(todos.manifest.dataHash, "7b38fc8e9d381bf9")
        assert.strictEqual(todos.manifest.indexHash, "cd0e6abb0225ca58")
      }).pipe(Effect.scoped, Effect.provide(EmptyDatabase))
  )

  it.layer(EmptyDatabase)((db) => {
    db.effect(
      "generated modules applied to an empty database pass the verifier, and the NOT NULL default is gone",
      () =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          const migrations = yield* load([first, second])
          yield* startUp(v1, { [first.id]: migrations[first.id] ?? Effect.die("missing") })
          assert.deepStrictEqual(yield* driftOf(v1), [])
          yield* sql`INSERT INTO docs (id, access_scope_id, title, label, rank, score, done, at, meta)
                   VALUES ('d1', 's', 't', null, 1, 0.5, true, null, '{"tags":[]}')`
          yield* startUp(v2, migrations)
          assert.deepStrictEqual(yield* driftOf(v2), [])
          // The default backfilled the existing row once and is not left in the catalog.
          const [row] = yield* sql<{ pinned: boolean }>`SELECT pinned FROM docs WHERE id = 'd1'`
          assert.strictEqual(row?.pinned, false)
          const defaults = yield* sql<{ n: number }>`
          SELECT count(*)::int AS n FROM pg_attrdef d
          JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
          WHERE d.adrelid = 'docs'::regclass AND a.attname = 'pinned'`
          assert.strictEqual(defaults[0]?.n, 0)
        })
    )
  })

  // Each case starts from a verified v1 database and breaks one thing by hand. A statement
  // prefixed with FAILS is expected to fail.
  const FAILS = "!"
  const drifted: ReadonlyArray<[string, string, string]> = [
    ["a declared column dropped", "ALTER TABLE docs DROP COLUMN score", "docs: declared column score is missing"],
    [
      "a scope-walk index with its key columns reversed",
      'DROP INDEX "docs__echo-scope";CREATE INDEX "docs__echo-scope" ON docs (id ASC, access_scope_id ASC NULLS FIRST)',
      "docs: index docs__echo-scope is"
    ],
    [
      "an index with its key columns reordered",
      "DROP INDEX docs__by_label;CREATE INDEX docs__by_label ON docs (rank ASC NULLS FIRST, label ASC NULLS FIRST, id ASC)",
      "docs: index docs__by_label is"
    ],
    [
      "an index with NULLS LAST instead of NULLS FIRST",
      "DROP INDEX docs__by_at;CREATE INDEX docs__by_at ON docs (at ASC NULLS LAST, id ASC)",
      "docs: index docs__by_at is"
    ],
    [
      'a text column without COLLATE "C"',
      'ALTER TABLE tags ALTER COLUMN name TYPE text COLLATE "default"',
      "tags: column name has collation"
    ],
    [
      "a NOT NULL column made nullable",
      "ALTER TABLE docs ALTER COLUMN title DROP NOT NULL",
      "docs: column title is nullable"
    ],
    [
      "a unique set removed",
      "ALTER TABLE docs DROP CONSTRAINT docs_title_rank_key",
      "docs: declared unique set (title, rank)"
    ],
    ["capture detached", "DROP TRIGGER echo_sync_capture ON tags", "tags: not attached"],
    [
      "a disabled capture trigger on scope_grants (never re-attached at startup)",
      "ALTER TABLE echo_sync.scope_grants DISABLE TRIGGER echo_sync_capture",
      "scope_grants: echo_sync capture trigger"
    ],
    [
      "a second captured relation of the same name, in a schema sorting after the good one",
      "CREATE SCHEMA zz;CREATE TABLE zz.tags (id text PRIMARY KEY);CREATE TRIGGER echo_sync_capture AFTER INSERT OR UPDATE OR DELETE ON zz.tags FOR EACH ROW EXECUTE FUNCTION echo_sync.echo_sync_capture_changes();CREATE TRIGGER echo_sync_reject_truncate BEFORE TRUNCATE ON zz.tags FOR EACH STATEMENT EXECUTE FUNCTION echo_sync.reject_truncate()",
      "tags: (in schema zz) declared column name is missing"
    ],
    [
      "two well-formed captured relations of the same name (the log cannot tell them apart)",
      "CREATE SCHEMA zz;CREATE TABLE zz.tags (LIKE public.tags INCLUDING ALL);CREATE TRIGGER echo_sync_capture AFTER INSERT OR UPDATE OR DELETE ON zz.tags FOR EACH ROW EXECUTE FUNCTION echo_sync.echo_sync_capture_changes();CREATE TRIGGER echo_sync_reject_truncate BEFORE TRUNCATE ON zz.tags FOR EACH STATEMENT EXECUTE FUNCTION echo_sync.reject_truncate()",
      "tags: captured in 2 schemas (public, zz)"
    ],
    [
      "a disabled truncate guard",
      "ALTER TABLE tags DISABLE TRIGGER echo_sync_reject_truncate",
      "tags: echo_sync truncate guard trigger"
    ],
    [
      "a capture trigger restricted to INSERT",
      "CREATE OR REPLACE TRIGGER echo_sync_capture AFTER INSERT ON tags FOR EACH ROW EXECUTE FUNCTION echo_sync.echo_sync_capture_changes()",
      "tags: echo_sync capture trigger"
    ],
    [
      "a capture trigger with a WHEN clause",
      "CREATE OR REPLACE TRIGGER echo_sync_capture AFTER INSERT OR UPDATE OR DELETE ON tags FOR EACH ROW WHEN (pg_trigger_depth() < 1) EXECUTE FUNCTION echo_sync.echo_sync_capture_changes()",
      "tags: echo_sync capture trigger"
    ],
    [
      "a capture trigger that fires only for UPDATE OF one column",
      "CREATE OR REPLACE TRIGGER echo_sync_capture AFTER INSERT OR UPDATE OF name OR DELETE ON tags FOR EACH ROW EXECUTE FUNCTION echo_sync.echo_sync_capture_changes()",
      "tags: echo_sync capture trigger"
    ],
    [
      "a declared index marked invalid",
      "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'docs__by_at'::regclass",
      "docs: index docs__by_at is"
    ],
    [
      "a unique set backed only by an index left invalid by a failed CREATE UNIQUE INDEX CONCURRENTLY",
      `ALTER TABLE docs DROP CONSTRAINT docs_title_rank_key;INSERT INTO docs (id, access_scope_id, title, label, rank, score, done, at, meta) VALUES ('d1', 's', 't', null, 1, 0, true, null, '{"tags":[]}'), ('d2', 's', 't', null, 1, 0, true, null, '{"tags":[]}');${FAILS}CREATE UNIQUE INDEX CONCURRENTLY docs_title_rank_key ON docs (title, rank)`,
      "docs: declared unique set (title, rank)"
    ],
    [
      "a unique set (title, rank) replaced by UNIQUE (title) INCLUDE (rank)",
      "ALTER TABLE docs DROP CONSTRAINT docs_title_rank_key;CREATE UNIQUE INDEX docs_title_inc ON docs (title) INCLUDE (rank)",
      "docs: declared unique set (title, rank)"
    ],
    [
      "a unique set made NULLS NOT DISTINCT",
      "ALTER TABLE docs DROP CONSTRAINT docs_title_rank_key;CREATE UNIQUE INDEX docs_title_rank_nnd ON docs (title, rank) NULLS NOT DISTINCT",
      "docs: declared unique set (title, rank)"
    ],
    [
      "a unique set whose key is DESC",
      "ALTER TABLE docs DROP CONSTRAINT docs_title_rank_key;CREATE UNIQUE INDEX docs_title_rank_desc ON docs (title, rank DESC)",
      "docs: declared unique set (title, rank)"
    ]
  ]
  for (const [label, sabotage, expected] of drifted) {
    it.layer(EmptyDatabase)((db) => {
      db.effect(`the verifier refuses ${label}`, () =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* startUp(v1, yield* load([first]))
          assert.deepStrictEqual(yield* driftOf(v1), [])
          for (const statement of sabotage.split(";")) {
            if (!statement.startsWith(FAILS)) {
              yield* sql.unsafe(statement)
              continue
            }
            // The sabotage itself must fail (leaving its debris behind), or the case tests nothing.
            const exit = yield* Effect.exit(sql.unsafe(statement.slice(FAILS.length)))
            assert.isTrue(exit._tag === "Failure", `expected "${statement}" to fail`)
          }
          const mismatches = yield* driftOf(v1)
          assert.isTrue(
            mismatches.some((m) => m.startsWith(expected)),
            `expected a mismatch starting "${expected}", got ${JSON.stringify(mismatches)}`
          )
        })
      )
    })
  }
  // Changes the policy allows: INCLUDE payload is not key; extra columns and indexes are fine.
  const accepted: ReadonlyArray<[string, string]> = [
    [
      "a declared index rebuilt as a covering index with an INCLUDE payload",
      "DROP INDEX docs__by_at;CREATE INDEX docs__by_at ON docs (at ASC NULLS FIRST, id ASC) INCLUDE (title)"
    ],
    [
      "a unique set backed by a unique index with an INCLUDE payload",
      "ALTER TABLE docs DROP CONSTRAINT docs_title_rank_key;CREATE UNIQUE INDEX docs_title_rank_inc ON docs (title, rank) INCLUDE (score)"
    ],
    [
      "an extra undeclared column and index",
      "ALTER TABLE docs ADD COLUMN extra int;CREATE INDEX docs_extra ON docs (extra)"
    ]
  ]
  for (const [label, change] of accepted) {
    it.layer(EmptyDatabase)((db) => {
      db.effect(`the verifier accepts ${label}`, () =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* startUp(v1, yield* load([first]))
          for (const statement of change.split(";")) yield* sql.unsafe(statement)
          assert.deepStrictEqual(yield* driftOf(v1), [])
        })
      )
    })
  }
  // The TODO sequence for renames and drops, carried out by hand exactly as written, reaches a
  // database the verifier accepts: table + column rename, a dropped column whose index the DROP
  // COLUMN removes and is then recreated under its name, and an index replaced under its name on
  // unchanged columns after a column rename.
  const NotesV1 = SyncTable.make(
    "notes",
    { ...base, title: Schema.String, legacy: Schema.NullOr(Schema.String), rank: Schema.Int },
    { indexes: { by_title: ["title"], by_legacy: ["legacy"], by_rank: ["rank"] } }
  )
  const CardsV1 = SyncTable.make(
    "cards",
    { ...base, title: Schema.String, rank: Schema.Int },
    { indexes: { by_title: ["title"] } }
  )
  const PapersV2 = SyncTable.make(
    "papers",
    { ...base, heading: Schema.String, rank: Schema.Int },
    { indexes: { by_title: ["heading", "rank"], by_rank: ["rank"], by_legacy: ["rank", "heading"] } }
  )
  const CardsV2 = SyncTable.make(
    "cards",
    { ...base, heading: Schema.String, rank: Schema.Int },
    { indexes: { by_title: ["rank"] } }
  )
  const renameV1 = Registry.make([NotesV1, CardsV1])
  const renameV2 = Registry.make([PapersV2, CardsV2])
  const renameFirst = Migrate.generate({
    previous: Migrate.emptySnapshot,
    registry: renameV1,
    existing: [],
    name: "init"
  })
  const renameSecond = Migrate.generate({
    previous: Migrate.decodeSnapshot(renameFirst.snapshot),
    registry: renameV2,
    intent: {
      renames: { tables: { papers: "notes" }, columns: { papers: { heading: "title" }, cards: { heading: "title" } } },
      acknowledgeDrops: ["papers.legacy"]
    },
    existing: [renameFirst.id],
    name: "rename"
  })
  it.layer(EmptyDatabase)((db) => {
    db.effect("the hand-carried TODO sequence for renames and drops passes the verifier", () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        const migrations = yield* load([renameFirst, renameSecond])
        yield* startUp(renameV1, { [renameFirst.id]: migrations[renameFirst.id] ?? Effect.die("missing") })
        assert.deepStrictEqual(yield* driftOf(renameV1), [])
        yield* sql`INSERT INTO notes (id, access_scope_id, title, legacy, rank) VALUES ('n1', 's', 't', 'x', 1)`
        const hand = renameSecond.steps.flatMap((s) =>
          s._tag === "Todo"
            ? s.lines.filter((l) => /^ {2}[A-Z]+ [^<]*;$/.test(l)).map((l) => l.trim().slice(0, -1))
            : []
        )
        assert.isTrue(hand.length > 0)
        assert.isFalse(renameSecond.steps.some((s) => s._tag === "Ddl"))
        for (const statement of hand) yield* sql.unsafe(statement)
        yield* startUp(renameV2, migrations)
        assert.deepStrictEqual(yield* driftOf(renameV2), [])
        const [row] = yield* sql<{ heading: string }>`SELECT heading FROM papers WHERE id = 'n1'`
        assert.strictEqual(row?.heading, "t")
      })
    )
  })
})
