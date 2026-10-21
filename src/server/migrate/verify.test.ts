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

describe.skipIf(adminUrl === "")("generated migrations and the startup verifier (DESIGN §11, real Postgres)", () => {
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

  // Each case starts from a verified v1 database and breaks one thing by hand.
  const drifted: ReadonlyArray<[string, string, string]> = [
    ["a declared column dropped", "ALTER TABLE docs DROP COLUMN score", "docs: declared column score is missing"],
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
    ["capture detached", "DROP TRIGGER echo_sync_capture ON tags", "tags: not attached"]
  ]
  for (const [label, sabotage, expected] of drifted) {
    it.layer(EmptyDatabase)((db) => {
      db.effect(`the verifier refuses ${label}`, () =>
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* startUp(v1, yield* load([first]))
          assert.deepStrictEqual(yield* driftOf(v1), [])
          for (const statement of sabotage.split(";")) yield* sql.unsafe(statement)
          const mismatches = yield* driftOf(v1)
          assert.isTrue(
            mismatches.some((m) => m.startsWith(expected)),
            `expected a mismatch starting "${expected}", got ${JSON.stringify(mismatches)}`
          )
        })
      )
    })
  }
})
