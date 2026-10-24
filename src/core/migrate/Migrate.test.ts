import { describe, expect, it } from "vitest"
import { Schema } from "effect"
import { AccessScopeId } from "../AccessScope.ts"
import * as Registry from "../Registry.ts"
import * as SyncTable from "../SyncTable.ts"
import * as Migrate from "./Migrate.ts"

const Id = Schema.String.pipe(Schema.brand("migrate/Id"))
const base = { id: Id, accessScopeId: AccessScopeId }

const snapshotOf = (...tables: ReadonlyArray<SyncTable.AnySyncTable>) =>
  Migrate.decodeSnapshot(Migrate.encodeSnapshot(Migrate.snapshotOf(Registry.make(tables))))
const ddlOf = (steps: ReadonlyArray<Migrate.Step>) => steps.flatMap((s) => (s._tag === "Ddl" ? [s.sql] : []))
const todos = (steps: ReadonlyArray<Migrate.Step>) => steps.filter((s) => s._tag === "Todo")
const lines = (steps: ReadonlyArray<Migrate.Step>) =>
  todos(steps).map((s) => (s._tag === "Todo" ? s.lines.join("\n") : ""))
/** The complete SQL statements the TODOs ask for, in the order a reader carries them out. */
const handStatements = (steps: ReadonlyArray<Migrate.Step>) =>
  todos(steps).flatMap((s) =>
    s._tag === "Todo" ? s.lines.filter((l) => /^ {2}[A-Z]+ [^<]*;$/.test(l)).map((l) => l.trim()) : []
  )

describe("the generator never emits DDL for a rename, drop, lossy type change or unique-set change (SPEC §13)", () => {
  const v1 = SyncTable.make(
    "docs",
    { ...base, title: Schema.String, rank: Schema.Int, body: Schema.String, legacy: Schema.String, extra: Schema.Int },
    { unique: [["title"]] }
  )
  const gone = SyncTable.make("gone", base)
  const before = snapshotOf(v1, gone)
  // title renamed to heading, legacy and extra dropped, rank integer → text, unique set moved
  // to body, table `gone` dropped, `gone` renamed to `kept`.
  const v2 = SyncTable.make(
    "docs",
    { ...base, heading: Schema.String, rank: Schema.String, body: Schema.String },
    { unique: [["body"]] }
  )
  const kept = SyncTable.make("kept", base)

  const cases: ReadonlyArray<[string, ReadonlyArray<SyncTable.AnySyncTable>, Migrate.Intent]> = [
    [
      "without intent (except the rename, which is otherwise an add + drop)",
      [v2],
      { renames: { columns: { docs: { heading: "title" } } } }
    ],
    [
      "with rename and drop intents",
      [v2, kept],
      {
        renames: { tables: { kept: "gone" }, columns: { docs: { heading: "title" } } },
        acknowledgeDrops: ["docs.legacy", "docs.extra"]
      }
    ]
  ]
  it.each(cases)("%s", (_, tables, intent) => {
    const steps = Migrate.plan(before, Registry.make(tables), intent)
    expect(ddlOf(steps)).toEqual([])
    const text = todos(steps)
      .flatMap((s) => (s._tag === "Todo" ? s.lines : []))
      .join("\n")
    for (const expected of [
      'RENAME COLUMN "title" TO "heading"',
      'DROP COLUMN "legacy"',
      'DROP COLUMN "extra"',
      "docs.rank changes kind integer → text",
      'ADD UNIQUE ("body")',
      "Unique set (title) removed"
    ]) {
      expect(text).toContain(expected)
    }
    expect(text).toContain(tables.length === 2 ? 'ALTER TABLE "gone" RENAME TO "kept"' : 'DROP TABLE "gone"')
    // The rendered module carries the TODOs as comments only.
    const source = Migrate.render("002_x", steps)
    expect(source).not.toContain("sql.unsafe")
    expect(
      source
        .split("\n")
        .filter((l) => /RENAME|DROP (COLUMN|TABLE)|ADD UNIQUE/.test(l))
        .every((l) => l.trimStart().startsWith("// TODO(migration)"))
    ).toBe(true)
  })

  it("an intent naming something the diff does not contain is refused, not ignored", () => {
    const registry = Registry.make([v2])
    expect(() => Migrate.plan(before, registry, { defaults: { "docs.body": "''" } })).toThrow(
      Migrate.MigrationPlanError
    )
    expect(() => Migrate.plan(before, registry, { acknowledgeDrops: ["docs.body"] })).toThrow(
      Migrate.MigrationPlanError
    )
    expect(() => Migrate.plan(before, registry, { renames: { columns: { docs: { heading: "nope" } } } })).toThrow(
      Migrate.MigrationPlanError
    )
  })
})

describe("DDL that depends on a TODO rename or kind change is a TODO too (SPEC §13)", () => {
  it("a combined table and column rename is one table TODO and one column TODO, no DDL, no intent error", () => {
    const v1 = SyncTable.make("docs", { ...base, title: Schema.String, body: Schema.String })
    const v2 = SyncTable.make("articles", { ...base, heading: Schema.String, body: Schema.String })
    const steps = Migrate.plan(snapshotOf(v1), Registry.make([v2]), {
      renames: { tables: { articles: "docs" }, columns: { articles: { heading: "title" } } }
    })
    expect(ddlOf(steps)).toEqual([])
    const text = lines(steps)
    expect(text.length).toBe(2)
    expect(text[0]).toContain('ALTER TABLE "docs" RENAME TO "articles"')
    expect(text[1]).toContain('ALTER TABLE "articles" RENAME COLUMN "title" TO "heading"')
  })

  it("an index on a renamed column is a TODO; unrelated changes in the same table stay DDL", () => {
    const v1 = SyncTable.make(
      "docs",
      { ...base, title: Schema.String, rank: Schema.Int },
      { indexes: { by_title: ["title"] } }
    )
    const v2 = SyncTable.make(
      "docs",
      { ...base, heading: Schema.String, rank: Schema.Int, note: Schema.NullOr(Schema.String) },
      { indexes: { by_heading: ["heading"], by_rank: ["rank"] } }
    )
    const steps = Migrate.plan(snapshotOf(v1), Registry.make([v2]), {
      renames: { columns: { docs: { heading: "title" } } }
    })
    expect(ddlOf(steps)).toEqual([
      'ALTER TABLE "docs" ADD COLUMN "note" text COLLATE "C"',
      'CREATE INDEX "docs__by_rank" ON "docs" ("rank" ASC NULLS FIRST, "id" ASC)'
    ])
    expect(handStatements(steps)).toEqual([
      'ALTER TABLE "docs" RENAME COLUMN "title" TO "heading";',
      'DROP INDEX "docs__by_title";',
      'CREATE INDEX "docs__by_heading" ON "docs" ("heading" ASC NULLS FIRST, "id" ASC);'
    ])
  })

  // The replacement reuses the index name, so its CREATE waits for the blocked DROP even
  // though the new key uses only unchanged columns.
  const replaced = SyncTable.make(
    "docs",
    { ...base, rank: Schema.Int, title: Schema.String },
    { indexes: { by_title: ["title"] } }
  )
  const replacements: ReadonlyArray<[string, SyncTable.AnySyncTable, Migrate.Intent, ReadonlyArray<string>]> = [
    [
      "rename",
      SyncTable.make(
        "docs",
        { ...base, rank: Schema.Int, heading: Schema.String },
        { indexes: { by_title: ["rank"] } }
      ),
      { renames: { columns: { docs: { heading: "title" } } } },
      ['ALTER TABLE "docs" RENAME COLUMN "title" TO "heading";', 'DROP INDEX "docs__by_title";']
    ],
    [
      "drop",
      SyncTable.make("docs", { ...base, rank: Schema.Int }, { indexes: { by_title: ["rank"] } }),
      // The index goes with the column: no DROP INDEX, only the replacement CREATE after it.
      { acknowledgeDrops: ["docs.title"] },
      ['ALTER TABLE "docs" DROP COLUMN "title";']
    ],
    [
      "kind change",
      SyncTable.make("docs", { ...base, rank: Schema.Int, title: Schema.Int }, { indexes: { by_title: ["rank"] } }),
      // Hand-carried add/backfill/drop may or may not have removed it.
      {},
      ['DROP INDEX IF EXISTS "docs__by_title";']
    ]
  ]
  it.each(replacements)(
    "an index replaced under its name after a column %s is one TODO unit, DROP before CREATE",
    (_, v2, intent, before) => {
      const steps = Migrate.plan(snapshotOf(replaced), Registry.make([v2]), intent)
      expect(ddlOf(steps)).toEqual([])
      expect(handStatements(steps)).toEqual([
        ...before,
        'CREATE INDEX "docs__by_title" ON "docs" ("rank" ASC NULLS FIRST, "id" ASC);'
      ])
    }
  )

  it("an index whose new key references a renamed column is one TODO unit, though its old key is unchanged", () => {
    const v1 = SyncTable.make(
      "docs",
      { ...base, title: Schema.String, rank: Schema.Int },
      { indexes: { by_rank: ["rank"] } }
    )
    const v2 = SyncTable.make(
      "docs",
      { ...base, heading: Schema.String, rank: Schema.Int },
      { indexes: { by_rank: ["rank", "heading"] } }
    )
    const steps = Migrate.plan(snapshotOf(v1), Registry.make([v2]), {
      renames: { columns: { docs: { heading: "title" } } }
    })
    expect(ddlOf(steps)).toEqual([])
    expect(handStatements(steps)).toEqual([
      'ALTER TABLE "docs" RENAME COLUMN "title" TO "heading";',
      'DROP INDEX "docs__by_rank";',
      'CREATE INDEX "docs__by_rank" ON "docs" ("rank" ASC NULLS FIRST, "heading" ASC NULLS FIRST, "id" ASC);'
    ])
  })

  it("the drop TODO names the indexes PostgreSQL drops with the column", () => {
    const steps = Migrate.plan(
      snapshotOf(replaced),
      Registry.make([SyncTable.make("docs", { ...base, rank: Schema.Int })]),
      { acknowledgeDrops: ["docs.title"] }
    )
    expect(ddlOf(steps)).toEqual([])
    expect(handStatements(steps)).toEqual(['ALTER TABLE "docs" DROP COLUMN "title";'])
    expect(lines(steps).join("\n")).toContain("PostgreSQL drops docs__by_title with the column")
  })

  it("every change on a renamed table is a TODO after it, using the explicitly renamed index names", () => {
    const v1 = SyncTable.make(
      "docs",
      { ...base, title: Schema.String, rank: Schema.Int },
      { indexes: { by_title: ["title"], by_rank: ["rank"] } }
    )
    const v2 = SyncTable.make(
      "articles",
      { ...base, title: Schema.String, rank: Schema.Int, note: Schema.NullOr(Schema.String) },
      { indexes: { by_title: ["title", "rank"], by_rank: ["rank"] } }
    )
    const steps = Migrate.plan(snapshotOf(v1), Registry.make([v2]), { renames: { tables: { articles: "docs" } } })
    expect(ddlOf(steps)).toEqual([])
    expect(handStatements(steps)).toEqual([
      'ALTER TABLE "docs" RENAME TO "articles";',
      'ALTER INDEX "docs__by_rank" RENAME TO "articles__by_rank";',
      'ALTER INDEX "docs__by_title" RENAME TO "articles__by_title";',
      'ALTER TABLE "articles" ADD COLUMN "note" text COLLATE "C";',
      'DROP INDEX "articles__by_title";',
      'CREATE INDEX "articles__by_title" ON "articles" ("title" ASC NULLS FIRST, "rank" ASC NULLS FIRST, "id" ASC);'
    ])
  })
})

describe("safe changes become exactly the pinned DDL", () => {
  it("a NOT NULL column with a default is added with it, then the default is dropped; a nullable one is added plain", () => {
    const v1 = SyncTable.make("notes", { ...base, body: Schema.String })
    const v2 = SyncTable.make("notes", {
      ...base,
      body: Schema.String,
      pinned: Schema.Boolean,
      color: Schema.NullOr(Schema.String)
    })
    const steps = Migrate.plan(snapshotOf(v1), Registry.make([v2]), { defaults: { "notes.pinned": "false" } })
    expect(ddlOf(steps)).toEqual([
      'ALTER TABLE "notes" ADD COLUMN "pinned" boolean NOT NULL DEFAULT false',
      'ALTER TABLE "notes" ALTER COLUMN "pinned" DROP DEFAULT',
      'ALTER TABLE "notes" ADD COLUMN "color" text COLLATE "C"'
    ])
  })

  it("a NOT NULL column without a default is added nullable with the SPEC §13 TODO, never SET NOT NULL", () => {
    const v1 = SyncTable.make("notes", base)
    const v2 = SyncTable.make("notes", { ...base, rank: Schema.Int })
    const steps = Migrate.plan(snapshotOf(v1), Registry.make([v2]))
    expect(ddlOf(steps)).toEqual(['ALTER TABLE "notes" ADD COLUMN "rank" bigint'])
    expect(todos(steps).length).toBe(1)
  })

  it("NOT NULL → nullable is DROP NOT NULL", () => {
    const v1 = SyncTable.make("notes", { ...base, rank: Schema.Int })
    const v2 = SyncTable.make("notes", { ...base, rank: Schema.NullOr(Schema.Int) })
    expect(ddlOf(Migrate.plan(snapshotOf(v1), Registry.make([v2])))).toEqual([
      'ALTER TABLE "notes" ALTER COLUMN "rank" DROP NOT NULL'
    ])
  })

  it("an index-only change is DROP/CREATE INDEX with SPEC §3 ordering and leaves dataHash unchanged (no re-download)", () => {
    const fields = { ...base, label: Schema.NullOr(Schema.String), rank: Schema.Int, at: Schema.Date }
    const v1 = SyncTable.make("items", fields, { indexes: { by_label: ["label"], stale: ["rank"] } })
    const v2 = SyncTable.make("items", fields, { indexes: { by_label: ["label", "rank"], by_at: ["at"] } })
    expect(v2.manifest.dataHash).toBe(v1.manifest.dataHash)
    expect(v2.manifest.indexHash).not.toBe(v1.manifest.indexHash)
    expect(ddlOf(Migrate.plan(snapshotOf(v1), Registry.make([v2])))).toEqual([
      'DROP INDEX "items__by_label"',
      'CREATE INDEX "items__by_label" ON "items" ("label" ASC NULLS FIRST, "rank" ASC NULLS FIRST, "id" ASC)',
      'DROP INDEX "items__stale"',
      'CREATE INDEX "items__by_at" ON "items" ("at" ASC NULLS FIRST, "id" ASC)'
    ])
    // A legacy snapshot has no fabricated history; index-only migrations still record the same floor identity.
    const first = Migrate.generate({
      previous: snapshotOf(v1),
      registry: Registry.make([v1]),
      existing: ["001_legacy"],
      name: "tracked"
    })
    const second = Migrate.generate({
      previous: Migrate.decodeSnapshot(first.snapshot),
      registry: Registry.make([v2]),
      existing: ["001_legacy", first.id],
      name: "indexed"
    })
    expect(Migrate.decodeSnapshot(second.snapshot).history).toEqual([
      { migration: "002_tracked", dataHash: Registry.make([v1]).dataHash },
      { migration: "003_indexed", dataHash: Registry.make([v2]).dataHash }
    ])
  })

  it("scope_grants is the engine's table and is never planned; no change plans nothing", () => {
    const t = SyncTable.make("notes", base)
    expect(Migrate.plan(snapshotOf(t), Registry.make([t]))).toEqual([])
    expect(Migrate.snapshotOf(Registry.make([t])).tables.map((m) => m.name)).toEqual(["notes"])
  })

  it("migrations are numbered after the highest existing one", () => {
    expect(Migrate.nextId([], "init")).toBe("001_init")
    expect(Migrate.nextId(["migrations/001_init.ts", "010_more"], "next")).toBe("011_next")
  })
})
