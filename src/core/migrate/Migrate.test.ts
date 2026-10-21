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

describe("the generator never emits DDL for a rename, drop, lossy type change or unique-set change (DESIGN §11)", () => {
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

  it("a NOT NULL column without a default is added nullable with the §11 TODO, never SET NOT NULL", () => {
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

  it("an index-only change is DROP/CREATE INDEX with §10 ordering and leaves dataHash unchanged (no re-download)", () => {
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
