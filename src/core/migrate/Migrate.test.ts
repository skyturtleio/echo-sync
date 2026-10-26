import { describe, expect, it } from "@effect/vitest"
import { Array as Arr, Effect, Option, Result, Schema, String as Str } from "effect"
import { AccessScopeId } from "../AccessScope.ts"
import * as Registry from "../Registry.ts"
import * as SyncTable from "../SyncTable.ts"
import * as Migrate from "./Migrate.ts"

const Id = Schema.String.pipe(Schema.brand("migrate/Id"))
const base = { id: Id, accessScopeId: AccessScopeId }

const snapshotOf = (...tables: ReadonlyArray<SyncTable.AnySyncTable>) => Migrate.snapshotOf(Registry.make(tables))
const ddlOf = (steps: ReadonlyArray<Migrate.Step>) => Arr.flatMap(steps, (s) => (s._tag === "Ddl" ? [s.sql] : []))
const todos = (steps: ReadonlyArray<Migrate.Step>) => Arr.filter(steps, (s) => s._tag === "Todo")
const lines = (steps: ReadonlyArray<Migrate.Step>) =>
  Arr.map(todos(steps), (s) => (s._tag === "Todo" ? Arr.join(s.lines, "\n") : ""))
/** The complete SQL statements the TODOs ask for, in the order a reader carries them out. */
const handStatements = (steps: ReadonlyArray<Migrate.Step>) =>
  Arr.flatMap(todos(steps), (s) =>
    s._tag === "Todo"
      ? Arr.map(
          Arr.filter(s.lines, (l) => /^ {2}[A-Z]+ [^<]*;$/.test(l)),
          Str.trim
        )
      : []
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
    const steps = Result.getOrThrow(Migrate.plan(before, Registry.make(tables), intent))
    expect(ddlOf(steps)).toEqual([])
    const text = Arr.join(
      Arr.flatMap(todos(steps), (s) => (s._tag === "Todo" ? s.lines : [])),
      "\n"
    )
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
      Arr.every(
        Arr.filter(Str.split(source, "\n"), (l) => /RENAME|DROP (COLUMN|TABLE)|ADD UNIQUE/.test(l)),
        (l) => Str.startsWith("// TODO(migration)")(Str.trimStart(l))
      )
    ).toBe(true)
  })

  it("an intent naming something the diff does not contain is refused, not ignored", () => {
    const registry = Registry.make([v2])
    for (const intent of [
      { defaults: { "docs.body": "''" } },
      { acknowledgeDrops: ["docs.body"] },
      { renames: { columns: { docs: { heading: "nope" } } } },
      { renames: { tables: { docs: "gone" } } },
      { renames: { tables: { kept: "missing" } } },
      { renames: { columns: { missing: { heading: "title" } } } },
      { renames: { columns: { docs: { missing: "title" } } } }
    ]) {
      const result = Migrate.plan(before, registry, intent)
      expect(Result.isFailure(result)).toBe(true)
      const error = Result.getOrThrow(Result.flip(result))
      expect(error).toBeInstanceOf(Migrate.MigrationPlanError)
      expect(error.stage).toBe("intent")
      expect(error.message).not.toBe("")
    }
  })
})

describe("DDL that depends on a TODO rename or kind change is a TODO too (SPEC §13)", () => {
  it("a combined table and column rename is one table TODO and one column TODO, no DDL, no intent error", () => {
    const v1 = SyncTable.make("docs", { ...base, title: Schema.String, body: Schema.String })
    const v2 = SyncTable.make("articles", { ...base, heading: Schema.String, body: Schema.String })
    const steps = Result.getOrThrow(
      Migrate.plan(snapshotOf(v1), Registry.make([v2]), {
        renames: { tables: { articles: "docs" }, columns: { articles: { heading: "title" } } }
      })
    )
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
    const steps = Result.getOrThrow(
      Migrate.plan(snapshotOf(v1), Registry.make([v2]), {
        renames: { columns: { docs: { heading: "title" } } }
      })
    )
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
      const steps = Result.getOrThrow(Migrate.plan(snapshotOf(replaced), Registry.make([v2]), intent))
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
    const steps = Result.getOrThrow(
      Migrate.plan(snapshotOf(v1), Registry.make([v2]), {
        renames: { columns: { docs: { heading: "title" } } }
      })
    )
    expect(ddlOf(steps)).toEqual([])
    expect(handStatements(steps)).toEqual([
      'ALTER TABLE "docs" RENAME COLUMN "title" TO "heading";',
      'DROP INDEX "docs__by_rank";',
      'CREATE INDEX "docs__by_rank" ON "docs" ("rank" ASC NULLS FIRST, "heading" ASC NULLS FIRST, "id" ASC);'
    ])
  })

  it("the drop TODO names the indexes PostgreSQL drops with the column", () => {
    const steps = Result.getOrThrow(
      Migrate.plan(snapshotOf(replaced), Registry.make([SyncTable.make("docs", { ...base, rank: Schema.Int })]), {
        acknowledgeDrops: ["docs.title"]
      })
    )
    expect(ddlOf(steps)).toEqual([])
    expect(handStatements(steps)).toEqual(['ALTER TABLE "docs" DROP COLUMN "title";'])
    expect(Arr.join(lines(steps), "\n")).toContain("PostgreSQL drops docs__by_title with the column")
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
    const steps = Result.getOrThrow(
      Migrate.plan(snapshotOf(v1), Registry.make([v2]), { renames: { tables: { articles: "docs" } } })
    )
    expect(ddlOf(steps)).toEqual([])
    expect(handStatements(steps)).toEqual([
      'ALTER TABLE "docs" RENAME TO "articles";',
      'ALTER INDEX "docs__by_rank" RENAME TO "articles__by_rank";',
      'ALTER INDEX "docs__by_title" RENAME TO "articles__by_title";',
      'ALTER INDEX "docs__echo-scope" RENAME TO "articles__echo-scope";',
      'ALTER TABLE "articles" ADD COLUMN "note" text COLLATE "C";',
      'DROP INDEX "articles__by_title";',
      'CREATE INDEX "articles__by_title" ON "articles" ("title" ASC NULLS FIRST, "rank" ASC NULLS FIRST, "id" ASC);'
    ])
  })
})

describe("safe changes become exactly the pinned DDL", () => {
  it.effect("nullable literal unions generate TEXT and an old JSON manifest requires a manual kind migration", () =>
    Effect.gen(function* () {
      const table = SyncTable.make("notes", { ...base, status: Schema.NullOr(Schema.Literals(["open", "done"])) })
      const registry = Registry.make([table])
      const generated = yield* Migrate.generate({
        previous: Migrate.emptySnapshot,
        registry,
        existing: [],
        name: "init"
      })
      expect(generated.source).toContain('"status" text COLLATE "C"')
      expect(generated.source).not.toContain('"status" jsonb')
      const snapshot = yield* Migrate.decodeSnapshot(generated.snapshot)
      const first = snapshot.tables[0]
      expect(
        first === undefined
          ? undefined
          : Option.getOrUndefined(Arr.findFirst(first.columns, (c) => c.name === "status"))
      ).toEqual({
        name: "status",
        kind: "text",
        nullable: true
      })
      for (const status of [null, "open", "done"]) {
        const row = { id: "n", access_scope_id: "s", status }
        for (const codec of [table.postgres.row, table.sqlite.row]) {
          expect(yield* Schema.encodeEffect(codec)(row)).toEqual(row)
          expect(yield* Schema.decodeUnknownEffect(codec)(row)).toEqual(row)
          const invalid = yield* Schema.decodeUnknownEffect(codec)({ ...row, status: "invalid" })
          const error = yield* Effect.flip(Schema.decodeUnknownEffect(table.wire)(invalid))
          expect(error.message).toMatch(/open|done/)
        }
      }
      // Reproduce the previously committed storage manifest, not a freshly classified declaration.
      const old = {
        ...snapshot,
        tables: Arr.map(snapshot.tables, (t) => ({
          ...t,
          columns: Arr.map(t.columns, (c) => (c.name === "status" ? { ...c, kind: "json" as const } : c))
        }))
      }
      const upgrade = yield* Migrate.generate({
        previous: old,
        registry,
        existing: [generated.id],
        name: "text_status"
      })
      expect(upgrade.source).toContain("notes.status changes kind json → text")
      expect(upgrade.source).not.toContain("sql.unsafe")
      expect((yield* Migrate.decodeSnapshot(upgrade.snapshot)).tables).toEqual(snapshot.tables)
    })
  )

  it("a NOT NULL column with a default is added with it, then the default is dropped; a nullable one is added plain", () => {
    const v1 = SyncTable.make("notes", { ...base, body: Schema.String })
    const v2 = SyncTable.make("notes", {
      ...base,
      body: Schema.String,
      pinned: Schema.Boolean,
      color: Schema.NullOr(Schema.String)
    })
    const steps = Result.getOrThrow(
      Migrate.plan(snapshotOf(v1), Registry.make([v2]), { defaults: { "notes.pinned": "false" } })
    )
    expect(ddlOf(steps)).toEqual([
      'ALTER TABLE "notes" ADD COLUMN "pinned" boolean NOT NULL DEFAULT false',
      'ALTER TABLE "notes" ALTER COLUMN "pinned" DROP DEFAULT',
      'ALTER TABLE "notes" ADD COLUMN "color" text COLLATE "C"'
    ])
  })

  it("a NOT NULL column without a default is added nullable with the SPEC §13 TODO, never SET NOT NULL", () => {
    const v1 = SyncTable.make("notes", base)
    const v2 = SyncTable.make("notes", { ...base, rank: Schema.Int })
    const steps = Result.getOrThrow(Migrate.plan(snapshotOf(v1), Registry.make([v2])))
    expect(ddlOf(steps)).toEqual(['ALTER TABLE "notes" ADD COLUMN "rank" bigint'])
    expect(todos(steps).length).toBe(1)
  })

  it("NOT NULL → nullable is DROP NOT NULL", () => {
    const v1 = SyncTable.make("notes", { ...base, rank: Schema.Int })
    const v2 = SyncTable.make("notes", { ...base, rank: Schema.NullOr(Schema.Int) })
    expect(ddlOf(Result.getOrThrow(Migrate.plan(snapshotOf(v1), Registry.make([v2]))))).toEqual([
      'ALTER TABLE "notes" ALTER COLUMN "rank" DROP NOT NULL'
    ])
  })

  it.effect(
    "an index-only change is DROP/CREATE INDEX with SPEC §3 ordering and leaves dataHash unchanged (no re-download)",
    () =>
      Effect.gen(function* () {
        const fields = { ...base, label: Schema.NullOr(Schema.String), rank: Schema.Int, at: Schema.Date }
        const v1 = SyncTable.make("items", fields, { indexes: { by_label: ["label"], stale: ["rank"] } })
        const v2 = SyncTable.make("items", fields, { indexes: { by_label: ["label", "rank"], by_at: ["at"] } })
        expect(v2.manifest.dataHash).toBe(v1.manifest.dataHash)
        expect(v2.manifest.indexHash).not.toBe(v1.manifest.indexHash)
        expect(ddlOf(Result.getOrThrow(Migrate.plan(snapshotOf(v1), Registry.make([v2]))))).toEqual([
          'DROP INDEX "items__by_label"',
          'CREATE INDEX "items__by_label" ON "items" ("label" ASC NULLS FIRST, "rank" ASC NULLS FIRST, "id" ASC)',
          'DROP INDEX "items__stale"',
          'CREATE INDEX "items__by_at" ON "items" ("at" ASC NULLS FIRST, "id" ASC)'
        ])
        // A legacy snapshot has no fabricated history; index-only migrations still record the same floor identity.
        const first = yield* Migrate.generate({
          previous: snapshotOf(v1),
          registry: Registry.make([v1]),
          existing: ["001_legacy"],
          name: "tracked"
        })
        const second = yield* Migrate.generate({
          previous: yield* Migrate.decodeSnapshot(first.snapshot),
          registry: Registry.make([v2]),
          existing: ["001_legacy", first.id],
          name: "indexed"
        })
        expect((yield* Migrate.decodeSnapshot(second.snapshot)).history).toEqual([
          { migration: "002_tracked", dataHash: Registry.make([v1]).dataHash },
          { migration: "003_indexed", dataHash: Registry.make([v2]).dataHash }
        ])
      })
  )

  it("scope_grants is the engine's table and is never planned; no change plans nothing", () => {
    const t = SyncTable.make("notes", base)
    expect(Migrate.plan(snapshotOf(t), Registry.make([t]))).toEqual(Result.succeed([]))
    expect(Arr.map(Migrate.snapshotOf(Registry.make([t])).tables, (m) => m.name)).toEqual(["notes"])
  })

  it("migrations are numbered after the highest existing one", () => {
    expect(Migrate.nextId([], "init")).toEqual(Result.succeed("001_init"))
    expect(Migrate.nextId(["migrations/001_init.ts", "010_more"], "next")).toEqual(Result.succeed("011_next"))
    expect(Migrate.nextId([], "bad-name")).toEqual(
      Result.fail(
        new Migrate.MigrationPlanError({
          stage: "name",
          message: 'migration name "bad-name" must match /^[a-z0-9_]+$/'
        })
      )
    )
  })

  it.effect("malformed snapshots are recoverable with their schema cause, and each execution decodes anew", () =>
    Effect.gen(function* () {
      for (const text of ["{", '{"version":2,"tables":[]}']) {
        const decoding = Migrate.decodeSnapshot(text)
        const first = yield* Effect.flip(decoding)
        const second = yield* Effect.flip(decoding)
        expect(first).toBeInstanceOf(Migrate.MigrationPlanError)
        expect(first.stage).toBe("snapshot-decode")
        expect(first.cause).toBeInstanceOf(Schema.SchemaError)
        expect(first.cause?.issue).toBeDefined()
        expect(second.cause).not.toBe(first.cause)
      }
      expect(yield* Migrate.decodeSnapshot(yield* Migrate.encodeSnapshot(Migrate.emptySnapshot))).toEqual(
        Migrate.emptySnapshot
      )
    })
  )

  it.effect(
    "snapshot encoding is lazy, revalidates on execution and preserves canonical bytes and schema failures",
    () =>
      Effect.gen(function* () {
        let reads = 0
        const snapshot: Migrate.Snapshot = {
          version: 1,
          get tables() {
            reads++
            return []
          }
        }
        const encoding = Migrate.encodeSnapshot(snapshot)
        expect(reads).toBe(0)
        expect(yield* encoding).toBe('{"tables":[],"version":1}\n')
        const afterFirst = reads
        expect(afterFirst).toBeGreaterThan(0)
        expect(yield* encoding).toBe('{"tables":[],"version":1}\n')
        expect(reads).toBeGreaterThan(afterFirst)
        // Simulate an invalid file/tool boundary value without lying to the type checker.
        Object.defineProperty(snapshot, "version", { value: 2 })
        const error = yield* Effect.flip(encoding)
        expect(error.stage).toBe("snapshot-encode")
        expect(error.cause).toBeInstanceOf(Schema.SchemaError)
      })
  )

  it.effect("generation reads inputs only on execution and rechecks name and intent as recoverable failures", () =>
    Effect.gen(function* () {
      let name = "init"
      let reads = 0
      const options = {
        previous: Migrate.emptySnapshot,
        registry: Registry.make([]),
        existing: [],
        get name() {
          reads++
          return name
        }
      }
      const generating = Migrate.generate(options)
      expect(reads).toBe(0)
      const first = yield* generating
      expect(first.id).toBe("001_init")
      name = "again"
      expect((yield* generating).id).toBe("001_again")
      name = "bad-name"
      expect((yield* Effect.flip(generating)).stage).toBe("name")
      const refused = yield* Effect.flip(
        Migrate.generate({ ...options, name: "valid", intent: { acknowledgeDrops: ["missing"] } })
      )
      expect(refused.stage).toBe("intent")
    })
  )
})
