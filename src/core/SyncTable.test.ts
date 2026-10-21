import { DateTime, Schema } from "effect"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { AccessScopeId } from "./AccessScope.ts"
import * as Registry from "./Registry.ts"
import * as SyncTable from "./SyncTable.ts"
import { type Dialect, pgAvailable, postgres, sqlite } from "./test/dialects.ts"

const ItemId = Schema.String.pipe(Schema.brand("ItemId"))
const Status = Schema.Literals(["open", "done"])
const Priority = Schema.Literals([1, 2, 3])
const Shape = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("circle"), r: Schema.Finite }),
  Schema.Struct({ kind: Schema.Literal("box"), at: Schema.Date })
])

/** One column of every supported kind, each also in nullable form. */
const Everything = SyncTable.make("everything", {
  id: ItemId,
  accessScopeId: AccessScopeId,
  text: Schema.String,
  textN: Schema.NullOr(Schema.String),
  status: Status,
  int: Schema.Int,
  intN: Schema.NullOr(Schema.Int),
  priority: Priority,
  dbl: Schema.Finite,
  dblN: Schema.NullOr(Schema.Finite),
  flag: Schema.Boolean,
  flagN: Schema.NullOr(Schema.Boolean),
  date: Schema.Date,
  dateN: Schema.NullOr(Schema.Date),
  utc: Schema.DateTimeUtc,
  utcN: Schema.NullOr(Schema.DateTimeUtc),
  doc: Schema.Struct({ tags: Schema.Array(Schema.String), when: Schema.Date }),
  docN: Schema.NullOr(Schema.Array(Schema.Int)),
  shape: Shape
})

type Row = typeof Everything.Row.Type
const scope = AccessScopeId.make("scope-1")

const rows: ReadonlyArray<Row> = [
  {
    id: ItemId.make("a"),
    accessScopeId: scope,
    text: "",
    textN: null,
    status: "open",
    int: Number.MAX_SAFE_INTEGER,
    intN: null,
    priority: 1,
    dbl: -1.5e-300,
    dblN: null,
    flag: false,
    flagN: null,
    date: new Date(SyncTable.minTimestampMillis),
    dateN: null,
    utc: DateTime.makeUnsafe(SyncTable.maxTimestampMillis),
    utcN: null,
    doc: { tags: [], when: new Date(-1) },
    docN: null,
    shape: { kind: "circle", r: 0.1 }
  },
  {
    id: ItemId.make("b"),
    accessScopeId: scope,
    text: "😀 ü 'quote' \"dq\" \\",
    textN: "",
    status: "done",
    int: Number.MIN_SAFE_INTEGER,
    intN: 0,
    priority: 3,
    dbl: Number.MIN_VALUE,
    dblN: -1.7976931348623157e308,
    flag: true,
    flagN: false,
    date: new Date(SyncTable.maxTimestampMillis),
    dateN: new Date(0),
    utc: DateTime.makeUnsafe(SyncTable.minTimestampMillis),
    utcN: DateTime.makeUnsafe(-1),
    doc: { tags: ["", "😀"], when: new Date(1_700_000_000_123) },
    docN: [Number.MAX_SAFE_INTEGER, -1],
    shape: { kind: "box", at: new Date(0) }
  }
]

const roundTrip = async (dialect: Dialect) => {
  await dialect.create(Everything)
  for (const row of rows) await dialect.insert(Everything, Schema.encodeSync(Everything.wire)(row))
  return (await dialect.readAll(Everything)).map((wire) => Schema.decodeUnknownSync(Everything.wire)(wire))
}

const expectSameRows = (actual: ReadonlyArray<Row>) => {
  expect(actual).toEqual(rows)
}

describe("SyncTable codec round-trip", () => {
  it("SQLite stores and returns every supported leaf type unchanged (boundaries, nulls, empty strings)", async () => {
    const db = sqlite()
    try {
      expectSameRows(await roundTrip(db))
    } finally {
      await db.close()
    }
  })

  describe.skipIf(!pgAvailable)("Postgres", () => {
    let db: Dialect
    beforeAll(async () => {
      db = await postgres()
    })
    afterAll(async () => db.close())
    it("Postgres stores and returns every supported leaf type unchanged (boundaries, nulls, empty strings)", async () => {
      expectSameRows(await roundTrip(db))
    })
  })
})

const expectRejected = (make: () => unknown, message: RegExp) =>
  expect(make).toThrow(
    expect.objectContaining({ name: "SyncTableDeclarationError", message: expect.stringMatching(message) })
  )

describe("SyncTable declaration-time rejection", () => {
  const base = { id: ItemId, accessScopeId: AccessScopeId }
  it("rejects a bare Schema.Number instead of guessing integer or double", () => {
    expectRejected(() => SyncTable.make("t", { ...base, n: Schema.Number }), /field "n".*bare Schema\.Number/)
  })

  it("rejects leaves with no portable column (bigint, binary, enum, optional key)", () => {
    // @ts-expect-error bigint has no column mapping
    expectRejected(() => SyncTable.make("t", { ...base, n: Schema.BigInt }), /field "n"/)
    // @ts-expect-error binary has no column mapping
    expectRejected(() => SyncTable.make("t", { ...base, b: Schema.Uint8Array }), /field "b"/)
    expectRejected(() => SyncTable.make("t", { ...base, e: Schema.Enum({ A: "a" }) }), /field "e"/)
    // @ts-expect-error optional keys are not supported
    expectRejected(() => SyncTable.make("t", { ...base, o: Schema.optionalKey(Schema.String) }), /field "o".*optional/)
  })

  it("rejects indexes on JSON columns and on unknown fields", () => {
    const jsonFields = { ...base, doc: Schema.Struct({ a: Schema.String }) }
    // @ts-expect-error JSON columns are not indexable
    expectRejected(() => SyncTable.make("t", jsonFields, { indexes: { by_doc: ["doc"] } }), /index "by_doc".*JSON/)
    // @ts-expect-error unknown index field
    expectRejected(() => SyncTable.make("t", { ...base }, { indexes: { by_x: ["nope"] } }), /unknown field "nope"/)
  })

  it("rejects field names that do not survive camelCase ↔ snake_case", () => {
    expectRejected(() => SyncTable.make("t", { ...base, foo_bar: Schema.String }), /field "foo_bar"/)
    expectRejected(() => SyncTable.make("t", { ...base, Foo: Schema.String }), /field "Foo"/)
  })

  it("a registry rejects two tables with the same sync name, including the system grants table", () => {
    const a = SyncTable.make("dup", { ...base })
    const b = SyncTable.make("dup", { ...base, x: Schema.String })
    expect(() => Registry.make([a, b])).toThrow(/duplicate table "dup"/)
    expect(() => Registry.make([SyncTable.make("scope_grants", { ...base })])).toThrow(/duplicate table/)
  })
})

describe("SyncTable manifest hash", () => {
  const fields = { id: ItemId, accessScopeId: AccessScopeId, title: Schema.String, at: Schema.Date }

  it("does not change when only indexes change (adding an index must not force a re-download)", () => {
    const plain = SyncTable.make("notes", fields)
    const indexed = SyncTable.make("notes", fields, { indexes: { by_at: ["at"] } })
    expect(indexed.manifest.dataHash).toBe(plain.manifest.dataHash)
    expect(indexed.manifest.indexHash).not.toBe(plain.manifest.indexHash)
    expect(Registry.make([indexed]).dataHash).toBe(Registry.make([plain]).dataHash)
  })

  it("does not change for documentation annotations or field order", () => {
    const a = SyncTable.make("notes", fields)
    const b = SyncTable.make("notes", {
      at: Schema.Date,
      title: Schema.String.annotate({ description: "shown in the list" }),
      accessScopeId: AccessScopeId,
      id: ItemId
    })
    expect(b.manifest.dataHash).toBe(a.manifest.dataHash)
  })

  it("changes when the data shape changes (nullability, type, refinement, nested JSON shape, name)", () => {
    const base = SyncTable.make("notes", fields).manifest.dataHash
    const variants = [
      SyncTable.make("notes", { ...fields, title: Schema.NullOr(Schema.String) }),
      SyncTable.make("notes", { ...fields, at: Schema.DateTimeUtc }),
      SyncTable.make("notes", { ...fields, title: Schema.NonEmptyString }),
      SyncTable.make("notes", { ...fields, extra: Schema.Struct({ a: Schema.String }) }),
      SyncTable.make("notes", { ...fields, extra: Schema.Struct({ a: Schema.Int }) }),
      SyncTable.make("notes2", fields)
    ].map((t) => t.manifest.dataHash)
    expect(new Set([base, ...variants]).size).toBe(variants.length + 1)
  })
})
