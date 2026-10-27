import { Array as Arr, DateTime, Effect, Exit, Option, Schema, SchemaGetter } from "effect"
import { describe, expect, it } from "@effect/vitest"
import { afterAll, beforeAll } from "vitest"
import { AccessScopeId } from "./AccessScope.ts"
import * as Registry from "./Registry.ts"
import * as SyncTable from "./SyncTable.ts"
import type { MutationStore } from "./Store.ts"
import { type Dialect, pgAvailable, postgres, sqlite } from "./test/dialects.ts"

const ItemId = Schema.String.pipe(Schema.brand("ItemId"))
it("catalog shape changes leave scoped hashes unchanged and catalog mutations cannot compile", () => {
  const airports = SyncTable.catalog(
    "airports",
    { id: ItemId, name: Schema.String },
    { indexes: { by_name: ["name"] } }
  )
  const changed = SyncTable.catalog("airports", { id: ItemId, name: Schema.String, code: Schema.String })
  expect(airports.manifest.dataHash).not.toBe(changed.manifest.dataHash)
  expect(Registry.make([airports]).dataHash).toBe(Registry.make([changed]).dataHash)
  expect(Registry.make([airports]).dataHash).toBe(Registry.make([]).dataHash)
  expect(Registry.make([airports]).get("airports")).toEqual(Option.none())
  expect(Arr.some(airports.columns, (column) => column.name === "access_scope_id")).toBe(false)
  expect(() => Registry.make([airports, SyncTable.catalog("airports__next", { id: ItemId })])).toThrow("reserved")
  const rejects = (store: MutationStore) => [
    // @ts-expect-error Catalogs cannot be written by mutation handlers.
    store.insert(airports, { id: ItemId.make("a"), name: "Airport" }),
    // @ts-expect-error Catalogs cannot be patched by mutation handlers.
    store.patch(airports, ItemId.make("a"), { name: "Changed" }),
    // @ts-expect-error Catalogs cannot be deleted by mutation handlers.
    store.delete(airports, ItemId.make("a"))
  ]
  expect(rejects).toBeTypeOf("function")
})
const Status = Schema.Literals(["open", "done"])
const Priority = Schema.Literals([1, 2, 3])
const Shape = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("circle"), r: Schema.Finite }),
  Schema.Struct({ kind: Schema.Literal("box"), at: Schema.Date })
])

// A field whose decoded side is a string stored as a timestamp column (the reviewer's case).
const IsoFromDate = Schema.NullOr(Schema.Date).pipe(
  Schema.decodeTo(Schema.NullOr(Schema.String), {
    decode: SchemaGetter.transform((d: Date | null) => (d === null ? null : d.toISOString())),
    // oxlint-disable-next-line effecttsgo/global-date -- This transform deliberately exercises a native Date encoded leaf with a string decoded side.
    encode: SchemaGetter.transform((iso: string | null) => (iso === null ? null : new Date(iso)))
  })
)
// oxlint-disable-next-line effecttsgo/global-date -- Schema's Date refinement requires a native Date bound; its preservation is under test.
const Past = Schema.Date.check(Schema.isLessThanDate(new Date("2100-01-01T00:00:00Z"))).pipe(Schema.brand("Past"))

/** One column of every supported kind, each also in nullable form, plus transformed leaves. */
const Everything = SyncTable.make("everything", {
  id: ItemId,
  accessScopeId: AccessScopeId,
  text: Schema.String,
  textN: Schema.NullOr(Schema.String),
  status: Status,
  statusN: Schema.NullOr(Status),
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
  utcN: Schema.NullOr(Schema.NullOr(Schema.DateTimeUtc)),
  doc: Schema.Struct({ tags: Schema.Array(Schema.String), when: Schema.Date }),
  docN: Schema.NullOr(Schema.Array(Schema.Int)),
  shape: Shape,
  iso: IsoFromDate,
  past: Schema.NullOr(Past),
  utcMs: Schema.NullOr(Schema.DateTimeUtcFromMillis),
  fin: Schema.NullOr(Schema.FiniteFromString),
  big: Schema.NullOr(Schema.BigIntFromString)
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
    statusN: null,
    int: Number.MAX_SAFE_INTEGER,
    intN: null,
    priority: 1,
    dbl: -1.5e-300,
    dblN: null,
    flag: false,
    flagN: null,
    // oxlint-disable-next-line effecttsgo/global-date -- Native Date at the portable lower timestamp bound is part of the codec fixture.
    date: new Date(SyncTable.minTimestampMillis),
    dateN: null,
    utc: DateTime.makeUnsafe(SyncTable.maxTimestampMillis),
    utcN: null,
    // oxlint-disable-next-line effecttsgo/global-date -- Nested native Dates must survive JSON-column encoding too.
    doc: { tags: [], when: new Date(-1) },
    docN: null,
    shape: { kind: "circle", r: 0.1 },
    iso: null,
    past: null,
    utcMs: null,
    fin: null,
    big: null
  },
  {
    id: ItemId.make("b"),
    accessScopeId: scope,
    text: "😀 ü 'quote' \"dq\" \\",
    textN: "",
    status: "done",
    statusN: "open",
    int: Number.MIN_SAFE_INTEGER,
    intN: 0,
    priority: 3,
    dbl: Number.MIN_VALUE,
    dblN: -1.7976931348623157e308,
    flag: true,
    flagN: false,
    // oxlint-disable-next-line effecttsgo/global-date -- Native Date at the portable upper timestamp bound is part of the codec fixture.
    date: new Date(SyncTable.maxTimestampMillis),
    // oxlint-disable-next-line effecttsgo/global-date -- Exercises the non-null branch of a nullable native Date column.
    dateN: new Date(0),
    utc: DateTime.makeUnsafe(SyncTable.minTimestampMillis),
    utcN: DateTime.makeUnsafe(-1),
    // oxlint-disable-next-line effecttsgo/global-date -- Nested Date millisecond precision must survive JSON-column encoding.
    doc: { tags: ["", "😀"], when: new Date(1_700_000_000_123) },
    docN: [Number.MAX_SAFE_INTEGER, -1],
    // oxlint-disable-next-line effecttsgo/global-date -- Tests a native Date nested inside a tagged union JSON column.
    shape: { kind: "box", at: new Date(0) },
    iso: "2001-02-03T04:05:06.789Z",
    // oxlint-disable-next-line effecttsgo/global-date -- Branded/refined native Date must retain its declared storage mapping.
    past: Past.make(new Date(-62_135_596_800_000)),
    utcMs: DateTime.makeUnsafe(1),
    fin: -0.25,
    big: 12_345_678_901_234_567_890n
  }
]

const roundTrip = (dialect: Dialect) =>
  Effect.gen(function* () {
    yield* Effect.promise(() => dialect.create(Everything))
    for (const row of rows) {
      const wire = yield* Schema.encodeEffect(Everything.wire)(row)
      yield* Effect.promise(() => dialect.insert(Everything, wire))
    }
    return yield* Effect.forEach(yield* Effect.promise(() => dialect.readAll(Everything)), (wire) =>
      Schema.decodeUnknownEffect(Everything.wire)(wire)
    )
  })

const expectSameRows = (actual: ReadonlyArray<Row>) => {
  expect(actual).toEqual(rows)
}

describe("SyncTable codec round-trip", () => {
  it.effect(
    "timestamp codecs reject out-of-range and non-integer milliseconds in both directions and retain Date refinements",
    () =>
      Effect.gen(function* () {
        for (const column of Everything.columns) {
          if (column.field !== "date" && column.field !== "utc") continue
          for (const ms of [SyncTable.minTimestampMillis, -1, 0, 1, SyncTable.maxTimestampMillis]) {
            const value = yield* Schema.decodeEffect(column.wire)(ms)
            expect(yield* Schema.encodeEffect(column.wire)(value)).toBe(ms)
            expect(yield* Schema.encodeEffect(column.sqlite)(ms)).toBe(ms)
            const binding = yield* Schema.encodeEffect(column.postgres)(ms)
            expect(Schema.is(Schema.Date)(binding)).toBe(true)
            expect(yield* Schema.decodeEffect(column.postgres)(binding)).toBe(ms)
          }
          for (const ms of [SyncTable.minTimestampMillis - 1, SyncTable.maxTimestampMillis + 1, 0.5, NaN, Infinity]) {
            expect(Exit.isFailure(yield* Effect.exit(Schema.decodeUnknownEffect(column.wire)(ms)))).toBe(true)
            yield* Effect.flip(Schema.encodeEffect(column.postgres)(ms))
            yield* Effect.flip(Schema.encodeEffect(column.sqlite)(ms))
          }
          const tooEarly = yield* Schema.decodeEffect(Schema.DateFromMillis)(SyncTable.minTimestampMillis - 1)
          yield* Effect.flip(Schema.decodeEffect(column.postgres)(tooEarly))
          yield* Effect.flip(
            Schema.encodeEffect(column.wire)(
              column.field === "date" ? tooEarly : DateTime.makeUnsafe(SyncTable.minTimestampMillis - 1)
            )
          )
        }
        for (const row of rows) {
          const wire = yield* Schema.encodeEffect(Everything.wire)(row)
          yield* Effect.flip(Schema.decodeEffect(Everything.wire)({ ...wire, past: 4_102_444_800_000 }))
        }
      })
  )

  it.effect("the wire form (JSON text, as sent over HTTP) returns every field unchanged", () =>
    Effect.gen(function* () {
      const codec = Schema.fromJsonString(Everything.wire)
      const back = yield* Effect.forEach(rows, (row) =>
        Effect.gen(function* () {
          const json = yield* Schema.encodeEffect(codec)(row)
          return yield* Schema.decodeUnknownEffect(codec)(json)
        })
      )
      expect(back).toEqual(rows)
    })
  )

  it.effect("SQLite stores and returns every supported leaf type unchanged (boundaries, nulls, empty strings)", () =>
    Effect.acquireUseRelease(
      Effect.sync(sqlite),
      (db) => Effect.map(roundTrip(db), expectSameRows),
      (db) => Effect.promise(() => db.close())
    )
  )

  describe.skipIf(!pgAvailable)("Postgres", () => {
    let db: Dialect
    beforeAll(() =>
      Effect.runPromise(
        Effect.gen(function* () {
          db = yield* Effect.promise(postgres)
        })
      )
    )
    afterAll(() => db.close())
    it.effect(
      "Postgres stores and returns every supported leaf type unchanged (boundaries, nulls, empty strings)",
      () => Effect.map(roundTrip(db), expectSameRows)
    )
  })
})

const expectRejected = (make: () => unknown, message: RegExp) =>
  expect(make).toThrow(
    // oxlint-disable-next-line typescript/no-unsafe-assignment -- Vitest's asymmetric matcher is typed any
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
    const encodedDefault = Schema.String.pipe(
      Schema.withDecodingDefaultKey(Effect.succeed("d"), { encodingStrategy: "omit" })
    )
    // @ts-expect-error an encoded-side default lets a stored row omit the key (INV-TBL-4 key presence)
    expectRejected(() => SyncTable.make("t", { ...base, x: encodedDefault }), /field "x".*encoded-side defaults/)
  })

  it("rejects an encoded-side default hidden under NullOr, but allows optional keys inside a JSON column", () => {
    const encodedDefault = Schema.String.pipe(
      Schema.withDecodingDefaultKey(Effect.succeed("d"), { encodingStrategy: "omit" })
    )
    expectRejected(
      () => SyncTable.make("t", { ...base, x: Schema.NullOr(encodedDefault) }),
      /field "x".*encoded-side defaults/
    )
    // Reviewer's repro: the same default behind Schema.suspend.
    expectRejected(
      () => SyncTable.make("t", { ...base, x: Schema.suspend(() => Schema.NullOr(encodedDefault)) }),
      /field "x".*encoded-side defaults/
    )
    const json = SyncTable.make("t", { ...base, j: Schema.Struct({ a: Schema.optionalKey(Schema.String) }) })
    expect(Option.getOrUndefined(Arr.findFirst(json.columns, (c) => c.field === "j"))?.kind).toBe("json")
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

  it("ignores refinements, brands and nested JSON schemas: validation is not storage (SPEC §3)", () => {
    const base = SyncTable.make("notes", fields).manifest.dataHash
    const same = Arr.map(
      [
        SyncTable.make("notes", { ...fields, title: Schema.NonEmptyString }),
        SyncTable.make("notes", { ...fields, title: Schema.String.pipe(Schema.brand("Title")) }),
        // oxlint-disable-next-line effecttsgo/global-date -- This assertion specifically verifies native Date refinements do not affect the manifest hash.
        SyncTable.make("notes", { ...fields, at: Schema.Date.check(Schema.isGreaterThanDate(new Date(0))) })
      ],
      (t) => t.manifest.dataHash
    )
    expect(same).toEqual([base, base, base])
    const fin = SyncTable.make("n", { id: ItemId, accessScopeId: AccessScopeId, n: Schema.FiniteFromString })
    const nonNeg = SyncTable.make("n", {
      id: ItemId,
      accessScopeId: AccessScopeId,
      n: Schema.FiniteFromString.check(Schema.isGreaterThanOrEqualTo(0))
    })
    expect(nonNeg.manifest.dataHash).toBe(fin.manifest.dataHash)
    const jsonA = SyncTable.make("n", {
      id: ItemId,
      accessScopeId: AccessScopeId,
      j: Schema.Struct({ a: Schema.String })
    })
    const jsonB = SyncTable.make("n", { id: ItemId, accessScopeId: AccessScopeId, j: Schema.Struct({ a: Schema.Int }) })
    expect(jsonB.manifest.dataHash).toBe(jsonA.manifest.dataHash)
  })

  it("changes when the storage shape changes (nullability, column kind, column set, table name)", () => {
    const base = SyncTable.make("notes", fields).manifest.dataHash
    const variants = Arr.map(
      [
        SyncTable.make("notes", { ...fields, title: Schema.NullOr(Schema.String) }),
        SyncTable.make("notes", { ...fields, title: Schema.Int }),
        SyncTable.make("notes", { ...fields, at: Schema.NullOr(Schema.Date) }),
        SyncTable.make("notes", { ...fields, extra: Schema.Struct({ a: Schema.String }) }),
        SyncTable.make("notes2", fields)
      ],
      (t) => t.manifest.dataHash
    )
    expect(new Set([base, ...variants]).size).toBe(variants.length + 1)
  })
})
