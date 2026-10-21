import { assert, describe, it } from "@effect/vitest"
import { Effect, Option, Schema } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Store, SyncTable } from "echo-sync"
import * as RowStore from "./rowStore.ts"
import { pgAvailable, RowId, TestDatabase } from "./test-support.ts"

const Things = SyncTable.make(
  "things",
  {
    id: RowId,
    accessScopeId: AccessScope.AccessScopeId,
    label: Schema.NullOr(Schema.String),
    rank: Schema.NullOr(Schema.Int),
    flag: Schema.Boolean,
    at: Schema.Date,
    meta: Schema.Struct({ tags: Schema.Array(Schema.String) })
  },
  { indexes: { by_label: ["label", "rank"], by_flag_at: ["flag", "at"] } }
)

// U+FFFF sorts after U+1F600 by UTF-16 code unit but before it by code point (DESIGN §10).
const BMP_MAX = "\uFFFF"
const EMOJI = "😀"
const rows = (
  [
    ["i1", EMOJI, 2, true, 10],
    ["i2", BMP_MAX, null, false, -5],
    ["i3", null, 1, true, 10],
    ["i4", "a", 1, false, 0],
    ["i5", "a", null, true, 7],
    ["i6", "B", 3, false, 7],
    ["i7", null, null, false, 3],
    ["i8", "a", 1, true, -5]
  ] as const
).map(([id, label, rank, flag, at]) =>
  Schema.encodeSync(Things.wire)({
    id: RowId.make(id),
    accessScopeId: AccessScope.AccessScopeId.make("s1"),
    label,
    rank,
    flag,
    at: new Date(at),
    meta: { tags: [id, "x"] }
  })
)

const plan = (
  index: Store.IndexRangePlan["index"],
  eq: ReadonlyArray<Store.KeyValue>,
  order: "asc" | "desc",
  lower?: Store.Bound,
  upper?: Store.Bound
): Store.IndexRangePlan => ({
  index,
  eq,
  order,
  lower: Option.fromNullishOr(lower),
  upper: Option.fromNullishOr(upper)
})

const plans = (["asc", "desc"] as const).flatMap((o) => [
  plan(Things.indexes.by_label, [], o),
  plan(Things.indexes.by_label, ["a"], o),
  plan(Things.indexes.by_label, [null], o),
  plan(Things.indexes.by_label, [], o, { value: "B", inclusive: false }, { value: EMOJI, inclusive: true }),
  plan(Things.indexes.by_label, ["a"], o, undefined, { value: 2, inclusive: false }),
  plan(Things.indexes.by_flag_at, [true], o, { value: 0, inclusive: false }),
  plan(Things.indexes.by_flag_at, [false], o, undefined, { value: 7, inclusive: true })
])

describe.skipIf(!pgAvailable)("Postgres RowStore (§4, §10)", () => {
  it.live("range returns exactly core's reference rows in core's reference order, and writes round-trip", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* sql.unsafe(Things.postgres.createTable)
      for (const ddl of Things.postgres.createIndexes) yield* sql.unsafe(ddl)
      const store = yield* RowStore.make(new Map([["things", "public"]]))
      for (const row of rows) yield* store.write(Things, String(row["id"]), Option.some(row))

      for (const p of plans) {
        const expected = rows.filter((r) => Store.planMatches(p, r)).toSorted(Store.planOrder(p))
        assert.deepStrictEqual(yield* store.range(Things, p), expected, JSON.stringify(p.eq))
      }
      assert.deepStrictEqual(yield* store.read(Things, "i1"), Option.fromUndefinedOr(rows[0]))
      yield* store.write(Things, "i1", Option.none())
      assert.isTrue(Option.isNone(yield* store.read(Things, "i1")))
    }).pipe(Effect.provide(TestDatabase))
  )
})
