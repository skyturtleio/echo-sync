import { DateTime, Effect, Option, Schema } from "effect"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { AccessScopeId, PrincipalId, type Role } from "./AccessScope.ts"
import { grantId, ScopeGrantId, ScopeGrants } from "./ScopeGrants.ts"
import { ClientId, MutationId } from "./Ids.ts"
import * as Mutation from "./Mutation.ts"
import * as Store from "./Store.ts"
import * as SyncTable from "./SyncTable.ts"
import { type Dialect, pgAvailable, postgres, sqlite } from "./test/dialects.ts"

const ItemId = Schema.String.pipe(Schema.brand("ItemId"))
const Items = SyncTable.make(
  "items",
  {
    id: ItemId,
    accessScopeId: AccessScopeId,
    label: Schema.NullOr(Schema.String),
    rank: Schema.NullOr(Schema.Int),
    flag: Schema.Boolean,
    at: Schema.Date
  },
  { indexes: { by_label: ["label", "rank"], by_flag_at: ["flag", "at"] } }
)
type Item = typeof Items.Row.Type
const s1 = AccessScopeId.make("s1")
const item = (id: string, label: string | null, rank: number | null, flag: boolean, at: number): Item => ({
  id: ItemId.make(id),
  accessScopeId: s1,
  label,
  rank,
  flag,
  at: new Date(at)
})

// Labels chosen where UTF-16 code-unit order and code-point order disagree: U+FFFF sorts after
// U+1F600 under JS `<` (surrogate 0xD83D < 0xFFFF) but before it by code point.
const BMP_MAX = "\uFFFF"
const EMOJI = "😀"
const items: ReadonlyArray<Item> = [
  item("i1", EMOJI, 2, true, 10),
  item("i2", BMP_MAX, null, false, -5),
  item("i3", null, 1, true, 10),
  item("i4", "a", 1, false, 0),
  item("i5", "a", null, true, 7),
  item("i6", "B", 3, false, 7),
  item("i7", null, null, false, 3),
  item("i8", "a", 1, true, -5)
]
const wire = items.map((r) => Schema.encodeSync(Items.wire)(r))

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
  upper: Option.fromNullishOr(upper),
  readableBy: Option.none(),
  limit: Option.none()
})
const plans: ReadonlyArray<Store.IndexRangePlan> = (["asc", "desc"] as const).flatMap((o) => [
  plan(Items.indexes.by_label, [], o),
  plan(Items.indexes.by_label, ["a"], o),
  plan(Items.indexes.by_label, [null], o),
  plan(Items.indexes.by_label, [], o, { value: "a", inclusive: true }),
  plan(Items.indexes.by_label, [], o, { value: "B", inclusive: false }, { value: EMOJI, inclusive: true }),
  plan(Items.indexes.by_label, ["a"], o, { value: 1, inclusive: true }),
  plan(Items.indexes.by_label, ["a"], o, undefined, { value: 2, inclusive: false }),
  plan(Items.indexes.by_flag_at, [true], o, { value: 0, inclusive: false }),
  plan(Items.indexes.by_flag_at, [false], o, undefined, { value: 7, inclusive: true })
])

const reference = (p: Store.IndexRangePlan): ReadonlyArray<string> =>
  wire
    .filter((r) => Store.planMatches(p, r))
    .toSorted(Store.planOrder(p))
    .map((r) => String(r["id"]))

const agrees = (dialect: () => Promise<Dialect>) => {
  let db: Dialect
  beforeAll(async () => {
    db = await dialect()
    await db.create(Items)
    for (const r of wire) await db.insert(Items, r)
  })
  afterAll(async () => db.close())
  it.each(plans.map((p, i) => [i, p] as const))(
    "plan %i returns the reference rows in the reference order",
    async (_, p) => {
      expect(await db.range(Items, p)).toEqual(reference(p))
    }
  )
}
describe("index ordering is one definition across dialects (DESIGN §10)", () => {
  it("the reference orders text by code point, NULL first ascending, id as tiebreak", () => {
    // Derived by hand: labels NULL(i3: rank 1, i7: rank NULL → NULL first), "B", "a"(rank NULL, 1:i4,i8), U+FFFF, 😀.
    expect(reference(plan(Items.indexes.by_label, [], "asc"))).toEqual(["i7", "i3", "i6", "i5", "i4", "i8", "i2", "i1"])
    // Range bounds never match NULL; eq(null) is IS NULL.
    expect(reference(plan(Items.indexes.by_label, [], "asc", { value: "", inclusive: true }))).not.toContain("i3")
    expect(reference(plan(Items.indexes.by_label, [null], "desc"))).toEqual(["i3", "i7"])
  })

  describe("SQLite", () => {
    agrees(async () => sqlite())
  })
  describe.skipIf(!pgAvailable)("Postgres", () => {
    agrees(postgres)
  })
})

const grantRow = (id: string) =>
  Schema.encodeSync(ScopeGrants.wire)({
    id: ScopeGrantId.make(id),
    accessScopeId: s1,
    principalId: PrincipalId.make("bob"),
    role: "reader",
    grantedAt: DateTime.makeUnsafe(0)
  })
const rejectsDuplicate = (dialect: () => Promise<Dialect>) => {
  it("a second row for the same pair under another id is refused by the database", async () => {
    const db = await dialect()
    try {
      await db.create(ScopeGrants)
      await db.insert(ScopeGrants, grantRow("fake-a"))
      await expect(db.insert(ScopeGrants, grantRow("fake-b"))).rejects.toThrow(/unique|UNIQUE|duplicate|failed/i)
    } finally {
      await db.close()
    }
  })
}
describe("scope_grants DDL makes duplicate (scope, principal) grants unrepresentable in storage", () => {
  describe("SQLite", () => {
    rejectsDuplicate(async () => sqlite())
  })
  describe.skipIf(!pgAvailable)("Postgres", () => {
    rejectsDuplicate(postgres)
  })
})

/** Minimal RowStore over Maps, ordered by the reference definition. */
const memoryRows = () => {
  const data = new Map<string, Map<string, SyncTable.EncodedRow>>()
  const table = (t: SyncTable.AnySyncTable) => {
    const existing = data.get(t.name)
    if (existing !== undefined) return existing
    const created = new Map<string, SyncTable.EncodedRow>()
    data.set(t.name, created)
    return created
  }
  const rows: Store.RowStore = {
    read: (t, id) => Effect.sync(() => Option.fromNullishOr(table(t).get(id))),
    write: (t, id, row) =>
      Effect.sync(() => {
        if (Option.isSome(row)) table(t).set(id, row.value)
        else table(t).delete(id)
      }),
    range: (t, p) => Effect.sync(() => Store.planResult(p, table(t).values(), table(ScopeGrants).values())),
    scan: (t) => Effect.sync(() => [...table(t).values()])
  }
  const snapshot = () => new Map([...data].map(([name, m]) => [name, new Map(m)]))
  return { rows, data, snapshot }
}

const ids = (rows: ReadonlyArray<{ readonly id: string }>) => rows.map((r) => r.id)

const roles = (granted: Partial<Record<string, Role>>): Store.StoreOptions => ({
  actor: PrincipalId.make("u1"),
  roleFor: (scopeId) => Effect.succeed(Option.fromNullishOr(granted[scopeId]))
})

describe("typed Store over a RowStore", () => {
  it("undo images restore the exact prior state after a handler that fails midway", async () => {
    const mem = memoryRows()
    const seed = Store.make(mem.rows, roles({ s1: "writer" })).store
    await Effect.runPromise(Effect.forEach(items.slice(0, 3), (r) => seed.insert(Items, r)))
    const before = mem.snapshot()

    const { store, touched } = Store.make(mem.rows, roles({ s1: "writer", s2: "writer" }))
    const failing = Effect.gen(function* () {
      yield* store.patch(Items, ItemId.make("i1"), { label: "x" })
      yield* store.patch(Items, ItemId.make("i1"), { rank: 99 }) // second write to the same row
      yield* store.delete(Items, ItemId.make("i2"))
      yield* store.insert(Items, item("new", "n", 1, true, 1))
      yield* store.patch(Items, ItemId.make("i3"), { accessScopeId: AccessScopeId.make("s2") }) // scope move
      return yield* Effect.fail("domain failure" as const)
    })
    expect((await Effect.runPromise(Effect.exit(failing)))._tag).toBe("Failure")
    expect(mem.data).not.toEqual(before)

    const images = await Effect.runPromise(touched)
    expect(images.map((i) => i.id)).toEqual(["i1", "i2", "new", "i3"]) // first-write order, one per row
    for (const image of images.toReversed())
      await Effect.runPromise(mem.rows.write(image.table, image.id, image.before))
    expect(mem.data).toEqual(before)
  })

  it("writes need writer on the row's scope, and a scope move needs it on both scopes", async () => {
    const mem = memoryRows()
    await Effect.runPromise(
      Store.make(mem.rows, roles({ s1: "writer" })).store.insert(Items, items[0] ?? item("x", null, null, true, 0))
    )
    const reader = Store.make(mem.rows, roles({ s1: "reader" })).store
    const exit = await Effect.runPromise(Effect.exit(reader.patch(Items, ItemId.make("i1"), { rank: 5 })))
    expect(exit).toMatchObject({ _tag: "Failure" })
    const mover = Store.make(mem.rows, roles({ s1: "writer", s2: "reader" }))
    const moved = await Effect.runPromise(
      Effect.flip(mover.store.patch(Items, ItemId.make("i1"), { accessScopeId: AccessScopeId.make("s2") }))
    )
    expect(moved).toMatchObject({ _tag: "ScopeForbidden", scopeId: "s2", required: "writer", held: "reader" })
    expect(await Effect.runPromise(mover.touched)).toEqual([]) // nothing written, nothing to undo
  })

  it("two grants for one (scope, principal) cannot both be written, nor can a patch change the pair", async () => {
    const mem = memoryRows()
    const { store } = Store.make(mem.rows, roles({ s1: "owner" }))
    const grant = (id: string, principal: string) => ({
      id: ScopeGrantId.make(id),
      accessScopeId: s1,
      principalId: PrincipalId.make(principal),
      role: "reader" as const,
      grantedAt: DateTime.makeUnsafe(0)
    })
    for (const fake of ["fake-a", "fake-b"]) {
      const err = await Effect.runPromise(Effect.flip(store.insert(ScopeGrants, grant(fake, "bob"))))
      expect(err).toMatchObject({ _tag: "InvalidRow", table: "scope_grants" })
    }
    const canonical = grantId(s1, PrincipalId.make("bob"))
    await Effect.runPromise(store.insert(ScopeGrants, grant(canonical, "bob")))
    const again = await Effect.runPromise(Effect.flip(store.insert(ScopeGrants, grant(canonical, "bob"))))
    expect(again).toMatchObject({ _tag: "RowAlreadyExists" })
    const moved = await Effect.runPromise(
      Effect.flip(store.patch(ScopeGrants, canonical, { principalId: PrincipalId.make("eve") }))
    )
    expect(moved).toMatchObject({ _tag: "InvalidRow" })
    await Effect.runPromise(store.patch(ScopeGrants, canonical, { role: "writer" }))
    expect([...(mem.data.get("scope_grants")?.keys() ?? [])]).toEqual([canonical])
  })

  it("queries return the first n rows the actor can read (own or * grant), skipping unreadable ones in index order", async () => {
    const mem = memoryRows()
    const s2 = AccessScopeId.make("s2")
    const s3 = AccessScopeId.make("s3")
    const admin = Store.make(mem.rows, roles({ s1: "owner", s2: "writer", s3: "owner" })).store
    // rank 1..6 in one label; readable: s1 (u1's own grant) and s3 (the * grant), not s2.
    const scopes = [s2, s1, s2, s3, s2, s1]
    await Effect.runPromise(
      Effect.forEach(scopes, (scope, i) =>
        admin.insert(Items, { ...item(`r${i + 1}`, "a", i + 1, true, 1), accessScopeId: scope })
      )
    )
    const u1 = PrincipalId.make("u1")
    for (const [scope, principal] of [
      [s1, u1],
      [s3, PrincipalId.make("*")]
    ] as const) {
      await Effect.runPromise(
        admin.insert(ScopeGrants, {
          id: grantId(scope, principal),
          accessScopeId: scope,
          principalId: principal,
          role: "reader",
          grantedAt: DateTime.makeUnsafe(0)
        })
      )
    }
    const store = Store.make(mem.rows, roles({ s1: "reader", s3: "reader" })).store
    const query = store.query(Items).withIndex("by_label", (q) => q.eq("label", "a"))
    expect(ids(await Effect.runPromise(query.collect))).toEqual(["r2", "r4", "r6"])
    expect(ids(await Effect.runPromise(query.take(2)))).toEqual(["r2", "r4"])
    expect(ids(await Effect.runPromise(query.order("desc").take(1)))).toEqual(["r6"])
    expect(ids(await Effect.runPromise(query.take(0)))).toEqual([])
  })
})

describe("mutation run against the typed store", () => {
  it("a domain error declared by the mutation becomes a JSON-encoded DomainError rejection", async () => {
    const TooLong = Schema.Struct({ _tag: Schema.Literal("TooLong"), max: Schema.Int })
    const rename = Mutation.make("items.rename", {
      args: Schema.Struct({ id: ItemId, label: Schema.String }),
      error: TooLong,
      handler: (ctx, { id, label }) =>
        label.length > 3 ? Effect.fail({ _tag: "TooLong" as const, max: 3 }) : ctx.store.patch(Items, id, { label })
    })
    const registry = Mutation.makeRegistry([rename])
    const mem = memoryRows()
    const { store } = Store.make(mem.rows, roles({ s1: "writer" }))
    const ctx: Mutation.MutationContext = {
      actor: PrincipalId.make("u"),
      now: DateTime.makeUnsafe(0),
      random: Mutation.seededRandom(ClientId.make("c"), MutationId.make("m")),
      store
    }
    const err = await Effect.runPromise(
      Effect.flip(Mutation.run(registry, { name: "items.rename", args: { id: "i1", label: "long" } }, ctx))
    )
    expect(err).toMatchObject({ _tag: "DomainError", mutation: "items.rename", error: { _tag: "TooLong", max: 3 } })
    const missing = await Effect.runPromise(
      Effect.flip(Mutation.run(registry, { name: "items.rename", args: { id: "i1", label: "ok" } }, ctx))
    )
    expect(missing).toMatchObject({ _tag: "RowNotFound", id: "i1" })
  })
})
