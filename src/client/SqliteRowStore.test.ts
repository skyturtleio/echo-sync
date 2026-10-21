// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "./testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { expect, it } from "@effect/vitest"
import { Effect, Layer, Option, Schema } from "effect"
import * as SqlClient from "effect/sql/SqlClient"
import { AccessScope, Registry, Store, SyncTable } from "../core/index.ts"
import * as Outbox from "./Outbox.ts"
import * as SqliteRowStore from "./SqliteRowStore.ts"

const ItemId = Schema.String.pipe(Schema.brand("ItemId"))
const itemFields = {
  id: ItemId,
  accessScopeId: AccessScope.AccessScopeId,
  label: Schema.NullOr(Schema.String),
  rank: Schema.NullOr(Schema.Int),
  flag: Schema.Boolean,
  at: Schema.Date,
  doc: Schema.Struct({ tags: Schema.Array(Schema.String) })
}
const noteFields = { id: ItemId, accessScopeId: AccessScope.AccessScopeId, body: Schema.String }
const Items = SyncTable.make("items", itemFields, {
  indexes: { by_label: ["label", "rank"], by_flag_at: ["flag", "at"] }
})
const Notes = SyncTable.make("notes", noteFields)
const registry = Registry.make([Items, Notes])

// Text where UTF-16 code-unit order and code-point order disagree (U+FFFF vs U+1F600), plus case and NULL.
const labels: ReadonlyArray<string | null> = [null, "", "a", "B", "b", "aa", "\uFFFF", "😀", "é", "z"]
const ranks: ReadonlyArray<number | null> = [null, -1, 0, 1, 2]
const times: ReadonlyArray<number> = [-5, 0, 3, 7, 10]

/** Deterministic PRNG (mulberry32) so a failure reproduces from its seed. */
const rng = (seed: number) => {
  let s = seed
  const next = () => {
    s = (s + 0x6d2b79f5) | 0
    let t = Math.imul(s ^ (s >>> 15), 1 | s)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
  const pick = <A>(xs: ReadonlyArray<A>, fallback: A): A => xs[Math.floor(next() * xs.length)] ?? fallback
  return { next, pick }
}

const randomRows = (r: ReturnType<typeof rng>, n: number): Array<SyncTable.EncodedRow> =>
  Array.from({ length: n }, (_, i) =>
    Schema.encodeSync(Items.wire)({
      id: ItemId.make(`i${String(i).padStart(3, "0")}`),
      accessScopeId: AccessScope.AccessScopeId.make(r.pick(["s1", "s2"], "s1")),
      label: r.pick(labels, null),
      rank: r.pick(ranks, null),
      flag: r.next() < 0.5,
      at: new Date(r.pick(times, 0)),
      doc: { tags: [r.pick(["x", "y"], "x")] }
    })
  )

const randomPlan = (r: ReturnType<typeof rng>): Store.IndexRangePlan => {
  const byLabel = r.next() < 0.5
  const index = byLabel ? Items.indexes.by_label : Items.indexes.by_flag_at
  const eq: Array<Store.KeyValue> = r.next() < 0.5 ? [byLabel ? r.pick(labels, null) : r.next() < 0.5] : []
  // The ranged column is key[eq.length]: label/flag with no eq value, rank/at after one.
  const domain: ReadonlyArray<string | number | boolean> =
    eq.length === 0
      ? byLabel
        ? labels.filter((l) => l !== null)
        : [true, false]
      : byLabel
        ? ranks.filter((x) => x !== null)
        : times
  const bound = (): Option.Option<Store.Bound> =>
    r.next() < 0.5 ? Option.none() : Option.some({ value: r.pick(domain, 0), inclusive: r.next() < 0.5 })
  return { index, eq, lower: bound(), upper: bound(), order: r.next() < 0.5 ? "asc" : "desc" }
}

const Db = Layer.effectDiscard(Effect.andThen(Outbox.migrate, SqliteRowStore.migrate(registry))).pipe(
  Layer.provideMerge(SqliteClient.layerMemory({}))
)
const storeFor = (reg: Registry.SyncRegistry) =>
  Effect.map(SqlClient.SqlClient, (sql) => ({ sql, store: SqliteRowStore.make(sql, reg) }))
const ids = (rows: ReadonlyArray<SyncTable.EncodedRow>) => rows.map((row) => row["id"])
const note = (id: string, scope: string, body: string): SyncTable.EncodedRow => ({ id, access_scope_id: scope, body })
const appendIntent = Outbox.append({ clientId: "c", mutationId: "m", name: "t", args: {} })
const pendingIds = Effect.map(Outbox.pending, (entries) => entries.map((e) => e.mutationId))

it.effect("range returns exactly core's reference rows in core's reference order (seeded random plans)", () =>
  Effect.gen(function* () {
    const { sql, store } = yield* storeFor(registry)
    for (const seed of [1, 2, 3]) {
      const r = rng(seed)
      const rows = randomRows(r, 80)
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* store.clearAll
          for (const row of rows) yield* store.write(Items, String(row["id"]), Option.some(row))
        })
      )
      for (let i = 0; i < 150; i++) {
        const plan = randomPlan(r)
        const expected = ids(rows.filter((row) => Store.planMatches(plan, row)).toSorted(Store.planOrder(plan)))
        const actual = ids(yield* store.range(Items, plan))
        expect(actual, `seed ${seed} plan ${i}: ${JSON.stringify({ ...plan, index: plan.index.name })}`).toEqual(
          expected
        )
      }
    }
  }).pipe(Effect.provide(Db))
)

it.effect("write/read round-trips every column kind; None deletes; scan orders ids by code point", () =>
  Effect.gen(function* () {
    const { store } = yield* storeFor(registry)
    const rows = randomRows(rng(9), 5)
    const odd = Schema.encodeSync(Items.wire)({
      id: ItemId.make("😀"),
      accessScopeId: AccessScope.AccessScopeId.make("s1"),
      label: "\uFFFF",
      rank: null,
      flag: true,
      at: new Date(1_700_000_000_123),
      doc: { tags: ["q", "r"] }
    })
    for (const row of [...rows, odd]) yield* store.write(Items, String(row["id"]), Option.some(row))
    expect(yield* store.read(Items, "😀")).toEqual(Option.some(odd))
    yield* store.write(Items, "😀", Option.some({ ...odd, flag: false }))
    expect((yield* store.read(Items, "😀")).pipe(Option.map((row) => row["flag"]))).toEqual(Option.some(false))
    yield* store.write(Items, "i000", Option.none())
    expect(yield* store.read(Items, "i000")).toEqual(Option.none())
    // "😀" (U+1F600) sorts after "i…" by code point and by UTF-16 alike; U+FFFF vs U+1F600 is covered by range.
    expect(ids(yield* store.scan(Items))).toEqual(["i001", "i002", "i003", "i004", "😀"])
  }).pipe(Effect.provide(Db))
)

it.effect("clearAll and purgeScope touch only replicated rows; the outbox and meta survive", () =>
  Effect.gen(function* () {
    const { sql, store } = yield* storeFor(registry)
    for (const row of randomRows(rng(4), 20)) yield* store.write(Items, String(row["id"]), Option.some(row))
    yield* store.write(Notes, "n1", Option.some(note("n1", "s1", "x")))
    yield* store.write(Notes, "n2", Option.some(note("n2", "s2", "y")))
    yield* appendIntent

    yield* store.purgeScope(AccessScope.AccessScopeId.make("s1"))
    const scopes = [...(yield* store.scan(Items)), ...(yield* store.scan(Notes))].map((row) => row["access_scope_id"])
    expect(scopes.length).toBeGreaterThan(0)
    expect(new Set(scopes)).toEqual(new Set(["s2"]))

    yield* store.clearAll
    for (const table of registry.tables) expect(yield* store.scan(table)).toEqual([])
    expect(yield* pendingIds).toEqual(["m"])
    const meta = yield* sql<{ n: number }>`SELECT count(*) AS n FROM echo_meta`
    expect(meta[0]?.n).toBe(3)
  }).pipe(Effect.provide(Db))
)

it.effect("an index-only change keeps rows; a data-shape change rebuilds the replica and keeps the outbox", () =>
  Effect.gen(function* () {
    const { store } = yield* storeFor(registry)
    yield* store.write(Notes, "n1", Option.some(note("n1", "s1", "x")))
    yield* appendIntent
    expect(yield* SqliteRowStore.migrate(registry)).toEqual({ rebuilt: false })

    const reindexedNotes = SyncTable.make("notes", noteFields, { indexes: { by_body: ["body"] } })
    const reindexed = Registry.make([SyncTable.make("items", itemFields), reindexedNotes])
    expect(reindexed.dataHash).toBe(registry.dataHash)
    expect(reindexed.indexHash).not.toBe(registry.indexHash)
    expect(yield* SqliteRowStore.migrate(reindexed)).toEqual({ rebuilt: false })
    const afterReindex = (yield* storeFor(reindexed)).store
    expect(ids(yield* afterReindex.scan(reindexedNotes))).toEqual(["n1"])
    const byBody = yield* afterReindex.range(reindexedNotes, {
      index: reindexedNotes.indexes.by_body,
      eq: ["x"],
      lower: Option.none(),
      upper: Option.none(),
      order: "asc"
    })
    expect(ids(byBody)).toEqual(["n1"])

    const reshapedNotes = SyncTable.make("notes", { ...noteFields, title: Schema.String })
    const reshaped = Registry.make([Items, reshapedNotes])
    expect(yield* SqliteRowStore.migrate(reshaped)).toEqual({ rebuilt: true })
    const afterReshape = (yield* storeFor(reshaped)).store
    expect(yield* afterReshape.scan(reshapedNotes)).toEqual([])
    yield* afterReshape.write(reshapedNotes, "n2", Option.some({ ...note("n2", "s1", "y"), title: "t" }))
    expect(ids(yield* afterReshape.scan(reshapedNotes))).toEqual(["n2"])
    expect(yield* pendingIds).toEqual(["m"])
  }).pipe(Effect.provide(Db))
)
