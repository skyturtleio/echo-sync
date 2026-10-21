/**
 * Store conformance suite: the properties the engine and the simulation rely on, checked against
 * any implementation of the storage ports. `MemoryStore` / `MemorySyncStorage` are the reference;
 * the SQLite replica (browser) and the Postgres `RowStore` (server) run the same checks.
 *
 * Runner-agnostic: every check is an `Effect` that fails with `ConformanceViolation` (seed and
 * case in the message) and otherwise returns how many cases it exercised. Register them with any
 * test runner, e.g.
 *
 *   for (const check of Conformance.syncStorage(makeSubject))
 *     it.effect(check.name, () => Effect.map(check.run, (cases) => assert.isAbove(cases, 0)))
 *
 * Factory contract: each call returns a fresh, empty store in which every table in `tables` exists
 * (create their DDL; a `SyncStorage` registry must contain them, since persisted undo images are
 * resolved by table name). The factory may use a `Scope`; each check runs in its own scope.
 */
import { Array as Arr, Deferred, Effect, Fiber, Option, Schema, Scheduler } from "effect"
import type { Scope } from "effect"
import * as Engine from "../../client/sync/Engine.ts"
import { AccessScope, Ids, Protocol, Store, SyncTable } from "../index.ts"
import type { Errors, Mutation } from "../index.ts"

// ---------------------------------------------------------------------------------------------
// Subjects and checks
// ---------------------------------------------------------------------------------------------

/** A row store plus the substrate's transaction (which owns commit/rollback). */
export interface RowStoreSubject<S extends Store.RowStore = Store.RowStore> {
  readonly rows: S
  readonly transaction: <A, E, R>(self: Effect.Effect<A, E, R>) => Effect.Effect<A, E | Errors.StoreUnavailable, R>
}

export interface SyncStorageSubject {
  readonly storage: Engine.SyncStorage
  /** A new `SyncStorage` over the same durable state (a reload: nothing in memory survives). */
  readonly reopen: Effect.Effect<Engine.SyncStorage, unknown>
  /** The released-row retention window passes: released markers are pruned (DESIGN §9). */
  readonly pruneReleased: Effect.Effect<void, unknown>
}

export interface Check<R> {
  readonly name: string
  /** Fails with `ConformanceViolation`; succeeds with the number of cases exercised. */
  readonly run: Effect.Effect<number, unknown, Exclude<R, Scope.Scope>>
}

export class ConformanceViolation extends Schema.TaggedError<ConformanceViolation>()("ConformanceViolation", {
  check: Schema.String,
  detail: Schema.String
}) {
  override get message() {
    return `${this.check}: ${this.detail}`
  }
}

const check = <R>(name: string, body: Effect.Effect<number, unknown, R>): Check<R> => ({
  name,
  run: Effect.scoped(body)
})

// ---------------------------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------------------------

const ItemId = Schema.String.check(Schema.isNonEmpty()).pipe(Schema.brand("conformance/ItemId"))
const TagId = Schema.String.check(Schema.isNonEmpty()).pipe(Schema.brand("conformance/TagId"))

/** Every indexable column kind, nullable and not, with multi-column indexes. */
export const Items = SyncTable.make(
  "conformance_items",
  {
    id: ItemId,
    accessScopeId: AccessScope.AccessScopeId,
    label: Schema.NullOr(Schema.String),
    rank: Schema.NullOr(Schema.Int),
    score: Schema.Finite,
    flag: Schema.Boolean,
    at: Schema.NullOr(Schema.Date),
    data: Schema.NullOr(Schema.Struct({ tags: Schema.Array(Schema.String), n: Schema.Finite }))
  },
  {
    indexes: {
      by_label_rank: ["label", "rank"],
      by_rank_label: ["rank", "label"],
      by_flag_at: ["flag", "at"],
      by_score: ["score"]
    }
  }
)

/** A second table, so clearAll / purgeScope / atomicity are checked across tables. */
export const Tags = SyncTable.make("conformance_tags", {
  id: TagId,
  accessScopeId: AccessScope.AccessScopeId,
  name: Schema.String
})

/** The tables a subject must create before running the suite. */
export const tables: ReadonlyArray<SyncTable.AnySyncTable> = [Items, Tags]

type Item = typeof Items.Row.Type
const s1 = AccessScope.AccessScopeId.make("s1")
const s2 = AccessScope.AccessScopeId.make("s2")
const encodeItem = Schema.encodeSync(Items.wire)
const item = (id: string, fields: Partial<Omit<Item, "id">> = {}) =>
  encodeItem({
    id: ItemId.make(id),
    accessScopeId: s1,
    label: null,
    rank: null,
    score: 0,
    flag: false,
    at: null,
    data: null,
    ...fields
  })
const tag = (id: string, scope: AccessScope.AccessScopeId, name: string) =>
  Schema.encodeSync(Tags.wire)({ id: TagId.make(id), accessScopeId: scope, name })

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

/** mulberry32. */
const prng = (seed: number) => {
  let a = seed >>> 0
  const next = () => {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
  const int = (n: number) => Math.floor(next() * n)
  const pick = <A>(xs: ReadonlyArray<A>): A => {
    const x = xs[int(xs.length)]
    if (x === undefined) throw new Error("pick from empty array")
    return x
  }
  return { int, pick, chance: (p: number) => next() < p }
}

/** JSON with bigints and table objects made comparable; key order as written. */
const canonical = (value: unknown): string =>
  JSON.stringify(value, (_, v: unknown) => (typeof v === "bigint" ? `${v}n` : isTable(v) ? { table: v.name } : v))
/** A table object (inside undo images) is identified by its sync name. */
const isTable = (v: unknown): v is { readonly name: string } =>
  typeof v === "object" && v !== null && "wire" in v && "columns" in v && "name" in v && typeof v.name === "string"

const violation = (name: string) => (detail: string) => new ConformanceViolation({ check: name, detail })

const rowsOf = (rows: Store.RowStore) =>
  Effect.all({ items: rows.scan(Items), tags: rows.scan(Tags) }).pipe(Effect.map(canonical))

/**
 * A scheduler for one fiber whose `shouldYield` answers true exactly once, at its `k`-th call:
 * the fiber parks at that evaluation boundary, where the caller interrupts it.
 */
const yieldAt = (k: number) => {
  const base = new Scheduler.MixedScheduler()
  const state = { ticks: 0, yielded: false }
  const scheduler: Scheduler.Scheduler = {
    executionMode: base.executionMode,
    shouldYield: () => {
      if (state.yielded || ++state.ticks !== k) return false
      state.yielded = true
      return true
    },
    makeDispatcher: () => base.makeDispatcher()
  }
  return { scheduler, state }
}

/**
 * Interrupts `run` at its k-th evaluation boundary for k = 1, 2, … until a run completes before
 * reaching k. Every effect operation is a boundary, including asynchronous I/O resumptions, so
 * the sweep covers the whole transaction. `observe` reads the durable outcome after each
 * attempt; it must equal `before` or `after`, never a mixture, and both must occur.
 */
const interruptSweep = <S, R>(options: {
  readonly name: string
  readonly fresh: Effect.Effect<S, unknown, R>
  readonly run: (subject: S) => Effect.Effect<unknown, unknown>
  readonly observe: (subject: S) => Effect.Effect<string, unknown>
  readonly before: string
  readonly after: string
}) =>
  Effect.gen(function* () {
    const fail = violation(options.name)
    const outcomes = new Set<string>()
    for (let k = 1; k <= 20_000; k++) {
      const subject = yield* options.fresh
      const { scheduler, state } = yieldAt(k)
      const fiber = yield* Effect.forkChild(
        options.run(subject).pipe(Effect.provideService(Scheduler.Scheduler, scheduler))
      )
      while (!state.yielded && fiber.pollUnsafe() === undefined) yield* Effect.yieldNow
      const completed = !state.yielded
      yield* Fiber.interrupt(fiber)
      const exit = yield* Fiber.await(fiber)
      if (exit._tag === "Failure" && !exit.cause.reasons.every((r) => r._tag === "Interrupt")) {
        return yield* fail(`boundary ${k}: the transaction failed instead of committing or being interrupted`)
      }
      const durableNow = yield* options.observe(subject)
      if (durableNow === options.before) outcomes.add("rolled back")
      else if (durableNow === options.after) outcomes.add("committed")
      else return yield* fail(`interrupted at boundary ${k}: partial commit ${durableNow}`)
      if (completed) {
        if (!outcomes.has("rolled back")) return yield* fail("no boundary rolled back: the sweep missed the body")
        return k
      }
    }
    return yield* fail("the transaction never completed within 20000 boundaries")
  })

// ---------------------------------------------------------------------------------------------
// RowStore
// ---------------------------------------------------------------------------------------------

const TEXT = ["", "a", "aa", "a😀", "B", "b", "Z", "é", "\uFFFF", "😀", "\u{10FFFF}"]

const randomItem = (rng: ReturnType<typeof prng>, id: string) =>
  item(id, {
    accessScopeId: rng.chance(0.5) ? s1 : s2,
    label: rng.chance(0.2) ? null : rng.pick(TEXT),
    rank: rng.chance(0.2) ? null : rng.int(7) - 3,
    score: rng.pick([-1.5, -1, 0, 0.25, 1, 2.5, 1e300]),
    flag: rng.chance(0.5),
    at: rng.chance(0.2) ? null : new Date(rng.pick([-86_400_000, -1, 0, 1, 1_700_000_000_000])),
    data: rng.chance(0.5) ? null : { tags: [rng.pick(TEXT)], n: rng.int(3) }
  })

const byId = (a: SyncTable.EncodedRow, b: SyncTable.EncodedRow) => Store.compareText(String(a["id"]), String(b["id"]))

/** The body of the RowStore atomicity checks: overwrite, insert, delete and insert across two tables. */
const rowBody = ({ rows }: RowStoreSubject) =>
  Effect.gen(function* () {
    yield* rows.write(Items, "keep", Option.some(item("keep", { rank: 2 })))
    yield* rows.write(Items, "new", Option.some(item("new")))
    yield* rows.write(Tags, "t", Option.none())
    yield* rows.write(Tags, "u", Option.some(tag("u", s2, "new")))
  })

const rowObserve = (subject: RowStoreSubject) => subject.transaction(rowsOf(subject.rows))

const indexes = [
  Items.indexes.by_label_rank,
  Items.indexes.by_rank_label,
  Items.indexes.by_flag_at,
  Items.indexes.by_score
]

/** A plan the typed builder can express: an eq prefix of declared fields, then bounds on the next. */
const randomPlan = (rng: ReturnType<typeof prng>, rows: ReadonlyArray<SyncTable.EncodedRow>): Store.IndexRangePlan => {
  const index = rng.pick(indexes)
  const valueOf = (column: SyncTable.Column, nullable: boolean): Store.KeyValue => {
    const v = rng.pick(rows)[column.name] ?? null
    if (v === null && !nullable) return valueOf(column, nullable)
    return typeof v === "string" || typeof v === "number" || typeof v === "boolean" || v === null ? v : null
  }
  const eqLength = rng.int(index.fields.length + 1)
  // Not `Arr.makeBy`: it produces at least one element.
  const eq = Array.from({ length: eqLength }, (_, i) => {
    const column = index.key[i]
    if (column === undefined) throw new Error("eq beyond key")
    return valueOf(column, column.nullable)
  })
  const ranged = eqLength < index.fields.length ? index.key[eqLength] : undefined
  const bound = (): Option.Option<Store.Bound> => {
    if (ranged === undefined || rng.chance(0.4)) return Option.none()
    const value = valueOf(ranged, false)
    return value === null ? Option.none() : Option.some({ value, inclusive: rng.chance(0.5) })
  }
  return { index, eq, lower: bound(), upper: bound(), order: rng.chance(0.5) ? "asc" : "desc" }
}

const describePlan = (plan: Store.IndexRangePlan) =>
  canonical({ index: plan.index.name, eq: plan.eq, lower: plan.lower, upper: plan.upper, order: plan.order })

export const rowStore = <R>(make: Effect.Effect<RowStoreSubject, unknown, R>): ReadonlyArray<Check<R>> => [
  check(
    "range returns exactly Store.planMatches rows in Store.planOrder, and scan orders by id code point (seeded random plans)",
    Effect.gen(function* () {
      const name = "range/scan ordering"
      let cases = 0
      for (let seed = 1; seed <= 8; seed++) {
        const rng = prng(seed)
        const { rows, transaction } = yield* make
        const data = Arr.makeBy(24, (i) => randomItem(rng, `${rng.pick(TEXT)}${i}`))
        yield* transaction(Effect.forEach(data, (row) => rows.write(Items, String(row["id"]), Option.some(row))))
        for (let p = 0; p < 40; p++) {
          const plan = randomPlan(rng, data)
          const expected = canonical(data.filter((row) => Store.planMatches(plan, row)).toSorted(Store.planOrder(plan)))
          const actual = canonical(yield* transaction(rows.range(Items, plan)))
          if (actual !== expected) {
            return yield* violation(name)(`seed ${seed}, plan ${describePlan(plan)}:\n got ${actual}\nwant ${expected}`)
          }
          cases++
        }
        const scanned = canonical(yield* transaction(rows.scan(Items)))
        if (scanned !== canonical(data.toSorted(byId))) {
          return yield* violation(name)(`seed ${seed}: scan is not ordered by id code point: ${scanned}`)
        }
      }
      return cases
    })
  ),
  check(
    "read returns the last written wire row exactly; a delete makes it absent",
    Effect.gen(function* () {
      const fail = violation("read/write")
      const { rows, transaction } = yield* make
      const first = item("a", {
        label: "😀",
        rank: -3,
        score: 1e300,
        flag: true,
        at: new Date(-1),
        data: { tags: ["", "é"], n: 0.5 }
      })
      const second = item("a", { label: "", accessScopeId: s2 })
      yield* transaction(rows.write(Items, "a", Option.some(first)))
      const one = canonical(yield* transaction(rows.read(Items, "a")))
      if (one !== canonical(Option.some(first))) return yield* fail(`after insert: ${one}`)
      yield* transaction(rows.write(Items, "a", Option.some(second)))
      const two = canonical(yield* transaction(rows.read(Items, "a")))
      if (two !== canonical(Option.some(second))) return yield* fail(`after overwrite: ${two}`)
      yield* transaction(
        Effect.andThen(rows.write(Items, "a", Option.none()), rows.write(Items, "missing", Option.none()))
      )
      if (Option.isSome(yield* transaction(rows.read(Items, "a")))) return yield* fail("row survived its delete")
      return 3
    })
  ),
  check(
    "a transaction that fails, dies or is interrupted at any boundary leaves all of its writes or none",
    Effect.gen(function* () {
      const name = "one commit/rollback decision"
      const seeded = Effect.gen(function* () {
        const subject = yield* make
        yield* subject.transaction(
          Effect.andThen(
            subject.rows.write(Items, "keep", Option.some(item("keep", { rank: 1 }))),
            subject.rows.write(Tags, "t", Option.some(tag("t", s1, "old")))
          )
        )
        return subject
      })
      const body = rowBody
      const observe = rowObserve
      const reference = yield* seeded
      const before = yield* observe(reference)
      yield* reference.transaction(body(reference))
      const after = yield* observe(reference)
      for (const ending of ["fail", "die"] as const) {
        const subject = yield* seeded
        yield* Effect.exit(
          subject.transaction(
            Effect.andThen(body(subject), ending === "fail" ? Effect.fail("abort") : Effect.die("crash"))
          )
        )
        if ((yield* observe(subject)) !== before)
          return yield* violation(name)(`a body that ${ending}s left writes behind`)
      }
      return yield* interruptSweep({
        name,
        fresh: seeded,
        run: (subject) => subject.transaction(body(subject)),
        observe,
        before,
        after
      })
    })
  ),
  check(
    "a failed or interrupted transaction does not undo a concurrently committed one",
    Effect.gen(function* () {
      const fail = violation("rollback isolation")
      let cases = 0
      for (const ending of ["fail", "interrupt"] as const) {
        const { rows, transaction } = yield* make
        const wrote = yield* Deferred.make<void>()
        const gate = yield* Deferred.make<void>()
        const a = yield* Effect.forkChild(
          Effect.exit(
            transaction(
              Effect.gen(function* () {
                yield* rows.write(Items, "a", Option.some(item("a")))
                yield* Deferred.succeed(wrote, undefined)
                yield* Deferred.await(gate)
                return yield* Effect.fail("abort")
              })
            )
          )
        )
        yield* Deferred.await(wrote)
        const b = yield* Effect.forkChild(transaction(rows.write(Items, "b", Option.some(item("b")))))
        for (let i = 0; i < 20; i++) yield* Effect.yieldNow
        if (ending === "fail") yield* Deferred.succeed(gate, undefined)
        else yield* Fiber.interrupt(a)
        yield* Fiber.join(b)
        yield* Fiber.await(a)
        const ids = (yield* transaction(rows.scan(Items))).map((row) => row["id"])
        if (canonical(ids) !== canonical(["b"])) return yield* fail(`after A ${ending}ed: ${canonical(ids)}`)
        cases++
      }
      return cases
    })
  )
]

// ---------------------------------------------------------------------------------------------
// ReplicaRowStore
// ---------------------------------------------------------------------------------------------

export const replicaRowStore = <R>(
  make: Effect.Effect<RowStoreSubject<Store.ReplicaRowStore>, unknown, R>
): ReadonlyArray<Check<R>> => [
  ...rowStore(make),
  check(
    "purgeScope deletes exactly the rows of that scope, in every table",
    Effect.gen(function* () {
      const { rows, transaction } = yield* make
      yield* transaction(
        Effect.all([
          rows.write(Items, "a", Option.some(item("a", { accessScopeId: s1 }))),
          rows.write(Items, "b", Option.some(item("b", { accessScopeId: s2 }))),
          rows.write(Tags, "c", Option.some(tag("c", s1, "x"))),
          rows.write(Tags, "d", Option.some(tag("d", s2, "y")))
        ])
      )
      yield* transaction(rows.purgeScope(s1))
      const left = yield* transaction(rowsOf(rows))
      const want = canonical({ items: [item("b", { accessScopeId: s2 })], tags: [tag("d", s2, "y")] })
      if (left !== want) return yield* violation("purgeScope")(`left ${left}`)
      return 1
    })
  )
]

// ---------------------------------------------------------------------------------------------
// SyncStorage
// ---------------------------------------------------------------------------------------------

const clientId = Ids.ClientId.make("conformance")
const envelope = (seq: Ids.Seq, mutationId: string): Mutation.MutationEnvelope => ({
  clientId,
  mutationId: Ids.MutationId.make(mutationId),
  seq,
  name: "conformance",
  args: { nested: { list: [1, "😀", null, true] }, big: 2 ** 53 - 1 }
})
const position = (p: bigint) => Ids.Position.make(p)

/** Allocates the next seq in a transaction that then rolls back: observes it without using it. */
const peekSeq = (storage: Engine.SyncStorage) =>
  storage.transaction(Effect.flatMap(storage.nextSeq, (seq) => Effect.fail({ peeked: seq }))).pipe(
    Effect.flip,
    Effect.flatMap((e) => ("peeked" in e ? Effect.succeed(e.peeked) : Effect.fail(e)))
  )

/** Everything the port can read back (notices have no reader), in one transaction, plus the next seq. */
const durableState = (storage: Engine.SyncStorage, ids: ReadonlyArray<string>) =>
  storage
    .transaction(
      Effect.all({
        rows: rowsOf(storage.rows),
        meta: storage.meta,
        pending: storage.pending,
        known: Effect.forEach(ids, (id) => storage.lookup(Ids.MutationId.make(id))),
        staged: storage.staged
      })
    )
    .pipe(Effect.flatMap((state) => Effect.map(peekSeq(storage), (next) => ({ ...state, next }))))
const durable = (storage: Engine.SyncStorage, ids: ReadonlyArray<string>) =>
  Effect.map(durableState(storage, ids), canonical)

const page = (continuation: string): typeof Protocol.PullPage.Type => ({
  _tag: "PullPage",
  changes: [
    { _tag: "upsert", table: Items.name, row: item("p", { label: "😀" }) },
    { _tag: "tombstone", table: Tags.name, id: "gone" },
    { _tag: "scopePurged", scopeId: s2 }
  ],
  acks: [
    {
      mutationId: Ids.MutationId.make("acked"),
      position: position(9_007_199_254_740_993n),
      outcome: { _tag: "Applied" }
    }
  ],
  continuation: Protocol.PullContinuation.make(continuation)
})

/** The body of the SyncStorage atomicity check: touches every kind of durable state. */
const syncBody = ({ storage }: SyncStorageSubject) =>
  Effect.gen(function* () {
    const seq = yield* storage.nextSeq
    yield* storage.appendPending({ envelope: envelope(seq, "new"), createdAt: 0, undo: [] })
    yield* storage.rows.write(Items, "keep", Option.some(item("keep", { rank: 2 })))
    yield* storage.rows.write(Tags, "t", Option.some(tag("t", s1, "x")))
    yield* storage.setUndo(seq, [{ table: Tags, id: "t", before: Option.none() }])
    yield* storage.release([Ids.Seq.make(1)])
    yield* storage.setMeta({ cursor: position(2n), pushedThrough: 1, resnapshot: false })
    yield* storage.stage(page("k"))
  })

export const syncStorage = <R>(make: Effect.Effect<SyncStorageSubject, unknown, R>): ReadonlyArray<Check<R>> => [
  ...replicaRowStore(Effect.map(make, ({ storage }) => ({ rows: storage.rows, transaction: storage.transaction }))),
  check(
    "meta, pending entries (args, undo), released markers and staged pages survive a reopen unchanged",
    Effect.gen(function* () {
      const fail = violation("durability")
      const { storage, reopen } = yield* make
      yield* storage.transaction(
        Effect.gen(function* () {
          const a = yield* storage.nextSeq
          const b = yield* storage.nextSeq
          yield* storage.appendPending({ envelope: envelope(a, "m-a"), createdAt: -1, undo: [] })
          yield* storage.appendPending({
            envelope: envelope(b, "m-b"),
            createdAt: 1_700_000_000_123,
            undo: [
              { table: Items, id: "x", before: Option.some(item("x", { label: "é", data: { tags: [], n: 1 } })) },
              { table: Tags, id: "y", before: Option.none() }
            ]
          })
          yield* storage.release([a])
          yield* storage.setMeta({ cursor: position(9_007_199_254_740_993n), pushedThrough: 1, resnapshot: true })
          yield* storage.stage(page("k1"))
          yield* storage.stage(page("k2"))
        })
      )
      const ids = ["m-a", "m-b", "never"]
      const before = yield* durable(storage, ids)
      const after = yield* durable(yield* reopen, ids)
      if (after !== before) return yield* fail(`before reopen ${before}\n after reopen ${after}`)
      return 1
    })
  ),
  check(
    "seq starts at 1 and is never reused: not after release, prune and reopen, and a rolled-back allocation leaves no gap",
    Effect.gen(function* () {
      const fail = violation("seq allocation")
      const { storage, reopen, pruneReleased } = yield* make
      const seqs = yield* storage.transaction(
        Effect.gen(function* () {
          const out: Array<Ids.Seq> = []
          for (const id of ["m1", "m2", "m3"]) {
            const seq = yield* storage.nextSeq
            yield* storage.appendPending({ envelope: envelope(seq, id), createdAt: 0, undo: [] })
            out.push(seq)
          }
          return out
        })
      )
      if (canonical(seqs) !== canonical([1, 2, 3])) return yield* fail(`first allocations ${canonical(seqs)}`)
      yield* storage.transaction(storage.release(seqs.slice(0, 2)))
      const pending = (yield* storage.transaction(storage.pending)).map((e) => e.envelope.seq)
      if (canonical(pending) !== canonical([3])) return yield* fail(`pending after release ${canonical(pending)}`)
      const released = yield* storage.transaction(storage.lookup(Ids.MutationId.make("m1")))
      if (Option.isNone(released) || released.value.seq !== 1) return yield* fail("lookup misses a released entry")
      const peeked = yield* peekSeq(storage)
      if (peeked !== 4) return yield* fail(`next seq ${peeked}, want 4`)
      if ((yield* peekSeq(storage)) !== 4) return yield* fail("a rolled-back allocation left a gap")
      yield* storage.transaction(storage.release([seqs[2] ?? Ids.Seq.make(3)]))
      yield* pruneReleased
      const reopened = yield* reopen
      const next = yield* peekSeq(reopened)
      // The outbox is now empty and every marker pruned: a MAX(seq)+1 allocator would restart at 1.
      if (next !== 4) return yield* fail(`after release + prune + reopen the next seq is ${next}, want 4`)
      return 4
    })
  ),
  check(
    "clearAll deletes every replica row and nothing else (outbox, undo, markers, meta, staging)",
    Effect.gen(function* () {
      const fail = violation("clearAll")
      const { storage, reopen } = yield* make
      yield* storage.transaction(
        Effect.gen(function* () {
          yield* storage.rows.write(Items, "a", Option.some(item("a")))
          yield* storage.rows.write(Tags, "b", Option.some(tag("b", s2, "x")))
          const one = yield* storage.nextSeq
          const two = yield* storage.nextSeq
          yield* storage.appendPending({ envelope: envelope(one, "r"), createdAt: 0, undo: [] })
          yield* storage.appendPending({
            envelope: envelope(two, "p"),
            createdAt: 0,
            undo: [{ table: Items, id: "a", before: Option.none() }]
          })
          yield* storage.release([one])
          yield* storage.setMeta({ cursor: position(5n), pushedThrough: 1, resnapshot: false })
          yield* storage.stage(page("k"))
        })
      )
      const ids = ["r", "p"]
      const { rows: populated, ...before } = yield* durableState(storage, ids)
      if (populated === canonical({ items: [], tags: [] })) return yield* fail("setup wrote no rows")
      yield* storage.transaction(storage.rows.clearAll)
      const { rows, ...after } = yield* durableState(yield* reopen, ids)
      if (rows !== canonical({ items: [], tags: [] })) return yield* fail(`rows left: ${rows}`)
      if (canonical(after) !== canonical(before)) {
        return yield* fail(`non-row state changed:\n${canonical(before)}\n${canonical(after)}`)
      }
      return 1
    })
  ),
  check(
    "undo images captured by Store.make persist through setUndo and a reopen, and restore the exact prior rows",
    Effect.gen(function* () {
      const fail = violation("undo round-trip")
      const { storage, reopen } = yield* make
      const seed = [item("i1", { label: "a", rank: 1 }), item("i2", { flag: true }), item("i3", { accessScopeId: s1 })]
      const seq = yield* storage.transaction(
        Effect.gen(function* () {
          for (const row of seed) yield* storage.rows.write(Items, String(row["id"]), Option.some(row))
          yield* storage.rows.write(Tags, "t", Option.some(tag("t", s1, "old")))
          const allocated = yield* storage.nextSeq
          yield* storage.appendPending({ envelope: envelope(allocated, "u"), createdAt: 0, undo: [] })
          return allocated
        })
      )
      const prior = yield* storage.transaction(rowsOf(storage.rows))
      const images = yield* storage.transaction(
        Effect.gen(function* () {
          const { store, touched } = Store.make(storage.rows, { roleFor: () => Effect.succeedSome("owner" as const) })
          yield* store.patch(Items, ItemId.make("i1"), { label: "b" })
          yield* store.patch(Items, ItemId.make("i1"), { rank: 99 })
          yield* store.delete(Items, ItemId.make("i2"))
          yield* store.patch(Items, ItemId.make("i3"), { accessScopeId: s2 })
          yield* store.insert(Tags, { id: TagId.make("n"), accessScopeId: s1, name: "new" })
          yield* store.patch(Tags, TagId.make("t"), { name: "renamed" })
          const captured = yield* touched
          yield* storage.setUndo(seq, captured)
          return captured
        })
      )
      const reopened = yield* reopen
      const persisted = (yield* reopened.transaction(reopened.pending))[0]?.undo ?? []
      if (canonical(persisted) !== canonical(images)) {
        return yield* fail(`persisted ${canonical(persisted)}\n captured ${canonical(images)}`)
      }
      yield* reopened.transaction(Engine.restore(reopened.rows, persisted))
      const restored = yield* reopened.transaction(rowsOf(reopened.rows))
      if (restored !== prior) return yield* fail(`restored ${restored}\n prior ${prior}`)
      return images.length
    })
  ),
  check(
    "a storage transaction interrupted at any boundary commits rows, outbox, seq, meta and staging together or not at all",
    Effect.gen(function* () {
      const name = "one commit/rollback decision (SyncStorage)"
      const ids = ["old", "new"]
      const seeded = Effect.gen(function* () {
        const subject = yield* make
        const { storage } = subject
        yield* storage.transaction(
          Effect.gen(function* () {
            yield* storage.rows.write(Items, "keep", Option.some(item("keep", { rank: 1 })))
            const seq = yield* storage.nextSeq
            yield* storage.appendPending({ envelope: envelope(seq, "old"), createdAt: 0, undo: [] })
          })
        )
        return subject
      })
      const body = syncBody
      const observe = (subject: SyncStorageSubject) => Effect.flatMap(subject.reopen, (s) => durable(s, ids))
      const reference = yield* seeded
      const before = yield* observe(reference)
      yield* reference.storage.transaction(body(reference))
      const after = yield* observe(reference)
      for (const ending of ["fail", "die"] as const) {
        const subject = yield* seeded
        yield* Effect.exit(
          subject.storage.transaction(
            Effect.andThen(body(subject), ending === "fail" ? Effect.fail("abort") : Effect.die("crash"))
          )
        )
        if ((yield* observe(subject)) !== before)
          return yield* violation(name)(`a body that ${ending}s left writes behind`)
      }
      return yield* interruptSweep({
        name,
        fresh: seeded,
        run: (subject) => subject.storage.transaction(body(subject)),
        observe,
        before,
        after
      })
    })
  )
]
