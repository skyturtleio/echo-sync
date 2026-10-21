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
 * Factory contract: each call returns a fresh, empty store in which every table in `tables` exists,
 * and core's `scope_grants` exists empty and accepts grants in `grantedScopes` (range plans filter
 * readable scopes through it) (create their DDL; a `SyncStorage` registry must contain them, since persisted undo images are
 * resolved by table name). The factory may use a `Scope`: every case (each seed, each interrupt
 * boundary) acquires its own subject in its own scope and closes it before the next case.
 *
 * Every expectation is a constant derived from the fixture the check wrote, never an observation
 * of a subject (DESIGN §14): a subject whose writes are no-ops fails every check.
 */
import { Array as Arr, Cause, DateTime, Deferred, Effect, Exit, Fiber, Option, Schema, Scheduler } from "effect"
import type { Scope } from "effect"
import * as Engine from "../../client/sync/Engine.ts"
import { AccessScope, Ids, Protocol, ScopeGrants, Store, SyncTable } from "../index.ts"
import { Errors } from "../index.ts"
import type { Mutation } from "../index.ts"

// ---------------------------------------------------------------------------------------------
// Subjects and checks
// ---------------------------------------------------------------------------------------------

/** A row store plus the substrate's transaction (which owns commit/rollback). */
export interface RowStoreSubject<S extends Store.RowStore = Store.RowStore> {
  readonly rows: S
  readonly transaction: <A, E, R>(self: Effect.Effect<A, E, R>) => Effect.Effect<A, E | Errors.StoreUnavailable, R>
  /** The substrate's scope claim (DESIGN §4); default: a replica's (`Engine.claimFromGrants`). */
  readonly claimScope?: Store.StoreOptions["claimScope"] | undefined
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

const check = <R>(name: string, run: Effect.Effect<number, unknown, Exclude<R, Scope.Scope>>): Check<R> => ({
  name,
  run
})

/** One case: acquire a subject in a fresh scope, use it, close it. */
const use = <S, R, A>(make: Effect.Effect<S, unknown, R>, f: (subject: S) => Effect.Effect<A, unknown>) =>
  Effect.scoped(Effect.flatMap(make, f))

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

// Readable-scope fixture: the actor u1 reads `mine` (own grant) and `open` (the `*` grant); `theirs`
// has a grant for u2 only, `s2` none.
const u1 = AccessScope.PrincipalId.make("u1")
const READERS: ReadonlyArray<AccessScope.PrincipalId> = [u1, AccessScope.Everyone]
const mine = AccessScope.AccessScopeId.make("mine")
const open = AccessScope.AccessScopeId.make("open")
const theirs = AccessScope.AccessScopeId.make("theirs")
/** Scopes the suite writes grants in; a store whose grants need the scope to exist must create them. */
export const grantedScopes: ReadonlyArray<AccessScope.AccessScopeId> = [mine, open, theirs]
const READABLE: ReadonlyArray<AccessScope.AccessScopeId> = [mine, open]
const UNREADABLE: ReadonlyArray<AccessScope.AccessScopeId> = [theirs, s2]
const grant = (
  scope: AccessScope.AccessScopeId,
  principal: AccessScope.PrincipalId,
  role: AccessScope.Role,
  id = ScopeGrants.grantId(scope, principal)
) => {
  const row = Schema.encodeSync(ScopeGrants.ScopeGrants.wire)({
    id,
    accessScopeId: scope,
    principalId: principal,
    role,
    grantedAt: DateTime.makeUnsafe(0)
  })
  return [id, row] as const
}
const GRANTS = [
  grant(mine, u1, "reader"),
  // A derived id: the server CHECK (DESIGN §4) rejects any other, so malformed-id rejection is tested
  // in src/server/postgres/grants.test.ts, not here.
  grant(open, AccessScope.Everyone, "writer"),
  grant(theirs, AccessScope.PrincipalId.make("u2"), "owner")
]
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

/**
 * JSON with bigints and table objects made comparable. Object keys are sorted at every depth
 * (key order is not part of any store contract); array order is kept.
 */
const canonical = (value: unknown): string =>
  JSON.stringify(value, (_, v: unknown) => {
    if (typeof v === "bigint") return `${v}n`
    if (isTable(v)) return { table: v.name }
    if (typeof v !== "object" || v === null || Array.isArray(v)) return v
    return Object.fromEntries(Object.entries(v).toSorted(([a], [b]) => Store.compareText(a, b)))
  })
/** A table object (inside undo images) is identified by its sync name. */
const isTable = (v: unknown): v is { readonly name: string } =>
  typeof v === "object" && v !== null && "wire" in v && "columns" in v && "name" in v && typeof v.name === "string"

const violation = (name: string) => (detail: string) => new ConformanceViolation({ check: name, detail })

const rowsOf = (rows: Store.RowStore) =>
  Effect.all({ items: rows.scan(Items), tags: rows.scan(Tags) }).pipe(Effect.map(canonical))

/**
 * A scheduler that requests interruption of the running fiber synchronously, from its `k`-th
 * `shouldYield` hook (the run loop calls it once per evaluation step, including the step that
 * resumes after asynchronous I/O). No polling parent is involved, so the request cannot race the
 * fiber; the run loop delivers it at its next interruption check, after at most one more
 * primitive (it checks for a pending interruption before calling the hook). Uninterruptible
 * regions, such as a store's commit/rollback finalizer, still run to completion.
 */
const interruptAt = (k: number) => {
  const base = new Scheduler.MixedScheduler()
  const state = { ticks: 0, fired: false }
  const scheduler: Scheduler.Scheduler = {
    executionMode: base.executionMode,
    shouldYield: (fiber) => {
      if (!state.fired && ++state.ticks === k) {
        state.fired = true
        fiber.interruptUnsafe()
      }
      return false
    },
    makeDispatcher: () => base.makeDispatcher()
  }
  return { scheduler, state }
}

/** One transaction under test: a fixture `seed`, a `body`, and how the subject runs a transaction. */
interface TransactionCase<S, R> {
  readonly name: string
  readonly make: Effect.Effect<S, unknown, R>
  readonly seed: (subject: S) => Effect.Effect<unknown, unknown>
  readonly body: (subject: S) => Effect.Effect<unknown, unknown>
  readonly within: (subject: S, self: Effect.Effect<unknown, unknown>) => Effect.Effect<unknown, unknown>
  /**
   * Reads the durable state through a reopened handle. Called only after the tested transaction
   * has finished: a reopen may close the subject's original handle.
   */
  readonly observe: (subject: S) => Effect.Effect<string, unknown>
  /** Reads the same state through the subject's live handle (no reopen). */
  readonly observeLive: (subject: S) => Effect.Effect<string, unknown>
  /** Durable state after `seed` alone, computed from the fixture. */
  readonly before: string
  /** Durable state after `seed` and a committed `body`, computed from the fixture. */
  readonly after: string
}

/**
 * For k = 1, 2, ... : in a fresh scope, acquire and seed a subject, run the transaction with an
 * interruption requested at its k-th checkpoint, observe the durable state, close the subject.
 * Stops at the first k the run completes before reaching. Every observation must equal the
 * fixture's `before` or `after`, never a mixture, and both must occur.
 */
const interruptSweep = <S, R>(options: TransactionCase<S, R>) =>
  Effect.gen(function* () {
    const fail = violation(options.name)
    const outcomes = new Set<string>()
    for (let k = 1; k <= 20_000; k++) {
      const trial = yield* use(options.make, (subject) =>
        Effect.gen(function* () {
          yield* options.seed(subject)
          const { scheduler, state } = interruptAt(k)
          const fiber = yield* Effect.forkChild(
            options.within(subject, options.body(subject)).pipe(Effect.provideService(Scheduler.Scheduler, scheduler))
          )
          const exit = yield* Fiber.await(fiber)
          if (exit._tag === "Failure" && !exit.cause.reasons.every((r) => r._tag === "Interrupt")) {
            return yield* fail(`boundary ${k}: the transaction failed instead of committing or being interrupted`)
          }
          return { completed: !state.fired, observed: yield* options.observe(subject) }
        })
      )
      if (trial.observed === options.before) outcomes.add("rolled back")
      else if (trial.observed === options.after) outcomes.add("committed")
      else return yield* fail(`interrupted at boundary ${k}:\n got ${trial.observed}`)
      if (trial.completed) {
        if (trial.observed !== options.after) return yield* fail("an uninterrupted run did not commit")
        if (!outcomes.has("rolled back")) return yield* fail("no boundary rolled back: the sweep missed the body")
        return k
      }
    }
    return yield* fail("the transaction never completed within 20000 boundaries")
  })

/**
 * Atomicity of one transaction: the seed reads back as `before`; a body that fails or dies
 * inside the transaction leaves `before`; then the interrupt sweep. Each case has its own subject.
 */
const atomicity = <S, R>(options: TransactionCase<S, R>) =>
  Effect.gen(function* () {
    const fail = violation(options.name)
    for (const ending of ["fail", "die"] as const) {
      yield* use(options.make, (subject) =>
        Effect.gen(function* () {
          yield* options.seed(subject)
          const seeded = yield* options.observeLive(subject)
          if (seeded !== options.before) return yield* fail(`seed:\n got ${seeded}\nwant ${options.before}`)
          const end = ending === "fail" ? Effect.fail("abort") : Effect.die("crash")
          const exit = yield* Effect.exit(options.within(subject, Effect.andThen(options.body(subject), end)))
          // Only the injected ending proves the body ran to its end; any other failure is a defect
          // of the subject (e.g. a closed handle), not a rollback to judge.
          const intended =
            exit._tag === "Failure" &&
            exit.cause.reasons.length === 1 &&
            exit.cause.reasons.every((r) =>
              ending === "fail" ? r._tag === "Fail" && r.error === "abort" : r._tag === "Die" && r.defect === "crash"
            )
          if (!intended) return yield* fail(`a body ending in ${ending} did not end that way: ${String(exit)}`)
          const left = yield* options.observe(subject)
          if (left !== options.before) return yield* fail(`a body that ${ending}s left\n ${left}`)
        })
      )
    }
    return yield* interruptSweep(options)
  })

// ---------------------------------------------------------------------------------------------
// RowStore
// ---------------------------------------------------------------------------------------------

// Code-point order differs from UTF-16 order between U+FFFF and astral characters.
const TEXT = ["", "a", "aa", "a\u{1F600}", "B", "b", "Z", "\u00E9", "\uFFFF", "\u{1F600}", "\u{10FFFF}"]

// Beyond int4 on both sides up to the safe-integer limits: int8 storage must round-trip and order
// these as numbers (DESIGN §10), not only small values.
const WIDE_INTS = [
  -Number.MAX_SAFE_INTEGER,
  -(2 ** 31 + 1),
  -(2 ** 31),
  2 ** 31 - 1,
  2 ** 31,
  2 ** 31 + 1,
  Number.MAX_SAFE_INTEGER
]

const randomItem = (rng: ReturnType<typeof prng>, id: string) =>
  item(id, {
    accessScopeId: rng.chance(0.5) ? s1 : s2,
    label: rng.chance(0.2) ? null : rng.pick(TEXT),
    rank: rng.chance(0.2) ? null : rng.chance(0.5) ? rng.int(7) - 3 : rng.pick(WIDE_INTS),
    score: rng.pick([-1.5, -1, 0, 0.25, 1, 2.5, 1e300]),
    flag: rng.chance(0.5),
    at: rng.chance(0.2) ? null : new Date(rng.pick([-86_400_000, -1, 0, 1, 1_700_000_000_000])),
    data: rng.chance(0.5) ? null : { tags: [rng.pick(TEXT)], n: rng.int(3) }
  })

const byId = (a: SyncTable.EncodedRow, b: SyncTable.EncodedRow) => Store.compareText(String(a["id"]), String(b["id"]))

const EMPTY_ROWS = canonical({ items: [], tags: [] })

const rowSeed = ({ rows, transaction }: RowStoreSubject) =>
  transaction(
    Effect.andThen(
      rows.write(Items, "keep", Option.some(item("keep", { rank: 1 }))),
      rows.write(Tags, "t", Option.some(tag("t", s1, "old")))
    )
  )

/** The body of the RowStore atomicity check: overwrite, insert, delete and insert across two tables. */
const rowBody = ({ rows }: RowStoreSubject) =>
  Effect.gen(function* () {
    yield* rows.write(Items, "keep", Option.some(item("keep", { rank: 2 })))
    yield* rows.write(Items, "new", Option.some(item("new")))
    yield* rows.write(Tags, "t", Option.none())
    yield* rows.write(Tags, "u", Option.some(tag("u", s2, "new")))
  })

const rowBefore = canonical({ items: [item("keep", { rank: 1 })], tags: [tag("t", s1, "old")] })
const rowAfter = canonical({ items: [item("keep", { rank: 2 }), item("new")], tags: [tag("u", s2, "new")] })

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
  return {
    index,
    eq,
    lower: bound(),
    upper: bound(),
    order: rng.chance(0.5) ? "asc" : "desc",
    readableBy: Option.none(),
    limit: Option.none()
  }
}

const describePlan = (plan: Store.IndexRangePlan) =>
  canonical({ index: plan.index.name, eq: plan.eq, lower: plan.lower, upper: plan.upper, order: plan.order })

export const rowStore = <R>(make: Effect.Effect<RowStoreSubject, unknown, R>): ReadonlyArray<Check<R>> => [
  check(
    "range returns exactly Store.planMatches rows in Store.planOrder, and scan orders by id code point (seeded random plans)",
    Effect.gen(function* () {
      const fail = violation("range/scan ordering")
      let cases = 0
      for (let seed = 1; seed <= 8; seed++) {
        cases += yield* use(make, ({ rows, transaction }) =>
          Effect.gen(function* () {
            const rng = prng(seed)
            const data = Arr.makeBy(24, (i) => randomItem(rng, `${rng.pick(TEXT)}${i}`))
            yield* transaction(Effect.forEach(data, (row) => rows.write(Items, String(row["id"]), Option.some(row))))
            for (let p = 0; p < 40; p++) {
              const plan = randomPlan(rng, data)
              const expected = canonical(
                data.filter((row) => Store.planMatches(plan, row)).toSorted(Store.planOrder(plan))
              )
              const actual = canonical(yield* transaction(rows.range(Items, plan)))
              if (actual !== expected) {
                return yield* fail(`seed ${seed}, plan ${describePlan(plan)}:\n got ${actual}\nwant ${expected}`)
              }
            }
            const scanned = canonical(yield* transaction(rows.scan(Items)))
            if (scanned !== canonical(data.toSorted(byId))) {
              return yield* fail(`seed ${seed}: scan is not ordered by id code point: ${scanned}`)
            }
            return 40
          })
        )
      }
      return cases
    })
  ),
  check(
    "a limited range with a readable-scope constraint returns the first n readable rows in index order, not the readable rows among the first n (open item 8)",
    Effect.gen(function* () {
      const fail = violation("scoped limit")
      let cases = 0
      for (let seed = 1; seed <= 6; seed++) {
        cases += yield* use(make, ({ rows, transaction }) =>
          Effect.gen(function* () {
            const rng = prng(seed)
            // Scores 0..39 in index order. The first and last five rows are unreadable, so a store
            // that limits before filtering returns fewer than n rows in either order.
            const scopes = Array.from({ length: 40 }, (_, i) =>
              i < 5 || i >= 35 ? rng.pick(UNREADABLE) : rng.chance(0.3) ? rng.pick(READABLE) : rng.pick(UNREADABLE)
            )
            const data = scopes.map((scope, i) => ({
              ...randomItem(rng, `r${String(i).padStart(2, "0")}`),
              access_scope_id: scope,
              score: i
            }))
            yield* transaction(
              Effect.all([
                Effect.forEach(GRANTS, ([id, row]) => rows.write(ScopeGrants.ScopeGrants, id, Option.some(row))),
                Effect.forEach(data, (row) => rows.write(Items, String(row["id"]), Option.some(row)))
              ])
            )
            const readableIds = data.filter((_, i) => READABLE.includes(scopes[i] ?? s2)).map((r) => r["id"])
            let n = 0
            for (const order of ["asc", "desc"] as const) {
              const inOrder = order === "asc" ? readableIds : readableIds.toReversed()
              for (const limit of [0, 1, 2, 3, 7, inOrder.length, inOrder.length + 5]) {
                const plan: Store.IndexRangePlan = {
                  index: Items.indexes.by_score,
                  eq: [],
                  lower: Option.none(),
                  upper: Option.none(),
                  order,
                  readableBy: Option.some(READERS),
                  limit: Option.some(limit)
                }
                const got = (yield* transaction(rows.range(Items, plan))).map((r) => r["id"])
                const want = inOrder.slice(0, limit)
                if (canonical(got) !== canonical(want)) {
                  return yield* fail(
                    `seed ${seed}, ${order}, limit ${limit}:\n got ${canonical(got)}\nwant ${canonical(want)}`
                  )
                }
                n++
              }
            }
            // Random plans (eq prefixes, bounds, both orders) with the scope constraint and a limit,
            // against core's reference over the same fixture.
            for (let p = 0; p < 30; p++) {
              const plan: Store.IndexRangePlan = {
                ...randomPlan(rng, data),
                readableBy: Option.some(READERS),
                limit: rng.chance(0.2) ? Option.none() : Option.some(rng.int(6))
              }
              const want = canonical(
                Store.planResult(
                  plan,
                  data,
                  GRANTS.map(([, row]) => row)
                )
              )
              const got = canonical(yield* transaction(rows.range(Items, plan)))
              if (got !== want)
                return yield* fail(`seed ${seed}, plan ${describePlan(plan)}:\n got ${got}\nwant ${want}`)
              n++
            }
            return n
          })
        )
      }
      return cases
    })
  ),
  check(
    "take(n) defects identically for NaN, -1, 1.5 and 2^53 (never clamped or unlimited); take(0) is []; collect is unlimited (DESIGN §10)",
    use(make, ({ rows, transaction }) =>
      Effect.gen(function* () {
        const fail = violation("take(n) limits")
        // 120 readable rows: more than any default page or limit a store might impose.
        const data = Array.from({ length: 120 }, (_, i) =>
          item(`t${String(i).padStart(3, "0")}`, { accessScopeId: mine, score: i })
        )
        yield* transaction(
          Effect.all([
            Effect.forEach(GRANTS, ([id, row]) => rows.write(ScopeGrants.ScopeGrants, id, Option.some(row))),
            Effect.forEach(data, (row) => rows.write(Items, String(row["id"]), Option.some(row)))
          ])
        )
        const { store } = Store.make(rows, {
          actor: u1,
          roleFor: () => Effect.succeedSome("reader" as const),
          claimScope: () => Effect.succeed(false),
          now: DateTime.makeUnsafe(0)
        })
        const query = store.query(Items).withIndex("by_score")
        const all = yield* transaction(query.collect)
        if (canonical(all.map((r) => r.id)) !== canonical(data.map((r) => r["id"]))) {
          return yield* fail(`collect returned ${all.length} rows, want all 120 in score order`)
        }
        const none = yield* transaction(query.take(0))
        if (none.length !== 0) return yield* fail(`take(0) returned ${none.length} rows`)
        let cases = 2
        for (const n of [Number.NaN, -1, 1.5, 2 ** 53, Number.POSITIVE_INFINITY]) {
          const want = `take(n) requires a non-negative safe integer, got ${n}`
          const viaStore = yield* Effect.exit(transaction(Effect.map(query.take(n), (found) => found.length)))
          const viaRange = yield* Effect.exit(
            transaction(
              Effect.map(
                rows.range(Items, {
                  index: Items.indexes.by_score,
                  eq: [],
                  lower: Option.none(),
                  upper: Option.none(),
                  order: "asc",
                  readableBy: Option.none(),
                  limit: Option.some(n)
                }),
                (found) => found.length
              )
            )
          )
          for (const [label, exit] of [
            ["take", viaStore],
            ["range", viaRange]
          ] as const) {
            const defects = Exit.isSuccess(exit)
              ? []
              : exit.cause.reasons.flatMap((r) => (Cause.isDieReason(r) ? [r.defect] : []))
            const ok = defects.some((d) => d instanceof RangeError && d.message === want)
            if (!ok) {
              return yield* fail(
                `${label} with limit ${n}: want a defect "${want}", got ${Exit.isSuccess(exit) ? `success (${exit.value} rows)` : String(exit.cause)}`
              )
            }
            cases++
          }
        }
        return cases
      })
    )
  ),
  check(
    "read returns the last written wire row exactly; a delete makes it absent",
    use(make, ({ rows, transaction }) =>
      Effect.gen(function* () {
        const fail = violation("read/write")
        const first = item("a", {
          label: "\u{1F600}",
          rank: -3,
          score: 1e300,
          flag: true,
          at: new Date(-1),
          data: { tags: ["", "\u00E9"], n: 0.5 }
        })
        const second = item("a", { label: "", accessScopeId: s2, rank: Number.MAX_SAFE_INTEGER })
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
    )
  ),
  check(
    "a transaction that fails, dies or is interrupted at any boundary leaves all of its writes or none",
    atomicity({
      name: "one commit/rollback decision",
      make,
      seed: rowSeed,
      body: rowBody,
      within: (subject, self) => subject.transaction(self),
      observe: rowObserve,
      observeLive: rowObserve,
      before: rowBefore,
      after: rowAfter
    })
  ),
  check(
    "a failed or interrupted transaction does not undo a concurrently committed one",
    Effect.gen(function* () {
      const fail = violation("rollback isolation")
      let cases = 0
      for (const ending of ["fail", "interrupt"] as const) {
        yield* use(make, ({ rows, transaction }) =>
          Effect.gen(function* () {
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
            const left = canonical(yield* transaction(rows.scan(Items)))
            const want = canonical([item("b")])
            if (left !== want) return yield* fail(`after A ${ending}ed:\n got ${left}\nwant ${want}`)
          })
        )
        cases++
      }
      return cases
    })
  ),
  check(
    "createScope writes exactly the actor's owner grant on a fresh id; an existing id is ScopeForbidden{owner} with nothing written; the actor * is refused before any claim (DESIGN §4)",
    use(make, (subject) =>
      Effect.gen(function* () {
        const { rows, transaction } = subject
        const fail = violation("createScope")
        const substrateClaim = subject.claimScope ?? Engine.claimFromGrants(rows)
        const claims: Array<string> = []
        const instance = (actor: AccessScope.PrincipalId) =>
          Store.make(rows, {
            actor,
            roleFor: () => Effect.succeedNone,
            claimScope: (scopeId) =>
              Effect.andThen(
                Effect.sync(() => claims.push(scopeId)),
                substrateClaim(scopeId)
              ),
            now: DateTime.makeUnsafe(0)
          })
        yield* transaction(
          Effect.forEach(GRANTS, ([id, row]) => rows.write(ScopeGrants.ScopeGrants, id, Option.some(row)))
        )
        const grants = Effect.map(transaction(rows.scan(ScopeGrants.ScopeGrants)), canonical)
        const seeded = yield* grants
        const fresh = AccessScope.AccessScopeId.make("created")
        const u2 = AccessScope.PrincipalId.make("u2")

        // Refused, nothing written: an existing scope (u2's grant), and any scope for the actor *.
        const refused = [
          { actor: u1, scope: theirs, claimed: [theirs] },
          { actor: AccessScope.Everyone, scope: fresh, claimed: [] }
        ]
        for (const { actor, scope, claimed } of refused) {
          claims.length = 0
          const { store, touched } = instance(actor)
          const exit = yield* Effect.exit(transaction(store.createScope(scope)))
          const want = canonical(new Errors.ScopeForbidden({ scopeId: scope, required: "owner", held: null }))
          const got = Exit.isFailure(exit) ? canonical(Cause.squash(exit.cause)) : "success"
          if (got !== want) return yield* fail(`${actor} creating ${scope}: got ${got}, want ${want}`)
          if ((yield* touched).length !== 0) return yield* fail(`${actor} creating ${scope} wrote rows`)
          if (canonical(claims) !== canonical(claimed)) {
            return yield* fail(`${actor} creating ${scope} claimed ${canonical(claims)}, want ${canonical(claimed)}`)
          }
          if ((yield* grants) !== seeded) return yield* fail(`${actor} creating ${scope} changed scope_grants`)
        }

        // A fresh id: exactly one new grant, the actor's, as owner, through the write path.
        const created = instance(u1)
        yield* transaction(created.store.createScope(fresh))
        const [newId, newRow] = grant(fresh, u1, "owner")
        const want = canonical(
          [...GRANTS.map(([, row]) => row), newRow].toSorted((a, b) =>
            Store.compareText(String(a["id"]), String(b["id"]))
          )
        )
        const got = yield* grants
        if (got !== want) return yield* fail(`after creating ${fresh}:\n got ${got}\nwant ${want}`)
        const images = canonical(yield* created.touched)
        const wantImages = canonical([{ table: ScopeGrants.ScopeGrants, id: newId, before: Option.none() }])
        if (images !== wantImages) return yield* fail(`undo images ${images}, want ${wantImages}`)

        // Now taken: another principal cannot create it again.
        const again = yield* Effect.exit(transaction(instance(u2).store.createScope(fresh)))
        if (Exit.isSuccess(again)) return yield* fail(`u2 re-created ${fresh}`)
        if ((yield* grants) !== want) return yield* fail(`u2's refused re-create changed scope_grants`)
        return refused.length + 2
      })
    )
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
    use(make, ({ rows, transaction }) =>
      Effect.gen(function* () {
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
        if (left !== want) return yield* violation("purgeScope")(`\n got ${left}\nwant ${want}`)
        return 1
      })
    )
  )
]

// ---------------------------------------------------------------------------------------------
// SyncStorage
// ---------------------------------------------------------------------------------------------

const clientId = Ids.ClientId.make("conformance")
const envelope = (seq: number, mutationId: string): Mutation.MutationEnvelope => ({
  clientId,
  mutationId: Ids.MutationId.make(mutationId),
  seq: Ids.Seq.make(seq),
  name: "conformance",
  args: { nested: { list: [1, "\u{1F600}", null, true] }, big: 2 ** 53 - 1 }
})
const entry = (seq: number, mutationId: string, createdAt: number, undo: ReadonlyArray<Store.UndoImage>) => ({
  envelope: envelope(seq, mutationId),
  createdAt,
  undo
})
const position = (p: bigint) => Ids.Position.make(p)
/** A fresh store's meta (`Engine.initialMeta`). */
const INITIAL_META: Engine.SyncMeta = { cursor: null, pushedThrough: 0, resnapshot: false }

/** Allocates the next seq in a transaction that then rolls back: observes it without using it. */
const peekSeq = (storage: Engine.SyncStorage) =>
  storage.transaction(Effect.flatMap(storage.nextSeq, (seq) => Effect.fail({ peeked: seq }))).pipe(
    Effect.flip,
    Effect.flatMap((e) => ("peeked" in e ? Effect.succeed(e.peeked) : Effect.fail(e)))
  )

interface DurableState {
  /** `canonical` rows of both tables. */
  readonly rows: string
  readonly meta: Engine.SyncMeta
  readonly pending: ReadonlyArray<Engine.PendingMutation>
  /** `lookup` of each probed mutation id. */
  readonly known: ReadonlyArray<Option.Option<Mutation.MutationEnvelope>>
  readonly staged: ReadonlyArray<typeof Protocol.PullPage.Type>
  /** The seq the next allocation returns. */
  readonly next: number
}

/** Everything the port can read back (notices have no reader), plus the next seq, canonical. */
const durable = (storage: Engine.SyncStorage, ids: ReadonlyArray<string>) =>
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
    .pipe(Effect.flatMap((state) => Effect.map(peekSeq(storage), (next) => canonical({ ...state, next }))))

const expectDurable = (
  fail: (detail: string) => ConformanceViolation,
  label: string,
  storage: Engine.SyncStorage,
  ids: ReadonlyArray<string>,
  want: DurableState
) =>
  Effect.gen(function* () {
    const got = yield* durable(storage, ids)
    const expected = canonical(want)
    if (got !== expected) return yield* fail(`${label}:\n got ${got}\nwant ${expected}`)
  })

const page = (continuation: string): typeof Protocol.PullPage.Type => ({
  _tag: "PullPage",
  changes: [
    { _tag: "upsert", table: Items.name, row: item("p", { label: "\u{1F600}" }) },
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

const syncSeed = ({ storage }: SyncStorageSubject) =>
  storage.transaction(
    Effect.gen(function* () {
      yield* storage.rows.write(Items, "keep", Option.some(item("keep", { rank: 1 })))
      const seq = yield* storage.nextSeq
      yield* storage.appendPending({ envelope: envelope(seq, "old"), createdAt: 0, undo: [] })
    })
  )

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

const SYNC_IDS = ["old", "new"]
const syncBefore = canonical({
  rows: canonical({ items: [item("keep", { rank: 1 })], tags: [] }),
  meta: INITIAL_META,
  pending: [entry(1, "old", 0, [])],
  known: [Option.some(envelope(1, "old")), Option.none()],
  staged: [],
  next: 2
} satisfies DurableState)
const syncAfter = canonical({
  rows: canonical({ items: [item("keep", { rank: 2 })], tags: [tag("t", s1, "x")] }),
  meta: { cursor: position(2n), pushedThrough: 1, resnapshot: false },
  pending: [entry(2, "new", 0, [{ table: Tags, id: "t", before: Option.none() }])],
  known: [Option.some(envelope(1, "old")), Option.some(envelope(2, "new"))],
  staged: [page("k")],
  next: 3
} satisfies DurableState)

const syncObserve = (subject: SyncStorageSubject) => Effect.flatMap(subject.reopen, (s) => durable(s, SYNC_IDS))

export const syncStorage = <R>(make: Effect.Effect<SyncStorageSubject, unknown, R>): ReadonlyArray<Check<R>> => [
  ...replicaRowStore(Effect.map(make, ({ storage }) => ({ rows: storage.rows, transaction: storage.transaction }))),
  check(
    "nextSeq allocates: two calls in one transaction, before any append, return 1 and 2; then meta, pending entries (args, undo), released markers and staged pages read back as written, before and after a reopen",
    use(make, ({ storage, reopen }) =>
      Effect.gen(function* () {
        const fail = violation("allocation and durability")
        const undo: ReadonlyArray<Store.UndoImage> = [
          { table: Items, id: "x", before: Option.some(item("x", { label: "\u00E9", data: { tags: [], n: 1 } })) },
          { table: Tags, id: "y", before: Option.none() }
        ]
        const meta: Engine.SyncMeta = { cursor: position(9_007_199_254_740_993n), pushedThrough: 1, resnapshot: true }
        const seqs = yield* storage.transaction(
          Effect.gen(function* () {
            const a = yield* storage.nextSeq
            const b = yield* storage.nextSeq
            yield* storage.appendPending({ envelope: envelope(a, "m-a"), createdAt: -1, undo: [] })
            yield* storage.appendPending({ envelope: envelope(b, "m-b"), createdAt: 1_700_000_000_123, undo })
            yield* storage.release([a])
            yield* storage.setMeta(meta)
            yield* storage.stage(page("k1"))
            yield* storage.stage(page("k2"))
            return [a, b]
          })
        )
        if (canonical(seqs) !== canonical([1, 2])) {
          return yield* fail(`two nextSeq calls in one transaction returned ${canonical(seqs)}, want [1,2]`)
        }
        const ids = ["m-a", "m-b", "never"]
        const want: DurableState = {
          rows: EMPTY_ROWS,
          meta,
          pending: [entry(2, "m-b", 1_700_000_000_123, undo)],
          known: [Option.some(envelope(1, "m-a")), Option.some(envelope(2, "m-b")), Option.none()],
          staged: [page("k1"), page("k2")],
          next: 3
        }
        yield* expectDurable(fail, "before reopen", storage, ids, want)
        yield* expectDurable(fail, "after reopen", yield* reopen, ids, want)
        return 2
      })
    )
  ),
  check(
    "seq is never reused: allocations start at 1 and continue after release, prune and reopen; a rolled-back allocation is given back",
    use(make, ({ storage, reopen, pruneReleased }) =>
      Effect.gen(function* () {
        const fail = violation("seq never reused")
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
        yield* storage.transaction(storage.release([Ids.Seq.make(1), Ids.Seq.make(2)]))
        const pending = canonical(yield* storage.transaction(storage.pending))
        if (pending !== canonical([entry(3, "m3", 0, [])])) return yield* fail(`pending after release ${pending}`)
        const released = canonical(yield* storage.transaction(storage.lookup(Ids.MutationId.make("m1"))))
        if (released !== canonical(Option.some(envelope(1, "m1")))) {
          return yield* fail(`lookup of a released entry: ${released}`)
        }
        const peeked = yield* peekSeq(storage)
        if (peeked !== 4) return yield* fail(`next seq ${peeked}, want 4`)
        if ((yield* peekSeq(storage)) !== 4) return yield* fail("a rolled-back allocation was not given back")
        yield* storage.transaction(storage.release([Ids.Seq.make(3)]))
        yield* pruneReleased
        // The outbox is now empty and every marker pruned: a MAX(seq)+1 allocator would restart at 1.
        const next = yield* peekSeq(yield* reopen)
        if (next !== 4) return yield* fail(`after release + prune + reopen the next seq is ${next}, want 4`)
        return 4
      })
    )
  ),
  check(
    "clearAll deletes every replica row and nothing else (outbox, undo, markers, meta, staging)",
    use(make, ({ storage, reopen }) =>
      Effect.gen(function* () {
        const fail = violation("clearAll")
        const undo: ReadonlyArray<Store.UndoImage> = [{ table: Items, id: "a", before: Option.none() }]
        const meta: Engine.SyncMeta = { cursor: position(5n), pushedThrough: 1, resnapshot: false }
        yield* storage.transaction(
          Effect.gen(function* () {
            yield* storage.rows.write(Items, "a", Option.some(item("a")))
            yield* storage.rows.write(Tags, "b", Option.some(tag("b", s2, "x")))
            const one = yield* storage.nextSeq
            const two = yield* storage.nextSeq
            yield* storage.appendPending({ envelope: envelope(one, "r"), createdAt: 0, undo: [] })
            yield* storage.appendPending({ envelope: envelope(two, "p"), createdAt: 0, undo })
            yield* storage.release([one])
            yield* storage.setMeta(meta)
            yield* storage.stage(page("k"))
          })
        )
        const ids = ["r", "p"]
        const rest = {
          meta,
          pending: [entry(2, "p", 0, undo)],
          known: [Option.some(envelope(1, "r")), Option.some(envelope(2, "p"))],
          staged: [page("k")],
          next: 3
        }
        const populated = canonical({ items: [item("a")], tags: [tag("b", s2, "x")] })
        yield* expectDurable(fail, "before clearAll", storage, ids, { rows: populated, ...rest })
        yield* storage.transaction(storage.rows.clearAll)
        yield* expectDurable(fail, "after clearAll and a reopen", yield* reopen, ids, { rows: EMPTY_ROWS, ...rest })
        return 1
      })
    )
  ),
  check(
    "undo images captured by Store.make persist through setUndo and a reopen, and restore the exact prior rows",
    use(make, ({ storage, reopen }) =>
      Effect.gen(function* () {
        const fail = violation("undo round-trip")
        const seedItems = [item("i1", { label: "a", rank: 1 }), item("i2", { flag: true }), item("i3")]
        const oldTag = tag("t", s1, "old")
        const prior = canonical({ items: seedItems, tags: [oldTag] })
        const seq = yield* storage.transaction(
          Effect.gen(function* () {
            for (const row of seedItems) yield* storage.rows.write(Items, String(row["id"]), Option.some(row))
            yield* storage.rows.write(Tags, "t", Option.some(oldTag))
            const allocated = yield* storage.nextSeq
            yield* storage.appendPending({ envelope: envelope(allocated, "u"), createdAt: 0, undo: [] })
            return allocated
          })
        )
        const seeded = yield* storage.transaction(rowsOf(storage.rows))
        if (seeded !== prior) return yield* fail(`seed:\n got ${seeded}\nwant ${prior}`)
        yield* storage.transaction(
          Effect.gen(function* () {
            const { store, touched } = Store.make(storage.rows, {
              actor: AccessScope.Everyone,
              roleFor: () => Effect.succeedSome("owner" as const),
              claimScope: () => Effect.succeed(false),
              now: DateTime.makeUnsafe(0)
            })
            yield* store.patch(Items, ItemId.make("i1"), { label: "b" })
            yield* store.patch(Items, ItemId.make("i1"), { rank: 99 })
            yield* store.delete(Items, ItemId.make("i2"))
            yield* store.patch(Items, ItemId.make("i3"), { accessScopeId: s2 })
            yield* store.insert(Tags, { id: TagId.make("n"), accessScopeId: s1, name: "new" })
            yield* store.patch(Tags, TagId.make("t"), { name: "renamed" })
            yield* storage.setUndo(seq, yield* touched)
          })
        )
        // First-touch pre-images in first-write order, one per row (Store.make's contract).
        const expected: ReadonlyArray<Store.UndoImage> = [
          ...seedItems.map((row) => ({ table: Items, id: String(row["id"]), before: Option.some(row) })),
          { table: Tags, id: "n", before: Option.none() },
          { table: Tags, id: "t", before: Option.some(oldTag) }
        ]
        const reopened = yield* reopen
        const persisted = (yield* reopened.transaction(reopened.pending))[0]?.undo ?? []
        if (canonical(persisted) !== canonical(expected)) {
          return yield* fail(`persisted ${canonical(persisted)}\n want ${canonical(expected)}`)
        }
        yield* reopened.transaction(
          Effect.forEach(Arr.reverse(persisted), (image) => reopened.rows.write(image.table, image.id, image.before), {
            discard: true
          })
        )
        const restored = yield* reopened.transaction(rowsOf(reopened.rows))
        if (restored !== prior) return yield* fail(`restored ${restored}\n prior ${prior}`)
        return expected.length
      })
    )
  ),
  check(
    "a storage transaction interrupted at any boundary commits rows, outbox, seq, meta and staging together or not at all",
    atomicity({
      name: "one commit/rollback decision (SyncStorage)",
      make,
      seed: syncSeed,
      body: syncBody,
      within: (subject, self) => subject.storage.transaction(self),
      observe: syncObserve,
      observeLive: (subject) => durable(subject.storage, SYNC_IDS),
      before: syncBefore,
      after: syncAfter
    })
  )
]
