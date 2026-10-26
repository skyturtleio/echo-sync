import { assert, describe, it } from "@effect/vitest"
import { Array as Arr, Cause, Effect, Exit, Option, String as Str } from "effect"
import { Store } from "../index.ts"
import * as Conformance from "./Conformance.ts"
import * as MemoryStore from "./MemoryStore.ts"

const register = (checks: ReadonlyArray<Conformance.Check<never>>) => {
  for (const check of checks) {
    const run = () => Effect.map(check.run, (cases) => assert.isAbove(cases, 0))
    if (
      check.name === "keyset pages preserve ties, direction, nulls and cursor boundaries across intervening inserts"
    ) {
      it.effect(
        "keyset pages preserve ties, direction, nulls and cursor boundaries across intervening inserts",
        run,
        60_000
      )
    } else it.effect(check.name, run, 60_000)
  }
}

/** The check must fail with a ConformanceViolation (not pass, not crash with something else). */
const mustCatch = (label: string, checks: ReadonlyArray<Conformance.Check<never>>) => {
  for (const check of checks) {
    it.effect(`${label} fails: ${check.name}`, () =>
      Effect.map(Effect.flip(check.run), (error) => assert.instanceOf(error, Conformance.ConformanceViolation))
    )
  }
}

it.effect("conformance preserves typed setup failures, defects and interruption without reducing mixed causes", () =>
  Effect.gen(function* () {
    for (const cause of [
      Cause.fail("setup refused"),
      Cause.die("setup defect"),
      Cause.interrupt(),
      Cause.fromReasons([
        Cause.makeFailReason("setup refused"),
        Cause.makeDieReason("cleanup defect"),
        Cause.makeInterruptReason()
      ])
    ]) {
      for (const check of Conformance.rowStore(Effect.failCause(cause))) {
        const exit = yield* Effect.exit(check.run)
        assert.deepStrictEqual(exit, Exit.failCause(cause))
      }
    }
  })
)

describe("MemoryStore conforms to ReplicaRowStore (the reference semantics)", () => {
  it.effect("literal search filters before limits and prefix ranges preserve Unicode boundaries", () =>
    Conformance.search(
      Effect.sync(() => {
        const rows = MemoryStore.make()
        return { rows, transaction: rows.transaction }
      })
    )
  )
  register(
    Conformance.replicaRowStore(
      Effect.sync(() => {
        const store = MemoryStore.make()
        return { rows: store, transaction: store.transaction }
      })
    )
  )
})

/** MemoryStore whose range reads `rewrite(plan)` and applies the readable-scope filter afterwards. */
const filtersAfter = (rewrite: (plan: Store.IndexRangePlan) => Store.IndexRangePlan) =>
  Effect.sync((): Conformance.RowStoreSubject => {
    const store = MemoryStore.make()
    return {
      rows: {
        ...store,
        range: (table, plan) =>
          Effect.map(store.range(table, rewrite(plan)), (rows) =>
            Store.planResult(
              { ...plan, limit: Option.none() },
              rows,
              store.tables().get("scope_grants")?.values() ?? []
            )
          )
      },
      transaction: store.transaction
    }
  })
const scopedLimit = (checks: ReadonlyArray<Conformance.Check<never>>) =>
  Arr.filter(checks, (check) => Str.startsWith("a limited range with a readable-scope constraint")(check.name))

describe("the scoped-limit check catches stores that do not push the scope filter under the limit (open item 8)", () => {
  // v1's Store.make: the limit applied to the store's rows before the readable-scope filter.
  mustCatch(
    "limit before the scope filter",
    scopedLimit(Conformance.rowStore(filtersAfter((plan) => ({ ...plan, readableBy: Option.none() }))))
  )
  mustCatch(
    "a store that ignores the limit",
    scopedLimit(
      Conformance.rowStore(filtersAfter((plan) => ({ ...plan, readableBy: Option.none(), limit: Option.none() })))
    )
  )
})

// Every row operation awaits a promise (like a real driver), and rollback is skipped when an
// interruption lands after any I/O has resumed: only a sweep whose interruption requests reach
// the steps following a resumption can see the partial commit.
const asyncStore = (broken: boolean) =>
  Effect.sync((): Conformance.RowStoreSubject => {
    const store = MemoryStore.make()
    const state = { resumed: false }
    const io = <A, E>(self: Effect.Effect<A, E>) =>
      Effect.gen(function* () {
        yield* Effect.promise(() => Promise.resolve())
        state.resumed = true
        return yield* self
      })
    return {
      rows: {
        read: (table, id) => io(store.read(table, id)),
        write: (table, id, row) => io(store.write(table, id, row)),
        range: (table, plan) => io(store.range(table, plan)),
        scan: (table) => io(store.scan(table))
      },
      transaction: (self) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.suspend(() => {
            const before = MemoryStore.cloneTables(store.tables())
            state.resumed = false
            return restore(self).pipe(
              Effect.onExit((exit) =>
                Effect.sync(() => {
                  if (exit._tag === "Success") return
                  const interrupted = Arr.some(exit.cause.reasons, (reason) => reason._tag === "Interrupt")
                  if (broken && interrupted && state.resumed) return // the bug
                  store.replace(before)
                })
              )
            )
          })
        )
    }
  })

describe("the interrupt sweep reaches checkpoints after asynchronous I/O resumes", () => {
  mustCatch(
    "a store whose rollback is skipped after an asynchronous resumption",
    Arr.filter(Conformance.rowStore(asyncStore(true)), (check) =>
      Str.startsWith("a transaction that fails")(check.name)
    )
  )
  // Control: the same asynchronous store with a correct rollback passes.
  register(
    Arr.filter(Conformance.rowStore(asyncStore(false)), (check) =>
      Str.startsWith("a transaction that fails")(check.name)
    )
  )
})
