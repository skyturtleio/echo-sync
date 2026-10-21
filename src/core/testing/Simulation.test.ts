import { assert, describe, it } from "@effect/vitest"
import { Effect, Exit, Option } from "effect"
import * as Engine from "../../client/sync/Engine.ts"
import type { PullResponse } from "../../client/sync/Wire.ts"
import * as MemoryStore from "./MemoryStore.ts"
import { COUNTERS, GRANTS, grantRow, runMutation } from "./SimApp.ts"
import * as Simulation from "./Simulation.ts"

// Reproduce a failure with SIM_SEED=<seed>; widen the sweep with SIM_SEEDS=<n>.
const only = Number(process.env["SIM_SEED"] ?? 0)
const seeds = only > 0 ? [only] : Array.from({ length: Number(process.env["SIM_SEEDS"] ?? 40) }, (_, i) => i + 1)

describe("reconciliation simulation", () => {
  it.effect(
    "random schedules keep every client = replay(pending, server@cursor) and lose/duplicate no intent",
    () =>
      Effect.gen(function* () {
        let midSetReloads = 0
        let resnapshots = 0
        let lateRetries = 0
        for (const seed of seeds) {
          const stats = yield* Simulation.run({ seed, steps: 400 })
          midSetReloads += stats.reloadsMidSet
          resnapshots += stats.resnapshots
          lateRetries += stats.lateRetries
        }
        // The schedule must actually reach the hard paths, or the sweep proves nothing.
        if (only === 0) assert.isAbove(midSetReloads, 0, "no reload mid-page-set happened")
        if (only === 0) assert.isAbove(resnapshots, 0, "no resnapshot happened")
        if (only === 0) assert.isAbove(lateRetries, 0, "no enqueue was retried after its original was released")
      }),
    60_000
  )

  it.effect(
    "a client that applies PullPages before PullDone is caught (the invariant check bites)",
    () =>
      Effect.gen(function* () {
        let caught = 0
        for (let seed = 1; seed <= 10; seed++) {
          const exit = yield* Effect.exit(Simulation.run({ seed, steps: 400, pullMode: "incremental" }))
          if (Exit.isFailure(exit)) caught++
        }
        assert.isAbove(caught, 5)
      }),
    60_000
  )

  it.effect(
    // Late enqueue retries are off in this run: DESIGN §16.5 allows ledger expiry only with §9
    // rule (1) in place (released-row markers), so no duplicate can exist when rows expire.
    "ledger GC'd while a client is behind (no duplicates, per §16.5): processed intents are released by the floor, never re-executed",
    () =>
      Effect.gen(function* () {
        let expired = 0
        for (const seed of seeds) expired += (yield* Simulation.run({ seed, steps: 400, ledgerGc: true })).expired
        if (only === 0) assert.isAbove(expired, 0, "no mutation was released by ledger expiry")
      }),
    60_000
  )

  it.effect("an ack on a PullPage whose payloads lack the effect neither drops nor double-applies the intent", () =>
    Effect.gen(function* () {
      // Server state: a counter at 0 in a writable scope; the client has installed it.
      const grant = grantRow("s", "p", "owner")
      const counter = { id: "x", access_scope_id: "s", value: 0, updated_at: 0 }
      const disk: Simulation.Disk = {
        replica: MemoryStore.make(
          new Map([
            [GRANTS, new Map([[grant.id, grant]])],
            [COUNTERS, new Map([["x", counter]])]
          ])
        ),
        meta: { ...Engine.initialMeta, cursor: 1n },
        outbox: [],
        staged: [],
        notices: []
      }
      const storage = Simulation.makeStorage(disk)
      const responses: Array<Option.Option<PullResponse>> = [
        // Page 1 (snapshot before the mutation committed elsewhere in the set) carries the ack.
        Option.some({
          _tag: "PullPage",
          changes: [],
          acks: [{ mutationId: "u-7f3a", position: 2n, outcome: { _tag: "Applied" } }],
          continuation: "k"
        }),
        Option.none(), // connection drops before the final page
        Option.some({
          _tag: "PullDone",
          changes: [{ _tag: "Upsert", table: COUNTERS, row: { ...counter, value: 5 } }],
          acks: [],
          nextCursor: 3n,
          expiredThrough: 0
        })
      ]
      const engine = yield* Engine.make({
        clientId: "c",
        storage,
        executor: { run: (envelope, createdAt) => runMutation(disk.replica, "p", envelope, createdAt) },
        transport: {
          push: () => Effect.die("unused"),
          pull: () => {
            const next = responses.shift() ?? Option.none()
            return Option.isSome(next)
              ? Effect.succeed(next.value)
              : Effect.fail(new Simulation.TransportFailed({ during: "pull" }))
          }
        }
      })
      const value = () => disk.replica.tables().get(COUNTERS)?.get("x")?.["value"]

      yield* engine.mutate("u-7f3a", "increment", { id: "x", by: 5 })
      yield* Effect.exit(engine.pull)
      // Acked but the covering state is not installed: still pending, still visible once.
      assert.strictEqual(disk.outbox.length, 1)
      assert.strictEqual(value(), 5)

      yield* engine.pull
      assert.strictEqual(disk.outbox.length, 0)
      assert.strictEqual(value(), 5) // 10 would mean the intent was replayed over its own result
      assert.strictEqual(disk.meta.cursor, 3n)
    })
  )
})
