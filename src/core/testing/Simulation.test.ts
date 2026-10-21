import { assert, describe, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Option } from "effect"
import * as Engine from "../../client/sync/Engine.ts"
import type { PullResponse } from "../../client/sync/Wire.ts"
import { ResnapshotRequired } from "../../client/sync/Wire.ts"
import * as MemoryStore from "./MemoryStore.ts"
import * as MemorySyncStorage from "./MemorySyncStorage.ts"
import * as RefServer from "./RefServer.ts"
import { COUNTERS, GRANTS, grantRow, runMutation } from "./SimApp.ts"
import * as Simulation from "./Simulation.ts"

// Reproduce a failure with SIM_SEED=<seed>; widen the sweep with SIM_SEEDS=<n>.
const only = Number(process.env["SIM_SEED"] ?? 0)
const seeds = only > 0 ? [only] : Array.from({ length: Number(process.env["SIM_SEEDS"] ?? 40) }, (_, i) => i + 1)

const grant = grantRow("s", "p", "owner")
const counter = { id: "x", access_scope_id: "s", value: 0, updated_at: 0 }

/** A client disk that has installed `grant` and `counter` at cursor 1. */
const installedDisk = (): MemorySyncStorage.Disk => ({
  ...MemorySyncStorage.makeDisk(),
  replica: MemoryStore.make(
    new Map([
      [GRANTS, new Map([[grant.id, grant]])],
      [COUNTERS, new Map([["x", counter]])]
    ])
  ),
  meta: { ...Engine.initialMeta, cursor: 1n }
})
const executorFor = (disk: MemorySyncStorage.Disk): Engine.Executor<never> => ({
  run: (envelope, createdAt) => runMutation(disk.replica, "p", envelope, createdAt)
})
const valueOf = (disk: MemorySyncStorage.Disk, id: string) => disk.replica.tables().get(COUNTERS)?.get(id)?.["value"]

describe("reconciliation simulation", () => {
  it.effect(
    "random schedules keep every client = replay(pending, server@cursor) and lose/duplicate no intent",
    () =>
      Effect.gen(function* () {
        const reached = { reloadsMidSet: 0, resnapshots: 0, lateRetries: 0, markerHits: 0, crashes: 0, interleaved: 0 }
        for (const seed of seeds) {
          const stats = yield* Simulation.run({ seed, steps: 400 })
          reached.reloadsMidSet += stats.reloadsMidSet
          reached.resnapshots += stats.resnapshots
          reached.lateRetries += stats.lateRetries
          reached.markerHits += stats.markerHits
          reached.crashes += stats.crashes
          reached.interleaved += stats.interleaved
        }
        // The schedule must actually reach the hard paths, or the sweep proves nothing.
        if (only === 0) {
          for (const [path, count] of Object.entries(reached))
            assert.isAbove(count, 0, `schedule never reached ${path}`)
        }
      }),
    60_000
  )

  it.effect(
    "a client that applies PullPages before PullDone is caught by an invariant (the check bites)",
    () =>
      Effect.gen(function* () {
        let caught = 0
        for (let seed = 1; seed <= 10; seed++) {
          // Only an invariant violation counts; any other failure fails this test.
          caught += yield* Simulation.run({ seed, steps: 400, pullMode: "incremental" }).pipe(
            Effect.as(0),
            Effect.catchTag("InvariantViolated", () => Effect.succeed(1))
          )
        }
        assert.isAbove(caught, 5)
      }),
    60_000
  )

  it.effect(
    // Late enqueue retries of pruned ids are off in this run: DESIGN §16.5 allows ledger expiry
    // only after the released-row retention window (§9 rule 1), so no duplicate exists then.
    "ledger GC'd while a client is behind (no duplicates, per §16.5): processed intents are released by the floor, never re-executed",
    () =>
      Effect.gen(function* () {
        let expired = 0
        for (const seed of seeds) expired += (yield* Simulation.run({ seed, steps: 400, ledgerGc: true })).expired
        if (only === 0) assert.isAbove(expired, 0, "no mutation was released by ledger expiry")
      }),
    60_000
  )

  it.effect("ResnapshotRequired after a suspended pull keeps seqs, pushedThrough and cursor written meanwhile", () =>
    Effect.gen(function* () {
      const disk = installedDisk()
      const arrived = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let pulls = 0
      const engine = yield* Engine.make({
        clientId: "c",
        storage: MemorySyncStorage.make(disk),
        executor: executorFor(disk),
        transport: {
          push: (request) =>
            Effect.succeed({
              results: request.mutations.map((m) => ({
                mutationId: m.mutationId,
                outcome: { _tag: "Applied" as const },
                position: 5n
              }))
            }),
          pull: () =>
            Effect.gen(function* () {
              pulls++
              if (pulls > 1) return yield* new Simulation.TransportFailed({ during: "replacement snapshot" })
              yield* Deferred.succeed(arrived, undefined)
              yield* Deferred.await(release)
              return yield* new ResnapshotRequired({ reason: "gcHorizon" })
            })
        }
      })
      const pulling = yield* Effect.forkChild(Effect.exit(engine.pull))
      yield* Deferred.await(arrived)
      const first = yield* engine.mutate("u-a", "increment", { id: "x", by: 1 })
      yield* engine.push
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(pulling)

      assert.isTrue(disk.meta.resnapshot)
      assert.strictEqual(disk.meta.cursor, 1n) // the installed replica is still authoritative here
      assert.strictEqual(disk.meta.pushedThrough, first.seq) // a stale write would make it resend
      const second = yield* engine.mutate("u-b", "increment", { id: "x", by: 1 })
      assert.deepStrictEqual([first.seq, second.seq], [1, 2])
    })
  )

  it.effect("an enqueue retried after its original was released (and the engine recreated) adds no intent", () =>
    Effect.gen(function* () {
      const server = RefServer.make({ pageSize: 10 })
      server.register("c", "p")
      yield* server.writeDirect([
        { table: GRANTS, id: grant.id, row: Option.some(grant) },
        { table: COUNTERS, id: "x", row: Option.some(counter) }
      ])
      const disk = MemorySyncStorage.makeDisk()
      const options = {
        clientId: "c",
        storage: MemorySyncStorage.make(disk),
        executor: executorFor(disk),
        transport: {
          push: (request: Parameters<typeof server.push>[0]) => server.push(request, Option.none()),
          pull: server.pull
        }
      }
      let engine = yield* Engine.make(options)
      yield* engine.pull
      const inc = yield* engine.mutate("u-inc", "increment", { id: "x", by: 5 })
      const create = yield* engine.mutate("u-create", "create", { id: "y", scope: "s" })
      yield* engine.mutate("u-remove", "remove", { id: "y" })
      yield* engine.push
      yield* engine.pull
      assert.strictEqual(disk.outbox.length, 0)

      engine = yield* Engine.make(options)
      assert.strictEqual((yield* engine.mutate("u-inc", "increment", { id: "x", by: 5 })).seq, inc.seq)
      assert.strictEqual((yield* engine.mutate("u-create", "create", { id: "y", scope: "s" })).seq, create.seq)
      assert.strictEqual(disk.outbox.length, 0)
      yield* engine.push
      yield* engine.pull
      assert.strictEqual(valueOf(disk, "x"), 5) // 10 would be the increment applied twice
      assert.isUndefined(disk.replica.tables().get(COUNTERS)?.get("y")) // a re-run create would resurrect y
      assert.isTrue(Option.isNone(yield* server.store.read(COUNTERS, "y")))
      assert.deepStrictEqual([...server.executions.values()], [1, 1, 1])
    })
  )

  it.effect("a failed or interrupted MemoryStore transaction does not erase a concurrently committed write", () =>
    Effect.gen(function* () {
      for (const ending of ["fail", "interrupt"] as const) {
        const store = MemoryStore.make()
        const wrote = yield* Deferred.make<void>()
        const gate = yield* Deferred.make<void>()
        const a = yield* Effect.forkChild(
          Effect.exit(
            store.transaction(
              Effect.gen(function* () {
                yield* store.write("t", "a", Option.some({ id: "a", access_scope_id: "s" }))
                yield* Deferred.succeed(wrote, undefined)
                yield* Deferred.await(gate)
                return yield* Effect.fail("abort")
              })
            )
          )
        )
        yield* Deferred.await(wrote)
        const b = yield* Effect.forkChild(
          store.transaction(store.write("t", "b", Option.some({ id: "b", access_scope_id: "s" })))
        )
        // Give B every chance to run while A is suspended (it must wait for A's permit).
        for (let i = 0; i < 10; i++) yield* Effect.yieldNow
        if (ending === "fail") yield* Deferred.succeed(gate, undefined)
        else yield* Fiber.interrupt(a)
        yield* Fiber.join(b)
        assert.deepStrictEqual([...(store.tables().get("t")?.keys() ?? [])], ["b"], ending)
      }
    })
  )

  it.effect("an ack on a PullPage whose payloads lack the effect neither drops nor double-applies the intent", () =>
    Effect.gen(function* () {
      const disk = installedDisk()
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
          changes: [{ _tag: "upsert", table: COUNTERS, row: { ...counter, value: 5 } }],
          acks: [],
          nextCursor: 3n,
          expiredThrough: 0
        })
      ]
      const engine = yield* Engine.make({
        clientId: "c",
        storage: MemorySyncStorage.make(disk),
        executor: executorFor(disk),
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

      yield* engine.mutate("u-7f3a", "increment", { id: "x", by: 5 })
      yield* Effect.exit(engine.pull)
      // Acked but the covering state is not installed: still pending, still visible once.
      assert.strictEqual(disk.outbox.length, 1)
      assert.strictEqual(valueOf(disk, "x"), 5)

      yield* engine.pull
      assert.strictEqual(disk.outbox.length, 0)
      assert.strictEqual(valueOf(disk, "x"), 5) // 10 would mean the intent was replayed over its own result
      assert.strictEqual(disk.meta.cursor, 3n)
    })
  )
})
