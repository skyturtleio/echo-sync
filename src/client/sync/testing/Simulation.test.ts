import { assert, describe, it } from "@effect/vitest"
import type { Scheduler } from "effect/Scheduler"
import { DateTime, Deferred, Effect, Fiber, Option, Schema } from "effect"
import * as Engine from "../Engine.ts"
import { Ids, Mutation, Protocol, ScopeGrants } from "../../../core/index.ts"
import type { SyncTable } from "../../../core/index.ts"
import * as MemoryStore from "../../../core/testing/MemoryStore.ts"
import * as MemorySyncStorage from "./MemorySyncStorage.ts"
import * as RefServer from "../../../core/testing/RefServer.ts"
import {
  clientRegistry,
  CounterId,
  Counters,
  grantRow,
  mutations,
  principal,
  scope
} from "../../../core/testing/SimApp.ts"
import * as Simulation from "./Simulation.ts"

// Reproduce a failure with SIM_SEED=<seed>; widen the sweep with SIM_SEEDS=<n>.
const only = Number(process.env["SIM_SEED"] ?? 0)
const seeds = only > 0 ? [only] : Array.from({ length: Number(process.env["SIM_SEEDS"] ?? 40) }, (_, i) => i + 1)

const grant = grantRow("s", "p", "owner")
const grantId = ScopeGrants.grantId(scope("s"), principal("p"))
const counterRow = (id: string, value: number): SyncTable.EncodedRow =>
  Schema.encodeSync(Counters.wire)({
    id: CounterId.make(id),
    accessScopeId: scope("s"),
    value,
    updatedAt: DateTime.makeUnsafe(0)
  })
const counter = counterRow("x", 0)
const id = Ids.MutationId.make
const clientId = Ids.ClientId.make("c")
const position = (p: bigint) => Ids.Position.make(p)
const engineOptions = { clientId, actor: principal("p"), registry: clientRegistry, mutations }

/** A client disk that has installed `grant` and `counter` at cursor 1. */
const installedDisk = (): MemorySyncStorage.Disk => ({
  ...MemorySyncStorage.makeDisk(),
  replica: MemoryStore.make(
    new Map([
      [ScopeGrants.ScopeGrants.name, new Map([[grantId, grant]])],
      [Counters.name, new Map([["x", counter]])]
    ])
  ),
  meta: { ...Engine.initialMeta, cursor: position(1n) }
})
const valueOf = (disk: MemorySyncStorage.Disk, rowId: string) =>
  disk.replica.tables().get(Counters.name)?.get(rowId)?.["value"]

/** An engine over `disk` whose pulls answer from `responses` in order (none left: offline). */
const scriptedPulls = (disk: MemorySyncStorage.Disk, responses: Array<Protocol.PullResponse>) =>
  Engine.make({
    ...engineOptions,
    storage: MemorySyncStorage.make(disk),
    transport: {
      push: () => Effect.die("unused"),
      pull: () => {
        const next = responses.shift()
        return next === undefined
          ? Effect.fail(new Simulation.TransportFailed({ during: "pull" }))
          : Effect.succeed(next)
      }
    }
  })

describe("reconciliation simulation", () => {
  it.effect(
    "random schedules keep every client = replay(pending, server@cursor) and lose/duplicate no intent",
    () =>
      Effect.gen(function* () {
        const reached = {
          reloadsMidSet: 0,
          resnapshots: 0,
          lateRetries: 0,
          markerHits: 0,
          backstopReleases: 0,
          unknownTableWrites: 0,
          crashes: 0,
          interleaved: 0,
          rebuildInstalls: 0
        }
        for (const seed of seeds) {
          const stats = yield* Simulation.run({ seed, steps: 400 })
          reached.reloadsMidSet += stats.reloadsMidSet
          reached.resnapshots += stats.resnapshots
          reached.lateRetries += stats.lateRetries
          reached.markerHits += stats.markerHits
          reached.backstopReleases += stats.backstopReleases
          reached.unknownTableWrites += stats.unknownTableWrites
          reached.crashes += stats.crashes
          reached.interleaved += stats.interleaved
          reached.rebuildInstalls += stats.rebuildInstalls
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
    "an allocator that reuses a released entry's seq is caught by the never-reused check",
    () =>
      Effect.gen(function* () {
        let caught = 0
        for (let seed = 1; seed <= 10; seed++) {
          caught += yield* Simulation.run({ seed, steps: 400, seqFromPending: true }).pipe(
            Effect.as(0),
            Effect.catchTag("InvariantViolated", (e) => Effect.succeed(e.invariant === "a seq is never reused" ? 1 : 0))
          )
        }
        assert.isAbove(caught, 5)
      }),
    60_000
  )

  it.effect(
    // Late enqueue retries of pruned ids run here too: the server keeps every expired identity
    // (DESIGN §16.5), so a retry under a fresh seq above the floor is still recognised.
    "ledger GC'd while a client is behind, with late retries of pruned ids: processed intents are released by the floor or the kept identity, never re-executed",
    () =>
      Effect.gen(function* () {
        let expired = 0
        let lateRetries = 0
        for (const seed of seeds) {
          const stats = yield* Simulation.run({ seed, steps: 400, ledgerGc: true })
          expired += stats.expired
          lateRetries += stats.lateRetries
        }
        if (only === 0) {
          assert.isAbove(expired, 0, "no mutation was released by ledger expiry")
          assert.isAbove(lateRetries, 0, "no late retry of a pruned id ran")
        }
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
        ...engineOptions,
        storage: MemorySyncStorage.make(disk),
        transport: {
          push: (request) =>
            Effect.succeed({
              results: request.mutations.map((m) => ({
                mutationId: m.mutationId,
                outcome: { _tag: "Applied" as const },
                position: position(5n)
              }))
            }),
          pull: () =>
            Effect.gen(function* () {
              pulls++
              if (pulls > 1) return yield* new Simulation.TransportFailed({ during: "replacement snapshot" })
              yield* Deferred.succeed(arrived, undefined)
              yield* Deferred.await(release)
              return yield* new Protocol.ResnapshotRequired({ reason: "gcHorizon" })
            })
        }
      })
      const pulling = yield* Effect.forkChild(Effect.exit(engine.pull))
      yield* Deferred.await(arrived)
      const first = yield* engine.mutate(id("u-a"), "increment", { id: "x", by: 1 })
      yield* engine.push
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(pulling)

      assert.isTrue(disk.meta.resnapshot)
      assert.strictEqual(disk.meta.cursor, position(1n)) // the installed replica is still authoritative here
      assert.strictEqual(disk.meta.pushedThrough, first.seq) // a stale write would make it resend
      const second = yield* engine.mutate(id("u-b"), "increment", { id: "x", by: 1 })
      assert.deepStrictEqual([first.seq, second.seq], [1, 2])
    })
  )

  it.effect("an enqueue retried after its original was released (and the engine recreated) adds no intent", () =>
    Effect.gen(function* () {
      const server = RefServer.make({ pageSize: 10 })
      server.register(clientId, principal("p"))
      yield* server.writeDirect([
        { table: ScopeGrants.ScopeGrants, id: grantId, row: Option.some(grant) },
        { table: Counters, id: "x", row: Option.some(counter) }
      ])
      const disk = MemorySyncStorage.makeDisk()
      const options = {
        ...engineOptions,
        storage: MemorySyncStorage.make(disk),
        transport: {
          push: (request: Protocol.PushRequest) => server.push(request, Option.none()),
          pull: server.pull
        }
      }
      let engine = yield* Engine.make(options)
      yield* engine.pull
      const inc = yield* engine.mutate(id("u-inc"), "increment", { id: "x", by: 5 })
      const create = yield* engine.mutate(id("u-create"), "create", { id: "y", scope: "s" })
      yield* engine.mutate(id("u-remove"), "remove", { id: "y" })
      yield* engine.push
      yield* engine.pull
      assert.strictEqual(disk.outbox.length, 0)

      engine = yield* Engine.make(options)
      assert.strictEqual((yield* engine.mutate(id("u-inc"), "increment", { id: "x", by: 5 })).seq, inc.seq)
      assert.strictEqual((yield* engine.mutate(id("u-create"), "create", { id: "y", scope: "s" })).seq, create.seq)
      assert.strictEqual(disk.outbox.length, 0)
      yield* engine.push
      yield* engine.pull
      assert.strictEqual(valueOf(disk, "x"), 5) // 10 would be the increment applied twice
      assert.isUndefined(disk.replica.tables().get(Counters.name)?.get("y")) // a re-run create would resurrect y
      assert.isTrue(Option.isNone(yield* server.store.read(Counters, "y")))
      assert.deepStrictEqual([...server.executions.values()], [1, 1, 1])
    })
  )

  it.effect(
    "the reference server's scope claim: an applied creation takes the id, a rejected one leaves it fresh, and an orphaned scope is never re-claimable (DESIGN §4)",
    () =>
      Effect.gen(function* () {
        const server = RefServer.make({ pageSize: 10 })
        const seqs = new Map<string, number>()
        const submit = (client: string, who: string, args: { scope: string; counter: string | null }) =>
          Effect.gen(function* () {
            server.register(client, principal(who))
            const seq = (seqs.get(client) ?? 0) + 1
            seqs.set(client, seq)
            const response = yield* server.push(
              {
                clientId: Ids.ClientId.make(client),
                mutations: [{ mutationId: id(`${client}-${seq}`), seq: Ids.Seq.make(seq), name: "createScope", args }]
              },
              Option.none()
            )
            const outcome = response.results[0]?.outcome
            return outcome === undefined ? "none" : outcome._tag === "Applied" ? "Applied" : outcome.error._tag
          })
        const owners = (scopeId: string) =>
          Effect.map(server.store.scan(ScopeGrants.ScopeGrants), (rows) =>
            rows.filter((r) => r["access_scope_id"] === scopeId).map((r) => `${r["principal_id"]}:${r["role"]}`)
          )
        yield* server.writeDirect([{ table: Counters, id: "taken", row: Option.some(counterRow("taken", 0)) }])

        // Applied: the id is taken, by p as owner; q cannot create it again.
        assert.strictEqual(yield* submit("cp", "p", { scope: "A", counter: null }), "Applied")
        assert.deepStrictEqual(yield* owners("A"), ["p:owner"])
        assert.strictEqual(yield* submit("cq", "q", { scope: "A", counter: null }), "ScopeForbidden")
        assert.deepStrictEqual(yield* owners("A"), ["p:owner"])

        // Rejected after the claim (the counter id exists): no grant, and the id stays fresh.
        assert.strictEqual(yield* submit("cq", "q", { scope: "B", counter: "taken" }), "RowAlreadyExists")
        assert.deepStrictEqual(yield* owners("B"), [])
        assert.strictEqual(yield* submit("cr", "r", { scope: "B", counter: null }), "Applied")
        assert.deepStrictEqual(yield* owners("B"), ["r:owner"])

        // Orphaned: every grant of A is gone (a replica would call A fresh), but A stays taken.
        yield* server.writeDirect([
          {
            table: ScopeGrants.ScopeGrants,
            id: ScopeGrants.grantId(scope("A"), principal("p")),
            row: Option.none()
          }
        ])
        assert.deepStrictEqual(yield* owners("A"), [])
        assert.strictEqual(yield* submit("cq", "q", { scope: "A", counter: null }), "ScopeForbidden")
        assert.deepStrictEqual(yield* owners("A"), [])
      })
  )

  // Reviewer's reproduction: interrupt at every scheduler boundary of one transaction.
  it("an interrupted storage transaction commits rows, seq and meta together or not at all", async () => {
    const split: Array<number> = []
    const outcomes = new Set<boolean>()
    for (let boundary = 1; boundary < 150; boundary++) {
      const disk = MemorySyncStorage.makeDisk()
      const storage = MemorySyncStorage.make(disk)
      const controller = new AbortController()
      let ticks = 0
      let yielded = false
      const tasks: Array<() => void> = []
      const scheduler: Scheduler = {
        executionMode: "async",
        shouldYield: () => !yielded && ++ticks === boundary && (yielded = true),
        makeDispatcher: () => ({
          scheduleTask: (task) => {
            tasks.push(task)
          },
          flush: () => {
            while (tasks.length > 0) tasks.shift()?.()
          }
        })
      }
      const running = Effect.runPromiseExit(
        storage.transaction(
          Effect.gen(function* () {
            yield* storage.nextSeq
            yield* storage.rows.write(Counters, "a", Option.some(counterRow("a", 0)))
            yield* storage.setMeta({ cursor: position(1n), pushedThrough: 1, resnapshot: false })
          })
        ),
        { scheduler, signal: controller.signal }
      )
      if (yielded) controller.abort()
      while (tasks.length > 0) tasks.shift()?.()
      await running
      const hasRow = disk.replica.tables().get(Counters.name)?.has("a") ?? false
      outcomes.add(hasRow)
      if (hasRow !== (disk.lastSeq === 1) || hasRow !== (disk.meta.cursor === position(1n))) split.push(boundary)
    }
    assert.deepStrictEqual(split, [])
    assert.strictEqual(outcomes.size, 2) // the sweep spans the transaction
  })

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
                yield* store.write(Counters, "a", Option.some(counterRow("a", 0)))
                yield* Deferred.succeed(wrote, undefined)
                yield* Deferred.await(gate)
                return yield* Effect.fail("abort")
              })
            )
          )
        )
        yield* Deferred.await(wrote)
        const b = yield* Effect.forkChild(
          store.transaction(store.write(Counters, "b", Option.some(counterRow("b", 0))))
        )
        // Give B every chance to run while A is suspended (it must wait for A's permit).
        for (let i = 0; i < 10; i++) yield* Effect.yieldNow
        if (ending === "fail") yield* Deferred.succeed(gate, undefined)
        else yield* Fiber.interrupt(a)
        yield* Fiber.join(b)
        assert.deepStrictEqual([...(store.tables().get(Counters.name)?.keys() ?? [])], ["b"], ending)
      }
    })
  )

  it.effect("an ack on a PullPage whose payloads lack the effect neither drops nor double-applies the intent", () =>
    Effect.gen(function* () {
      const disk = installedDisk()
      const responses: Array<Option.Option<Protocol.PullResponse>> = [
        // Page 1 (snapshot before the mutation committed elsewhere in the set) carries the ack.
        Option.some({
          _tag: "PullPage",
          changes: [],
          acks: [{ mutationId: id("u-7f3a"), position: position(2n), outcome: { _tag: "Applied" } }],
          continuation: Protocol.PullContinuation.make("k")
        }),
        Option.none(), // connection drops before the final page
        Option.some({
          _tag: "PullDone",
          changes: [{ _tag: "upsert", table: Counters.name, row: counterRow("x", 5) }],
          acks: [],
          nextCursor: position(3n),
          expiredThrough: Ids.SeqFloor.make(0)
        })
      ]
      const engine = yield* Engine.make({
        ...engineOptions,
        storage: MemorySyncStorage.make(disk),
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

      yield* engine.mutate(id("u-7f3a"), "increment", { id: "x", by: 5 })
      yield* Effect.exit(engine.pull)
      // Acked but the covering state is not installed: still pending, still visible once.
      assert.strictEqual(disk.outbox.length, 1)
      assert.strictEqual(valueOf(disk, "x"), 5)

      yield* engine.pull
      assert.strictEqual(disk.outbox.length, 0)
      assert.strictEqual(valueOf(disk, "x"), 5) // 10 would mean the intent was replayed over its own result
      assert.strictEqual(disk.meta.cursor, position(3n))
    })
  )

  it.effect(
    "a Rejected{ClientExpired} push result above the installed cursor is released on receipt with an OutcomeUnknown notice; an Applied one there still waits for its ack",
    () =>
      Effect.gen(function* () {
        const disk = installedDisk() // cursor 1
        const base = valueOf(disk, "x")
        const engine = yield* Engine.make({
          ...engineOptions,
          storage: MemorySyncStorage.make(disk),
          transport: {
            // An expired identity answers m1 at position 5, above the cursor; no pull will ever ack it.
            push: () =>
              Effect.succeed<Protocol.PushResponse>({
                results: [
                  {
                    mutationId: id("m1"),
                    position: position(5n),
                    outcome: { _tag: "Rejected", error: new Mutation.ClientExpired({ mutationId: id("m1") }) }
                  },
                  { mutationId: id("m2"), position: position(6n), outcome: { _tag: "Applied" } }
                ]
              }),
            pull: () => Effect.die("unused")
          }
        })
        yield* engine.mutate(id("m1"), "increment", { id: "x", by: 1 })
        yield* engine.mutate(id("m2"), "increment", { id: "x", by: 10 })
        assert.strictEqual((yield* engine.push).processed, 2)
        assert.deepStrictEqual(
          disk.outbox.map((entry) => entry.envelope.mutationId),
          ["m2"],
          "m1 released, m2 waits"
        )
        assert.deepStrictEqual(
          disk.notices.map((notice) => [notice._tag, notice.envelope.mutationId]),
          [["OutcomeUnknown", "m1"]]
        )
        // m1's optimistic effect is gone (its effect, if any, comes with a pull); m2's stays.
        assert.strictEqual(valueOf(disk, "x"), Number(base) + 10)
      })
  )

  it.effect("a known-table row that does not decode fails the install whole: no ack, cursor or staging change", () =>
    Effect.gen(function* () {
      const disk = installedDisk()
      const ack = { mutationId: id("u-1"), position: position(2n), outcome: { _tag: "Applied" as const } }
      const engine = yield* scriptedPulls(disk, [
        { _tag: "PullPage", changes: [], acks: [ack], continuation: Protocol.PullContinuation.make("k") },
        {
          _tag: "PullDone",
          // Covers the intent's ack, but carries only an id: not a counters row.
          changes: [
            { _tag: "upsert", table: Counters.name, row: counterRow("w", 1) },
            { _tag: "upsert", table: Counters.name, row: { id: "x" } }
          ],
          acks: [],
          nextCursor: position(3n),
          expiredThrough: Ids.SeqFloor.make(0)
        }
      ])
      yield* engine.mutate(id("u-1"), "increment", { id: "x", by: 5 })

      const error = yield* Effect.flip(engine.pull)
      assert.strictEqual(error._tag, "MalformedChange")
      assert.strictEqual(disk.outbox.length, 1) // a released intent would have no covering row
      assert.strictEqual(disk.meta.cursor, position(1n))
      assert.strictEqual(disk.staged.length, 1)
      assert.deepStrictEqual(disk.replica.tables().get(Counters.name)?.get("x"), { ...counter, value: 5 })
      assert.isUndefined(disk.replica.tables().get(Counters.name)?.get("w")) // nothing from the set installed
    })
  )

  it.effect("a pulled row is stored in canonical wire form (excess columns dropped, values normalized)", () =>
    Effect.gen(function* () {
      const disk = installedDisk()
      const engine = yield* scriptedPulls(disk, [
        {
          _tag: "PullDone",
          changes: [
            { _tag: "upsert", table: Counters.name, row: { ...counterRow("x", 7), updated_at: -0, legacy: "drop me" } }
          ],
          acks: [],
          nextCursor: position(2n),
          expiredThrough: Ids.SeqFloor.make(0)
        }
      ])
      yield* engine.pull
      const stored = disk.replica.tables().get(Counters.name)?.get("x")
      assert.deepStrictEqual(stored, counterRow("x", 7))
      assert.isTrue(Object.is(stored?.["updated_at"], 0)) // -0 would differ from every other writer's 0
    })
  )
})
