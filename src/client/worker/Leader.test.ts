// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { expect, it } from "@effect/vitest"
import { Deferred, Effect, Exit, Fiber, Layer, Option, Schema } from "effect"
import * as SqlClient from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Protocol, Registry, SyncTable } from "echo-sync"
import * as Engine from "../sync/Engine.ts"
import * as SqliteSyncStorage from "../sync/SqliteSyncStorage.ts"
import * as SyncLoop from "../sync/SyncLoop.ts"
import { enqueueAndWake } from "./Leader.ts"

const T = SyncTable.make("t", { id: Schema.String.pipe(Schema.brand("TId")), accessScopeId: AccessScope.AccessScopeId })
const registry = Registry.make([T])
const Db = Layer.effectDiscard(SqliteSyncStorage.migrate(registry)).pipe(
  Layer.provideMerge(SqliteClient.layerMemory({}))
)
const decodeDone = Schema.decodeUnknownSync(Protocol.PullDone)
const position = Schema.decodeUnknownSync(Ids.Position)("1")

it.live("an enqueue cancelled while its COMMIT runs still wakes the loop: the entry is pushed with no other wake", () =>
  Effect.gen(function* () {
    const real = SqliteSyncStorage.make(yield* SqlClient.SqlClient, registry)
    const committed = yield* Deferred.make<void>()
    const gate = yield* Deferred.make<void>()
    const state = { gateNext: false }
    // The interrupt lands after COMMIT but before the transaction returns, as it can while withTransaction
    // masks COMMIT: the entry is durable, and the caller's fiber is interrupted on the way out.
    const storage: Engine.SyncStorage = {
      ...real,
      transaction: <A, E, R>(self: Effect.Effect<A, E, R>) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.tap(restore(real.transaction(self)), () => {
            if (!state.gateNext) return Effect.void
            state.gateNext = false
            return Effect.andThen(Deferred.succeed(committed, undefined), Deferred.await(gate))
          })
        )
    }
    const pushed: Array<string> = []
    const pushObserved = yield* Deferred.make<void>()
    const transport: Engine.Transport<never> = {
      push: (request) =>
        Effect.andThen(
          Effect.sync(() => void pushed.push(...request.mutations.map((m) => m.mutationId))),
          Effect.as(Deferred.succeed(pushObserved, undefined), {
            results: request.mutations.map((m) => ({
              mutationId: m.mutationId,
              position,
              outcome: { _tag: "Applied" as const }
            }))
          })
        ),
      pull: () =>
        Effect.succeed(decodeDone({ _tag: "PullDone", changes: [], acks: [], nextCursor: "1", expiredThrough: 0 }))
    }
    const engine = yield* Engine.make({
      clientId: Ids.ClientId.make("c"),
      actor: AccessScope.PrincipalId.make("me"),
      storage,
      registry,
      mutations: Mutation.makeRegistry([Mutation.make("add", { args: Schema.Struct({}), handler: () => Effect.void })]),
      transport
    })
    // The loop's first cycle (nothing to push, one pull) must be over before the gate is armed, so the
    // gated transaction is the enqueue's and the loop is idle, waiting for a wake.
    const pulled = yield* Deferred.make<void>()
    const observed: Engine.Engine<never> = {
      ...engine,
      pull: Effect.ensuring(engine.pull, Deferred.succeed(pulled, undefined))
    }
    const loop = yield* SyncLoop.make({ engine: observed, isFatalPush: () => false })
    yield* Effect.forkChild(loop.run)
    yield* Deferred.await(pulled)

    state.gateNext = true
    const enqueue = yield* Effect.forkChild(enqueueAndWake(engine, loop.wake)(Ids.MutationId.make("m"), "add", {}))
    yield* Deferred.await(committed)
    // COMMIT is done and the permit released; only the wrapper's tap holds the enqueue's fiber.
    expect((yield* real.transaction(real.pending)).map((e) => e.envelope.mutationId)).toEqual(["m"])
    expect(pushed).toEqual([])
    const interrupting = yield* Effect.forkChild(Fiber.interrupt(enqueue))
    yield* Effect.yieldNow
    yield* Deferred.succeed(gate, undefined)
    yield* Fiber.join(interrupting)
    expect(Exit.hasInterrupts(yield* Fiber.await(enqueue))).toBe(true)

    // Nothing else wakes the loop (no pokes, no further enqueue): the finalizer's wake must be enough.
    const push = yield* Deferred.await(pushObserved).pipe(Effect.timeout("2 seconds"), Effect.option)
    expect(Option.isSome(push) ? "push observed" : "no push within 2 s").toBe("push observed")
    expect(pushed).toEqual(["m"])
    expect((yield* loop.status).lastError).toBeNull()
  }).pipe(Effect.provide(Db))
)
