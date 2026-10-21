// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { expect, it } from "@effect/vitest"
import { Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect"
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
    const transport: Engine.Transport<never> = {
      push: (request) =>
        Effect.sync(() => {
          pushed.push(...request.mutations.map((m) => m.mutationId))
          return {
            results: request.mutations.map((m) => ({
              mutationId: m.mutationId,
              position,
              outcome: { _tag: "Applied" as const }
            }))
          }
        }),
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
    const loop = yield* SyncLoop.make({ engine, isFatalPush: () => false })
    yield* Effect.forkChild(loop.run)
    yield* Effect.sleep("100 millis") // the loop's first cycle: nothing pending, then it waits for a wake

    state.gateNext = true
    const enqueue = yield* Effect.forkChild(enqueueAndWake(engine, loop.wake)(Ids.MutationId.make("m"), "add", {}))
    yield* Deferred.await(committed)
    const interrupting = yield* Effect.forkChild(Fiber.interrupt(enqueue))
    yield* Effect.yieldNow
    yield* Deferred.succeed(gate, undefined)
    yield* Fiber.join(interrupting)
    expect(Exit.hasInterrupts(yield* Fiber.await(enqueue))).toBe(true)
    expect((yield* real.transaction(real.pending)).map((e) => e.envelope.mutationId)).toEqual(["m"])

    // Nothing else wakes the loop (no pokes, no further enqueue): the finalizer's wake must be enough.
    yield* Effect.sleep("300 millis")
    expect(pushed).toEqual(["m"])
    expect((yield* loop.status).lastError).toBeNull()
  }).pipe(Effect.provide(Db))
)
