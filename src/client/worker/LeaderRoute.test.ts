import { expect, it } from "@effect/vitest"
import { Cause, Deferred, Effect, Exit, Fiber, Option, Schema, Stream } from "effect"
import { RpcClientDefect, RpcClientError } from "effect/rpc/RpcClientError"
import * as LeaderRoute from "./LeaderRoute.ts"
import { type LeaderInfo, LeaderUnavailable } from "./LeaderRpcs.ts"
import type { LeaderConnection } from "./TabPlatform.ts"

const info = (leaderId: string): LeaderInfo => ({ leaderId, clientId: "c", dbName: "db", syncing: false })

/** A leader whose `leaderInfo` and `syncStatus` run `call` and `stream`; every other method is unused. */
const leader = (
  call: LeaderConnection["leaderInfo"],
  stream: LeaderConnection["syncStatus"] = () => Stream.never
): LeaderConnection => ({
  exec: () => Effect.die("unused"),
  enqueue: () => Effect.die("unused"),
  subscribe: () => Stream.die("unused"),
  subscribeSnapshot: () => Stream.die("unused"),
  subscribeRaw: () => Stream.die("unused"),
  leaderInfo: call,
  syncStatus: stream,
  requestSync: () => Effect.die("unused"),
  presence: () => Stream.die("unused"),
  track: () => Effect.die("unused"),
  notices: () => Stream.die("unused"),
  dismissNotices: () => Effect.die("unused"),
  exportPending: () => Effect.die("unused")
})

const connected = (leaderId: string, client: LeaderConnection) =>
  Effect.map(Deferred.make<LeaderUnavailable>(), (lost) => ({ _tag: "Connected" as const, leaderId, client, lost }))

/** The failed request's `LeaderUnavailable`, if that is how it failed. */
const unavailable = <A, E>(exit: Exit.Exit<A, E>) =>
  Option.filter(Exit.isFailure(exit) ? Exit.findErrorOption(exit) : Option.none(), (e): e is E & LeaderUnavailable =>
    Schema.is(LeaderUnavailable)(e)
  )

/** The `LeaderUnavailable` reason a request failed with; anything else (success, interruption) is "other". */
const reasonOf = <A, E>(exit: Exit.Exit<A, E>) =>
  Option.match(unavailable(exit), { onNone: () => "other", onSome: (e) => e.reason })

it.effect("a request issued before any leader connects must wait for one, not fail or use a stale connection", () =>
  Effect.gen(function* () {
    const route = LeaderRoute.makeUnsafe()
    const first = yield* Effect.forkChild(route.withLeader((client) => client.leaderInfo()))
    yield* Effect.yieldNow
    expect(first.pollUnsafe()).toBeUndefined()
    route.publish(
      yield* connected(
        "a",
        leader(() => Effect.succeed(info("a")))
      )
    )
    expect(yield* Fiber.join(first)).toEqual(info("a"))
    // A handover: `a` is withdrawn while `b` handshakes, so a request now waits for `b`, never reaches `a`.
    route.publish(undefined)
    const second = yield* Effect.forkChild(route.withLeader((client) => client.leaderInfo()))
    yield* Effect.yieldNow
    expect(second.pollUnsafe()).toBeUndefined()
    route.publish(
      yield* connected(
        "b",
        leader(() => Effect.succeed(info("b")))
      )
    )
    expect(yield* Fiber.join(second)).toEqual(info("b"))
  })
)

it.effect("a displaced connection must fail its in-flight request and stream with its LeaderLost, not hang", () =>
  Effect.gen(function* () {
    const route = LeaderRoute.makeUnsafe()
    const connection = yield* connected(
      "a",
      leader(
        () => Effect.never,
        () => Stream.never
      )
    )
    route.publish(connection)
    const call = yield* Effect.forkChild(Effect.exit(route.withLeader((client) => client.leaderInfo())))
    const stream = yield* Effect.forkChild(
      Effect.exit(Stream.runDrain(route.streamWithLeader((client) => client.syncStatus())))
    )
    yield* Effect.yieldNow
    // As LeaderClient displaces: record the loss first, the RPC client is torn down after.
    yield* Deferred.succeed(connection.lost, LeaderRoute.lostError("a"))
    expect(reasonOf(yield* Fiber.join(call))).toBe("LeaderLost")
    expect(reasonOf(yield* Fiber.join(stream))).toBe("LeaderLost")
    // A request against the displaced connection, before the next is published, fails the same way at once.
    expect(reasonOf(yield* Effect.exit(route.withLeader((client) => client.leaderInfo())))).toBe("LeaderLost")
  })
)

it.effect("a transport failure must surface as LeaderLost naming the leader, never as a raw RpcClientError", () =>
  Effect.gen(function* () {
    const route = LeaderRoute.makeUnsafe()
    const transport = new RpcClientError({ reason: new RpcClientDefect({ message: "port closed", cause: null }) })
    route.publish(
      yield* connected(
        "a",
        leader(() => Effect.fail(transport))
      )
    )
    const exit = yield* Effect.exit(route.withLeader((client) => client.leaderInfo()))
    expect(Option.map(unavailable(exit), (e) => [e.reason, e.message])).toEqual(
      Option.some(["LeaderLost", "leader a went away: RpcClientDefect: port closed"])
    )
  })
)

it.effect("leader transport mapping preserves compound causes and never classifies arbitrary callback failures", () =>
  Effect.gen(function* () {
    const transport = new RpcClientError({ reason: new RpcClientDefect({ message: "closed", cause: null }) })
    const failure = Cause.makeFailReason(transport)
    for (const other of [Cause.makeDieReason("bug"), Cause.makeInterruptReason(123)]) {
      for (const reasons of [
        [failure, other],
        [other, failure]
      ]) {
        const route = LeaderRoute.makeUnsafe()
        const cause = Cause.fromReasons(reasons)
        route.publish(
          yield* connected(
            "a",
            leader(
              () => Effect.failCause(cause),
              () => Stream.failCause(cause)
            )
          )
        )
        const exits = [
          yield* Effect.exit(route.withLeader((client) => client.leaderInfo())),
          yield* Effect.exit(Stream.runDrain(route.streamWithLeader((client) => client.syncStatus())))
        ]
        for (const exit of exits) {
          expect(exit._tag).toBe("Failure")
          if (exit._tag === "Failure") {
            expect(exit.cause.reasons).toContain(other)
            expect(exit.cause.reasons).toHaveLength(2)
            expect(reasonOf(exit)).toBe("LeaderLost")
          }
        }
        const callback = yield* Effect.exit(route.withLeader(() => Effect.failCause(cause)))
        expect(callback).toEqual(Exit.failCause(cause))
        const callbackStream = yield* Effect.exit(
          Stream.runDrain(route.streamWithLeader(() => Stream.failCause(cause)))
        )
        expect(callbackStream).toEqual(Exit.failCause(cause))
      }
    }
  })
)

it.effect(
  "after reset every request fails Reset: waiting, in flight (even as interruption) and later, never LeaderLost",
  () =>
    Effect.gen(function* () {
      const waiting = LeaderRoute.makeUnsafe()
      const waiter = yield* Effect.forkChild(Effect.exit(waiting.withLeader((client) => client.leaderInfo())))
      yield* Effect.yieldNow
      waiting.reset()
      expect(reasonOf(yield* Fiber.join(waiter))).toBe("Reset")

      const route = LeaderRoute.makeUnsafe()
      const teardown = yield* Deferred.make<void>()
      // The shut-down RPC client answers the call in flight with interruption, the stream with a transport error.
      const transport = new RpcClientError({ reason: new RpcClientDefect({ message: "closed", cause: null }) })
      route.publish(
        yield* connected(
          "a",
          leader(
            () => Effect.andThen(Deferred.await(teardown), Effect.interrupt),
            () => Stream.fromEffect(Effect.andThen(Deferred.await(teardown), Effect.fail(transport)))
          )
        )
      )
      const call = yield* Effect.forkChild(Effect.exit(route.withLeader((client) => client.leaderInfo())))
      const stream = yield* Effect.forkChild(
        Effect.exit(Stream.runDrain(route.streamWithLeader((client) => client.syncStatus())))
      )
      yield* Effect.yieldNow
      route.reset()
      yield* Deferred.succeed(teardown, undefined)
      expect(reasonOf(yield* Fiber.join(call))).toBe("Reset")
      expect(reasonOf(yield* Fiber.join(stream))).toBe("Reset")

      // Publishing after reset is ignored: later requests still fail Reset.
      route.publish(
        yield* connected(
          "b",
          leader(() => Effect.succeed(info("b")))
        )
      )
      expect(reasonOf(yield* Effect.exit(route.withLeader((client) => client.leaderInfo())))).toBe("Reset")
      expect(
        reasonOf(yield* Effect.exit(Stream.runDrain(route.streamWithLeader((client) => client.syncStatus()))))
      ).toBe("Reset")
    })
)
