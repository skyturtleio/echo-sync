import { expect, it } from "@effect/vitest"
import { Effect, Fiber, Queue, Schema, Stream } from "effect"
import { TestClock } from "effect/testing"
import { AccessScope, Ids, Protocol } from "echo-sync"
import * as Presence from "./Presence.ts"

class Denied extends Schema.TaggedError<Denied>()("Denied", {}) {}
class Dropped extends Schema.TaggedError<Dropped>()("Dropped", {}) {}

const room = AccessScope.AccessScopeId.make("room-1")
const tabId = Protocol.TabId.make("tab-1")
const snapshot = (state: Schema.Json): Protocol.RoomElement => ({
  _tag: "snapshot",
  room,
  entries: [{ principalId: AccessScope.PrincipalId.make("p"), clientId: Ids.ClientId.make("c"), tabId, state }]
})
const heartbeat: Protocol.RoomElement = { _tag: "heartbeat" }

/** A server whose every `room` connection is a queue the test feeds; records joins and tracks. */
const fakeServer = Effect.gen(function* () {
  const joins: Array<Schema.Json> = []
  const tracks: Array<Schema.Json> = []
  const connections = yield* Queue.unbounded<Queue.Queue<Protocol.RoomElement, Denied | Dropped>>()
  const upstream: Presence.Upstream<Denied | Dropped> = {
    room: (payload) =>
      Stream.unwrap(
        Effect.gen(function* () {
          joins.push(payload.state)
          const q = yield* Queue.unbounded<Protocol.RoomElement, Denied | Dropped>()
          yield* Queue.offer(connections, q)
          return Stream.fromQueue(q)
        })
      ),
    track: (payload) => Effect.sync(() => void tracks.push(payload.state))
  }
  const nextConnection = Queue.take(connections)
  return { joins, tracks, upstream, nextConnection }
})

const settle = Effect.repeat(Effect.yieldNow, { times: 50 })
const isDenied = (e: Denied | Dropped): e is Denied => e._tag === "Denied"

it.effect(
  "heartbeats never reach the tab stream; a silent connection is rejoined with the latest tracked state and the stream stays open",
  () =>
    Effect.gen(function* () {
      const server = yield* fakeServer
      const presence = Presence.make({ upstream: server.upstream, isFatal: isDenied, idleTimeout: "45 seconds" })
      const seen: Array<Protocol.RoomSnapshot> = []
      const fiber = yield* Effect.forkChild(
        Stream.runForEach(presence.join({ room, tabId, state: "s0" }), (s) => Effect.sync(() => void seen.push(s)))
      )
      const first = yield* server.nextConnection
      yield* Queue.offer(first, snapshot("s0"))
      yield* settle
      yield* presence.track({ room, tabId, state: "s1" })
      // Heartbeats keep the connection alive past the idle timeout and are never emitted.
      for (let i = 0; i < 3; i++) {
        yield* TestClock.adjust("30 seconds")
        yield* Queue.offer(first, heartbeat)
        yield* settle
      }
      expect(seen.map((s) => s._tag)).toEqual(["snapshot"])
      expect(server.joins).toEqual(["s0"])
      // Now the connection goes silent: no element for 45 s ends it, and the rejoin (after a 1 s backoff) carries s1.
      yield* TestClock.adjust("46 seconds")
      yield* settle
      const second = yield* server.nextConnection
      expect(server.joins).toEqual(["s0", "s1"])
      yield* Queue.offer(second, snapshot("s1"))
      yield* settle
      expect(seen.length).toBe(2)
      expect(fiber.pollUnsafe()).toBeUndefined()
      expect(server.tracks).toEqual(["s1"])
    })
)

it.effect("a track that races a join is sent again after the join's first snapshot", () =>
  Effect.gen(function* () {
    const server = yield* fakeServer
    const presence = Presence.make({ upstream: server.upstream, isFatal: isDenied })
    yield* Effect.forkChild(Stream.runDrain(presence.join({ room, tabId, state: "s0" })))
    const first = yield* server.nextConnection
    // Tracked after the join read s0 but before the server answered: the server may have dropped it.
    yield* presence.track({ room, tabId, state: "s1" })
    expect(server.tracks).toEqual(["s1"])
    yield* Queue.offer(first, snapshot("s0"))
    yield* settle
    expect(server.tracks).toEqual(["s1", "s1"])
    // No change since the join: no catch-up after a later snapshot or rejoin.
    yield* Queue.fail(first, new Dropped())
    yield* TestClock.adjust("1 second")
    const second = yield* server.nextConnection
    yield* Queue.offer(second, snapshot("s1"))
    yield* settle
    expect(server.joins).toEqual(["s0", "s1"])
    expect(server.tracks).toEqual(["s1", "s1"])
  })
)

it.effect(
  "track without an open subscription is a no-op; a fatal error ends the subscription, a transient one rejoins",
  () =>
    Effect.gen(function* () {
      const server = yield* fakeServer
      const presence = Presence.make({ upstream: server.upstream, isFatal: isDenied })
      yield* presence.track({ room, tabId, state: "nobody" })
      expect(server.tracks).toEqual([])
      const fiber = yield* Effect.forkChild(Stream.runDrain(presence.join({ room, tabId, state: "s0" })))
      yield* Queue.fail(yield* server.nextConnection, new Dropped())
      yield* TestClock.adjust("1 second")
      yield* Queue.fail(yield* server.nextConnection, new Denied())
      const exit = yield* Fiber.await(fiber)
      expect(exit._tag === "Failure" && String(exit.cause).includes("Denied")).toBe(true)
      expect(server.joins.length).toBe(2)
      // The ended subscription is gone: a track is a no-op again.
      yield* presence.track({ room, tabId, state: "late" })
      expect(server.tracks).toEqual([])
    })
)
