import { expect, it } from "@effect/vitest"
import { Effect, Fiber, Queue, References, type Schema, Stream } from "effect"
import { TestClock } from "effect/testing"
import { AccessScope, Ids, Protocol } from "echo-sync"
import * as Presence from "./Presence.ts"
import * as Transport from "./Transport.ts"

const room = AccessScope.AccessScopeId.make("room-1")
const tabId = Protocol.TabId.make("tab-1")
const snapshot = (state: Schema.Json): Protocol.RoomElement => ({
  _tag: "snapshot",
  room,
  entries: [{ principalId: AccessScope.PrincipalId.make("p"), clientId: Ids.ClientId.make("c"), tabId, state }]
})
const heartbeat: Protocol.RoomElement = { _tag: "heartbeat" }

/** A server whose every `room` connection is a queue the test feeds; records joins and tracks. */
const makeServer = (options: { readonly hangTracks?: boolean } = {}) =>
  Effect.gen(function* () {
    const joins: Array<Schema.Json> = []
    const tracks: Array<Schema.Json> = []
    const opened: Array<Queue.Queue<Protocol.RoomElement, Transport.PresenceError>> = []
    const connections = yield* Queue.unbounded<Queue.Queue<Protocol.RoomElement, Transport.PresenceError>>()
    const upstream: Presence.Upstream<Transport.PresenceError> = {
      room: (payload) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const q = yield* Queue.unbounded<Protocol.RoomElement, Transport.PresenceError>()
            // Together, so `opened[i]` is the connection of `joins[i]`.
            joins.push(payload.state)
            opened.push(q)
            yield* Queue.offer(connections, q)
            return Stream.fromQueue(q)
          })
        ),
      // `hangTracks`: every track reaches the server, and its reply never comes back.
      track: (payload) =>
        Effect.andThen(
          Effect.sync(() => void tracks.push(payload.state)),
          options.hangTracks === true ? Effect.never : Effect.void
        )
    }
    const nextConnection = Queue.take(connections)
    return { joins, tracks, opened, upstream, nextConnection }
  })
const fakeServer = makeServer()

const settle = Effect.repeat(Effect.yieldNow, { times: 50 })
const dropped = () => new Protocol.ServerUnavailable({ message: "dropped" })

it.effect(
  "heartbeats never reach the tab stream; a silent connection is rejoined with the latest tracked state and the stream stays open",
  () =>
    Effect.gen(function* () {
      const server = yield* fakeServer
      const presence = Presence.make({
        upstream: server.upstream,
        isFatal: Transport.isFatalPresence,
        idleTimeout: "45 seconds"
      })
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
    const presence = Presence.make({ upstream: server.upstream, isFatal: Transport.isFatalPresence })
    yield* Effect.forkChild(Stream.runDrain(presence.join({ room, tabId, state: "s0" })))
    const first = yield* server.nextConnection
    // Tracked after the join read s0 but before the server answered: recorded, delivered after the snapshot.
    yield* presence.track({ room, tabId, state: "s1" })
    yield* settle
    expect(server.tracks).toEqual([])
    yield* Queue.offer(first, snapshot("s0"))
    yield* settle
    expect(server.tracks).toEqual(["s1"])
    // No change since the join: no catch-up after a later snapshot or rejoin.
    yield* Queue.fail(first, dropped())
    yield* TestClock.adjust("1 second")
    const second = yield* server.nextConnection
    yield* Queue.offer(second, snapshot("s1"))
    yield* settle
    expect(server.joins).toEqual(["s0", "s1"])
    expect(server.tracks).toEqual(["s1"])
  })
)

it.effect(
  "track without an open subscription is a no-op; Unauthorized and other transient errors rejoin, only Forbidden ends the subscription",
  () =>
    Effect.gen(function* () {
      const server = yield* fakeServer
      const presence = Presence.make({ upstream: server.upstream, isFatal: Transport.isFatalPresence })
      yield* presence.track({ room, tabId, state: "nobody" })
      expect(server.tracks).toEqual([])
      const fiber = yield* Effect.forkChild(Stream.runDrain(presence.join({ room, tabId, state: "s0" })))
      yield* Queue.fail(yield* server.nextConnection, dropped())
      yield* TestClock.adjust("1 second")
      // A credential failure (DESIGN §9.1 Credentials) is retried like any other, never fatal.
      yield* Queue.fail(yield* server.nextConnection, new Protocol.Unauthorized({ message: "expired" }))
      yield* TestClock.adjust("2 seconds")
      expect(fiber.pollUnsafe()).toBeUndefined()
      yield* Queue.fail(yield* server.nextConnection, new Protocol.Forbidden({ room }))
      const exit = yield* Fiber.await(fiber)
      expect(exit._tag === "Failure" && String(exit.cause).includes("Forbidden")).toBe(true)
      expect(server.joins.length).toBe(3)
      // The ended subscription is gone: a track is a no-op again.
      yield* presence.track({ room, tabId, state: "late" })
      expect(server.tracks).toEqual([])
    })
)

it.effect(
  "a track send that never completes blocks neither the first snapshot nor the idle-timeout rejoin, which carries the latest state",
  () =>
    Effect.gen(function* () {
      const server = yield* makeServer({ hangTracks: true })
      const presence = Presence.make({ upstream: server.upstream, isFatal: Transport.isFatalPresence })
      const seen: Array<Protocol.RoomSnapshot> = []
      const fiber = yield* Effect.forkChild(
        Stream.runForEach(presence.join({ room, tabId, state: "s0" }), (s) => Effect.sync(() => void seen.push(s)))
      )
      const first = yield* server.nextConnection
      yield* presence.track({ room, tabId, state: "s1" })
      yield* Queue.offer(first, snapshot("s0"))
      yield* settle
      // The send of s1 hangs; the snapshot is delivered anyway, and a later track returns at once.
      expect([seen.length, server.tracks]).toEqual([1, ["s1"]])
      const tracked = yield* Effect.forkChild(presence.track({ room, tabId, state: "s2" }))
      yield* settle
      expect(tracked.pollUnsafe()?._tag).toBe("Success")
      // Silence: the idle timeout ends the connection (interrupting the hung send) and the rejoin carries s2.
      yield* TestClock.adjust("46 seconds")
      yield* settle
      const second = yield* server.nextConnection
      expect(server.joins).toEqual(["s0", "s2"])
      yield* Queue.offer(second, snapshot("s2"))
      yield* settle
      expect(seen.length).toBe(2)
      expect(server.tracks).toEqual(["s1"])
      expect(fiber.pollUnsafe()).toBeUndefined()
    })
)

it.effect("a subscription cancelled at any point leaves nothing registered: a later track is never sent", () =>
  Effect.gen(function* () {
    // Preemption inside the opening fiber (a tiny budget on it), swept against when the parent cancels it.
    for (let ops = 3; ops < 35; ops++) {
      for (let turns = 0; turns < 20; turns++) {
        const server = yield* fakeServer
        const presence = Presence.make({ upstream: server.upstream, isFatal: Transport.isFatalPresence })
        const fiber = yield* Effect.forkChild(
          Stream.runDrain(presence.join({ room, tabId, state: "s0" })).pipe(
            Effect.provideService(References.MaxOpsBeforeYield, ops)
          )
        )
        yield* Effect.repeat(Effect.yieldNow, { times: turns })
        yield* Fiber.interrupt(fiber)
        yield* presence.track({ room, tabId, state: "late" })
        yield* settle
        expect({ ops, turns, tracks: server.tracks }).toEqual({ ops, turns, tracks: [] })
      }
    }
  })
)
