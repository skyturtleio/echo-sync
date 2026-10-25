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
const makeServer = (
  options: {
    readonly hangTracks?: boolean
    /** How the server answers a track of `state` once it recorded it (default: at once). */
    readonly answer?: (state: Schema.Json) => Effect.Effect<void, Transport.PresenceError>
    /** Each join's preparation step (credentials), before its request is sent (default: none). */
    readonly prepare?: Effect.Effect<void, Transport.PresenceError>
  } = {}
) =>
  Effect.gen(function* () {
    const joins: Array<Schema.Json> = []
    const tracks: Array<Schema.Json> = []
    const opened: Array<Queue.Queue<Protocol.RoomElement, Transport.PresenceError>> = []
    const connections = yield* Queue.unbounded<Queue.Queue<Protocol.RoomElement, Transport.PresenceError>>()
    const upstream: Presence.Upstream<Transport.PresenceError> = {
      // The request is sent (and the join recorded) when the stream runs, after preparation.
      room: (payload) =>
        Effect.as(
          options.prepare ?? Effect.void,
          Stream.unwrap(
            Effect.gen(function* () {
              const q = yield* Queue.unbounded<Protocol.RoomElement, Transport.PresenceError>()
              // Together, so `opened[i]` is the connection of `joins[i]`.
              joins.push(payload.state)
              opened.push(q)
              yield* Queue.offer(connections, q)
              return Stream.fromQueue(q)
            })
          )
        ),
      // `hangTracks`: every track reaches the server, and its reply never comes back.
      track: (payload) =>
        Effect.andThen(
          Effect.sync(() => void tracks.push(payload.state)),
          options.hangTracks === true ? Effect.never : (options.answer?.(payload.state) ?? Effect.void)
        )
    }
    const nextConnection = Queue.take(connections)
    return { joins, tracks, opened, upstream, nextConnection }
  })
const fakeServer = makeServer()

const settle = Effect.repeat(Effect.yieldNow, { times: 50 })
const dropped = () => new Protocol.ServerUnavailable({ message: "dropped" })

it.effect("replacement ends the old stream during a connection or backoff without unregistering the newer sender", () =>
  Effect.gen(function* () {
    for (const inBackoff of [false, true]) {
      const server = yield* fakeServer
      const presence = Presence.make({ upstream: server.upstream, isFatal: Transport.isFatalPresence })
      const old = yield* Effect.forkChild(Stream.runDrain(presence.join({ room, tabId, state: "old", revision: 2 })))
      const first = yield* server.nextConnection
      if (inBackoff) yield* Queue.fail(first, dropped())
      yield* settle
      // Equal revisions replace too; a lower revision is covered separately below.
      const newer = yield* Effect.forkChild(Stream.runDrain(presence.join({ room, tabId, state: "new", revision: 2 })))
      const second = yield* server.nextConnection
      yield* Queue.offer(second, snapshot("new"))
      yield* settle
      // No clock advance: replacement must end even a subscription sleeping in backoff.
      expect(old.pollUnsafe()?._tag).toBe("Success")
      yield* presence.track({ room, tabId, state: "tracked", revision: 3 })
      yield* settle
      expect(server.tracks).toEqual(["tracked"])
      yield* TestClock.adjust("2 seconds")
      expect(server.joins).toEqual(["old", "new"])
      expect(newer.pollUnsafe()).toBeUndefined()
      yield* Fiber.interrupt(newer)
      yield* presence.track({ room, tabId, state: "closed", revision: 4 })
      yield* settle
      expect(server.tracks).toEqual(["tracked"])
    }
  })
)

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
        Stream.runForEach(presence.join({ room, tabId, state: "s0", revision: 0 }), (s) =>
          Effect.sync(() => void seen.push(s))
        )
      )
      const first = yield* server.nextConnection
      yield* Queue.offer(first, snapshot("s0"))
      yield* settle
      yield* presence.track({ room, tabId, state: "s1", revision: 1 })
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
    yield* Effect.forkChild(Stream.runDrain(presence.join({ room, tabId, state: "s0", revision: 0 })))
    const first = yield* server.nextConnection
    // Tracked after the join read s0 but before the server answered: recorded, delivered after the snapshot.
    yield* presence.track({ room, tabId, state: "s1", revision: 1 })
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
      yield* presence.track({ room, tabId, state: "nobody", revision: 1 })
      expect(server.tracks).toEqual([])
      const fiber = yield* Effect.forkChild(Stream.runDrain(presence.join({ room, tabId, state: "s0", revision: 0 })))
      yield* Queue.fail(yield* server.nextConnection, dropped())
      yield* TestClock.adjust("1 second")
      // A credential failure (SPEC §10.3 Credentials) is retried like any other, never fatal.
      yield* Queue.fail(
        yield* server.nextConnection,
        new Protocol.Unauthorized({ reason: "authenticationRefused", message: "expired" })
      )
      yield* TestClock.adjust("2 seconds")
      expect(fiber.pollUnsafe()).toBeUndefined()
      yield* Queue.fail(yield* server.nextConnection, new Protocol.Forbidden({ room }))
      const exit = yield* Fiber.await(fiber)
      expect(exit._tag === "Failure" && String(exit.cause).includes("Forbidden")).toBe(true)
      expect(server.joins.length).toBe(3)
      // The ended subscription is gone: a track is a no-op again.
      yield* presence.track({ room, tabId, state: "late", revision: 2 })
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
        Stream.runForEach(presence.join({ room, tabId, state: "s0", revision: 0 }), (s) =>
          Effect.sync(() => void seen.push(s))
        )
      )
      const first = yield* server.nextConnection
      yield* presence.track({ room, tabId, state: "s1", revision: 1 })
      yield* Queue.offer(first, snapshot("s0"))
      yield* settle
      // The send of s1 hangs; the snapshot is delivered anyway, and a later track returns at once.
      expect([seen.length, server.tracks]).toEqual([1, ["s1"]])
      const tracked = yield* Effect.forkChild(presence.track({ room, tabId, state: "s2", revision: 2 }))
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
          Stream.runDrain(presence.join({ room, tabId, state: "s0", revision: 0 })).pipe(
            Effect.provideService(References.MaxOpsBeforeYield, ops)
          )
        )
        yield* Effect.repeat(Effect.yieldNow, { times: turns })
        yield* Fiber.interrupt(fiber)
        yield* presence.track({ room, tabId, state: "late", revision: 1 })
        yield* settle
        expect({ ops, turns, tracks: server.tracks }).toEqual({ ops, turns, tracks: [] })
      }
    }
  })
)

it.effect("a join carries a genuine (state, revision) pair even when a track races its start", () =>
  Effect.gen(function* () {
    // States are named after their revision, so a torn read shows up as a mismatched pair.
    for (let ops = 3; ops < 35; ops++) {
      for (let turns = 0; turns < 20; turns++) {
        const server = yield* fakeServer
        const pairs: Array<string> = []
        const presence = Presence.make({
          upstream: {
            room: (p) =>
              Effect.andThen(
                Effect.sync(() => void pairs.push(`${p.state}@${p.revision}`)),
                server.upstream.room(p)
              ),
            track: server.upstream.track
          },
          isFatal: Transport.isFatalPresence
        })
        const fiber = yield* Effect.forkChild(
          Stream.runDrain(presence.join({ room, tabId, state: "s0", revision: 0 })).pipe(
            Effect.provideService(References.MaxOpsBeforeYield, ops)
          )
        )
        yield* Effect.repeat(Effect.yieldNow, { times: turns })
        yield* presence.track({ room, tabId, state: "s1", revision: 1 })
        yield* settle
        const torn = pairs.filter((pair) => pair !== "s0@0" && pair !== "s1@1")
        expect({ ops, turns, torn }).toEqual({ ops, turns, torn: [] })
        yield* Fiber.interrupt(fiber)
      }
    }
  })
)

/** Heartbeats on `connection` every 5 ms of test time, for `ms`; the room stream stays healthy throughout. */
const heartbeatFor = (connection: Queue.Queue<Protocol.RoomElement, Transport.PresenceError>, ms: number) =>
  Effect.gen(function* () {
    for (let t = 0; t < ms; t += 5) {
      yield* Queue.offer(connection, heartbeat)
      yield* TestClock.adjust("5 millis")
      yield* settle
    }
  })

it.effect(
  "a hung send is retried with the latest state while heartbeats keep the room stream healthy: no rejoin needed",
  () =>
    Effect.gen(function* () {
      const server = yield* makeServer({ answer: (state) => (state === "s1" ? Effect.never : Effect.void) })
      const presence = Presence.make({
        upstream: server.upstream,
        isFatal: Transport.isFatalPresence,
        idleTimeout: "30 millis",
        retryDelay: "10 millis"
      })
      const fiber = yield* Effect.forkChild(Stream.runDrain(presence.join({ room, tabId, state: "s0", revision: 0 })))
      const first = yield* server.nextConnection
      yield* Queue.offer(first, snapshot("s0"))
      yield* settle
      yield* presence.track({ room, tabId, state: "s1", revision: 1 })
      yield* settle
      yield* presence.track({ room, tabId, state: "s2", revision: 2 })
      yield* heartbeatFor(first, 200)
      // s1's send never answers; its deadline expires and the retry carries s2. The connection never ended.
      expect(server.tracks).toEqual(["s1", "s2"])
      expect(server.joins).toEqual(["s0"])
      expect(fiber.pollUnsafe()).toBeUndefined()
    })
)

it.effect("a send that dies is retried with the latest state; the stream and its connection stay up", () =>
  Effect.gen(function* () {
    const server = yield* makeServer({
      answer: (state) => (state === "s1" ? Effect.die(new Error("transport bug")) : Effect.void)
    })
    const presence = Presence.make({
      upstream: server.upstream,
      isFatal: Transport.isFatalPresence,
      idleTimeout: "30 millis",
      retryDelay: "10 millis"
    })
    const fiber = yield* Effect.forkChild(Stream.runDrain(presence.join({ room, tabId, state: "s0", revision: 0 })))
    const first = yield* server.nextConnection
    yield* Queue.offer(first, snapshot("s0"))
    yield* settle
    yield* presence.track({ room, tabId, state: "s1", revision: 1 })
    yield* settle
    yield* presence.track({ room, tabId, state: "s2", revision: 2 })
    yield* heartbeatFor(first, 100)
    expect(server.tracks).toEqual(["s1", "s2"])
    expect(server.joins).toEqual(["s0"])
    expect(fiber.pollUnsafe()).toBeUndefined()
  })
)

it.effect(
  "a late lower-revision join to the leader is superseded: it ends empty with no upstream join, and the incumbent keeps streaming and tracking",
  () =>
    Effect.gen(function* () {
      const server = yield* fakeServer
      const presence = Presence.make({ upstream: server.upstream, isFatal: Transport.isFatalPresence })
      const seen: Array<Protocol.RoomSnapshot> = []
      const incumbent = yield* Effect.forkChild(
        Stream.runForEach(presence.join({ room, tabId, state: "s2", revision: 2 }), (s) =>
          Effect.sync(() => void seen.push(s))
        )
      )
      const first = yield* server.nextConnection
      yield* Queue.offer(first, snapshot("s2"))
      yield* settle
      // The incumbent advances past the late join's revision through a track.
      yield* presence.track({ room, tabId, state: "s3", revision: 3 })
      yield* settle
      // A join@2 from an abandoned attempt arrives late.
      const late = yield* Effect.forkChild(
        Stream.runCollect(presence.join({ room, tabId, state: "stale", revision: 2 }))
      )
      yield* settle
      expect(server.joins).toEqual(["s2"])
      expect(incumbent.pollUnsafe()).toBeUndefined()
      expect(yield* Fiber.join(late)).toEqual([])
      yield* Queue.offer(first, snapshot("s3"))
      yield* presence.track({ room, tabId, state: "s4", revision: 4 })
      yield* settle
      expect(seen.length).toBe(2)
      expect(server.tracks).toEqual(["s3", "s4"])
    })
)

it.effect("a slow credential step does not count against idleTimeout: the clock starts when the join is sent", () =>
  Effect.gen(function* () {
    let prepared = 0
    const server = yield* makeServer({
      prepare: Effect.andThen(
        Effect.sync(() => void prepared++),
        Effect.sleep("40 seconds")
      )
    })
    const presence = Presence.make({
      upstream: server.upstream,
      isFatal: Transport.isFatalPresence,
      idleTimeout: "30 seconds"
    })
    const seen: Array<Protocol.RoomSnapshot> = []
    const fiber = yield* Effect.forkChild(
      Stream.runForEach(presence.join({ room, tabId, state: "s0", revision: 0 }), (s) =>
        Effect.sync(() => void seen.push(s))
      )
    )
    yield* settle
    yield* TestClock.adjust("40 seconds")
    const first = yield* server.nextConnection
    yield* Queue.offer(first, snapshot("s0"))
    yield* settle
    expect(seen.length).toBe(1)
    expect(prepared).toBe(1)
    expect(server.joins).toEqual(["s0"])
    expect(fiber.pollUnsafe()).toBeUndefined()
  })
)

it.effect(
  "the leader keeps the highest revision it accepted: a lower one after a higher one is never sent or re-joined with",
  () =>
    Effect.gen(function* () {
      const server = yield* fakeServer
      const sent: Array<number> = []
      const joined: Array<number> = []
      const presence = Presence.make({
        upstream: {
          room: (p) =>
            Effect.andThen(
              Effect.sync(() => void joined.push(p.revision)),
              server.upstream.room(p)
            ),
          track: (p) =>
            Effect.andThen(
              server.upstream.track(p),
              Effect.sync(() => void sent.push(p.revision))
            )
        },
        isFatal: Transport.isFatalPresence
      })
      yield* Effect.forkChild(Stream.runDrain(presence.join({ room, tabId, state: "s0", revision: 0 })))
      const first = yield* server.nextConnection
      yield* Queue.offer(first, snapshot("s0"))
      yield* settle
      yield* presence.track({ room, tabId, state: "s2", revision: 2 })
      yield* presence.track({ room, tabId, state: "s1", revision: 1 })
      yield* settle
      // The connection drops: the rejoin carries the highest accepted pair, not the last call's.
      yield* Queue.fail(first, dropped())
      yield* TestClock.adjust("1 second")
      yield* settle
      expect({ tracks: server.tracks, sent, joins: server.joins, joined }).toEqual({
        tracks: ["s2"],
        sent: [2],
        joins: ["s0", "s2"],
        joined: [0, 2]
      })
    })
)
