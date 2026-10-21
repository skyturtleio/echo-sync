import { expect, it } from "@effect/vitest"
import { Cause, Clock, Effect, Queue, Schema, Stream } from "effect"
import { RpcClientError } from "effect/rpc"
import { TestClock } from "effect/testing"
import { Ids, Protocol } from "echo-sync"
import * as Engine from "./Engine.ts"
import * as SyncLoop from "./SyncLoop.ts"
import * as Transport from "./Transport.ts"

// The real fatal rule and core's real error, not stand-ins (reviewer question 1).
const malformed = () =>
  new Protocol.MalformedEnvelope({
    clientId: Ids.ClientId.make("c"),
    mutationId: Ids.MutationId.make("m"),
    message: "bad envelope"
  })
type Malformed = Protocol.MalformedEnvelope
class Offline extends Schema.TaggedError<Offline>()("Offline", {}) {}

/** Fake engine: `pushFails` decides each push attempt's outcome; counts calls. */
const fakeEngine = (pushFails: (attempt: number) => Malformed | Offline | undefined) => {
  const calls = { push: 0, pull: 0 }
  const engine: Engine.Engine<Malformed | Offline> = {
    mutate: () => Effect.die("unused"),
    push: Effect.suspend(() => {
      const error = pushFails(calls.push++)
      return error === undefined ? Effect.succeed({ processed: 1, stopped: false }) : Effect.fail(error)
    }),
    pull: Effect.sync(() => void calls.pull++)
  }
  return { engine, calls }
}

const settle = Effect.repeat(Effect.yieldNow, { times: 50 })
// Typed as the protocol's element union (not narrowed per tag), as the transport delivers them.
const element = (tag: "poke" | "heartbeat"): typeof Protocol.PokesElement.Type => ({ _tag: tag })
const poke = element("poke")
const heartbeat = element("heartbeat")

it.effect("an integrity error halts push for good while pull keeps running", () =>
  Effect.gen(function* () {
    const { engine, calls } = fakeEngine(() => malformed())
    const loop = yield* SyncLoop.make({ engine, isFatalPush: Transport.isFatalPush })
    yield* Effect.forkChild(loop.run)
    yield* settle
    expect((yield* loop.status).push).toEqual({ _tag: "Halted", message: "MalformedEnvelope: bad envelope" })
    // A halt is not a failure: the halting cycle still pulled, and no backoff delays the next one.
    expect(calls.pull).toBe(1)
    expect((yield* loop.status).lastError).toBeNull()
    yield* TestClock.adjust("1 minute")
    yield* settle
    expect(calls.pull).toBe(1) // no backoff retry was scheduled
    // Neither waking nor time passing retries the push; pull still runs on every cycle, at once.
    for (let i = 0; i < 3; i++) {
      yield* loop.wake
      yield* settle
      expect(calls.pull).toBe(i + 2)
      yield* TestClock.adjust("1 minute")
    }
    expect(calls.push).toBe(1)
    expect(calls.pull).toBe(4)
    expect((yield* loop.status).lastError).toBeNull()
  })
)

it.effect("a transient push failure is retried with backoff and clears once a cycle succeeds", () =>
  Effect.gen(function* () {
    const { engine, calls } = fakeEngine((attempt) => (attempt < 2 ? new Offline() : undefined))
    const loop = yield* SyncLoop.make({ engine, isFatalPush: Transport.isFatalPush })
    yield* Effect.forkChild(loop.run)
    yield* settle
    expect(calls.push).toBe(1)
    expect((yield* loop.status).lastError).not.toBeNull()
    yield* TestClock.adjust("999 millis")
    expect(calls.push).toBe(1)
    yield* TestClock.adjust("1 millis")
    yield* settle
    expect(calls.push).toBe(2)
    // Second delay doubles to 2 s.
    yield* TestClock.adjust("1999 millis")
    expect(calls.push).toBe(2)
    yield* TestClock.adjust("1 millis")
    yield* settle
    expect(calls.push).toBe(3)
    expect(yield* loop.status).toEqual({ push: { _tag: "Active" }, lastError: null })
  })
)

const rpcFailure = () =>
  new RpcClientError.RpcClientError({
    reason: new RpcClientError.RpcClientDefect({ message: "socket closed", cause: null })
  })

it.effect(
  "a defect beside a transient failure halts push (whole-cause classification); a pull defect is transient",
  () =>
    Effect.gen(function* () {
      const calls = { push: 0, pull: 0 }
      const engine: Engine.Engine<RpcClientError.RpcClientError> = {
        mutate: () => Effect.die("unused"),
        push: Effect.suspend(() => {
          calls.push++
          return Effect.failCause(Cause.combine(Cause.fail(rpcFailure()), Cause.die(new Error("undecodable request"))))
        }),
        pull: Effect.suspend(() => {
          calls.pull++
          return Effect.die(new Error("bad page"))
        })
      }
      const loop = yield* SyncLoop.make({ engine, isFatalPush: Transport.isFatalPush })
      const fiber = yield* Effect.forkChild(loop.run)
      yield* settle
      const status = yield* loop.status
      expect(status.push._tag).toBe("Halted")
      // The loop survived the pull defect and reports it as a transient failure.
      expect(status.lastError).toContain("bad page")
      expect(fiber.pollUnsafe()).toBeUndefined()
      yield* TestClock.adjust("1 minute")
      yield* settle
      // Push never retried; pull kept retrying with backoff.
      expect(calls.push).toBe(1)
      expect(calls.pull).toBeGreaterThan(2)
    })
)

it.effect("a transient failure alone (RpcClientError) is retried, never halts", () =>
  Effect.gen(function* () {
    const calls = { push: 0 }
    const engine: Engine.Engine<RpcClientError.RpcClientError> = {
      mutate: () => Effect.die("unused"),
      push: Effect.suspend(() => {
        calls.push++
        return Effect.fail(rpcFailure())
      }),
      pull: Effect.void
    }
    const loop = yield* SyncLoop.make({ engine, isFatalPush: Transport.isFatalPush })
    yield* Effect.forkChild(loop.run)
    yield* settle
    yield* TestClock.adjust("1 second")
    yield* settle
    expect(calls.push).toBe(2)
    expect((yield* loop.status).push).toEqual({ _tag: "Active" })
  })
)

it.effect("a pull that fails with MalformedChange is retried with backoff and never halts push", () =>
  Effect.gen(function* () {
    const calls = { push: 0, pull: 0 }
    const engine: Engine.Engine<never> = {
      mutate: () => Effect.die("unused"),
      push: Effect.sync(() => {
        calls.push++
        return { processed: 0, stopped: false }
      }),
      pull: Effect.suspend(() => {
        calls.pull++
        return calls.pull === 1
          ? Effect.fail(new Engine.MalformedChange({ table: "t", message: "row does not decode" }))
          : Effect.void
      })
    }
    const loop = yield* SyncLoop.make({ engine, isFatalPush: Transport.isFatalPush })
    yield* Effect.forkChild(loop.run)
    yield* settle
    const failed = yield* loop.status
    expect(failed.push).toEqual({ _tag: "Active" })
    expect(failed.lastError).toContain("MalformedChange")
    yield* TestClock.adjust("999 millis")
    yield* settle
    expect(calls.pull).toBe(1)
    yield* TestClock.adjust("1 millis")
    yield* settle
    expect(calls).toEqual({ push: 2, pull: 2 })
    expect(yield* loop.status).toEqual({ push: { _tag: "Active" }, lastError: null })
  })
)

it.effect(
  "the pokes stream is reconnected with backoff after a clean end, a failure and a defect; every poke wakes",
  () =>
    Effect.gen(function* () {
      const attempts: Array<number> = []
      const wakes = { n: 0 }
      const connect = Stream.unwrap(
        Effect.gen(function* () {
          const attempt = attempts.length + 1
          attempts.push(Number(yield* Clock.currentTimeMillis))
          if (attempt === 1) return Stream.make(poke, poke) // two pokes, then a clean end
          if (attempt === 2) return Stream.fail(new Offline())
          if (attempt === 3) return Stream.die(new Error("bad frame"))
          return Stream.as(Stream.tick("15 seconds"), heartbeat) // live: heartbeats only
        })
      )
      const fiber = yield* Effect.forkChild(SyncLoop.followPokes({ connect, wake: Effect.sync(() => void wakes.n++) }))
      yield* settle
      expect(attempts).toEqual([0])
      expect(wakes.n).toBe(2)
      // Attempt 1 delivered pokes: base delay 1 s. Attempts 2 and 3 delivered none: 2 s, then 4 s.
      for (const ms of [1000, 2000, 4000]) {
        yield* TestClock.adjust(ms - 1)
        yield* settle
        const before = attempts.length
        yield* TestClock.adjust(1)
        yield* settle
        expect(attempts.length).toBe(before + 1)
      }
      expect(attempts).toEqual([0, 1000, 3000, 7000])
      expect(wakes.n).toBe(2) // only pokes wake: reconnecting itself does not
      expect(fiber.pollUnsafe()).toBeUndefined()
      for (let i = 0; i < 40; i++) {
        yield* TestClock.adjust("15 seconds")
        yield* settle
      }
      expect(attempts.length).toBe(4) // a live (heartbeating) connection is not replaced
      expect(wakes.n).toBe(2)
    })
)

it.effect("only stream elements wake: a stream with no initial poke wakes nothing until it delivers one", () =>
  Effect.gen(function* () {
    const wakes = { n: 0 }
    const notifications = yield* Queue.unbounded<typeof poke>()
    const connections = { n: 0 }
    // Attempt 1 ends at once with no element; attempt 2 is a live stream that has sent nothing yet.
    const connect = Stream.unwrap(
      Effect.sync(() => (++connections.n === 1 ? Stream.empty : Stream.fromQueue(notifications)))
    )
    yield* Effect.forkChild(SyncLoop.followPokes({ connect, wake: Effect.sync(() => void wakes.n++) }))
    yield* settle
    yield* TestClock.adjust("1 second")
    yield* settle
    expect([connections.n, wakes.n]).toEqual([2, 0])
    yield* Queue.offer(notifications, poke)
    yield* settle
    expect(wakes.n).toBe(1)
  })
)

it.effect("the server's initial poke, the first element of each stream, wakes the loop on every (re)connect", () =>
  Effect.gen(function* () {
    const wakes = { n: 0 }
    const connections = { n: 0 }
    // Like the real server: one poke right after LISTEN; the first connection then ends.
    const connect = Stream.unwrap(
      Effect.sync(() => (++connections.n === 1 ? Stream.make(poke) : Stream.concat(Stream.make(poke), Stream.never)))
    )
    yield* Effect.forkChild(SyncLoop.followPokes({ connect, wake: Effect.sync(() => void wakes.n++) }))
    yield* settle
    expect([connections.n, wakes.n]).toEqual([1, 1])
    yield* TestClock.adjust("1 second")
    yield* settle
    expect([connections.n, wakes.n]).toEqual([2, 2])
  })
)

it.effect(
  "a pokes stream silent past idleTimeout is ended and reconnected, and the new stream's initial poke wakes; heartbeats keep it open without waking",
  () =>
    Effect.gen(function* () {
      const wakes = { n: 0 }
      const first = yield* Queue.unbounded<typeof Protocol.PokesElement.Type>()
      const connections = { n: 0 }
      // Connection 1 is fed by the test, then goes silent without ending (a dead proxy); connection 2
      // is a live server stream: its initial poke, then nothing.
      const connect = Stream.unwrap(
        Effect.sync(() =>
          ++connections.n === 1 ? Stream.fromQueue(first) : Stream.concat(Stream.make(poke), Stream.never)
        )
      )
      yield* Effect.forkChild(
        SyncLoop.followPokes({ connect, wake: Effect.sync(() => void wakes.n++), idleTimeout: "45 seconds" })
      )
      yield* Queue.offer(first, poke)
      yield* settle
      expect([connections.n, wakes.n]).toEqual([1, 1])
      // Heartbeats every 30 s: never 45 s without an element, so the stream stays; none of them wakes.
      for (let i = 0; i < 3; i++) {
        yield* TestClock.adjust("30 seconds")
        yield* Queue.offer(first, heartbeat)
        yield* settle
      }
      expect([connections.n, wakes.n]).toEqual([1, 1])
      // Silent from here: ended exactly 45 s after the last element, reconnected after the 1 s backoff.
      yield* TestClock.adjust("44999 millis")
      yield* settle
      yield* TestClock.adjust("1 second")
      yield* settle
      expect([connections.n, wakes.n]).toEqual([1, 1])
      yield* TestClock.adjust("1 millis")
      yield* settle
      expect([connections.n, wakes.n]).toEqual([2, 2])
    })
)
