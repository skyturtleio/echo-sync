import { expect, it } from "@effect/vitest"
import { Cause, Effect, Schema } from "effect"
import { RpcClientError } from "effect/rpc"
import { TestClock } from "effect/testing"
import { Ids, Protocol } from "echo-sync"
import type * as Engine from "./Engine.ts"
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
