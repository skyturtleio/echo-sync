import { expect, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { TestClock } from "effect/testing"
import type * as Engine from "./Engine.ts"
import * as SyncLoop from "./SyncLoop.ts"

class Malformed extends Schema.TaggedError<Malformed>()("MalformedEnvelope", { message: Schema.String }) {}
class Offline extends Schema.TaggedError<Offline>()("Offline", {}) {}

/** Fake engine: `pushFails` decides each push attempt's outcome; counts calls. */
const fakeEngine = (pushFails: (attempt: number) => Malformed | Offline | undefined) => {
  const calls = { push: 0, pull: 0 }
  const engine: Engine.Engine<never, Malformed | Offline> = {
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
    const { engine, calls } = fakeEngine(() => new Malformed({ message: "bad envelope" }))
    const loop = yield* SyncLoop.make({ engine, isFatalPush: (e) => e._tag === "MalformedEnvelope" })
    yield* Effect.forkChild(loop.run)
    yield* settle
    expect((yield* loop.status).push).toEqual({ _tag: "Halted", message: "MalformedEnvelope: bad envelope" })
    // Neither waking nor time passing retries the push; pull still runs on every cycle.
    for (let i = 0; i < 3; i++) {
      yield* loop.wake
      yield* settle
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
    const loop = yield* SyncLoop.make({ engine, isFatalPush: (e) => e._tag === "MalformedEnvelope" })
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
