/**
 * The FoldKit adapter (DESIGN §16.17) over the real tab-side semantics (`LeaderClient.tabApi`, DESIGN §9.1),
 * with the leader calls faked. FoldKit's runtime is not needed: its contracts are plain `Stream`/`Effect`.
 */
import { expect, it } from "@effect/vitest"
import { Cause, Deferred, Effect, Exit, Fiber, Logger, References, Schema, Stream } from "effect"
import { Mutation, Query } from "echo-sync"
import { LeaderClient, LeaderRpcs } from "echo-sync/client"
import * as Echo from "./index.ts"

const n = Query.make("n", { args: Schema.Struct({}), success: Schema.Number, handler: () => Effect.succeed(0) })
const m = Mutation.make("m", { args: Schema.Struct({ v: Schema.Number }), handler: () => Effect.void })

const lost = new LeaderRpcs.LeaderUnavailable({ reason: "LeaderLost", message: "injected" })
const SECRET = "Bearer secret-token https://internal/sync"

const calls = (over: Partial<LeaderClient.LeaderCalls>): LeaderClient.LeaderCalls => ({
  enqueue: () => Effect.die("unused"),
  subscribe: () => Stream.die("unused"),
  notices: Stream.die("unused"),
  dismissNotices: () => Effect.die("unused"),
  syncStatus: Stream.die("unused"),
  requestSync: Effect.die("unused"),
  presence: () => Stream.die("unused"),
  track: () => Effect.die("unused"),
  ...over
})

const client = (c: LeaderClient.LeaderCalls): Echo.ClientSource<never, never> =>
  Effect.succeed({ ...LeaderClient.tabApi(c), status: Stream.die("unused") })

type Out<A, E> = { readonly _tag: "Value"; readonly value: A } | Echo.Failure<E>
const h = <A, E>(): Echo.StreamHandlers<A, E, Out<A, E>, Out<A, E>> => ({
  onValue: (value) => ({ _tag: "Value", value }),
  onFailure: (failure) => failure
})
const eh = <A, E>(): Echo.EffectHandlers<A, E, Out<A, E>, Out<A, E>> => ({
  onSuccess: (value) => ({ _tag: "Value", value }),
  onFailure: (failure) => failure
})

const status = (pendingMutations: number): LeaderClient.SyncStatus => ({
  push: { _tag: "Active" },
  lastError: null,
  pendingMutations,
  lastSyncedAt: null,
  phase: "idle",
  nextRetryAt: null
})

const logged: Array<unknown> = []
const recording = Logger.layer([Logger.make((options) => void logged.push(options.message))])

it.effect(
  "a defect is one carrier-free Defect message that ends the stream, logged nowhere, never hidden by an error",
  () =>
    Effect.gen(function* () {
      // Wrong implementations: a defect escapes to FoldKit (crashing the app), arrives twice, carries or logs the
      // cause, or hides behind a typed error raised alongside it.
      const sqlFailure = new LeaderRpcs.SqlFailure({ message: "x" })
      const fromSource = yield* Stream.runCollect(Echo.syncStatus(Effect.die(SECRET), h()))
      expect(fromSource).toEqual([{ _tag: "Defect" }])
      // A source whose own scope fails on release: still one message, not an escaped failure.
      const scopedSource = Effect.andThen(
        Effect.addFinalizer(() => Effect.die(SECRET)),
        Effect.fail(sqlFailure)
      )
      expect(yield* Stream.runCollect(Echo.syncStatus(scopedSource, h()))).toEqual([{ _tag: "Defect" }])
      expect(yield* Echo.requestSync(scopedSource, eh())).toEqual({ _tag: "Defect" })
      const midStream = yield* Stream.runCollect(
        Echo.subscribe(
          client(calls({ subscribe: () => Stream.concat(Stream.succeed(1), Stream.die(new Error(SECRET))) })),
          n,
          {},
          h()
        )
      )
      expect(midStream).toEqual([{ _tag: "Value", value: 1 }, { _tag: "Defect" }])
      const both = Cause.combine(Cause.fail(sqlFailure), Cause.die(SECRET))
      expect(
        yield* Stream.runCollect(
          Echo.notices(
            Effect.map(client(calls({})), (c) => ({ ...c, notices: Stream.failCause(both) })),
            h()
          )
        )
      ).toEqual([{ _tag: "Defect" }])
      const command = yield* Echo.dismissNotices(
        client(calls({ dismissNotices: () => Effect.failCause(both) })),
        [1],
        eh()
      )
      expect(command).toEqual({ _tag: "Defect" })
      const mutated = yield* Echo.mutate(client(calls({ enqueue: () => Effect.die(SECRET) })), m, { v: 1 }, eh())
      expect(mutated).toEqual({ _tag: "Defect" })
      expect(JSON.stringify([fromSource, midStream, command, mutated])).not.toContain("secret")
      expect(logged).toEqual([])
    }).pipe(Effect.provide(recording), Effect.provideService(References.MinimumLogLevel, "All"))
)

it.effect("a typed error is the stream's last message and the adapter never re-subscribes", () =>
  Effect.gen(function* () {
    let opens = 0
    const subscribe: LeaderClient.LeaderCalls["subscribe"] = () =>
      Stream.suspend(() => {
        opens++
        return Stream.concat(Stream.succeed(1), Stream.fail(new LeaderRpcs.SqlFailure({ message: "x" })))
      })
    const out = yield* Stream.runCollect(Echo.subscribe(client(calls({ subscribe })), n, {}, h()))
    expect(out).toEqual([
      { _tag: "Value", value: 1 },
      { _tag: "Error", error: new LeaderRpcs.SqlFailure({ message: "x" }) }
    ])
    expect(opens).toBe(1)
  })
)

it.live("interruption, upstream or of the adapter's own fiber, never produces a failure message", () =>
  Effect.gen(function* () {
    // Wrong implementation: interruption read as a defect, so FoldKit's teardown dispatches a spurious failure.
    const seen: Array<Out<number, unknown>> = []
    const selfInterrupted = yield* Effect.exit(
      Stream.runForEach(
        Echo.subscribe(
          client(calls({ subscribe: () => Stream.concat(Stream.succeed(1), Stream.fromEffect(Effect.interrupt)) })),
          n,
          {},
          h()
        ),
        (o) => Effect.sync(() => void seen.push(o))
      )
    )
    expect(Exit.hasInterrupts(selfInterrupted)).toBe(true)
    expect(seen).toEqual([{ _tag: "Value", value: 1 }])

    const first = yield* Deferred.make<void>()
    const running = yield* Effect.forkChild(
      Stream.runForEach(
        Echo.subscribe(client(calls({ subscribe: () => Stream.concat(Stream.succeed(2), Stream.never) })), n, {}, h()),
        (o) =>
          Effect.andThen(
            Effect.sync(() => void seen.push(o)),
            Deferred.succeed(first, undefined)
          )
      )
    )
    yield* Deferred.await(first)
    yield* Fiber.interrupt(running)
    let failures = 0
    const command = yield* Effect.forkChild(
      Echo.mutate(
        client(calls({ enqueue: () => Effect.never })),
        m,
        { v: 1 },
        {
          onSuccess: () => "saved",
          onFailure: () => {
            failures++
            return "failed"
          }
        }
      )
    )
    yield* Effect.sleep("20 millis")
    yield* Fiber.interrupt(command)
    expect(seen).toEqual([
      { _tag: "Value", value: 1 },
      { _tag: "Value", value: 2 }
    ])
    expect(failures).toBe(0)
  })
)

it.effect("identical consecutive emissions are each delivered, unchanged (no dedup, no derived state)", () =>
  Effect.gen(function* () {
    const a = status(1)
    const out = yield* Stream.runCollect(
      Echo.syncStatus(client(calls({ syncStatus: Stream.make(a, a, status(0)) })), h())
    )
    expect(out).toEqual([
      { _tag: "Value", value: a },
      { _tag: "Value", value: a },
      { _tag: "Value", value: status(0) }
    ])
  })
)

it.live("a subscription keeps its last emission across LeaderLost: no failure message, then the fresh one", () =>
  Effect.gen(function* () {
    let opens = 0
    const subscribe: LeaderClient.LeaderCalls["subscribe"] = () =>
      Stream.suspend(() => {
        opens++
        return opens === 1 ? Stream.concat(Stream.succeed(1), Stream.fail(lost)) : Stream.succeed(2)
      })
    const out = yield* Stream.runCollect(Echo.subscribe(client(calls({ subscribe })), n, {}, h()))
    expect(out).toEqual([
      { _tag: "Value", value: 1 },
      { _tag: "Value", value: 2 }
    ])
  })
)

it.live("mutate's message exists only after the leader's commit, and only LeaderLost is retried, by the client", () =>
  Effect.gen(function* () {
    const commit = yield* Deferred.make<LeaderRpcs.Enqueued>()
    const ids: Array<string> = []
    const enqueue: LeaderClient.LeaderCalls["enqueue"] = (envelope) =>
      Effect.suspend(() => {
        ids.push(envelope.mutationId)
        return ids.length === 1 ? Effect.fail(lost) : Deferred.await(commit)
      })
    const running = yield* Effect.forkChild(Echo.mutate(client(calls({ enqueue })), m, { v: 1 }, eh()))
    yield* Effect.sleep("150 millis")
    // Enqueued (twice, across the LeaderLost) but not committed: no message yet.
    expect(ids.length).toBe(2)
    expect(new Set(ids).size).toBe(1)
    expect(running.pollUnsafe()).toBeUndefined()
    const enqueued = { clientId: "c", mutationId: ids[0] ?? "", seq: 7 }
    yield* Deferred.succeed(commit, enqueued)
    expect(yield* Fiber.join(running)).toEqual({ _tag: "Value", value: enqueued })

    // Any other error is delivered after one attempt: the adapter adds no retry of its own.
    let attempts = 0
    const failed = yield* Echo.mutate(
      client(
        calls({
          enqueue: () =>
            Effect.suspend(() => {
              attempts++
              return Effect.fail(new LeaderRpcs.LeaderUnavailable({ reason: "LeaderFailed", message: "x" }))
            })
        })
      ),
      m,
      { v: 1 },
      eh()
    )
    expect(failed._tag).toBe("Error")
    expect(attempts).toBe(1)
  })
)
