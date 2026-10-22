import { expect, it } from "@effect/vitest"
import { Cause, Context, Deferred, Effect, Exit, Layer, Option, Scope, Stream } from "effect"
import { TestClock } from "effect/testing"
import { AccessScope } from "echo-sync"
import type * as Rpc from "effect/rpc"
import { RpcClientDefect, RpcClientError } from "effect/rpc/RpcClientError"
import { afterEach, vi } from "vitest"
import { layer, LeaderClient, type TabApiOptions } from "./LeaderClient.ts"
import { LeaderUnavailable } from "./LeaderRpcs.ts"
import type * as Platform from "./platform.ts"

// Only the browser/worker transport is replaced: the public layer builds the tab API and sender.
const remote = vi.hoisted(() => ({
  shared: false,
  held: vi.fn<() => Effect.Effect<boolean>>(),
  /** The tab lock; by default held for nothing and released at once. */
  holdLock: vi.fn<() => Effect.Effect<void, never, Scope.Scope>>(),
  track: vi.fn<() => Effect.Effect<void>>(),
  presence: vi.fn<() => Stream.Stream<{ room: AccessScope.AccessScopeId; entries: [] }>>(),
  notices: vi.fn<() => Stream.Stream<unknown, unknown>>(),
  syncStatus: vi.fn<() => Stream.Stream<unknown, unknown>>()
}))
vi.mock("effect/rpc", async (original) => {
  const actual = await original<typeof Rpc>()
  const effect = await import("effect")
  return {
    ...actual,
    RpcClient: {
      ...actual.RpcClient,
      make: () => effect.Effect.succeed(remote),
      layerProtocolWorker: () => effect.Layer.empty
    }
  }
})
vi.mock("./platform.ts", async (original) => {
  const actual = await original<typeof Platform>()
  const effect = await import("effect")
  return {
    ...actual,
    tabCapability: () => ({ sharedWorker: remote.shared }),
    isLockHeld: () => remote.held(),
    holdLock: () => (remote.holdLock.getMockImplementation() === undefined ? effect.Effect.void : remote.holdLock()),
    awaitLockReleased: () => effect.Effect.never
  }
})

afterEach(() => {
  remote.shared = false
  remote.holdLock.mockReset()
  vi.unstubAllGlobals()
})

it.effect(
  "the layer must honor the configured presence deadline and retry delay without shortening the 45 s default",
  () =>
    Effect.gen(function* () {
      vi.stubGlobal("navigator", { storage: { persist: () => Promise.resolve(false) } })
      const room = AccessScope.AccessScopeId.make("room")
      for (const tuning of [
        { presenceSendTimeout: "100 millis", presenceRetryDelay: "20 millis" },
        {}
      ] satisfies Array<TabApiOptions>) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            let attempts = 0
            const started = yield* Deferred.make<void>()
            const joined = yield* Deferred.make<void>()
            remote.track.mockImplementation(() =>
              Effect.andThen(
                Effect.sync(() => {
                  attempts++
                }),
                Effect.andThen(Deferred.succeed(started, undefined), Effect.never)
              )
            )
            remote.presence.mockImplementation(() => Stream.concat(Stream.succeed({ room, entries: [] }), Stream.never))
            const worker = new EventTarget()
            // Platform edge: this fake dedicated worker only announces election; RPCs above model a hung leader.
            const spawnLeader = (): Worker =>
              Object.assign(worker, {
                onmessage: null,
                onmessageerror: null,
                onerror: null,
                postMessage: (message: { _tag: string }) => {
                  if (message._tag === "Init")
                    worker.dispatchEvent(
                      new MessageEvent("message", {
                        data: { _tag: "Elected", leaderId: "leader" }
                      })
                    )
                },
                terminate: () => {}
              })
            const context = yield* Layer.build(
              layer({
                appId: "deadline",
                profile: "test",
                principal: "me",
                spawnLeader,
                spawnRouter: () => {
                  throw new Error("single-tab")
                },
                ...tuning
              })
            )
            const client = yield* LeaderClient.pipe(Effect.provide(context))
            yield* client.presence(room, "initial").pipe(
              Stream.tap(() => Deferred.succeed(joined, undefined)),
              Stream.runDrain,
              Effect.forkScoped
            )
            yield* Deferred.await(joined)
            yield* client.track(room, "latest")
            yield* Deferred.await(started)
            const deadline = "presenceSendTimeout" in tuning ? 100 : 45_000
            const retry = "presenceRetryDelay" in tuning ? 20 : 1_000
            yield* TestClock.adjust(deadline - 1)
            expect(attempts).toBe(1)
            yield* TestClock.adjust(retry)
            expect(attempts).toBe(1)
            yield* TestClock.adjust(1)
            expect(attempts).toBe(2)
          })
        )
      }
    })
)

it.live("closing a client cannot abandon a transferred port queued for acceptance or waiting for its epoch check", () =>
  Effect.gen(function* () {
    vi.stubGlobal("navigator", { storage: { persist: () => Promise.resolve(false) } })
    remote.shared = true
    for (const waitForQuery of [false, true]) {
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const queried = yield* Deferred.make<void>()
      let queries = 0
      remote.held.mockImplementation(() =>
        Effect.gen(function* () {
          queries++
          yield* Deferred.succeed(queried, undefined)
          return yield* Effect.never
        })
      )
      const control = new MessageChannel()
      const transferred = new MessageChannel()
      const closed = yield* Deferred.make<void>()
      transferred.port2.addEventListener("close", () => Deferred.doneUnsafe(closed, Effect.void))
      const worker = Object.assign(new EventTarget(), {
        onmessage: null,
        onmessageerror: null,
        onerror: null,
        postMessage() {},
        terminate() {}
      })
      const shared = Object.assign(new EventTarget(), { port: control.port1, onerror: null })
      yield* Layer.buildWithScope(
        layer({
          appId: "ports",
          profile: "pending",
          principal: "me",
          spawnLeader: () => worker,
          spawnRouter: () => shared
        }),
        scope
      )
      control.port1.dispatchEvent(
        new MessageEvent("message", {
          data: {
            _tag: "Connected",
            leaderId: "epoch",
            port: transferred.port1
          }
        })
      )
      if (waitForQuery) yield* Deferred.await(queried)
      yield* Scope.close(scope, Exit.void)
      yield* Deferred.await(closed).pipe(Effect.timeout("1 second"))
      expect(queries).toBe(waitForQuery ? 1 : 0)
      control.port2.close()
      transferred.port2.close()
    }
  })
)

it.live("closing the client's scope while a reset is releasing its locks returns only once they are released", () =>
  Effect.gen(function* () {
    vi.stubGlobal("navigator", { storage: { persist: () => Promise.resolve(false) } })
    // The tab lock's release suspends until `gate` opens, so the reset's close of the client's topology is in
    // progress (already detached from the client's scope) when the client's scope is closed.
    const releasing = yield* Deferred.make<void>()
    const gate = yield* Deferred.make<void>()
    let released = false
    remote.holdLock.mockImplementation(() =>
      Effect.addFinalizer(() =>
        Deferred.succeed(releasing, undefined).pipe(
          Effect.andThen(Deferred.await(gate)),
          Effect.andThen(Effect.sync(() => (released = true)))
        )
      )
    )
    const worker = Object.assign(new EventTarget(), {
      onmessage: null,
      onmessageerror: null,
      onerror: null,
      postMessage() {},
      terminate() {}
    })
    const scope = yield* Scope.make()
    const context = yield* Layer.buildWithScope(
      layer({
        appId: "reset",
        profile: "join",
        principal: "me",
        spawnLeader: () => worker,
        spawnRouter: () => {
          throw new Error("single-tab")
        }
      }),
      scope
    )
    const client = Context.get(context, LeaderClient)
    const channel = new BroadcastChannel("echo:reset:join:reset")
    // oxlint-disable-next-line unicorn/require-post-message-target-origin -- BroadcastChannel has no target origin
    channel.postMessage({ _tag: "ResetSignal", appId: "reset", profile: "join" })
    channel.close()
    yield* Deferred.await(releasing).pipe(Effect.timeout("2 seconds"))
    setTimeout(() => Deferred.doneUnsafe(gate, Effect.void), 50)
    yield* Scope.close(scope, Exit.void)
    expect(released).toBe(true)
    expect((yield* Stream.runHead(client.status)).pipe(Option.map((s) => s.reset))).toEqual(Option.some(true))
  })
)

// Platform edge: a fake dedicated worker that only announces election; the RPCs are `remote`'s.
const electedWorker = (): Worker => {
  const worker = new EventTarget()
  return Object.assign(worker, {
    onmessage: null,
    onmessageerror: null,
    onerror: null,
    postMessage: (message: { _tag: string }) => {
      if (message._tag === "Init")
        worker.dispatchEvent(new MessageEvent("message", { data: { _tag: "Elected", leaderId: "leader" } }))
    },
    terminate: () => {}
  })
}

const typedFailure = new Error("typed leader failure")
const lostTransport = new RpcClientError({ reason: new RpcClientDefect({ message: "port closed", cause: null }) })
const label = (u: unknown) =>
  u instanceof LeaderUnavailable && u.reason === "LeaderLost" ? "LeaderLost" : u === typedFailure ? "X" : "other"

it.live(
  "a leader stream's defect must reach the tab, and only a defect-free all-LeaderLost cause may resubscribe",
  () =>
    Effect.gen(function* () {
      vi.stubGlobal("navigator", { storage: { persist: () => Promise.resolve(false) } })
      const fresh = [{ fresh: true }]
      const scenarios = [
        // DESIGN §16.23: Fail(X)+Die must not collapse to Fail(X) on its way to the consumer.
        { first: Cause.combine(Cause.fail(typedFailure), Cause.die("bug")), opens: 1, fails: ["X"], dies: ["bug"] },
        // A plain LeaderLost resubscribes: the consumer gets the next leader's emission and no error.
        { first: Cause.fail(lostTransport), opens: 2, fails: undefined, dies: [] },
        // LeaderLost with a defect is a leader that died of a bug: end with both, never resubscribe silently.
        {
          first: Cause.combine(Cause.fail(lostTransport), Cause.die("bug")),
          opens: 1,
          fails: ["LeaderLost"],
          dies: ["bug"]
        },
        // Recovery needs every failure to be LeaderLost, not the first one: X beside it ends the stream.
        {
          first: Cause.combine(Cause.fail(lostTransport), Cause.fail(typedFailure)),
          opens: 1,
          fails: ["LeaderLost", "X"],
          dies: []
        }
      ] as const
      for (const stream of ["notices", "syncStatus"] as const) {
        for (const scenario of scenarios) {
          yield* Effect.scoped(
            Effect.gen(function* () {
              let opens = 0
              remote[stream].mockImplementation(() =>
                Stream.suspend(() => (++opens === 1 ? Stream.failCause(scenario.first) : Stream.make(fresh)))
              )
              const context = yield* Layer.build(
                layer({
                  appId: "cause",
                  profile: "test",
                  principal: "me",
                  spawnLeader: electedWorker,
                  spawnRouter: () => {
                    throw new Error("single-tab")
                  }
                })
              )
              const client = yield* LeaderClient.pipe(Effect.provide(context))
              const open: Stream.Stream<unknown, unknown> = stream === "notices" ? client.notices : client.syncStatus
              const exit = yield* open.pipe(Stream.take(1), Stream.runCollect, Effect.timeout("5 seconds"), Effect.exit)
              expect(opens).toBe(scenario.opens)
              if (scenario.fails === undefined) return expect(exit).toEqual(Exit.succeed([fresh]))
              if (Exit.isSuccess(exit)) return expect.fail(`${stream} ended without the leader's failure`)
              expect(exit.cause.reasons.filter(Cause.isFailReason).map((r) => label(r.error))).toEqual(scenario.fails)
              expect(exit.cause.reasons.filter(Cause.isDieReason).map((r) => r.defect)).toEqual(scenario.dies)
            })
          )
        }
      }
    })
)
