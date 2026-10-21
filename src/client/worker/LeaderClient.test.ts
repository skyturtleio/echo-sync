import { expect, it } from "@effect/vitest"
import { Deferred, Effect, Layer, Stream } from "effect"
import { TestClock } from "effect/testing"
import { AccessScope } from "echo-sync"
import type * as Rpc from "effect/rpc"
import { afterEach, vi } from "vitest"
import { layer, LeaderClient, type TabApiOptions } from "./LeaderClient.ts"
import type * as Platform from "./platform.ts"

// Only the browser/worker transport is replaced: the public layer builds the tab API and sender.
const remote = vi.hoisted(() => ({
  track: vi.fn<() => Effect.Effect<void>>(),
  presence: vi.fn<() => Stream.Stream<{ room: AccessScope.AccessScopeId; entries: [] }>>()
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
    tabCapability: () => ({ sharedWorker: false }),
    holdLock: () => effect.Effect.void,
    awaitLockReleased: () => effect.Effect.never
  }
})

afterEach(() => vi.unstubAllGlobals())

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
