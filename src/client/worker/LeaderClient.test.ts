import { expect, it } from "@effect/vitest"
import { Cause, Context, Deferred, Effect, Exit, Layer, Option, Scope, Stream } from "effect"
import { TestClock } from "effect/testing"
import { AccessScope } from "echo-sync"
import { RpcClientDefect, RpcClientError } from "effect/rpc/RpcClientError"
import { layer, LeaderClient, type TabApiOptions } from "./LeaderClient.ts"
import { LeaderUnavailable, SqlFailure, SyncDisabled } from "./LeaderRpcs.ts"
import { type LeaderConnection, TabPlatform } from "./TabPlatform.ts"
import type { SyncStatus } from "../sync/SyncLoop.ts"

// Only the browser is replaced (`TabPlatform`): the public layer builds the tab API and sender.
const unused: LeaderConnection = {
  exec: () => Effect.die("exec not expected"),
  enqueue: () => Effect.die("enqueue not expected"),
  subscribe: () => Stream.die("subscribe not expected"),
  subscribeSnapshot: () => Stream.die("subscribeSnapshot not expected"),
  subscribeRaw: () => Stream.die("subscribeRaw not expected"),
  leaderInfo: () => Effect.die("leaderInfo not expected"),
  syncStatus: () => Stream.die("syncStatus not expected"),
  requestSync: () => Effect.die("requestSync not expected"),
  presence: () => Stream.die("presence not expected"),
  track: () => Effect.die("track not expected"),
  notices: () => Stream.die("notices not expected"),
  dismissNotices: () => Effect.die("dismissNotices not expected"),
  exportPending: () => Effect.die("exportPending not expected")
}

/** A browser whose leader answers with `leader`; lock holds and checks are the test's. */
const browser = (options: {
  readonly leader?: Partial<LeaderConnection>
  readonly sharedWorker?: boolean
  /** The tab lock; by default held for nothing and released at once. */
  readonly holdLock?: Effect.Effect<void, never, Scope.Scope>
  readonly isLockHeld?: Effect.Effect<boolean>
}): TabPlatform => ({
  capability: () => ({ unsupported: undefined, sharedWorker: options.sharedWorker ?? false }),
  holdLock: () => options.holdLock ?? Effect.void,
  isLockHeld: () => options.isLockHeld ?? Effect.succeed(true),
  awaitLockReleased: () => Effect.never,
  persistStorage: Effect.void,
  connect: () => Effect.succeed({ ...unused, ...options.leader })
})

it.effect(
  "the layer must honor the configured presence deadline and retry delay without shortening the 45 s default",
  () =>
    Effect.gen(function* () {
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
            const leader: Partial<LeaderConnection> = {
              track: () =>
                Effect.andThen(
                  Effect.sync(() => {
                    attempts++
                  }),
                  Effect.andThen(Deferred.succeed(started, undefined), Effect.never)
                ),
              presence: () =>
                Stream.concat(Stream.succeed({ _tag: "snapshot" as const, room, entries: [] }), Stream.never)
            }
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
            ).pipe(Effect.provideService(TabPlatform, browser({ leader })))
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
    for (const waitForQuery of [false, true]) {
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const queried = yield* Deferred.make<void>()
      let queries = 0
      const isLockHeld = Effect.gen(function* () {
        queries++
        yield* Deferred.succeed(queried, undefined)
        return yield* Effect.never
      })
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
      ).pipe(Effect.provideService(TabPlatform, browser({ sharedWorker: true, isLockHeld })))
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
    // The tab lock's release suspends until `gate` opens, so the reset's close of the client's topology is in
    // progress (already detached from the client's scope) when the client's scope is closed.
    const releasing = yield* Deferred.make<void>()
    const gate = yield* Deferred.make<void>()
    let released = false
    const holdLock = Effect.addFinalizer(() =>
      Deferred.succeed(releasing, undefined).pipe(
        Effect.andThen(Deferred.await(gate)),
        Effect.andThen(Effect.sync(() => (released = true)))
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
    ).pipe(Effect.provideService(TabPlatform, browser({ holdLock })))
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

// Platform edge: a fake dedicated worker that only announces election; the RPCs are the `TabPlatform`'s.
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

const lostTransport = new RpcClientError({ reason: new RpcClientDefect({ message: "port closed", cause: null }) })
const idle: SyncStatus = {
  push: { _tag: "Active" },
  lastError: null,
  pendingMutations: 0,
  lastSyncedAt: null,
  phase: "idle",
  nextRetryAt: null
}

/** Each stream with a typed failure of its own error union and the value a fresh leader emits. */
const check = <X, A>(
  stream: "notices" | "syncStatus",
  typed: X,
  fresh: A,
  leader: (open: () => Stream.Stream<A, X | RpcClientError>) => Partial<LeaderConnection>
) =>
  Effect.gen(function* () {
    const label = (u: unknown) =>
      u instanceof LeaderUnavailable && u.reason === "LeaderLost" ? "LeaderLost" : u === typed ? "X" : "other"
    const scenarios: ReadonlyArray<{
      readonly first: Cause.Cause<X | RpcClientError>
      readonly opens: number
      readonly fails: ReadonlyArray<string> | undefined
      readonly dies: ReadonlyArray<string>
    }> = [
      // INV-BRW-4: Fail(X)+Die must not collapse to Fail(X) on its way to the consumer.
      { first: Cause.combine(Cause.fail(typed), Cause.die("bug")), opens: 1, fails: ["X"], dies: ["bug"] },
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
        first: Cause.combine(Cause.fail(lostTransport), Cause.fail(typed)),
        opens: 1,
        fails: ["LeaderLost", "X"],
        dies: []
      }
    ]
    for (const scenario of scenarios) {
      yield* Effect.scoped(
        Effect.gen(function* () {
          let opens = 0
          const open = () =>
            Stream.suspend(() => (++opens === 1 ? Stream.failCause(scenario.first) : Stream.make(fresh)))
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
          ).pipe(Effect.provideService(TabPlatform, browser({ leader: leader(open) })))
          const client = yield* LeaderClient.pipe(Effect.provide(context))
          const tab: Stream.Stream<unknown, unknown> = stream === "notices" ? client.notices : client.syncStatus
          const exit = yield* tab.pipe(Stream.take(1), Stream.runCollect, Effect.timeout("5 seconds"), Effect.exit)
          expect(opens).toBe(scenario.opens)
          if (scenario.fails === undefined) return expect(exit).toEqual(Exit.succeed([fresh]))
          if (Exit.isSuccess(exit)) return expect.fail(`${stream} ended without the leader's failure`)
          expect(exit.cause.reasons.filter(Cause.isFailReason).map((r) => label(r.error))).toEqual(scenario.fails)
          expect(exit.cause.reasons.filter(Cause.isDieReason).map((r) => r.defect)).toEqual(scenario.dies)
        })
      )
    }
  })

it.live(
  "a leader stream's defect must reach the tab, and only a defect-free all-LeaderLost cause may resubscribe",
  () =>
    Effect.gen(function* () {
      yield* check("notices", new SqlFailure({ message: "typed leader failure" }), [], (open) => ({ notices: open }))
      yield* check("syncStatus", new SyncDisabled(), idle, (open) => ({ syncStatus: open }))
    })
)
