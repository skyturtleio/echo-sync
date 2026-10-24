import { assert, describe, it } from "@effect/vitest"
import type * as PgClient from "@effect/sql-pg/PgClient"
import { Clock, Deferred, Effect, Exit, Fiber, Layer, Option, Queue, Scheduler, Stream } from "effect"
import * as TestClock from "effect/testing/TestClock"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as HttpRouter from "effect/http/HttpRouter"
import { RpcClient, RpcSerialization } from "effect/rpc"
import type { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Protocol } from "echo-sync"
import * as Presence from "./presence.ts"
import { layerHttp } from "./rpc.ts"
import { grant, pgAvailable, registry, revoke, TestDatabase } from "./test-support.ts"

const principalHeader = "x-test-principal"

/**
 * The server's real HTTP mount (SyncRpcs and PresenceRpcs on one path) over the test database,
 * and a factory of presence clients that authenticate as a given principal.
 */
const server = Effect.gen(function* () {
  const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
  const app = layerHttp({
    registry,
    mutations: Mutation.makeRegistry([]),
    principalFor: (headers) => Effect.succeed(AccessScope.PrincipalId.make(headers[principalHeader] ?? "nobody"))
  }).pipe(Layer.provide(Layer.succeedContext(db)))
  const web = HttpRouter.toWebHandler(app, { disableLogger: true })
  yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
  return (principal: string) => {
    const fetch: typeof globalThis.fetch = Object.assign(
      (input: RequestInfo | URL, init?: RequestInit) => {
        const request = new Request(input, init)
        request.headers.set(principalHeader, principal)
        return web.handler(request)
      },
      { preconnect: globalThis.fetch.preconnect }
    )
    return RpcClient.make(Protocol.PresenceRpcs).pipe(
      Effect.provide(
        RpcClient.layerProtocolHttp({ url: "http://echo.test/sync" }).pipe(
          Layer.provide([
            RpcSerialization.layerNdjson,
            FetchHttpClient.layer.pipe(Layer.provide(Layer.succeed(FetchHttpClient.Fetch)(fetch)))
          ])
        )
      )
    )
  }
})

type PresenceClient = Effect.Success<ReturnType<Effect.Success<typeof server>>>

const s1 = AccessScope.AccessScopeId.make("s1")
const payload = (clientId: string, tabId: string, state: Protocol.PresenceEntry["state"] = null, revision = 0) => ({
  clientId: Ids.ClientId.make(clientId),
  room: s1,
  tabId: Protocol.TabId.make(tabId),
  state,
  revision
})

const settle = Effect.repeat(Effect.yieldNow, { times: 50 })

const entry = (tabId: string) => ({ principalId: AccessScope.PrincipalId.make("p"), ...payload("c1", tabId) })

/** Joins and records every snapshot; `exit` resolves with how the stream ended. */
const open = (client: PresenceClient, p: ReturnType<typeof payload>) =>
  Effect.gen(function* () {
    const snapshots = yield* Queue.unbounded<Protocol.RoomSnapshot>()
    const seen: Array<ReadonlyArray<string>> = []
    const fiber = yield* Effect.forkChild(
      Effect.exit(
        Stream.runForEach(client.room(p), (element) =>
          element._tag === "snapshot"
            ? Effect.andThen(
                Effect.sync(() => void seen.push(show(element))),
                Queue.offer(snapshots, element)
              )
            : Effect.void
        )
      )
    )
    return { snapshots, seen, fiber }
  })

/** The room as a list of `principal/client/tab=state`, which is what every assertion compares. */
const show = (element: Protocol.RoomElement) =>
  element._tag === "heartbeat"
    ? ["(heartbeat)"]
    : element.entries.map((e) => `${e.principalId}/${e.clientId}/${e.tabId}=${JSON.stringify(e.state)}`)

/** A stream's typed failure as JSON; `expected` is the one a revoked or ungranted stream ends with. */
const forbidden = (exit: Exit.Exit<unknown, unknown>) =>
  Exit.isFailure(exit) ? Option.map(Exit.findErrorOption(exit), (e) => JSON.stringify(e)) : Option.none()
/** A request's typed failure tag, if it failed. */
const tagOf = (exit: Exit.Exit<unknown, { readonly _tag: string }>) =>
  Exit.isFailure(exit) ? Option.map(Exit.findErrorOption(exit), (e) => e._tag) : Option.none()
const expected = Option.some(JSON.stringify(new Protocol.Forbidden({ room: s1 })))

/** Takes snapshots until one shows exactly `want`; fails with the last one seen after 5 s. */
const until = (snapshots: Queue.Queue<Protocol.RoomSnapshot>, want: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    let last: ReadonlyArray<string> = []
    const found = yield* Effect.timeoutOption(
      Effect.gen(function* () {
        while (true) {
          last = show(yield* Queue.take(snapshots))
          if (JSON.stringify(last) === JSON.stringify(want)) return
        }
      }),
      "5 seconds"
    )
    if (Option.isNone(found)) {
      return assert.fail(`wanted ${JSON.stringify(want)}, last snapshot ${JSON.stringify(last)}`)
    }
  })

describe("presence state (SPEC §10)", () => {
  it.effect(
    "a presence entry lives exactly while its stream runs: completion, a failing consumer and interruption each remove it",
    () =>
      Effect.gen(function* () {
        const presence = yield* Presence.make({ heartbeatInterval: "1 hour" })
        const room = Effect.map(presence.snapshot(s1), show)

        // Completion: the consumer takes the join snapshot and stops.
        const [first] = yield* Stream.runCollect(Stream.take(presence.join(entry("done")), 1))
        assert.deepStrictEqual(first === undefined ? [] : show(first), ["p/c1/done=null"])
        assert.deepStrictEqual(yield* room, [])

        // Failure: the consumer fails on the join snapshot.
        const failed = yield* Effect.exit(Stream.runForEach(presence.join(entry("fails")), () => Effect.fail("boom")))
        assert.isTrue(Exit.isFailure(failed))
        assert.deepStrictEqual(yield* room, [])

        // Interruption: joined (seen in the room), then interrupted.
        const joined = yield* Queue.unbounded<void>()
        const fiber = yield* Effect.forkChild(
          Stream.runForEach(presence.join(entry("open")), () => Queue.offer(joined, undefined))
        )
        yield* Queue.take(joined)
        assert.deepStrictEqual(yield* room, ["p/c1/open=null"])
        yield* Fiber.interrupt(fiber)
        assert.deepStrictEqual(yield* room, [])
      })
  )

  it.effect(
    "a same-key rejoin ends the replaced stream, so it is never left observing an abandoned room after the room is recreated",
    () =>
      Effect.gen(function* () {
        const presence = yield* Presence.make({ heartbeatInterval: "1 hour" })
        const room = Effect.map(presence.snapshot(s1), show)
        const watch = (tabId: string) =>
          Effect.gen(function* () {
            const seen: Array<ReadonlyArray<string>> = []
            const ready = yield* Queue.unbounded<void>()
            const fiber = yield* Effect.forkChild(
              Stream.runForEach(presence.join(entry(tabId)), (snapshot) =>
                Effect.andThen(
                  Effect.sync(() => void seen.push(show(snapshot))),
                  Queue.offer(ready, undefined)
                )
              )
            )
            yield* Queue.take(ready)
            return { seen, fiber }
          })
        const a = yield* watch("k") // A holds key K
        const b = yield* watch("k") // B rejoins K: replaces A's entry
        yield* settle
        assert.isDefined(a.fiber.pollUnsafe(), "the replaced stream has ended")
        assert.isTrue(Exit.isSuccess(yield* Fiber.await(a.fiber)))
        // B leaves as the last member: the room is gone. C joins: a new room.
        yield* Fiber.interrupt(b.fiber)
        assert.deepStrictEqual(yield* room, [])
        const c = yield* watch("c")
        assert.deepStrictEqual(yield* room, ["p/c1/c=null"])
        assert.deepStrictEqual(c.seen, [["p/c1/c=null"]])
      })
  )

  it.effect(
    "an idle room stream carries a heartbeat every heartbeatInterval, never silent longer than one interval, and a heartbeat changes no room state",
    () =>
      Effect.gen(function* () {
        const presence = yield* Presence.make({ heartbeatInterval: "15 seconds" })
        const seen: Array<string> = []
        yield* Effect.forkChild(
          Stream.runForEach(presence.join(entry("idle")), (element) =>
            Effect.flatMap(Clock.currentTimeMillis, (at) =>
              Effect.sync(() => void seen.push(`${at} ${show(element).join(",")}`))
            )
          )
        )
        yield* settle
        assert.deepStrictEqual(seen, ["0 p/c1/idle=null"])
        for (let i = 1; i <= 4; i++) {
          yield* TestClock.adjust("14999 millis")
          yield* settle
          assert.strictEqual(seen.length, i, `silent until the interval ends (${i})`)
          yield* TestClock.adjust("1 millis")
          yield* settle
        }
        assert.deepStrictEqual(seen, [
          "0 p/c1/idle=null",
          "15000 (heartbeat)",
          "30000 (heartbeat)",
          "45000 (heartbeat)",
          "60000 (heartbeat)"
        ])
        // The room is exactly the join's: no heartbeat added, removed or changed an entry.
        assert.deepStrictEqual(show(yield* presence.snapshot(s1)), ["p/c1/idle=null"])
      })
  )

  it.effect("the first element of every room stream is the join snapshot, even when a heartbeat is already due", () =>
    Effect.gen(function* () {
      // Interval 0: a heartbeat is due at once, so only sequencing (not timing) puts the snapshot first.
      const presence = yield* Presence.make({ heartbeatInterval: 0 })
      // Budgets 1 and 2 livelock a forked fiber in the pinned runtime (effect 4.0.5).
      for (const ops of [3, 5, 10, 2048]) {
        const first = yield* Stream.runCollect(Stream.take(presence.join(entry(`first-${ops}`)), 3)).pipe(
          Effect.provideService(Scheduler.MaxOpsBeforeYield, ops)
        )
        assert.strictEqual(
          first[0]?._tag,
          "snapshot",
          `MaxOpsBeforeYield ${ops}: ${first.map((e) => e._tag).join(",")}`
        )
      }
    })
  )

  it.effect(
    "a change pending when a heartbeat arrives, or arriving while a heartbeat is pending, is still delivered as a snapshot: a heartbeat never displaces a change",
    () =>
      Effect.gen(function* () {
        // A consumer that stops pulling after the join snapshot until released, so later elements
        // wait behind it; once released and quiet, its last snapshot must show the room's state.
        const slowWatch = Effect.gen(function* () {
          const presence = yield* Presence.make({ heartbeatInterval: "15 seconds" })
          const seen: Array<string> = []
          const release = yield* Deferred.make<void>()
          yield* Effect.forkChild(
            Stream.runForEach(presence.join(entry("slow")), (element) =>
              Effect.andThen(
                Effect.sync(() => void seen.push(show(element).join(","))),
                Effect.suspend(() => (seen.length === 1 ? Deferred.await(release) : Effect.void))
              )
            )
          )
          yield* settle
          const lastSnapshot = Effect.gen(function* () {
            assert.deepStrictEqual(seen, ["p/c1/slow=null"], "the consumer is still blocked")
            yield* Deferred.succeed(release, undefined)
            yield* settle
            return seen.filter((e) => e !== "(heartbeat)").at(-1)
          })
          return { presence, seen, lastSnapshot }
        })

        // Changes first, then a heartbeat while the last change is still undelivered. Two changes, so
        // the last one is still pending even behind a stream stage that reads one element ahead.
        const a = yield* slowWatch
        yield* a.presence.track({ ...entry("slow"), state: "earlier", revision: 1 })
        yield* settle
        yield* a.presence.track({ ...entry("slow"), state: "changed", revision: 2 })
        yield* settle
        yield* TestClock.adjust("15 seconds")
        yield* settle
        assert.strictEqual(yield* a.lastSnapshot, 'p/c1/slow="changed"')
        assert.include(a.seen, "(heartbeat)")

        // A heartbeat first, then the change while the heartbeat is still undelivered.
        const b = yield* slowWatch
        yield* TestClock.adjust("15 seconds")
        yield* settle
        yield* b.presence.track({ ...entry("slow"), state: "changed", revision: 1 })
        yield* settle
        assert.strictEqual(yield* b.lastSnapshot, 'p/c1/slow="changed"')
        assert.include(b.seen, "(heartbeat)")
      })
  )

  it.effect(
    "a track delayed before it applies cannot overwrite a newer one: s1 held, s2 applied, s1 released, the room shows s2",
    () =>
      Effect.gen(function* () {
        const presence = yield* Presence.make({ heartbeatInterval: "1 hour" })
        const state = Effect.map(presence.snapshot(s1), (r) => r.entries[0]?.state)
        yield* Effect.forkChild(Stream.runDrain(presence.join(entry("t"))))
        yield* settle
        // s1's request is held before the server touches the entry (a slow network, a cancelled fetch).
        const gate = yield* Deferred.make<void>()
        const late = yield* Effect.forkChild(
          Effect.andThen(Deferred.await(gate), presence.track({ ...entry("t"), state: "s1", revision: 1 }))
        )
        assert.isTrue(yield* presence.track({ ...entry("t"), state: "s2", revision: 2 }))
        yield* Deferred.succeed(gate, undefined)
        assert.isFalse(yield* Fiber.join(late))
        assert.strictEqual(yield* state, "s2")
      })
  )

  it.effect("after a handover rejoin with (s2, 2), a late track(s1, 1) from the old leader's attempt is ignored", () =>
    Effect.gen(function* () {
      const presence = yield* Presence.make({ heartbeatInterval: "1 hour" })
      const state = Effect.map(presence.snapshot(s1), (r) => r.entries[0]?.state)
      const old = yield* Effect.forkChild(Stream.runDrain(presence.join(entry("t"))))
      yield* settle
      const gate = yield* Deferred.make<void>()
      const late = yield* Effect.forkChild(
        Effect.andThen(Deferred.await(gate), presence.track({ ...entry("t"), state: "s1", revision: 1 }))
      )
      // The new leader re-joins with the tab's current pair; it replaces the old entry and ends its stream.
      yield* Effect.forkChild(Stream.runDrain(presence.join({ ...entry("t"), state: "s2", revision: 2 })))
      yield* settle
      assert.isTrue(Exit.isSuccess(yield* Fiber.await(old)))
      yield* Deferred.succeed(gate, undefined)
      assert.isFalse(yield* Fiber.join(late))
      assert.strictEqual(yield* state, "s2")
      // A newer track still applies.
      assert.isTrue(yield* presence.track({ ...entry("t"), state: "s3", revision: 3 }))
      assert.strictEqual(yield* state, "s3")
    })
  )

  it.effect(
    "close then reopen: a late old-lifetime track@5 is ignored by the reopened entry@6, whose next state applies",
    () =>
      Effect.gen(function* () {
        const presence = yield* Presence.make({ heartbeatInterval: "1 hour" })
        const state = Effect.map(presence.snapshot(s1), (r) => r.entries[0]?.state)
        const old = yield* Effect.forkChild(
          Stream.runDrain(presence.join({ ...entry("t"), state: "old", revision: 4 }))
        )
        yield* settle
        const gate = yield* Deferred.make<void>()
        const late = yield* Effect.forkChild(
          Effect.andThen(Deferred.await(gate), presence.track({ ...entry("t"), state: "old", revision: 5 }))
        )
        yield* Fiber.interrupt(old)
        yield* Effect.forkChild(Stream.runDrain(presence.join({ ...entry("t"), state: "new", revision: 6 })))
        yield* settle
        yield* Deferred.succeed(gate, undefined)
        assert.isFalse(yield* Fiber.join(late))
        assert.strictEqual(yield* state, "new")
        assert.isTrue(yield* presence.track({ ...entry("t"), state: "desired", revision: 7 }))
        assert.strictEqual(yield* state, "desired")
      })
  )

  it.effect(
    "a late join@2 against the current entry@3 is superseded: the entry and its stream are untouched, the late stream ends empty",
    () =>
      Effect.gen(function* () {
        const presence = yield* Presence.make({ heartbeatInterval: "1 hour" })
        const state = Effect.map(presence.snapshot(s1), (r) => r.entries[0]?.state)
        const current = yield* Effect.forkChild(
          Stream.runDrain(presence.join({ ...entry("t"), state: "current", revision: 3 }))
        )
        yield* settle
        const late = yield* Stream.runCollect(presence.join({ ...entry("t"), state: "old", revision: 2 }))
        yield* settle
        assert.deepStrictEqual(late, [])
        assert.isUndefined(current.pollUnsafe(), "the current stream is still open")
        assert.strictEqual(yield* state, "current")
        // The late join installed nothing: a track@3 is still not newer, a track@4 is.
        assert.isFalse(yield* presence.track({ ...entry("t"), state: "x", revision: 3 }))
        assert.isTrue(yield* presence.track({ ...entry("t"), state: "y", revision: 4 }))
        assert.strictEqual(yield* state, "y")
        // An equal revision is a same-attempt retry: it installs and replaces, as before.
        yield* Effect.forkChild(Stream.runDrain(presence.join({ ...entry("t"), state: "z", revision: 4 })))
        yield* settle
        assert.isDefined(current.pollUnsafe(), "an equal-revision join replaced the current stream")
        assert.strictEqual(yield* state, "z")
      })
  )

  it.effect("a track interrupted at any point either changed nothing or was announced to every observer", () =>
    Effect.gen(function* () {
      const presence = yield* Presence.make({ heartbeatInterval: "1 hour" })
      const latest: { state: unknown } = { state: undefined }
      const ready = yield* Queue.unbounded<void>()
      yield* Effect.forkChild(
        Stream.runForEach(presence.join(entry("watch")), (snapshot) =>
          Effect.andThen(
            Effect.sync(
              () => void (latest.state = snapshot._tag === "snapshot" ? snapshot.entries[0]?.state : latest.state)
            ),
            Queue.offer(ready, undefined)
          )
        )
      )
      yield* Queue.take(ready)
      // Sweep where the tracking fiber is interrupted: it yields every `ops` operations (1 livelocks
      // the pinned runtime) and is interrupted after `yields` turns of this fiber.
      let n = 0
      for (let ops = 2; ops < 12; ops++) {
        for (let yields = 0; yields < 30; yields++) {
          n++
          const tracking = yield* Effect.forkChild(
            presence
              .track({ ...entry("watch"), state: n, revision: n })
              .pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, ops))
          )
          yield* Effect.repeat(Effect.yieldNow, { times: yields })
          yield* Fiber.interrupt(tracking)
          yield* settle
          const installed = (yield* presence.snapshot(s1)).entries[0]?.state
          // Whatever the room now holds, the observer has been told.
          assert.deepStrictEqual(latest.state, installed, `ops ${ops}, yields ${yields}`)
        }
      }
    })
  )
  it.effect(
    "a revoke committed while a join is in flight cannot leave an entry behind: revoke checked before the entry exists, while its admit runs, and after it",
    () =>
      Effect.gen(function* () {
        const presence = yield* Presence.make({ heartbeatInterval: "1 hour" })
        const room = Effect.map(presence.snapshot(s1), show)
        // The grant table: does p hold s1? `holds` and `admit` both read it, as the handlers read
        // scope_grants; the join's pre-check is the caller's and passed before each case starts.
        const granted = { p: true }
        const holds = () => Effect.succeed((_: unknown, principalId: string) => principalId !== "p" || granted.p)
        const revoked = Effect.andThen(
          Effect.sync(() => void (granted.p = false)),
          presence.reevaluate(holds)
        )

        // 1. The revoke commits and its check runs before the entry is installed: the check sees
        //    no stream, so only the join's own re-check (admit) can catch it.
        yield* revoked
        const early = yield* Effect.exit(
          Stream.runCollect(
            presence.join(
              entry("early"),
              Effect.sync(() => granted.p)
            )
          )
        )
        assert.deepStrictEqual(forbidden(early), expected, "early revoke")
        assert.deepStrictEqual(yield* room, [])

        // 2. The admit read the grant before the revoke, and the revoke's check runs while the
        //    admit is still in flight: the check removes the installed entry.
        granted.p = true
        const gate = yield* Deferred.make<void>()
        const seen: Array<ReadonlyArray<string>> = []
        const during = yield* Effect.forkChild(
          Effect.exit(
            Stream.runForEach(
              presence.join(entry("during"), Effect.sync(() => granted.p).pipe(Effect.tap(() => Deferred.await(gate)))),
              (element) => Effect.sync(() => void seen.push(show(element)))
            )
          )
        )
        yield* settle
        assert.deepStrictEqual(yield* room, ["p/c1/during=null"], "installed, admit pending")
        yield* revoked
        yield* Deferred.succeed(gate, undefined)
        assert.deepStrictEqual(forbidden(yield* Fiber.join(during)), expected, "revoke during admit")
        assert.deepStrictEqual(seen, [], "a revoked join emits no snapshot")
        assert.deepStrictEqual(yield* room, [])

        // 3. The revoke lands after the join completed: the check ends it.
        granted.p = true
        const late = yield* Effect.forkChild(
          Effect.exit(
            Stream.runDrain(
              presence.join(
                entry("late"),
                Effect.sync(() => granted.p)
              )
            )
          )
        )
        yield* settle
        assert.deepStrictEqual(yield* room, ["p/c1/late=null"])
        yield* revoked
        assert.deepStrictEqual(forbidden(yield* Fiber.join(late)), expected, "late revoke")
        assert.deepStrictEqual(yield* room, [])
      })
  )
  it.effect(
    "a principal's entry admitted while a revocation check is in flight cannot survive it: all its entries leave in the check's one step or the newcomer is refused",
    () =>
      Effect.gen(function* () {
        const presence = yield* Presence.make({ heartbeatInterval: "1 hour" })
        const room = Effect.map(presence.snapshot(s1), show)
        const granted = { p: true }
        const admit = Effect.sync(() => granted.p)
        const a = yield* Effect.forkChild(Effect.exit(Stream.runDrain(presence.join(entry("a"), admit))))
        yield* settle
        // The check captures A and waits before its grant read.
        const asking = yield* Deferred.make<void>()
        const answer = yield* Deferred.make<void>()
        const check = yield* Effect.forkChild(
          presence.reevaluate(() =>
            Deferred.succeed(asking, undefined).pipe(
              Effect.andThen(Deferred.await(answer)),
              Effect.andThen(Effect.sync(() => granted.p)),
              Effect.map((held) => (_: unknown, principalId: string) => principalId !== "p" || held)
            )
          )
        )
        yield* Deferred.await(asking)
        // B joins while the check waits; an admit run now would still see the grant.
        const bSeen: Array<ReadonlyArray<string>> = []
        const b = yield* Effect.forkChild(
          Effect.exit(
            Stream.runForEach(presence.join(entry("b"), admit), (element) =>
              Effect.sync(() => void bSeen.push(show(element)))
            )
          )
        )
        yield* settle
        // The revoke commits; the check reads it.
        granted.p = false
        yield* Deferred.succeed(answer, undefined)
        yield* Fiber.join(check)
        assert.deepStrictEqual(yield* room, [], "after the one check, no entry of p is left")
        yield* settle
        assert.deepStrictEqual(forbidden(yield* Fiber.join(a)), expected)
        assert.deepStrictEqual(forbidden(yield* Fiber.join(b)), expected)
        assert.deepStrictEqual(bSeen, [], "the newcomer received nothing")
      })
  )

  it.effect("a check whose denial predates a re-grant does not end a newcomer admitted after the re-grant", () =>
    Effect.gen(function* () {
      const presence = yield* Presence.make({ heartbeatInterval: "1 hour" })
      const room = Effect.map(presence.snapshot(s1), show)
      const granted = { p: true }
      const admit = Effect.sync(() => granted.p)
      const a = yield* Effect.forkChild(Effect.exit(Stream.runDrain(presence.join(entry("a"), admit))))
      yield* settle
      // The revoke commits; the check captures A, reads the denial, and waits before removing.
      granted.p = false
      const asked = yield* Deferred.make<void>()
      const answer = yield* Deferred.make<void>()
      const check = yield* Effect.forkChild(
        presence.reevaluate(() =>
          Effect.sync(() => granted.p).pipe(
            Effect.tap(() => Deferred.succeed(asked, undefined)),
            Effect.tap(() => Deferred.await(answer)),
            Effect.map((held) => (_: unknown, principalId: string) => principalId !== "p" || held)
          )
        )
      )
      yield* Deferred.await(asked)
      // Re-granted; B joins meanwhile, and its admit sees the new grant.
      granted.p = true
      const b = yield* Effect.forkChild(Effect.exit(Stream.runDrain(presence.join(entry("b"), admit))))
      yield* settle
      yield* Deferred.succeed(answer, undefined)
      yield* Fiber.join(check)
      yield* settle
      assert.deepStrictEqual(forbidden(yield* Fiber.join(a)), expected, "the stale denial still ends A")
      assert.isUndefined(b.pollUnsafe(), "B, admitted after the re-grant, is open")
      assert.deepStrictEqual(yield* room, ["p/c1/b=null"])
    })
  )

  it.effect(
    "a revocation while the join's admit is still pending ends the stream Forbidden at once, and the admit's later failure never surfaces",
    () =>
      Effect.gen(function* () {
        const presence = yield* Presence.make({ heartbeatInterval: "1 hour" })
        const room = Effect.map(presence.snapshot(s1), show)
        const gate = yield* Deferred.make<void>()
        const pending = yield* Effect.forkChild(
          Effect.exit(
            Stream.runDrain(
              presence.join(entry("pending"), Effect.andThen(Deferred.await(gate), Effect.fail("late admit failure")))
            )
          )
        )
        yield* settle
        assert.deepStrictEqual(yield* room, ["p/c1/pending=null"], "installed, admit pending")
        yield* presence.reevaluate(() => Effect.succeed(() => false))
        yield* settle
        assert.deepStrictEqual(yield* room, [])
        // The admit gate is still closed.
        assert.isDefined(pending.pollUnsafe(), "the stream ended without the admit completing")
        assert.deepStrictEqual(forbidden(yield* Fiber.join(pending)), expected)
        yield* Deferred.succeed(gate, undefined)
        yield* settle
        assert.deepStrictEqual(forbidden(yield* Fiber.join(pending)), expected)
      })
  )
  it.effect(
    "a revocation that completed while admit ran wins over admit's later outcome: the join ends Forbidden, not with admit's failure or a pass",
    () =>
      Effect.gen(function* () {
        const presence = yield* Presence.make({ heartbeatInterval: "1 hour" })
        const room = Effect.map(presence.snapshot(s1), show)
        const revokeAll = presence.reevaluate(() => Effect.succeed(() => false))
        // No yield between the revoke and admit's outcome: only precedence, not timing, decides.
        const failing = yield* Effect.exit(
          Stream.runDrain(presence.join(entry("fails"), Effect.andThen(revokeAll, Effect.fail("late admit failure"))))
        )
        assert.deepStrictEqual(forbidden(failing), expected)
        const passing = yield* Effect.exit(
          Stream.runCollect(presence.join(entry("passes"), Effect.andThen(revokeAll, Effect.succeed(true))))
        )
        assert.deepStrictEqual(forbidden(passing), expected)
        assert.deepStrictEqual(yield* room, [])
        // Replaced (a same-key rejoin) while admit ran, then admit fails: the old stream ends empty
        // and the newcomer keeps the entry. The rejoin is forked into the test's own scope (not the
        // old admission's, which the replacement closes) and is uninterruptible until joined, so the
        // replacement is complete before admit's failure; the test scope interrupts it at the end.
        const outer = yield* Effect.scope
        const joined = yield* Deferred.make<void>()
        const rejoin = Effect.uninterruptible(
          Effect.andThen(
            Effect.forkIn(outer)(
              Stream.runForEach(presence.join({ ...entry("k"), revision: 1 }), () =>
                Deferred.succeed(joined, undefined)
              )
            ),
            Deferred.await(joined)
          )
        )
        const replaced = yield* Effect.exit(
          Stream.runCollect(presence.join(entry("k"), Effect.andThen(rejoin, Effect.fail("late admit failure"))))
        )
        assert.isTrue(Exit.isSuccess(replaced) && replaced.value.length === 0, "replaced: ends empty")
        assert.deepStrictEqual(yield* room, ["p/c1/k=null"])
      }).pipe(Effect.scoped)
  )
})

describe.skipIf(!pgAvailable)("presence over the sync mount (SPEC §10)", () => {
  it.live(
    "a presence entry is dropped when its room stream ends: a client that goes away over HTTP is removed for everyone",
    () =>
      Effect.gen(function* () {
        yield* grant("p", "s1")
        const connect = yield* server
        const observer = yield* open(yield* connect("p"), payload("c0", "watch"))
        yield* until(observer.snapshots, ["p/c0/watch=null"])
        const leaving = yield* open(yield* connect("p"), payload("c1", "t1", { cursor: 3 }))
        yield* until(observer.snapshots, ["p/c0/watch=null", 'p/c1/t1={"cursor":3}'])
        yield* until(leaving.snapshots, ["p/c0/watch=null", 'p/c1/t1={"cursor":3}'])
        yield* Fiber.interrupt(leaving.fiber) // the tab closes: its request is aborted
        yield* until(observer.snapshots, ["p/c0/watch=null"])
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    30_000
  )

  it.live("a principal without a grant in the room gets Forbidden and never appears in it", () =>
    Effect.gen(function* () {
      yield* grant("p", "s1")
      yield* grant("q", "elsewhere") // a grant, but not in this room
      const connect = yield* server
      const observer = yield* open(yield* connect("p"), payload("c0", "watch"))
      yield* until(observer.snapshots, ["p/c0/watch=null"])

      const outsider = yield* connect("q")
      const exit = yield* Effect.exit(
        Stream.runDrain(outsider.room(payload("cq", "t1"))).pipe(Effect.timeout("3 seconds"))
      )
      assert.deepStrictEqual(
        Exit.isFailure(exit) ? Option.map(Exit.findErrorOption(exit), (e) => e._tag) : Option.none(),
        Option.some("Forbidden")
      )
      // Its track is a no-op (it holds no entry), and a later change shows the room without it.
      yield* outsider.track(payload("cq", "t1", "sneaking in", 1))
      yield* (yield* connect("p")).track(payload("c0", "watch", "changed", 1))
      yield* until(observer.snapshots, ['p/c0/watch="changed"'])
      assert.deepStrictEqual(
        observer.seen.flat().filter((e) => !e.startsWith("p/")),
        []
      )
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live(
    "a join or track asserting another replica principal than the session's is Unauthorized and changes no entry (SPEC §5)",
    () =>
      Effect.gen(function* () {
        yield* grant("p", "s1")
        yield* grant("q", "s1") // q may join s1, so only the assertion can keep it out
        const connect = yield* server
        const observer = yield* open(yield* connect("p"), payload("c0", "watch"))
        yield* until(observer.snapshots, ["p/c0/watch=null"])
        const q = yield* connect("q")
        const asserting = (p: ReturnType<typeof payload>, principal: string) => ({
          ...p,
          principal: AccessScope.PrincipalId.make(principal)
        })

        // A join as q asserting replica principal p: refused before any grant check or entry.
        const joined = yield* Effect.exit(
          Stream.runDrain(q.room(asserting(payload("cq", "t1"), "p"))).pipe(Effect.timeout("3 seconds"))
        )
        assert.deepStrictEqual(tagOf(joined), Option.some("Unauthorized"))

        // q's own entry (asserting q) joins; a track of it asserting p changes nothing.
        const own = yield* open(q, asserting(payload("cq", "t2"), "q"))
        yield* until(observer.snapshots, ["p/c0/watch=null", "q/cq/t2=null"])
        const tracked = yield* Effect.exit(q.track(asserting(payload("cq", "t2", "switched", 1), "p")))
        assert.deepStrictEqual(tagOf(tracked), Option.some("Unauthorized"))
        yield* (yield* connect("p")).track(payload("c0", "watch", "changed", 1))
        yield* until(observer.snapshots, ['p/c0/watch="changed"', "q/cq/t2=null"])
        assert.isFalse(observer.seen.flat().some((e) => e.includes("switched") || e.includes("/t1=")))
        yield* Fiber.interrupt(own.fiber)
      }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )

  it.live(
    "a revoked principal's room streams end Forbidden and all its entries leave the others' next snapshot, with no action from them",
    () =>
      Effect.gen(function* () {
        yield* grant("p", "s1")
        yield* grant("q", "s1")
        const connect = yield* server
        const observer = yield* open(yield* connect("p"), payload("c0", "watch"))
        const q = yield* connect("q")
        const tab1 = yield* open(q, payload("cq", "t1"))
        const tab2 = yield* open(q, payload("cq", "t2"))
        const all = ["p/c0/watch=null", "q/cq/t1=null", "q/cq/t2=null"]
        yield* until(observer.snapshots, all)
        yield* until(tab2.snapshots, all)
        const before = observer.seen.length

        yield* revoke("q", "s1")
        for (const tab of [tab1, tab2]) {
          const exit = yield* Fiber.join(tab.fiber).pipe(Effect.timeout("5 seconds"))
          assert.deepStrictEqual(forbidden(exit), expected)
          // It never saw the room without itself: nothing of the room reached it after removal.
          assert.isTrue(tab.seen.every((snapshot) => snapshot.some((e) => e.startsWith("q/"))))
        }
        yield* until(observer.snapshots, ["p/c0/watch=null"])
        // Both entries left in one step: no snapshot showed one of them gone and the other still there.
        assert.deepStrictEqual(
          observer.seen.slice(before).filter((snapshot) => snapshot.filter((e) => e.startsWith("q/")).length === 1),
          []
        )
        // A track for a removed entry is the existing no-op.
        yield* q.track(payload("cq", "t1", "after revoke", 1))
        yield* (yield* connect("p")).track(payload("c0", "watch", "changed", 1))
        yield* until(observer.snapshots, ['p/c0/watch="changed"'])
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    30_000
  )

  it.live(
    "a grant change that leaves the principal holding the scope, or touches another principal or scope, ends no stream",
    () =>
      Effect.gen(function* () {
        yield* grant("p", "s1")
        yield* grant("q", "s1", "s2")
        yield* grant("*", "s1")
        yield* grant("r", "s2")
        const connect = yield* server
        const observer = yield* open(yield* connect("p"), payload("c0", "watch"))
        const q = yield* connect("q")
        const qs1 = yield* open(q, payload("cq", "t1"))
        const qs2 = yield* open(q, { ...payload("cq", "t2"), room: AccessScope.AccessScopeId.make("s2") })
        const r = yield* open(yield* connect("r"), {
          ...payload("cr", "t1"),
          room: AccessScope.AccessScopeId.make("s2")
        })
        yield* until(observer.snapshots, ["p/c0/watch=null", "q/cq/t1=null"])
        yield* until(qs2.snapshots, ["q/cq/t2=null", "r/cr/t1=null"])

        yield* revoke("q", "s1") // q still holds s1 through `*`
        yield* grant("u", "s1", "s3") // an unrelated principal gains grants
        yield* revoke("u", "s1")
        yield* grant("q", "s3") // q gains an unrelated scope
        yield* revoke("q", "s3")
        // r loses its only grant in s2: once its stream has ended, every earlier change has been checked.
        yield* revoke("r", "s2")
        yield* until(qs2.snapshots, ["q/cq/t2=null"])
        assert.isTrue(Exit.isFailure(yield* Fiber.join(r.fiber).pipe(Effect.timeout("5 seconds"))))
        assert.isUndefined(qs1.fiber.pollUnsafe(), "q's s1 stream is open")
        assert.isUndefined(qs2.fiber.pollUnsafe(), "q's s2 stream is open")
        assert.isUndefined(observer.fiber.pollUnsafe(), "p's stream is open")

        // The `*` grant was what kept q in s1: revoking it ends q there, and only there.
        yield* revoke("*", "s1")
        yield* until(observer.snapshots, ["p/c0/watch=null"])
        assert.isTrue(Exit.isFailure(yield* Fiber.join(qs1.fiber).pipe(Effect.timeout("5 seconds"))))
        yield* q.track({ ...payload("cq", "t2", "still here", 1), room: AccessScope.AccessScopeId.make("s2") })
        yield* until(qs2.snapshots, ['q/cq/t2="still here"'])
        assert.isUndefined(observer.fiber.pollUnsafe(), "p's own grant still holds")
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    30_000
  )

  it.live(
    "two tabs of one client are two entries: each tracks and leaves on its own, and a rejoined tab is not dropped by its old stream",
    () =>
      Effect.gen(function* () {
        yield* grant("p", "s1")
        const connect = yield* server
        const client = yield* connect("p")
        const observer = yield* open(client, payload("c0", "watch"))
        const tab1 = yield* open(client, payload("c1", "t1", "one"))
        const tab2 = yield* open(client, payload("c1", "t2", "two"))
        yield* until(observer.snapshots, ["p/c0/watch=null", 'p/c1/t1="one"', 'p/c1/t2="two"'])

        yield* client.track(payload("c1", "t1", "one, moved", 1))
        yield* until(observer.snapshots, ["p/c0/watch=null", 'p/c1/t1="one, moved"', 'p/c1/t2="two"'])

        // Tab 2 reconnects before its old stream has ended: the old stream's end must not drop it.
        const tab2again = yield* open(client, payload("c1", "t2", "two, again"))
        yield* until(tab2again.snapshots, ["p/c0/watch=null", 'p/c1/t1="one, moved"', 'p/c1/t2="two, again"'])
        // The rejoin ended the old stream: an entry has exactly one observing stream (SPEC §10).
        assert.isTrue(Exit.isSuccess(yield* Fiber.join(tab2.fiber)))
        yield* Fiber.interrupt(tab1.fiber)
        yield* until(observer.snapshots, ["p/c0/watch=null", 'p/c1/t2="two, again"'])
        yield* client.track(payload("c1", "t2", "still here", 1))
        yield* until(observer.snapshots, ["p/c0/watch=null", 'p/c1/t2="still here"'])
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    30_000
  )
})
