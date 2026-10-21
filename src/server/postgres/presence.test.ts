import { assert, describe, it } from "@effect/vitest"
import type * as PgClient from "@effect/sql-pg/PgClient"
import { Effect, Exit, Fiber, Layer, Option, Queue, Scheduler, Stream } from "effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as HttpRouter from "effect/http/HttpRouter"
import { RpcClient, RpcSerialization } from "effect/rpc"
import type { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Protocol } from "echo-sync"
import * as Presence from "./presence.ts"
import { layerHttp } from "./rpc.ts"
import { grant, pgAvailable, registry, TestDatabase } from "./test-support.ts"

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
const payload = (clientId: string, tabId: string, state: Protocol.PresenceEntry["state"] = null) => ({
  clientId: Ids.ClientId.make(clientId),
  room: s1,
  tabId: Protocol.TabId.make(tabId),
  state
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
        Stream.runForEach(client.room(p), (snapshot) =>
          Effect.andThen(
            Effect.sync(() => void seen.push(show(snapshot))),
            Queue.offer(snapshots, snapshot)
          )
        )
      )
    )
    return { snapshots, seen, fiber }
  })

/** The room as a list of `principal/client/tab=state`, which is what every assertion compares. */
const show = (snapshot: Protocol.RoomSnapshot) =>
  snapshot.entries.map((e) => `${e.principalId}/${e.clientId}/${e.tabId}=${JSON.stringify(e.state)}`)

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

describe("presence state (§12)", () => {
  it.effect(
    "a presence entry lives exactly while its stream runs: completion, a failing consumer and interruption each remove it",
    () =>
      Effect.gen(function* () {
        const presence = yield* Presence.make
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
        const presence = yield* Presence.make
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

  it.effect("a track interrupted at any point either changed nothing or was announced to every observer", () =>
    Effect.gen(function* () {
      const presence = yield* Presence.make
      const latest: { state: unknown } = { state: undefined }
      const ready = yield* Queue.unbounded<void>()
      yield* Effect.forkChild(
        Stream.runForEach(presence.join(entry("watch")), (snapshot) =>
          Effect.andThen(
            Effect.sync(() => void (latest.state = snapshot.entries[0]?.state)),
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
              .track({ ...entry("watch"), state: n })
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
})

describe.skipIf(!pgAvailable)("presence over the sync mount (§12)", () => {
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

  it.live("a principal without a grant in the room gets Unauthorized and never appears in it", () =>
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
        Option.some("Unauthorized")
      )
      // Its track is a no-op (it holds no entry), and a later change shows the room without it.
      yield* outsider.track(payload("cq", "t1", "sneaking in"))
      yield* (yield* connect("p")).track(payload("c0", "watch", "changed"))
      yield* until(observer.snapshots, ['p/c0/watch="changed"'])
      assert.deepStrictEqual(
        observer.seen.flat().filter((e) => !e.startsWith("p/")),
        []
      )
    }).pipe(Effect.scoped, Effect.provide(TestDatabase))
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

        yield* client.track(payload("c1", "t1", "one, moved"))
        yield* until(observer.snapshots, ["p/c0/watch=null", 'p/c1/t1="one, moved"', 'p/c1/t2="two"'])

        // Tab 2 reconnects before its old stream has ended: the old stream's end must not drop it.
        const tab2again = yield* open(client, payload("c1", "t2", "two, again"))
        yield* until(tab2again.snapshots, ["p/c0/watch=null", 'p/c1/t1="one, moved"', 'p/c1/t2="two, again"'])
        // The rejoin ended the old stream: an entry has exactly one observing stream (§12).
        assert.isTrue(Exit.isSuccess(yield* Fiber.join(tab2.fiber)))
        yield* Fiber.interrupt(tab1.fiber)
        yield* until(observer.snapshots, ["p/c0/watch=null", 'p/c1/t2="two, again"'])
        yield* client.track(payload("c1", "t2", "still here"))
        yield* until(observer.snapshots, ["p/c0/watch=null", 'p/c1/t2="still here"'])
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    30_000
  )
})
