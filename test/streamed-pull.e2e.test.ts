// oxlint-disable-next-line import/no-unassigned-import -- Node fetch shim for the real SQLite wasm driver
import "../src/client/testing/nodeWasm.ts"
import { describe, expect, it } from "@effect/vitest"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import type * as PgClient from "@effect/sql-pg/PgClient"
import { Context, Deferred, Effect, Fiber, Layer, Option, Schema, Stream } from "effect"
import { FetchHttpClient, HttpRouter } from "effect/http"
import { Rpc, RpcClient, RpcGroup, RpcSerialization, RpcServer } from "effect/rpc"
import * as Reactivity from "effect/reactivity/Reactivity"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Protocol } from "echo-sync"
import * as Engine from "../src/client/sync/Engine.ts"
import * as SqliteSyncStorage from "../src/client/sync/SqliteSyncStorage.ts"
import * as Transport from "../src/client/sync/Transport.ts"
import { layerHttp } from "../src/server/postgres/rpc.ts"
import { grant, Items, pgAvailable, registry, TestDatabase } from "../src/server/postgres/test-support.ts"

const actor = AccessScope.PrincipalId.make("p")
const mutations = Mutation.makeRegistry([])
const version = { wireVersion: Protocol.wireVersion, dataHash: registry.dataHash }
const request: Protocol.PullRequest = {
  clientId: Ids.ClientId.make("stream-client"),
  principal: actor,
  clientVersion: version,
  cursor: null,
  continuation: Option.none()
}
const done: Protocol.PullResponse = {
  _tag: "PullDone",
  changes: [],
  acks: [],
  nextCursor: Ids.Position.make(1n),
  expiredThrough: Ids.SeqFloor.make(0)
}

const connect = Effect.fnUntraced(function* (app: Layer.Layer<never, never, HttpRouter.HttpRouter>) {
  const web = HttpRouter.toWebHandler(app, { disableLogger: true })
  yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
  let requests = 0
  const fetch: typeof globalThis.fetch = Object.assign(
    (input: RequestInfo | URL, init?: RequestInit) => {
      requests++
      return web.handler(new Request(input, init))
    },
    { preconnect: globalThis.fetch.preconnect }
  )
  const protocol = Transport.layerProtocolHttp("http://echo.test/sync").pipe(
    Layer.provide(Layer.succeed(FetchHttpClient.Fetch)(fetch))
  )
  // Fetch is read from the request fiber, not captured by the HTTP client layer.
  const rpc = yield* Transport.make.pipe(Effect.provide(protocol))
  const transport: Engine.Transport = {
    push: (r) => rpc.push(r).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch)),
    pull: (r) => rpc.pull(r).pipe(Stream.provideService(FetchHttpClient.Fetch, fetch))
  }
  return { transport, protocol, fetch, requests: () => requests }
})

const device = Effect.fnUntraced(function* (transport: Engine.Transport, sqlite?: SqliteClient.SqliteClient) {
  const sql = sqlite ?? (yield* SqliteClient.makeMemory({}))
  yield* SqliteSyncStorage.migrate(registry).pipe(Effect.provideService(SqlClient, sql))
  const storage = yield* SqliteSyncStorage.make(sql, registry)
  const engine = yield* Engine.make({ clientId: request.clientId, actor, registry, mutations, storage, transport })
  return { engine, storage, sqlite: sql }
})

describe.skipIf(!pgAvailable)("streamed pull over real HTTP codecs and SQLite", () => {
  it.live(
    "one HTTP pull stages incrementally, reads a fresh snapshot per page, and exposes nothing before done",
    () =>
      Effect.gen(function* () {
        const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
        const pg = Context.get(db, SqlClient)
        yield* grant("p", "s")
        yield* pg`INSERT INTO items (id, access_scope_id, body) VALUES ('a','s','one'), ('b','s','two'), ('c','s','old')`
        const waiting = yield* Deferred.make<void>()
        const resume = yield* Deferred.make<void>()
        let pages = 0
        const connection = yield* connect(
          layerHttp({
            registry,
            mutations,
            pageLimit: 2,
            principalFor: () => Effect.succeed(actor),
            clientPolicy: () =>
              Effect.gen(function* () {
                // First call negotiates the epoch; then pause before the second data page.
                if (++pages === 3) {
                  yield* Deferred.succeed(waiting, undefined)
                  yield* Deferred.await(resume)
                }
              })
          }).pipe(Layer.provide(Layer.succeedContext(db)))
        )
        const replica = yield* device(connection.transport)
        const running = yield* Effect.forkChild(replica.engine.pull)
        yield* Deferred.await(waiting)
        // Wait for the first durable page, not merely its receipt at the RPC queue.
        yield* replica.storage
          .transaction(replica.storage.stagedProgress)
          .pipe(Effect.repeat({ until: (progress) => progress.count === 1 }), Effect.timeout("5 seconds"))
        expect(yield* replica.storage.transaction(replica.storage.rows.scan(Items))).toEqual([])
        expect((yield* replica.storage.transaction(replica.storage.meta)).cursor).toBeNull()
        yield* pg`UPDATE items SET body = 'new snapshot' WHERE id = 'c'`
        yield* Deferred.succeed(resume, undefined)
        yield* Fiber.join(running)
        expect(connection.requests()).toBe(2) // epoch negotiation + one complete data stream
        expect(pages).toBeGreaterThan(1)
        expect(yield* replica.storage.transaction(replica.storage.rows.read(Items, "c"))).toEqual(
          Option.some({ id: "c", access_scope_id: "s", body: "new snapshot" })
        )
        expect(yield* replica.storage.transaction(replica.storage.rows.scan(Items))).toHaveLength(3)
        expect(yield* replica.storage.transaction(replica.storage.staged)).toEqual([])
      }).pipe(Effect.scoped, Effect.provide([TestDatabase, Reactivity.layer])),
    20_000
  )

  it.live(
    "a truncated stream resumes after the last durable page when the engine is reconstructed",
    () =>
      Effect.gen(function* () {
        const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
        const pg = Context.get(db, SqlClient)
        yield* grant("p", "s")
        yield* pg`INSERT INTO items (id, access_scope_id, body) VALUES ('a','s','one'), ('b','s','two'), ('c','s','three')`
        const connection = yield* connect(
          layerHttp({
            registry,
            mutations,
            pageLimit: 1,
            principalFor: () => Effect.succeed(actor)
          }).pipe(Layer.provide(Layer.succeedContext(db)))
        )
        const first = yield* device({
          ...connection.transport,
          pull: (r) => connection.transport.pull(r).pipe(Stream.take(1))
        })
        expect((yield* Effect.flip(first.engine.pull))._tag).toBe("PullProtocolError")
        const progress = yield* first.storage.transaction(first.storage.stagedProgress)
        expect(progress.count).toBe(1)
        expect(yield* first.storage.transaction(first.storage.rows.scan(Items))).toEqual([])
        const resumed = yield* device(
          {
            ...connection.transport,
            pull: (r) => {
              expect(r.continuation).toEqual(progress.continuation)
              return connection.transport.pull(r)
            }
          },
          first.sqlite
        )
        yield* resumed.engine.pull
        expect(connection.requests()).toBe(3) // epoch negotiation + interrupted stream + resume
        expect(yield* resumed.storage.transaction(resumed.storage.rows.scan(Items))).toEqual([
          { id: "a", access_scope_id: "s", body: "one" },
          { id: "b", access_scope_id: "s", body: "two" },
          { id: "c", access_scope_id: "s", body: "three" }
        ])
      }).pipe(Effect.scoped, Effect.provide([TestDatabase, Reactivity.layer])),
    20_000
  )

  it.live(
    "a legacy unary client receives ClientOutdated before a streaming server registers or serves it",
    () =>
      Effect.gen(function* () {
        const db = yield* Effect.context<SqlClient | PgClient.PgClient>()
        const pg = Context.get(db, SqlClient)
        const connection = yield* connect(
          layerHttp({ registry, mutations, principalFor: () => Effect.succeed(actor) }).pipe(
            Layer.provide(Layer.succeedContext(db))
          )
        )
        const legacyGroup = RpcGroup.make(
          Rpc.make("pull", {
            payload: Protocol.PullRequest,
            success: Protocol.PullResponse,
            error: Schema.Union([Protocol.ClientOutdated, Protocol.Unauthorized])
          })
        )
        const legacy = yield* RpcClient.make(legacyGroup).pipe(Effect.provide(connection.protocol))
        for (const wireVersion of [0, 1]) {
          const error = yield* Effect.flip(
            legacy.pull({ ...request, clientVersion: { ...version, wireVersion } })
          ).pipe(Effect.provideService(FetchHttpClient.Fetch, connection.fetch))
          expect(error._tag).toBe("ClientOutdated")
        }
        expect(yield* pg`SELECT client_id FROM echo_sync.clients WHERE client_id = ${request.clientId}`).toEqual([])
      }).pipe(Effect.scoped, Effect.provide(TestDatabase)),
    20_000
  )
})

it.live("an old unary server cannot make a streaming client certify completion or discard durable staging", () =>
  Effect.gen(function* () {
    const legacy = RpcGroup.make(Rpc.make("pull", { payload: Protocol.PullRequest, success: Protocol.PullResponse }))
    const connection = yield* connect(
      RpcServer.layerHttp({ group: legacy, path: "/sync", protocol: "http" }).pipe(
        Layer.provide([legacy.toLayer({ pull: () => Effect.succeed(done) }), RpcSerialization.layerNdjson])
      )
    )
    const replica = yield* device(connection.transport)
    const staged: typeof Protocol.PullPage.Type = {
      _tag: "PullPage",
      changes: [],
      acks: [],
      continuation: Protocol.PullContinuation.make("saved")
    }
    yield* replica.storage.transaction(replica.storage.stage(staged))
    expect((yield* Effect.flip(replica.engine.pull))._tag).toBe("PullProtocolError")
    expect(yield* replica.storage.transaction(replica.storage.staged)).toEqual([staged])
    expect((yield* replica.storage.transaction(replica.storage.meta)).cursor).toBeNull()
  }).pipe(Effect.scoped, Effect.provide(Reactivity.layer))
)

it.live("a stalled pull consumer backpressures page production and cancellation releases the server stream", () =>
  Effect.gen(function* () {
    let produced = 0
    let closed = false
    const entered = yield* Deferred.make<void>()
    const hold = yield* Deferred.make<void>()
    const group = RpcGroup.make(
      Rpc.make("pull", {
        payload: Protocol.PullRequest,
        success: Protocol.PullResponse,
        stream: true
      })
    )
    const connection = yield* connect(
      RpcServer.layerHttp({
        group,
        path: "/sync",
        protocol: "http",
        streamBufferSize: 1
      }).pipe(
        Layer.provide([
          group.toLayer({
            pull: () =>
              Stream.paginate(0, (n) =>
                Effect.sync(() => {
                  produced++
                  const page: Protocol.PullResponse =
                    n === 999
                      ? done
                      : {
                          _tag: "PullPage",
                          changes: [],
                          acks: [],
                          continuation: Protocol.PullContinuation.make(String(n))
                        }
                  return [[page], n === 999 ? Option.none<number>() : Option.some(n + 1)]
                })
              ).pipe(
                Stream.ensuring(
                  Effect.sync(() => {
                    closed = true
                  })
                )
              )
          }),
          RpcSerialization.layerNdjson
        ])
      )
    )
    const running = yield* Effect.forkChild(
      connection.transport
        .pull(request)
        .pipe(Stream.runForEach(() => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(hold)))))
    )
    yield* Deferred.await(entered)
    yield* Effect.repeat(Effect.yieldNow, { times: 200 })
    // Deliberately small frames: an unbounded queue could materialize all 1000 without any I/O.
    // This is a pipeline check, not a claim about browser/kernel socket buffer sizes.
    expect(produced).toBeLessThan(1000)
    expect(closed).toBe(false)
    yield* Fiber.interrupt(running)
    yield* Effect.repeat(Effect.yieldNow, { times: 200 })
    expect(closed).toBe(true)
    const stopped = produced
    yield* Effect.repeat(Effect.yieldNow, { times: 200 })
    expect(produced).toBe(stopped)
  }).pipe(Effect.scoped)
)
