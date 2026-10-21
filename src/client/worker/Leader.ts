/**
 * Leader-candidate worker (DESIGN §9). Every tab spawns one of these as a dedicated `Worker`; Web Locks
 * let exactly one of them, the leader, open the OPFS database. The leader serves `LeaderRpcs` to every
 * tab on a `MessagePort` brokered by the router (or by its own tab when there is no SharedWorker).
 *
 * Usage, in an app-owned worker entry file:
 *
 * ```ts
 * import { runLeaderWorker } from "echo-sync/client/worker"
 * runLeaderWorker(self)
 * ```
 */
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import * as BrowserWorkerRunner from "@effect/platform-browser/BrowserWorkerRunner"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/sql/SqlClient"
import type { Row } from "effect/sql/SqlConnection"
import type { SqlError } from "effect/sql/SqlError"
import { type Rpc, type RpcGroup, RpcServer } from "effect/rpc"
import * as Outbox from "../Outbox.ts"
import { LeaderRpcs, SqlFailure } from "./LeaderRpcs.ts"
import { type Endpoint, type FromLeader, isToLeader, type ToLeader } from "./messages.ts"
import * as OpfsSqlite from "./OpfsSqlite.ts"
import * as Platform from "./platform.ts"

const causeMessage = (cause: unknown): string =>
  typeof cause === "string"
    ? cause
    : typeof cause === "object" && cause !== null && "message" in cause && typeof cause.message === "string"
      ? cause.message
      : String(cause)

/** Keep SQLite's own message: "Failed to execute statement" alone is useless to a caller. */
const toFailure = (error: SqlError): SqlFailure =>
  new SqlFailure({ message: `${error.message}: ${causeMessage(error.reason.cause)}` })

const send = (endpoint: Endpoint, message: FromLeader) => endpoint.postMessage(message, { transfer: [] })

/** Open the database: the OPFS host on one end of a channel, the Effect SQL client on the other. */
const openDatabase = (identity: Platform.ReplicaIdentity) =>
  Effect.gen(function* () {
    const poolDirectory = Platform.poolDirectory(identity)
    yield* Platform.awaitOpfsPoolFree(poolDirectory)
    const channel = new MessageChannel()
    const opfs = yield* OpfsSqlite.run({ port: channel.port1, poolDirectory, dbName: Platform.dbName(identity) }).pipe(
      Effect.forkScoped
    )
    const context = yield* Layer.build(
      SqliteClient.layer({ worker: Effect.succeed(channel.port2), installReactivityHooks: true })
    ).pipe(
      // OpfsWorker failing to open never sends "ready", so the client would wait forever.
      Effect.raceFirst(
        Fiber.join(opfs).pipe(Effect.flatMap(() => Effect.die(new Error("OPFS SQLite worker exited before ready"))))
      )
    )
    const sql = Context.get(context, SqlClient.SqlClient)
    yield* Outbox.migrate.pipe(Effect.provideService(SqlClient.SqlClient, sql))
    return sql
  })

const makeHandlers = (sql: SqlClient.SqlClient, info: { leaderId: string; clientId: string; dbName: string }) =>
  LeaderRpcs.toHandlers({
    exec: ({ sql: text, params }) => sql.unsafe<Row>(text, params).pipe(Effect.mapError(toFailure)),
    enqueue: ({ envelope }) =>
      Outbox.append({ clientId: info.clientId, ...envelope }).pipe(
        Effect.map(({ clientId, mutationId, seq }) => ({ clientId, mutationId, seq })),
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.mapError(toFailure)
      ),
    subscribe: ({ query }) =>
      sql
        .reactive(
          query.tables,
          // One read transaction per emission, so a batch never mixes replica versions (DESIGN §10).
          sql.withTransaction(Effect.forEach(query.statements, (s) => sql.unsafe<Row>(s.sql, s.params)))
        )
        .pipe(
          Stream.map((results) => ({ results })),
          Stream.mapError(toFailure)
        ),
    leaderInfo: () => Effect.succeed(info)
  })

const serveTab = (
  identity: Platform.ReplicaIdentity,
  tabId: string,
  port: MessagePort,
  handlers: Context.Context<Rpc.ToHandler<RpcGroup.Rpcs<typeof LeaderRpcs>>>
) =>
  RpcServer.make(LeaderRpcs).pipe(
    Effect.provide(RpcServer.layerProtocolWorkerRunner.pipe(Layer.provide(BrowserWorkerRunner.layerMessagePort(port)))),
    Effect.provide(handlers),
    // Tabs never say goodbye reliably; their lifetime lock does.
    Effect.raceFirst(Platform.awaitLockReleased(Platform.tabLockName(identity, tabId))),
    Effect.ensuring(Effect.sync(() => port.close()))
  )

const program = (endpoint: Endpoint, inbox: Queue.Queue<ToLeader>) =>
  Effect.gen(function* () {
    let init = yield* Queue.take(inbox)
    while (init._tag !== "Init") init = yield* Queue.take(inbox)
    const identity: Platform.ReplicaIdentity = { appId: init.appId, profile: init.profile }
    const invalid = Platform.validateIdentity(identity) ?? Platform.leaderHostCapability()
    if (invalid !== undefined) return send(endpoint, { _tag: "Failed", message: invalid })

    if (!(yield* Platform.tryHoldLock(Platform.leaderLockName(identity)))) {
      send(endpoint, { _tag: "Waiting" })
      yield* Platform.holdLock(Platform.leaderLockName(identity))
    }
    // The leader lock is held from here until this worker dies.
    const leaderId = crypto.randomUUID()
    yield* Platform.holdLock(Platform.leaderEpochLockName(identity, leaderId))
    const dbName = Platform.dbName(identity)
    const sql = yield* openDatabase(identity)
    const clientId = yield* Outbox.clientId.pipe(Effect.provideService(SqlClient.SqlClient, sql))
    const handlers = yield* makeHandlers(sql, { leaderId, clientId, dbName })
    send(endpoint, { _tag: "Elected", leaderId })

    while (true) {
      const message = yield* Queue.take(inbox)
      switch (message._tag) {
        case "Connect":
          yield* Effect.forkScoped(serveTab(identity, message.tabId, message.port, handlers))
          break
        case "RouterPort":
          listen(message.port, inbox)
          break
        case "Init":
          break
      }
    }
  }).pipe(
    Effect.scoped,
    Effect.catchCause((cause) =>
      Effect.sync(() => send(endpoint, { _tag: "Failed", message: String(cause) })).pipe(
        Effect.andThen(Effect.logError("echo leader failed", cause))
      )
    )
  )

const listen = (
  endpoint: Pick<Endpoint, "addEventListener"> & { start?: () => void },
  inbox: Queue.Queue<ToLeader>
) => {
  endpoint.addEventListener("message", (event) => {
    if (isToLeader(event.data)) Queue.offerUnsafe(inbox, event.data)
  })
  endpoint.start?.()
}

/** Entry point for the leader-candidate dedicated worker. Pass the worker's global `self`. */
export const runLeaderWorker = (self: Endpoint): void => {
  const inbox = Effect.runSync(Queue.unbounded<ToLeader>())
  listen(self, inbox)
  Effect.runFork(program(self, inbox))
}
