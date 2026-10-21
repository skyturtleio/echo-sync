/**
 * Leader-candidate worker (DESIGN §9). Every tab spawns one of these as a dedicated `Worker`; Web Locks
 * let exactly one of them, the leader, open the OPFS database. The leader serves `LeaderRpcs` to every
 * tab on a `MessagePort` brokered by the router (or by its own tab when there is no SharedWorker).
 *
 * Usage, in an app-owned worker entry file:
 *
 * ```ts
 * import { runLeaderWorker } from "echo-sync/client/worker"
 * runLeaderWorker(self, { registry, mutations, syncUrl: "/sync" })
 * ```
 *
 * The leader owns the reconciliation engine (DESIGN §8, §9): `enqueue` runs `Engine.mutate` (optimistic
 * apply + outbox append in one transaction), and with a `syncUrl` it runs the sync loop (push/pull with
 * backoff, the §6 fatal-push halt) and follows the server's pokes stream, reconnecting on any end. A
 * new leader starts a fresh loop: a push halt is in memory and re-trips on that leader's first push.
 */
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import * as BrowserWorkerRunner from "@effect/platform-browser/BrowserWorkerRunner"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/sql/SqlClient"
import type { Row } from "effect/sql/SqlConnection"
import type { SqlError } from "effect/sql/SqlError"
import { type Rpc, type RpcGroup, RpcServer } from "effect/rpc"
import { AccessScope, type Errors, Ids, type Mutation, type Registry } from "echo-sync"
import * as Outbox from "../Outbox.ts"
import * as Engine from "../sync/Engine.ts"
import * as SqliteSyncStorage from "../sync/SqliteSyncStorage.ts"
import * as SyncLoop from "../sync/SyncLoop.ts"
import * as Transport from "../sync/Transport.ts"
import { LeaderRpcs, SqlFailure, SyncDisabled } from "./LeaderRpcs.ts"
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

const unavailable = (error: Errors.StoreUnavailable) => new SqlFailure({ message: error.message })

const send = (endpoint: Endpoint, message: FromLeader) => endpoint.postMessage(message, { transfer: [] })

/** Open the database: the OPFS host on one end of a channel, the Effect SQL client on the other. */
const openDatabase = (identity: Platform.ReplicaIdentity, registry: Registry.SyncRegistry) =>
  Effect.gen(function* () {
    const poolDirectory = Platform.poolDirectory(identity)
    yield* Platform.awaitOpfsPoolFree(poolDirectory)
    // Fails here, before SqliteClient's (uninterruptible) wait for "ready" can start.
    const serve = yield* OpfsSqlite.open({ poolDirectory, dbName: Platform.dbName(identity) })
    const channel = new MessageChannel()
    yield* Effect.forkScoped(serve(channel.port1))
    const context = yield* Layer.build(
      SqliteClient.layer({ worker: Effect.succeed(channel.port2), installReactivityHooks: true })
    )
    const sql = Context.get(context, SqlClient.SqlClient)
    // Engine tables and the replica schema in one transaction (a manifest change rebuilds the replica).
    yield* Effect.andThen(SqliteSyncStorage.migrate(registry), Outbox.pruneReleased()).pipe(
      Effect.provideService(SqlClient.SqlClient, sql)
    )
    return sql
  })

/** A replica without `syncUrl`: the engine never calls its transport, because no loop runs. */
const localOnly: Engine.Transport<never> = {
  push: () => Effect.die(new Error("echo-sync: local-only replica has no transport")),
  pull: () => Effect.die(new Error("echo-sync: local-only replica has no transport"))
}

/**
 * The leader's `enqueue`: `Engine.mutate`, then wake the sync loop however the mutate ended. The wake is
 * a finalizer, not a success tap: an interrupt that lands while COMMIT runs (masked) still commits the
 * entry, and a skipped wake would leave it unsent until some unrelated event (a quiet pokes stream has
 * no timer). A wake after a mutate that saved nothing costs one extra, coalesced cycle.
 */
export const enqueueAndWake =
  <TE>(engine: Engine.Engine<TE>, wake: Effect.Effect<void>) =>
  (mutationId: Ids.MutationId, name: string, args: Schema.Json) =>
    engine.mutate(mutationId, name, args).pipe(Effect.ensuring(wake))

const makeHandlers = (
  sql: SqlClient.SqlClient,
  storage: SqliteSyncStorage.SqliteSyncStorage,
  engine: Engine.Engine<unknown>,
  loop: SyncLoop.SyncLoop | undefined,
  info: { leaderId: string; clientId: string; dbName: string; syncing: boolean }
) =>
  LeaderRpcs.toHandlers({
    // Every RPC takes the storage permit, so one queued behind a long install is cancellable while it waits.
    exec: ({ sql: text, params }) => storage.admit(sql.unsafe<Row>(text, params).pipe(Effect.mapError(toFailure))),
    enqueue: ({ envelope }) =>
      enqueueAndWake(engine, loop?.wake ?? Effect.void)(envelope.mutationId, envelope.name, envelope.args).pipe(
        Effect.map(({ clientId, mutationId, seq }) => ({ clientId, mutationId, seq })),
        Effect.catchTag("StoreUnavailable", (error) => Effect.fail(unavailable(error)))
      ),
    subscribe: ({ query }) =>
      sql
        .reactive(
          query.tables,
          // One read transaction per emission, so a batch never mixes replica versions (DESIGN §10).
          storage
            .transaction(
              Effect.forEach(query.statements, (s) => sql.unsafe<Row>(s.sql, s.params)).pipe(Effect.mapError(toFailure))
            )
            .pipe(Effect.catchTag("StoreUnavailable", (error) => Effect.fail(unavailable(error))))
        )
        .pipe(Stream.map((results) => ({ results }))),
    leaderInfo: () => Effect.succeed(info),
    syncStatus: () => (loop === undefined ? Stream.fail(new SyncDisabled()) : loop.changes),
    notices: () => storage.admit(SqliteSyncStorage.notices(sql)).pipe(Effect.mapError(unavailable)),
    // Reads the outbox only: never depends on push state, so a halted push cannot block recovery.
    exportPending: () =>
      storage.transaction(storage.pending).pipe(
        Effect.map((pending) => pending.map(({ envelope, createdAt }) => ({ envelope, createdAt }))),
        Effect.mapError(unavailable)
      )
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

/** A dedicated worker's global scope, as far as the leader uses it. */
export interface WorkerScope extends Endpoint {
  close(): void
}

/** What the app's leader worker entry provides. */
export interface LeaderOptions {
  /** The app's synced tables (the client registry). */
  readonly registry: Registry.SyncRegistry
  /** The app's mutations; `enqueue` runs them optimistically. */
  readonly mutations: Mutation.MutationRegistry
  /**
   * Where `Protocol.SyncRpcs` is mounted (HTTP, NDJSON). Omit for a local-only replica: `enqueue`
   * still applies and persists, nothing is pushed or pulled.
   */
  readonly syncUrl?: string
  /**
   * Request headers for the sync server, given the principal this replica acts as (its tab's
   * `principal`): the credentials the server's `principalFor` authenticates (DESIGN §4).
   */
  readonly syncHeaders?: (principal: AccessScope.PrincipalId) => Readonly<Record<string, string>>
}

const program = (endpoint: WorkerScope, inbox: Queue.Queue<ToLeader>, options: LeaderOptions) =>
  Effect.gen(function* () {
    let init = yield* Queue.take(inbox)
    while (init._tag !== "Init") init = yield* Queue.take(inbox)
    const identity: Platform.ReplicaIdentity = { appId: init.appId, profile: init.profile }
    const invalid = Platform.validateIdentity(identity) ?? Platform.leaderHostCapability()
    if (invalid !== undefined) return yield* Effect.fail(new Error(invalid))
    const actor = yield* Schema.decodeUnknownEffect(AccessScope.PrincipalId)(init.principal)

    if (!(yield* Platform.tryHoldLock(Platform.leaderLockName(identity)))) {
      send(endpoint, { _tag: "Waiting" })
      yield* Platform.holdLock(Platform.leaderLockName(identity))
    }
    // The leader lock is held from here until this worker dies.
    const leaderId = crypto.randomUUID()
    yield* Platform.holdLock(Platform.leaderEpochLockName(identity, leaderId))
    const dbName = Platform.dbName(identity)
    const { registry, mutations, syncUrl } = options
    const sql = yield* openDatabase(identity, registry)
    const clientId = yield* Outbox.clientId.pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.flatMap(Schema.decodeUnknownEffect(Ids.ClientId))
    )
    const storage = SqliteSyncStorage.make(sql, registry)
    let loop: SyncLoop.SyncLoop | undefined
    let engine: Engine.Engine<unknown>
    if (syncUrl === undefined) {
      engine = yield* Engine.make({ clientId, actor, storage, registry, mutations, transport: localOnly })
    } else {
      // The RPC protocol lives in this scope: it closes with the leader.
      const protocol = yield* Layer.build(Transport.layerProtocolHttp(syncUrl, options.syncHeaders?.(actor)))
      const transport = yield* Transport.make.pipe(Effect.provide(protocol))
      engine = yield* Engine.make({ clientId, actor, storage, registry, mutations, transport })
      const sync = yield* SyncLoop.make({ engine, isFatalPush: Transport.isFatalPush })
      yield* Effect.forkScoped(sync.run)
      yield* Effect.forkScoped(SyncLoop.followPokes({ connect: transport.pokes(clientId), wake: sync.wake }))
      yield* sync.wake
      loop = sync
    }
    const handlers = yield* makeHandlers(sql, storage, engine, loop, {
      leaderId,
      clientId,
      dbName,
      syncing: loop !== undefined
    })
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
    // Closing the scope releases the leader and epoch locks and the pool's access handles.
    Effect.scoped,
    Effect.catchCause((cause) =>
      Effect.logError("echo leader candidate cannot serve", cause).pipe(
        Effect.andThen(
          Effect.sync(() => {
            send(endpoint, { _tag: "Failed", message: String(cause) })
            // A candidate that cannot serve must not linger (DESIGN §9): ending the worker guarantees that
            // anything a failed open acquired outside the scope (e.g. a half-built handle pool) and every
            // Web Lock is released, so the next queued candidate is elected.
            endpoint.close()
          })
        )
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
export const runLeaderWorker = (self: WorkerScope, options: LeaderOptions): void => {
  const inbox = Effect.runSync(Queue.unbounded<ToLeader>())
  listen(self, inbox)
  Effect.runFork(program(self, inbox, options))
}
