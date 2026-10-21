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
import * as Cause from "effect/Cause"
import type * as Context from "effect/Context"
import * as DateTime from "effect/DateTime"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Scope from "effect/Scope"
import * as SqlClient from "effect/sql/SqlClient"
import type { Row } from "effect/sql/SqlConnection"
import type { SqlError } from "effect/sql/SqlError"
import { type Rpc, type RpcGroup, RpcServer } from "effect/rpc"
import { AccessScope, type Errors, Ids, type Mutation, Query, type Registry, Store } from "echo-sync"
import * as Outbox from "../Outbox.ts"
import * as Engine from "../sync/Engine.ts"
import * as SqliteSyncStorage from "../sync/SqliteSyncStorage.ts"
import * as SyncLoop from "../sync/SyncLoop.ts"
import * as Transport from "../sync/Transport.ts"
import { LeaderRpcs, RawExecDisabled, SqlFailure, SyncDisabled } from "./LeaderRpcs.ts"
import { type Endpoint, type FromLeader, isToLeader, type ResetOutcome, type ToLeader } from "./messages.ts"
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

/**
 * Open the database without migrating it: the OPFS host on one end of a channel, the Effect SQL client on
 * the other. The leader and `Reset` both open through here, so there is one OPFS host path.
 */
const openConnection = (identity: Platform.ReplicaIdentity) =>
  Effect.gen(function* () {
    const poolDirectory = Platform.poolDirectory(identity)
    yield* Platform.awaitOpfsPoolFree(poolDirectory)
    // Fails here, before SqliteClient's (uninterruptible) wait for "ready" can start.
    const serve = yield* OpfsSqlite.open({ poolDirectory, dbName: Platform.dbName(identity) })
    const channel = new MessageChannel()
    yield* Effect.forkScoped(serve(channel.port1))
    // Our own `Reactivity`, so registered queries can re-register their keys per run (the layer hides its own).
    const reactivity = yield* Reactivity.make
    const sql: SqlClient.SqlClient = yield* SqliteClient.make({
      worker: Effect.succeed(channel.port2),
      installReactivityHooks: true
    }).pipe(Effect.provideService(Reactivity.Reactivity, reactivity))
    return { sql, reactivity }
  })

/** The leader's database: `openConnection`, then the engine tables and the replica schema. */
const openDatabase = (identity: Platform.ReplicaIdentity, registry: Registry.SyncRegistry) =>
  Effect.gen(function* () {
    const { sql, reactivity } = yield* openConnection(identity)
    // Engine tables and the replica schema in one transaction (a manifest change rebuilds the replica).
    yield* Effect.andThen(SqliteSyncStorage.migrate(registry), Outbox.pruneReleased()).pipe(
      Effect.provideService(SqlClient.SqlClient, sql)
    )
    return { sql, reactivity }
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

/**
 * A stream of `run`'s results that re-runs after every invalidation of the keys its latest run passed to
 * `watch` (DESIGN §10.1). `run` must call `watch` while it still holds the storage permit: writes are
 * admitted through the same permit and invalidate as they execute, so none can fall between the read and
 * the registration. A failed run ends the stream.
 */
export const liveRuns = <A, E, R>(
  reactivity: Reactivity.Reactivity,
  run: (watch: (keys: ReadonlyArray<string>) => Effect.Effect<void>) => Effect.Effect<A, E, R>
): Stream.Stream<A, E, R> =>
  Stream.unwrap(
    Effect.gen(function* () {
      // Capacity 1, sliding: any number of invalidations during a run coalesce into one re-run.
      const dirty = yield* Queue.sliding<void>(1)
      Queue.offerUnsafe(dirty, undefined)
      let cancel: (() => void) | undefined
      yield* Scope.addFinalizer(
        yield* Effect.scope,
        Effect.sync(() => cancel?.())
      )
      const watch = (keys: ReadonlyArray<string>) =>
        Effect.sync(() => {
          cancel?.()
          cancel = reactivity.registerUnsafe(keys, () => void Queue.offerUnsafe(dirty, undefined))
        })
      return Stream.fromEffectRepeat(Effect.andThen(Queue.take(dirty), run(watch)))
    })
  )

export interface HandlerOptions {
  readonly sql: SqlClient.SqlClient
  readonly reactivity: Reactivity.Reactivity
  readonly storage: SqliteSyncStorage.SqliteSyncStorage
  readonly engine: Engine.Engine<unknown>
  readonly loop: SyncLoop.SyncLoop | undefined
  readonly actor: AccessScope.PrincipalId
  readonly queries: Query.QueryRegistry
  readonly allowRawExec: boolean
  readonly info: { leaderId: string; clientId: string; dbName: string; syncing: boolean }
}

/** The leader's `LeaderRpcs` handlers. Exported for tests, which serve them without a worker. */
export const makeHandlers = ({
  sql,
  reactivity,
  storage,
  engine,
  loop,
  actor,
  queries,
  allowRawExec,
  info
}: HandlerOptions) => {
  /**
   * Run `self` with the connection in `query_only`, so any write in it fails and its transaction rolls
   * back. Use inside the admitted transaction: the flag is switched off again before COMMIT/ROLLBACK and
   * before the permit is released, so the next admitted writer never sees it (DESIGN §10.1).
   */
  const readOnly = <A, E, R>(self: Effect.Effect<A, E, R>) =>
    Effect.orDie(sql`PRAGMA query_only = ON`).pipe(
      Effect.andThen(self),
      Effect.ensuring(Effect.orDie(sql`PRAGMA query_only = OFF`))
    )
  return LeaderRpcs.toHandlers({
    // Every RPC takes the storage permit, so one queued behind a long install is cancellable while it waits.
    exec: ({ sql: text, params }) =>
      allowRawExec
        ? storage.admit(sql.unsafe<Row>(text, params).pipe(Effect.mapError(toFailure)))
        : Effect.fail(new RawExecDisabled()),
    enqueue: ({ envelope }) =>
      enqueueAndWake(engine, loop?.wake ?? Effect.void)(envelope.mutationId, envelope.name, envelope.args).pipe(
        Effect.map(({ clientId, mutationId, seq }) => ({ clientId, mutationId, seq })),
        Effect.catchTag("StoreUnavailable", (error) => Effect.fail(unavailable(error)))
      ),
    subscribe: ({ name, args }) =>
      liveRuns(reactivity, (watch) =>
        // One read transaction per run: the result and the tables it read are one replica version, and
        // the new dependency set is registered before any later write is admitted (DESIGN §10.1).
        // Read-only twice over: handlers only get a `ReadStore` projection, and `query_only` stops any write
        // that reaches the connection anyway, so nothing commits outside the outbox.
        storage
          .transaction(
            readOnly(
              Effect.gen(function* () {
                const rows = storage.rows
                // Query handlers get a ReadStore, so createScope is unreachable: no claim, no clock.
                const instance = Store.make(rows, {
                  actor,
                  roleFor: Engine.roleFromGrants(rows, actor),
                  claimScope: () => Effect.succeed(false),
                  now: DateTime.makeUnsafe(0)
                })
                const result = yield* Query.run(queries, name, args, instance.store).pipe(
                  // A failed run still read tables; the stream ends with its error either way.
                  Effect.ensuring(Effect.flatMap(instance.reads, (tables) => watch([...tables])))
                )
                return result
              })
            )
          )
          .pipe(Effect.catchTag("StoreUnavailable", (error) => Effect.fail(unavailable(error))))
      ),
    subscribeRaw: ({ query }) => {
      // Caller SQL can switch `query_only` off itself, so it is no security boundary: raw reads are a
      // harness tool behind the same flag as `exec`, refused before the connection is touched (DESIGN §10.1).
      if (!allowRawExec) return Stream.fail(new RawExecDisabled())
      return sql
        .reactive(
          query.tables,
          // One read transaction per emission, so a batch never mixes replica versions (DESIGN §10).
          // `query_only` makes a writing statement fail inside the transaction, which then rolls back;
          // it is switched off again before COMMIT/ROLLBACK, still inside the permit (DESIGN §10.1).
          storage
            .transaction(
              readOnly(
                Effect.forEach(query.statements, (s) => sql.unsafe<Row>(s.sql, s.params)).pipe(
                  Effect.mapError(toFailure)
                )
              )
            )
            .pipe(Effect.catchTag("StoreUnavailable", (error) => Effect.fail(unavailable(error))))
        )
        .pipe(Stream.map((results) => ({ results })))
    },
    leaderInfo: () => Effect.succeed(info),
    syncStatus: () => (loop === undefined ? Stream.fail(new SyncDisabled()) : loop.changes),
    requestSync: () => (loop === undefined ? Effect.fail(new SyncDisabled()) : loop.wake),
    notices: () =>
      sql.reactive(["echo_notices"], storage.admit(SqliteSyncStorage.notices(sql)).pipe(Effect.mapError(unavailable))),
    dismissNotices: ({ ids }) =>
      storage.transaction(SqliteSyncStorage.dismissNotices(sql, ids)).pipe(Effect.mapError(unavailable)),
    // Reads the outbox only: never depends on push state, so a halted push cannot block recovery.
    exportPending: () =>
      storage.transaction(storage.pending).pipe(
        Effect.map((pending) => pending.map(({ envelope, createdAt }) => ({ envelope, createdAt }))),
        Effect.mapError(unavailable)
      )
  })
}

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
  /** The app's registered queries (DESIGN §10.1); `subscribe` runs them by name. Defaults to none. */
  readonly queries?: Query.QueryRegistry
  /**
   * Serve the raw SQL RPCs, `exec` and `subscribeRaw` (DESIGN §10.1). Dev harnesses and diagnostics only;
   * apps never set it: raw SQL can write outside the outbox (including by switching `query_only` off), so
   * this flag, not `query_only`, is the boundary. Without it both fail with `RawExecDisabled`.
   */
  readonly allowRawExec?: boolean
  /**
   * Where `Protocol.SyncRpcs` is mounted (HTTP, NDJSON). Omit for a local-only replica: `enqueue`
   * still applies and persists, nothing is pushed or pulled.
   */
  readonly syncUrl?: string
  /**
   * Request headers for the sync server, given the principal this replica acts as (its tab's
   * `principal`): the credentials the server's `principalFor` authenticates (DESIGN §4, §9.1
   * Credentials). The Effect runs for every outgoing request (each push, each pull, each stream
   * (re)connect), so a renewed credential is used by the next request without a leader restart. The
   * app owns refresh: return what should be on the wire now; after a failed renewal return what you
   * have, and the server's `Unauthorized` is retried.
   */
  readonly syncHeaders?: (principal: AccessScope.PrincipalId) => Effect.Effect<Readonly<Record<string, string>>>
  /**
   * A pokes stream with no element (poke or heartbeat) for this long is ended and reconnected
   * (DESIGN §6 Liveness). Default 45 s; must exceed the server's `heartbeatInterval` (default 15 s).
   */
  readonly idleTimeout?: Duration.Input
}

type Init = Extract<ToLeader, { readonly _tag: "Init" }>
type Reset = Extract<ToLeader, { readonly _tag: "Reset" }>

/** The OPFS directory for `path`, or `undefined` if some part of it does not exist. */
const opfsDirectory = (path: string) =>
  Effect.tryPromise({
    try: async () => {
      let dir = await navigator.storage.getDirectory()
      for (const part of path.split("/")) {
        try {
          dir = await dir.getDirectoryHandle(part)
        } catch (cause) {
          if (cause instanceof DOMException && cause.name === "NotFoundError") return undefined
          throw cause
        }
      }
      return dir
    },
    catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause)))
  })

/** Unreleased outbox entries; a database without an outbox (never migrated) has none. */
const countUnreleased = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    const table = yield* sql<{ readonly n: number }>`
      SELECT count(*) AS n FROM sqlite_master WHERE type = 'table' AND name = 'echo_outbox'`
    if ((table[0]?.n ?? 0) === 0) return 0
    const pending = yield* sql<{ readonly n: number }>`SELECT count(*) AS n FROM echo_outbox WHERE released_at IS NULL`
    return pending[0]?.n ?? 0
  })

/**
 * `resetReplica`'s worker half (DESIGN §9.1). Takes the leader lock only if it is free, so it never queues
 * ahead of or behind a leader. Under it, without `discardPending`, it opens the database through the
 * leader's own OPFS host (no migration), counts the outbox and closes the database and the pool; any
 * failure there is `ResetFailed` with nothing removed. With `discardPending` it never opens the database.
 * Then it removes `echo-sync/<appId>/<profile>` and releases the lock last. A candidate queued meanwhile
 * is elected only after the directory is gone, so it creates a fresh replica.
 */
const reset = (message: Reset): Effect.Effect<ResetOutcome> =>
  Effect.gen(function* () {
    const identity: Platform.ReplicaIdentity = { appId: message.appId, profile: message.profile }
    const invalid = Platform.validateIdentity(identity) ?? Platform.leaderHostCapability()
    if (invalid !== undefined) return { _tag: "ResetFailed", message: invalid } satisfies ResetOutcome
    const poolDirectory = Platform.poolDirectory(identity)
    return yield* Effect.scoped(
      Effect.gen(function* () {
        if (!(yield* Platform.tryHoldLock(Platform.leaderLockName(identity)))) {
          return { _tag: "ReplicaInUse" } satisfies ResetOutcome
        }
        if ((yield* opfsDirectory(poolDirectory)) === undefined) return { _tag: "Done" } satisfies ResetOutcome
        if (message.discardPending) {
          // Never opens the database (DESIGN §9.1): a corrupt or partially removed replica can still be reset.
          // Only waits, as a new leader does, for a predecessor's access handles to be released.
          yield* Platform.awaitOpfsPoolFree(poolDirectory)
        } else {
          // Fails closed: a database that does not open or an outbox that cannot be read is `ResetFailed`
          // (below), never "0 pending". The inner scope closes the database and the pool before any removal.
          const count = yield* Effect.scoped(
            Effect.flatMap(openConnection(identity), ({ sql }) => countUnreleased(sql))
          )
          if (count > 0) return { _tag: "PendingWrites", count } satisfies ResetOutcome
        }
        const parent = yield* opfsDirectory(`echo-sync/${identity.appId}`)
        if (parent !== undefined) {
          yield* Effect.tryPromise({
            try: () => parent.removeEntry(identity.profile, { recursive: true }),
            catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause)))
          })
        }
        return { _tag: "Done" } satisfies ResetOutcome
      })
    )
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.succeed({ _tag: "ResetFailed", message: Cause.pretty(cause) } satisfies ResetOutcome)
    )
  )

const program = (endpoint: WorkerScope, inbox: Queue.Queue<ToLeader>, options: LeaderOptions, init: Init) =>
  Effect.gen(function* () {
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
    const { sql, reactivity } = yield* openDatabase(identity, registry)
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
      const sync = yield* SyncLoop.make({
        engine,
        isFatalPush: Transport.isFatalPush,
        pendingMutations: storage.pendingCount
      })
      yield* Effect.forkScoped(sync.run)
      yield* Effect.forkScoped(
        SyncLoop.followPokes({ connect: transport.pokes(clientId), wake: sync.wake, idleTimeout: options.idleTimeout })
      )
      yield* sync.wake
      loop = sync
    }
    const handlers = yield* makeHandlers({
      sql,
      reactivity,
      storage,
      engine,
      loop,
      actor,
      queries: options.queries ?? Query.makeRegistry([]),
      allowRawExec: options.allowRawExec ?? false,
      info: { leaderId, clientId, dbName, syncing: loop !== undefined }
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
  Effect.runFork(
    Effect.gen(function* () {
      let first = yield* Queue.take(inbox)
      while (first._tag !== "Init" && first._tag !== "Reset") first = yield* Queue.take(inbox)
      if (first._tag === "Init") return yield* program(self, inbox, options, first)
      const outcome = yield* reset(first)
      // Every lock and handle is released by now (the reset's scope closed); the tab ends this worker.
      send(self, { _tag: "ResetOutcome", outcome })
    })
  )
}
