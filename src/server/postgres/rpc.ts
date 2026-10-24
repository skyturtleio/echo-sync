import * as PgClient from "@effect/sql-pg/PgClient"
import {
  Cause,
  Context,
  DateTime,
  Deferred,
  Duration,
  Effect,
  Layer,
  Match,
  Option,
  Predicate,
  PubSub,
  Queue,
  Ref,
  Result,
  Schema,
  Stream
} from "effect"
import type { Headers } from "effect/http/Headers"
import * as HttpRouter from "effect/http/HttpRouter"
import * as HttpServerRequest from "effect/http/HttpServerRequest"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { SqlClient } from "effect/sql/SqlClient"
import { isSqlError, type SqlError } from "effect/sql/SqlError"
import { AccessScope, Ids, Mutation, Protocol, type Registry, Store } from "echo-sync"
import { assertPrincipal, type ClientForbidden, type LedgerOptions, registerClient, runBatch } from "./ledger.ts"
import * as Presence from "./presence.ts"
import { type ClientPolicy, pull } from "./pull.ts"
import { checkEpoch } from "./epoch.ts"
import * as RowStore from "./rowStore.ts"
import { resolveTables } from "./schema.ts"
import * as Telemetry from "../../internal/Telemetry.ts"

export interface SyncServerOptions {
  readonly registry: Registry.SyncRegistry
  readonly mutations: Mutation.MutationRegistry
  /**
   * Authenticates and applies request policy to the real HTTP request's headers (SPEC §4).
   * Body-supplied RPC message headers are never exposed. Runs for every call / stream open.
   */
  readonly principalFor: (headers: Headers) => Effect.Effect<AccessScope.PrincipalId, Protocol.Unauthorized>
  /** After authentication, before any sync work. Omitted = serve, including legacy clients. */
  readonly clientPolicy?: ClientPolicy | undefined
  /** Upserts and tombstones per pull page. Default 500. */
  readonly pageLimit?: number | undefined
  readonly ledger?: LedgerOptions | undefined
  /**
   * Cadence of the `heartbeat` element on every open `pokes` stream (INV-LOOP-7), so a
   * proxy or the client's `idleTimeout` never sees a live stream as idle. Default 15 s; must stay
   * below the clients' `idleTimeout` (default 45 s).
   */
  readonly heartbeatInterval?: Duration.Input | undefined
}

/** The telemetry outcome of a sync handler's typed failures (SPEC §14). */
const protocolOutcome = Telemetry.byFailure(
  (
    error:
      | Protocol.Unauthorized
      | Protocol.MalformedEnvelope
      | Protocol.ClientOutdated
      | Protocol.ResnapshotRequired
      | Protocol.ServerUnavailable
  ): Telemetry.Outcome =>
    Match.valueTags(error, {
      Unauthorized: (): Telemetry.Outcome => "unauthorized",
      MalformedEnvelope: (): Telemetry.Outcome => "halted",
      ClientOutdated: (): Telemetry.Outcome => "outdated",
      ResnapshotRequired: (): Telemetry.Outcome => "resnapshot",
      ServerUnavailable: (): Telemetry.Outcome => "unavailable"
    })
)

/** `notify` channel of `echo_sync.next_position()`: one hint per committed writing transaction. */
export const pokeChannel = "echo_sync"

const forbidden = (error: ClientForbidden) =>
  new Protocol.Unauthorized({
    reason: "clientPrincipalMismatch",
    message: `client ${error.clientId} belongs to another principal`
  })

/** `*` grants access; it is never an authenticated actor (SPEC §4). */
const authenticate = (principalFor: SyncServerOptions["principalFor"]) =>
  Effect.flatMap(requestHeaders, principalFor).pipe(
    Effect.filterOrFail(
      (principalId) => principalId !== AccessScope.Everyone,
      () =>
        new Protocol.Unauthorized({
          reason: "wildcardPrincipal",
          message: "the everyone principal cannot authenticate"
        })
    )
  )

/** `pageLimit` is a positive safe integer: zero would make every pull page forever. */
const checkPageLimit = (pageLimit: number | undefined): Effect.Effect<void> =>
  pageLimit === undefined || (Number.isSafeInteger(pageLimit) && pageLimit > 0)
    ? Effect.void
    : Effect.die(new RangeError(`pageLimit requires a positive safe integer, got ${pageLimit}`))

const serverUnavailable = (error: SqlError) =>
  new Protocol.ServerUnavailable({ message: `${error.reason._tag}: ${error.message}` })

const isNotSql = <E>(error: E | SqlError): error is Exclude<E, SqlError> => !isSqlError(error)

/**
 * Whole-cause classification of a handler failure, precedence defect > transient > typed
 * (SPEC §5, §8). A `SqlError` may arrive as a failure or, from the pinned client's `orDie` rollback on a
 * dead connection, as a defect; both are infrastructure. Any other defect keeps the whole cause
 * as defects, so a programmer bug is never reported as retryable. Without either, the typed
 * failures (and any interruption) pass through.
 */
const classifyCause = <E>(
  cause: Cause.Cause<E>
):
  | { readonly _tag: "defect"; readonly cause: Cause.Cause<never> }
  | { readonly _tag: "unavailable"; readonly error: SqlError }
  | { readonly _tag: "typed"; readonly cause: Cause.Cause<Exclude<E, SqlError>> } => {
  if (cause.reasons.some((r) => Cause.isDieReason(r) && !isSqlError(r.defect))) {
    return {
      _tag: "defect",
      cause: Cause.fromReasons(cause.reasons.map((r) => (Cause.isFailReason(r) ? Cause.makeDieReason(r.error) : r)))
    }
  }
  for (const r of cause.reasons) {
    const error = Cause.isFailReason(r) ? r.error : Cause.isDieReason(r) ? r.defect : undefined
    if (isSqlError(error)) return { _tag: "unavailable", error }
  }
  return {
    _tag: "typed",
    cause: Cause.fromReasons(
      cause.reasons.flatMap((r): ReadonlyArray<Cause.Reason<Exclude<E, SqlError>>> =>
        Cause.isFailReason(r) ? (isNotSql(r.error) ? [Cause.makeFailReason(r.error)] : []) : [r]
      )
    )
  }
}

/** Pull/pokes: infrastructure ⇒ `ServerUnavailable`; a defect from the server is always a bug. */
const toServerFailure = <E>(
  cause: Cause.Cause<E>
): Effect.Effect<never, Exclude<E, SqlError> | Protocol.ServerUnavailable> => {
  const verdict = classifyCause(cause)
  return verdict._tag === "unavailable"
    ? Effect.fail(serverUnavailable(verdict.error))
    : Effect.failCause<Exclude<E, SqlError>>(verdict.cause)
}

/** Matches by `_tag`, as `catchTag` does, so a `ClientForbidden` from another module instance matches too. */
const isClientForbidden = <E>(e: E | ClientForbidden): e is ClientForbidden => Predicate.isTagged(e, "ClientForbidden")

/**
 * `ClientForbidden` ⇒ `Unauthorized` with `catchTag`'s semantics (the first failure; any other
 * cause passes whole). `catchTag` itself does not typecheck over a generic `E`, so this is
 * `catchFilter` with a tag filter that names the remainder.
 */
const mapForbidden = <A, E, R>(
  self: Effect.Effect<A, E | ClientForbidden, R>
): Effect.Effect<A, E | Protocol.Unauthorized, R> =>
  Effect.catchFilter<A, E | ClientForbidden, R, ClientForbidden, never, Protocol.Unauthorized, never, E>(
    self,
    (e: E | ClientForbidden): Result.Result<ClientForbidden, E> =>
      isClientForbidden(e) ? Result.succeed(e) : Result.fail(e),
    (e: ClientForbidden) => Effect.fail(forbidden(e))
  )

/**
 * The pull/pokes failure mapping (SPEC §8, §10), for any transport: infrastructure (`SqlError`) ⇒
 * `ServerUnavailable`; a client id bound to another principal (`ClientForbidden`) ⇒
 * `Unauthorized`; any other defect stays a defect (a server bug); other typed failures pass.
 */
export const serverFailures = <A, E, R>(
  self: Effect.Effect<A, E | ClientForbidden | SqlError, R>
): Effect.Effect<A, Exclude<E, SqlError> | Protocol.ServerUnavailable | Protocol.Unauthorized, R> => {
  const classified: Effect.Effect<A, Exclude<E, SqlError> | Protocol.ServerUnavailable | ClientForbidden, R> =
    Effect.catchCause(self, toServerFailure)
  return mapForbidden(classified)
}

/**
 * The push failure mapping (SPEC §8), for any transport, applied over authentication and the
 * batch together: infrastructure anywhere (a `SqlError` failure or defect, e.g. a session lookup
 * in `principalFor`) is an empty results list, a transient stop; `ClientForbidden` ⇒
 * `Unauthorized`; any other defect stays a defect (the client halts push on it). Idempotent, so
 * it may wrap `executeBatch`, which already applies it.
 */
export const pushFailures = <E, R>(
  self: Effect.Effect<Protocol.PushResponse, E | ClientForbidden | SqlError, R>
): Effect.Effect<Protocol.PushResponse, Exclude<E, SqlError> | Protocol.Unauthorized, R> => {
  const classified: Effect.Effect<Protocol.PushResponse, Exclude<E, SqlError> | ClientForbidden, R> = Effect.catchCause(
    self,
    (cause) => {
      const verdict = classifyCause(cause)
      return verdict._tag === "unavailable"
        ? Effect.succeed<Protocol.PushResponse>({ results: [] })
        : Effect.failCause(verdict.cause)
    }
  )
  return mapForbidden(classified)
}

const decodeRole = Schema.decodeUnknownOption(AccessScope.Role)

/**
 * The actor's effective role on a scope: the strongest of its own grant and the `*` grant. Read
 * inside the mutation's transaction (the client joins the fiber's transaction), so the authorization check and the write see one snapshot.
 */
export const roleFor = (sql: SqlClient, actor: string) => (scopeId: AccessScope.AccessScopeId) =>
  Effect.gen(function* () {
    const rows = yield* sql<{ role: unknown }>`
      SELECT role FROM echo_sync.scope_grants
      WHERE access_scope_id = ${scopeId} AND principal_id IN (${actor}, '*')`.pipe(
      Effect.mapError(RowStore.unavailable)
    )
    return Option.fromUndefinedOr(AccessScope.strongest(rows.flatMap((r) => Option.toArray(decodeRole(r.role)))))
  })

/**
 * The authoritative scope claim (SPEC §4): inserts the `access_scopes` row inside the mutation's
 * transaction and savepoint, so it commits or rolls back with the grant. The mutation runs at
 * SERIALIZABLE (ledger.ts): a concurrent creator blocks on the first one's uncommitted row and,
 * once that commits, fails with 40001; the ledger retries it, and the retry finds the row (no
 * row returned, so `ScopeForbidden`). A scope whose grants were all revoked keeps its row, so it
 * is never fresh again.
 */
export const claimScope = (sql: SqlClient) => (scopeId: AccessScope.AccessScopeId) =>
  sql<{ id: string }>`
    INSERT INTO echo_sync.access_scopes (id) VALUES (${scopeId}) ON CONFLICT DO NOTHING RETURNING id`.pipe(
    Effect.map((rows) => rows.length === 1),
    Effect.mapError(RowStore.unavailable)
  )

/** One confirmed `LISTEN` on the shared connection. */
interface ListenSession {
  /** Every notification on the channel, fanned out; pokes carry no payload, so it slides. */
  readonly pubsub: PubSub.PubSub<void>
  /**
   * One tick per `heartbeatInterval` while this `LISTEN` is confirmed. Separate from `pubsub`: a
   * heartbeat sliding over a not-yet-taken poke in one capacity-1 queue would lose the poke.
   */
  readonly heartbeats: PubSub.PubSub<void>
  /** Fails with the connection's error once this `LISTEN` is lost. */
  readonly lost: Deferred.Deferred<never, SqlError>
}

/**
 * Delay before the `attempt`-th consecutive `LISTEN` retry (0-based): exponential from 50 ms,
 * capped at 5 s. The count resets once a `LISTEN` is confirmed, so a loss after a healthy period
 * starts again at 50 ms.
 */
const listenBackoff = (attempt: number) => Duration.millis(Math.min(50 * 2 ** attempt, 5_000))

/**
 * The process's single `LISTEN` on `pokeChannel` (SPEC §10.1). `PgClient.listen` reserves a pool
 * connection for its scope, so one per `pokes` stream would exhaust the pool at `poolSize`
 * connected clients and block every push and pull. Lives in the caller's scope
 * (`layerNotifications`) and returns once the first attempt has settled, so a server normally
 * serves with its `LISTEN` already in place.
 *
 * Each attempt (reserve, `LISTEN`, pump notifications) is one scoped session, and every session
 * ends in failure: a `LISTEN` that fails after its reservation, or a confirmed one whose
 * connection is lost (which also ends every subscribed stream). The scope closes before the
 * backoff, so a failed attempt never keeps its pool connection, and every retry, including one
 * after an established connection was lost, waits `listenBackoff`.
 *
 * The returned stream is one `pokes` subscription: it subscribes to the current session first and
 * emits the initial poke only then, so a commit after that poke notifies it and a commit before
 * it is covered by the pull the poke triggers (subscribe-then-poke). Without a confirmed `LISTEN`
 * it fails `ServerUnavailable` and the client reconnects with backoff. After that poke, the stream
 * also carries a `heartbeat` every `heartbeatInterval` from the session's clock (SPEC §10.1
 * Liveness), so an idle stream is never silent for longer than one interval.
 */
export const sharedPokes = (
  pg: PgClient.PgClient,
  options: {
    /** Heartbeat cadence of every subscribed stream. */
    readonly heartbeatInterval: Duration.Input
    /** Runs with each backoff delay just before its sleep starts (tests synchronize their clock on it). */
    readonly onBackoff?: ((delay: Duration.Duration) => Effect.Effect<void>) | undefined
  }
) =>
  Effect.gen(function* () {
    const current = yield* Ref.make(Option.none<ListenSession>())
    const ready = yield* Deferred.make<void>()
    const attempts = yield* Ref.make(0)
    const session = Effect.scoped(
      Effect.gen(function* () {
        const queue = yield* pg.listen(pokeChannel)
        const live: ListenSession = {
          pubsub: yield* PubSub.sliding<void>(1),
          heartbeats: yield* PubSub.sliding<void>(1),
          lost: yield* Deferred.make<never, SqlError>()
        }
        yield* Effect.forkScoped(
          Effect.forever(
            Effect.andThen(Effect.sleep(options.heartbeatInterval), PubSub.publish(live.heartbeats, undefined))
          )
        )
        yield* Ref.set(attempts, 0)
        yield* Ref.set(current, Option.some(live))
        yield* Deferred.succeed(ready, undefined)
        const error = yield* Effect.flip(
          Effect.forever(Effect.andThen(Queue.take(queue), PubSub.publish(live.pubsub, undefined)))
        )
        yield* Ref.set(current, Option.none())
        yield* Deferred.fail(live.lost, error)
        return yield* Effect.fail(error)
      })
    )
    const backoff = Effect.gen(function* () {
      yield* Deferred.succeed(ready, undefined)
      const delay = listenBackoff(yield* Ref.getAndUpdate(attempts, (n) => n + 1))
      if (options.onBackoff !== undefined) yield* options.onBackoff(delay)
      yield* Effect.sleep(delay)
    })
    yield* Effect.forkScoped(Effect.forever(Effect.catch(session, () => backoff)))
    yield* Deferred.await(ready)
    return Stream.unwrap(
      Effect.gen(function* () {
        const live = yield* Ref.get(current)
        if (Option.isNone(live)) {
          return yield* new Protocol.ServerUnavailable({ message: "LISTEN is not established" })
        }
        const notifications = yield* PubSub.subscribe(live.value.pubsub)
        const heartbeats = yield* PubSub.subscribe(live.value.heartbeats)
        if (yield* Deferred.isDone(live.value.lost)) yield* Deferred.await(live.value.lost)
        const poke: typeof Protocol.PokesElement.Type = { _tag: "poke" }
        const heartbeat: typeof Protocol.PokesElement.Type = { _tag: "heartbeat" }
        return Stream.concat(
          Stream.succeed(poke),
          Stream.merge(
            Stream.as(Stream.fromSubscription(notifications), poke),
            Stream.as(Stream.fromSubscription(heartbeats), heartbeat)
          )
        ).pipe(Stream.interruptWhen(Deferred.await(live.value.lost)))
      })
    )
  })

/**
 * The process's single `LISTEN` (`sharedPokes`), shared by the sync handlers (each `pokes` stream)
 * and the presence handlers (revocation, SPEC §10.2). Each run of the stream is one
 * subscription: an immediate `poke`, then one per committed writing transaction, plus heartbeats.
 */
export class Notifications extends Context.Service<
  Notifications,
  Stream.Stream<typeof Protocol.PokesElement.Type, Protocol.ServerUnavailable | SqlError>
>()("echo-sync/server/Notifications") {}

/** Builds `Notifications` in the layer's scope; requires `PgClient`. */
export const layerNotifications = (options: Pick<SyncServerOptions, "heartbeatInterval">) =>
  Layer.effect(
    Notifications,
    Effect.gen(function* () {
      return yield* sharedPokes(yield* PgClient.PgClient, {
        heartbeatInterval: options.heartbeatInterval ?? "15 seconds"
      })
    }).pipe(Telemetry.boundary)
  )

/**
 * Push for an authenticated `actor`, for any transport (SPEC §8): resolves the registry's
 * tables, runs each mutation through the ledger (`runBatch`) with a `Store` scoped to the actor,
 * and returns the processed prefix as a `PushResponse`. Never fails with infrastructure: a SQL
 * failure before the first mutation (catalog read) is an empty result list, and `runBatch` turns
 * one inside the batch into a short list, so a defect always means a server bug (the client halts
 * push on any defect). A client id bound to another principal, or a `request.principal` that is not
 * `actor` (SPEC §5, checked first), fails `Unauthorized`.
 */
export const executeBatch = (
  options: Pick<SyncServerOptions, "registry" | "mutations" | "ledger" | "clientPolicy">,
  actor: AccessScope.PrincipalId,
  request: Protocol.PushRequest
): Effect.Effect<
  Protocol.PushResponse,
  Protocol.Unauthorized | Protocol.MalformedEnvelope | Protocol.ClientOutdated | Protocol.ResnapshotRequired,
  SqlClient
> =>
  Effect.gen(function* () {
    yield* assertPrincipal(actor, request.principal)
    if (options.clientPolicy !== undefined) yield* options.clientPolicy(request.clientVersion)
    yield* checkEpoch(request.epoch)
    const sql = yield* SqlClient
    const tables = yield* resolveTables(options.registry)
    const rowStore = yield* RowStore.make(new Map(tables.map((t) => [t.table.name, t.schema])))
    const items = request.mutations.map((m) => ({
      mutationId: m.mutationId,
      seq: BigInt(m.seq),
      body: Effect.gen(function* () {
        // Inside the ledger body: known identities replay before any clock check.
        const serverTime = yield* DateTime.now
        const serverNow = DateTime.toEpochMillis(serverTime)
        if (m.createdAt !== undefined && m.createdAt - serverNow > Protocol.maxMutationFutureSkewMillis) {
          return yield* new Mutation.ClockSkew({ createdAt: m.createdAt, serverNow })
        }
        const now = m.createdAt === undefined ? serverTime : DateTime.makeUnsafe(m.createdAt)
        const { store } = Store.make(rowStore, {
          actor,
          roleFor: roleFor(sql, actor),
          claimScope: claimScope(sql),
          now
        })
        yield* Mutation.run(options.mutations, m, {
          actor,
          now,
          random: Mutation.seededRandom(request.clientId, m.mutationId),
          store
        })
      })
    }))
    const batch = yield* runBatch(actor, request.clientId, items, options.ledger)
    const response: Protocol.PushResponse = {
      results: batch.processed.map((p) => ({
        mutationId: Ids.MutationId.make(p.mutationId),
        position: Ids.Position.make(p.position),
        outcome: p.outcome
      }))
    }
    return response
  }).pipe(
    // Infrastructure before the first mutation (catalog read): nothing processed, a
    // transient stop. runBatch already turns infrastructure failures inside it into one.
    pushFailures,
    Telemetry.boundary
  )

// The pinned HTTP protocol preserves the request fiber's context for all handler fibers,
// including streams. Its RPC metadata headers, unlike this service, include body-supplied values.
const requestHeaders = Effect.gen(function* () {
  const request = yield* Effect.serviceOption(HttpServerRequest.HttpServerRequest)
  if (Option.isNone(request)) {
    return yield* Effect.die(new Error("echo-sync RPC handlers require layerHttp; non-HTTP mounts are unsupported"))
  }
  return request.value.headers
})

/**
 * Handlers for `Protocol.SyncRpcs` over Postgres (SPEC §8, §7).
 *
 * Pull and pokes report infrastructure failures as `ServerUnavailable`.
 * Push never fails with an infrastructure error: every SQL failure (connection, pool, `40001`
 * after the bounded retry) stops the batch as a short results list, so the client only ever sees
 * a defect for a server bug (the client halts push on any defect, INV-LOOP-3).
 */
export const handlers = (options: SyncServerOptions) =>
  Layer.unwrap(
    Effect.as(
      checkPageLimit(options.pageLimit),
      Protocol.SyncRpcs.toLayer(
        Effect.gen(function* () {
          const sql = yield* SqlClient
          const pokes = yield* Notifications
          return Protocol.SyncRpcs.of({
            push: (request) =>
              Effect.gen(function* () {
                const actor = yield* authenticate(options.principalFor)
                return yield* executeBatch(options, actor, request)
              }).pipe(
                pushFailures,
                Effect.tap((response) =>
                  Telemetry.annotate({
                    "echo.batch_size": request.mutations.length,
                    "echo.processed": response.results.length,
                    "echo.applied": response.results.filter((r) => r.outcome._tag === "Applied").length,
                    "echo.rejected": response.results.filter((r) => r.outcome._tag === "Rejected").length,
                    "echo.result": response.results.length < request.mutations.length ? "stopped" : "ok"
                  })
                ),
                Effect.provideService(SqlClient, sql),
                (work) => Telemetry.observe(work, "echo.server.push", protocolOutcome)
              ),
            pull: (request) =>
              Effect.gen(function* () {
                const principalId = yield* authenticate(options.principalFor)
                return yield* pull(
                  options.registry,
                  {
                    principalId,
                    clientId: request.clientId,
                    epoch: request.epoch,
                    clientVersion: request.clientVersion,
                    principal: request.principal,
                    cursor: request.cursor,
                    continuation: request.continuation,
                    limit: options.pageLimit ?? 500
                  },
                  options.clientPolicy
                )
              }).pipe(
                serverFailures,
                Effect.tap((response) =>
                  Telemetry.annotate({
                    "echo.result": response._tag === "PullDone" ? "done" : "page",
                    "echo.changes": response.changes.length,
                    "echo.acks": response.acks.length
                  })
                ),
                Effect.provideService(SqlClient, sql),
                (work) => Telemetry.observe(work, "echo.server.pull", protocolOutcome)
              ),
            pokes: ({ clientId, principal, epoch }) =>
              Stream.unwrap(
                Effect.gen(function* () {
                  const principalId = yield* authenticate(options.principalFor)
                  yield* assertPrincipal(principalId, principal)
                  yield* checkEpoch(epoch)
                  yield* registerClient(principalId, clientId)
                  return pokes
                }).pipe(Effect.provideService(SqlClient, sql))
              ).pipe(
                // A lost LISTEN connection ends the stream typed; the client reconnects on any end (INV-LOOP-7).
                Stream.catchCause((cause) => Stream.fromEffect(toServerFailure(cause))),
                Stream.catchTag("ClientForbidden", (e) => Stream.fail(forbidden(e))),
                (work) => Telemetry.observeStream(work, "echo.server.pokes", protocolOutcome)
              )
          })
        })
      )
    )
  )

/**
 * Handlers for `Protocol.PresenceRpcs` (SPEC §10): process-local presence, authorized like
 * reads. `room` requires a grant of any role in the room's scope (the principal's or `*`), checked
 * on join and again once the entry is installed (`Presence.join`'s `admit`); `track` only changes
 * an entry the same principal holds, so it needs no grant lookup of its own.
 *
 * Revocation (SPEC §10.2): every notification on the shared `LISTEN` (one per committed
 * writing transaction, grant changes included, plus one when each subscription starts) re-checks
 * every `(room, principal)` with an open stream against `scope_grants` in one query, and
 * `Presence.reevaluate` removes and ends `Forbidden` the ones that no longer hold the scope. A
 * notification that arrives while a check runs coalesces into one more check after it, so the
 * last committed revoke is always seen by a check that starts after it. A lost `LISTEN` or a
 * failed check restarts the watch, whose first poke re-checks everything.
 */
export const presenceHandlers = (options: Pick<SyncServerOptions, "principalFor" | "heartbeatInterval">) =>
  Protocol.PresenceRpcs.toLayer(
    Effect.gen(function* () {
      const sql = yield* SqlClient
      const notifications = yield* Notifications
      const presence = yield* Presence.make({ heartbeatInterval: options.heartbeatInterval ?? "15 seconds" })
      const holds = (
        pairs: ReadonlyArray<{ readonly room: AccessScope.AccessScopeId; readonly principalId: string }>
      ) =>
        sql<{ scope: string; principal: string }>`
          SELECT DISTINCT access_scope_id AS scope, principal_id AS principal FROM echo_sync.scope_grants
          WHERE access_scope_id IN ${sql.in([...new Set(pairs.map((p) => p.room))])}
            AND principal_id IN ${sql.in([...new Set(pairs.map((p) => p.principalId)), "*"])}`.pipe(
          Effect.map((rows) => {
            const granted = new Set(rows.map((r) => JSON.stringify([r.scope, r.principal])))
            return (room: AccessScope.AccessScopeId, principalId: string) =>
              granted.has(JSON.stringify([room, principalId])) || granted.has(JSON.stringify([room, "*"]))
          })
        )
      const watch = Stream.runForEach(
        Stream.filter(notifications, (element) => element._tag === "poke"),
        () => presence.reevaluate(holds)
      )
      yield* Effect.forkScoped(
        Effect.forever(
          Effect.catchCause(watch, (cause) =>
            Cause.hasInterrupts(cause)
              ? Effect.failCause(cause)
              : Effect.andThen(
                  Telemetry.log("Warn", "presence revocation watch restarting", cause),
                  Effect.sleep("1 second")
                )
          )
        )
      )
      const holdsRoom = (principalId: AccessScope.PrincipalId, room: AccessScope.AccessScopeId) =>
        roleFor(
          sql,
          principalId
        )(room).pipe(
          Effect.map(Option.isSome),
          Effect.catchTag("StoreUnavailable", (e) =>
            Effect.fail(new Protocol.ServerUnavailable({ message: e.message }))
          )
        )
      return Protocol.PresenceRpcs.of({
        room: (payload) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const principalId = yield* authenticate(options.principalFor)
              yield* assertPrincipal(principalId, payload.principal)
              yield* checkEpoch(payload.epoch).pipe(serverFailures)
              if (!(yield* holdsRoom(principalId, payload.room))) {
                return yield* new Protocol.Forbidden({ room: payload.room })
              }
              return presence.join({ principalId, ...payload }, holdsRoom(principalId, payload.room))
            })
          ),
        track: (payload) =>
          Effect.gen(function* () {
            const principalId = yield* authenticate(options.principalFor)
            yield* assertPrincipal(principalId, payload.principal)
            yield* checkEpoch(payload.epoch).pipe(serverFailures)
            yield* presence.track({ principalId, ...payload })
          })
      })
    }).pipe(Telemetry.boundary)
  )

/**
 * Mounts `Protocol.SyncRpcs` and `Protocol.PresenceRpcs` on `path` of the app's `HttpRouter`,
 * HTTP POST with NDJSON framing, which is what the browser's transport speaks (SPEC §10).
 * Each group's handlers are provided, not merged, so neither leaks into the app's context.
 */
export const layerHttp = (options: SyncServerOptions & { readonly path?: HttpRouter.PathInput | undefined }) =>
  RpcServer.layerHttp({
    group: Protocol.SyncRpcs.merge(Protocol.PresenceRpcs),
    path: options.path ?? "/sync",
    protocol: "http"
  }).pipe(
    // Route-scoped, before RpcServer constructs its automatic span and handler context.
    Layer.provide(HttpRouter.middleware((work) => Telemetry.boundary(work)).layer),
    Layer.provide([handlers(options), presenceHandlers(options), RpcSerialization.layerNdjson]),
    Layer.provide(layerNotifications(options))
  )
