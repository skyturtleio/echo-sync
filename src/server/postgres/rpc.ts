import * as PgClient from "@effect/sql-pg/PgClient"
import { Cause, DateTime, Deferred, Duration, Effect, Layer, Option, PubSub, Queue, Ref, Schema, Stream } from "effect"
import type { Headers } from "effect/http/Headers"
import type * as HttpRouter from "effect/http/HttpRouter"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { SqlClient } from "effect/sql/SqlClient"
import { isSqlError, type SqlError } from "effect/sql/SqlError"
import { AccessScope, Ids, Mutation, Protocol, type Registry, Store } from "echo-sync"
import { type ClientForbidden, type LedgerOptions, registerClient, runBatch } from "./ledger.ts"
import { pull } from "./pull.ts"
import * as RowStore from "./rowStore.ts"
import { resolveTables } from "./schema.ts"

export interface SyncServerOptions {
  readonly registry: Registry.SyncRegistry
  readonly mutations: Mutation.MutationRegistry
  /** Authenticates the request (DESIGN §4). The principal is never taken from the payload. */
  readonly principalFor: (headers: Headers) => Effect.Effect<AccessScope.PrincipalId, Protocol.Unauthorized>
  /** Upserts and tombstones per pull page. Default 500. */
  readonly pageLimit?: number | undefined
  readonly ledger?: LedgerOptions | undefined
  /**
   * Cadence of the `heartbeat` element on every open `pokes` stream (DESIGN §6 Liveness), so a
   * proxy or the client's `idleTimeout` never sees a live stream as idle. Default 15 s; must stay
   * below the clients' `idleTimeout` (default 45 s).
   */
  readonly heartbeatInterval?: Duration.Input | undefined
}

/** `notify` channel of `echo_sync.next_position()`: one hint per committed writing transaction. */
export const pokeChannel = "echo_sync"

const forbidden = (error: ClientForbidden) =>
  new Protocol.Unauthorized({ message: `client ${error.clientId} belongs to another principal` })

const serverUnavailable = (error: SqlError) =>
  new Protocol.ServerUnavailable({ message: `${error.reason._tag}: ${error.message}` })

const isNotSql = <E>(error: E | SqlError): error is Exclude<E, SqlError> => !isSqlError(error)

/**
 * Whole-cause classification of a handler failure, precedence defect > transient > typed (DESIGN
 * §6). A `SqlError` may arrive as a failure or, from the pinned client's `orDie` rollback on a
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
 * The authoritative scope claim (DESIGN §4): inserts the `access_scopes` row inside the mutation's
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
 * The process's single `LISTEN` on `pokeChannel` (DESIGN §6). `PgClient.listen` reserves a pool
 * connection for its scope, so one per `pokes` stream would exhaust the pool at `poolSize`
 * connected clients and block every push and pull. Lives in the caller's (the handlers layer's)
 * scope and returns once the first attempt has settled, so a server normally serves with its
 * `LISTEN` already in place.
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
 * also carries a `heartbeat` every `heartbeatInterval` from the session's clock (DESIGN §6
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
 * Handlers for `Protocol.SyncRpcs` over Postgres (DESIGN §6, §8).
 *
 * Pull and pokes report infrastructure failures as `ServerUnavailable`.
 * Push never fails with an infrastructure error: every SQL failure (connection, pool, `40001`
 * after the bounded retry) stops the batch as a short results list, so the client only ever sees
 * a defect for a server bug (the client halts push on any defect, §6).
 */
export const handlers = (options: SyncServerOptions) =>
  Protocol.SyncRpcs.toLayer(
    Effect.gen(function* () {
      const sql = yield* SqlClient
      const pokes = yield* sharedPokes(yield* PgClient.PgClient, {
        heartbeatInterval: options.heartbeatInterval ?? "15 seconds"
      })
      return Protocol.SyncRpcs.of({
        push: (request, { headers }) =>
          Effect.gen(function* () {
            const actor = yield* options.principalFor(headers)
            const tables = yield* resolveTables(options.registry)
            const rowStore = yield* RowStore.make(new Map(tables.map((t) => [t.table.name, t.schema])))
            const items = request.mutations.map((m) => ({
              mutationId: m.mutationId,
              seq: BigInt(m.seq),
              body: Effect.gen(function* () {
                const now = yield* DateTime.now
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
            Effect.catchCause((cause) => {
              const verdict = classifyCause(cause)
              return verdict._tag === "unavailable"
                ? Effect.succeed<Protocol.PushResponse>({ results: [] })
                : Effect.failCause(verdict.cause)
            }),
            Effect.catchTag("ClientForbidden", (e) => Effect.fail(forbidden(e))),
            Effect.provideService(SqlClient, sql)
          ),
        pull: (request, { headers }) =>
          Effect.gen(function* () {
            const principalId = yield* options.principalFor(headers)
            return yield* pull(options.registry, {
              principalId,
              clientId: request.clientId,
              cursor: request.cursor,
              continuation: request.continuation,
              limit: options.pageLimit ?? 500
            })
          }).pipe(
            Effect.catchCause(toServerFailure),
            Effect.catchTag("ClientForbidden", (e) => Effect.fail(forbidden(e))),
            Effect.provideService(SqlClient, sql)
          ),
        pokes: ({ clientId }, { headers }) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const principalId = yield* options.principalFor(headers)
              yield* registerClient(principalId, clientId)
              return pokes
            }).pipe(Effect.provideService(SqlClient, sql))
          ).pipe(
            // A lost LISTEN connection ends the stream typed; the client reconnects on any end (§6).
            Stream.catchCause((cause) => Stream.fromEffect(toServerFailure(cause))),
            Stream.catchTag("ClientForbidden", (e) => Stream.fail(forbidden(e)))
          )
      })
    })
  )

/**
 * Mounts `Protocol.SyncRpcs` on `path` of the app's `HttpRouter`, HTTP POST with NDJSON framing,
 * which is what the browser's transport speaks (DESIGN §6).
 */
export const layerHttp = (options: SyncServerOptions & { readonly path?: HttpRouter.PathInput | undefined }) =>
  RpcServer.layerHttp({ group: Protocol.SyncRpcs, path: options.path ?? "/sync", protocol: "http" }).pipe(
    Layer.provide([handlers(options), RpcSerialization.layerNdjson])
  )
