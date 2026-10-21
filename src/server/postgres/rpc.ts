import * as PgClient from "@effect/sql-pg/PgClient"
import { Cause, DateTime, Effect, Layer, Option, Schema, Stream } from "effect"
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
const roleFor = (sql: SqlClient, actor: string) => (scopeId: AccessScope.AccessScopeId) =>
  Effect.gen(function* () {
    const rows = yield* sql<{ role: unknown }>`
      SELECT role FROM echo_sync.scope_grants
      WHERE access_scope_id = ${scopeId} AND principal_id IN (${actor}, '*')`.pipe(
      Effect.mapError(RowStore.unavailable)
    )
    return Option.fromUndefinedOr(AccessScope.strongest(rows.flatMap((r) => Option.toArray(decodeRole(r.role)))))
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
      const pg = yield* PgClient.PgClient
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
                const { store } = Store.make(rowStore, { actor, roleFor: roleFor(sql, actor) })
                yield* Mutation.run(options.mutations, m, {
                  actor,
                  now: yield* DateTime.now,
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
              // listen returns only after Postgres confirms LISTEN, so every commit after this
              // point notifies. The initial poke makes the client pull now, covering every commit
              // before it: subscribe-then-poke leaves no reconnect gap (DESIGN §6).
              const notifications = yield* pg.listen(pokeChannel)
              return Stream.concat(
                Stream.succeed<typeof Protocol.Poke.Type>({}),
                Stream.fromQueue(notifications).pipe(Stream.map((): typeof Protocol.Poke.Type => ({})))
              )
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
