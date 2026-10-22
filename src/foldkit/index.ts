/**
 * FoldKit adapter (DESIGN §16.17): a thin translation of the `echo-sync/client` tab-side API (DESIGN §9.1)
 * into the two shapes FoldKit runs, with no sync semantics of its own.
 *
 * - Streams (`subscribe`, `syncStatus`, `notices`, `status`) are `Stream<message, never, R>`, for a
 *   `Subscription` entry's `dependenciesToStream` (or `Subscription.persistentEntry`). Each upstream
 *   emission becomes one message, value unchanged: handover, re-subscription and "keep the last emission"
 *   are the tab client's, so the adapter neither dedups, derives nor invents states.
 * - Effects (`mutate`, `requestSync`, `dismissNotices`) are `Effect<message, never, R>`, for a `Command`'s
 *   `execute`. `mutate`'s message exists only once `LeaderClient.mutate` completed, after the leader's commit.
 *
 * The client is passed as an Effect source: a FoldKit `ManagedResource`'s `.get`, or the `LeaderClient`
 * service itself. Its error (e.g. `ResourceNotAvailable`) is delivered like any other.
 *
 * Failures never reach FoldKit's runtime, which crashes the application on any failed Subscription or
 * Command. A typed error becomes `onFailure({ _tag: "Error", error })`; a defect (also one alongside a typed
 * error) becomes
 * `onFailure({ _tag: "Defect" })`, carrying nothing of the cause and logging nothing (a cause can hold header
 * values, URLs or SQLite text; DESIGN §9.1, §16.20). An interruption-only cause stays an interruption, so
 * FoldKit tearing a Subscription or Command down never yields a message.
 *
 * Termination: `onFailure` is the terminal element of an adapter stream. The tab client already absorbs
 * `LeaderLost` (re-subscription, `mutate`/`requestSync`/`dismissNotices` retry); any error it lets through
 * ends its stream (DESIGN §9.1), and the adapter never retries or re-subscribes on its own. Re-opening is the
 * app's decision, e.g. by changing the Subscription's dependencies. This module imports nothing from
 * `foldkit`.
 */
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import type { Mutation, Query } from "echo-sync"
import type { LeaderClient, LeaderRpcs } from "echo-sync/client"

/** The tab-side surface an adapter may use; `enqueue`, `exec` and `subscribeRaw` are not part of it. */
export type TabClient = Pick<
  LeaderClient.LeaderClient["Service"],
  "mutate" | "subscribe" | "notices" | "dismissNotices" | "status" | "syncStatus" | "requestSync"
>

/**
 * Where the client comes from: `LeaderClient.LeaderClient`, or a `ManagedResource`'s `.get`. A source is a
 * lookup: the client's lifetime is owned elsewhere, and anything the source itself scopes is released as soon
 * as the client is obtained, inside the adapter's failure translation.
 */
export type ClientSource<E, R> = Effect.Effect<TabClient, E, R>

/** A typed error of the call (or of the client source), or a defect, whose content is never carried. */
export type Failure<E> = { readonly _tag: "Error"; readonly error: E } | { readonly _tag: "Defect" }

/** Two message types, so a FoldKit `Command`'s `messages` stay exactly the two the handlers produce. */
export interface StreamHandlers<A, E, V, F> {
  readonly onValue: (value: A) => V
  /** The terminal element: delivered at most once, then the stream ends. */
  readonly onFailure: (failure: Failure<E>) => F
}

export interface EffectHandlers<A, E, V, F> {
  readonly onSuccess: (value: A) => V
  readonly onFailure: (failure: Failure<E>) => F
}

const recover = <E, F>(onFailure: (failure: Failure<E>) => F, cause: Cause.Cause<E>): Effect.Effect<F> => {
  if (Cause.hasInterruptsOnly(cause)) return Effect.interrupt
  // Any defect wins over a typed error in the same cause: a defect must never hide behind an ordinary error.
  const error = Cause.hasDies(cause) ? Option.none() : Cause.findErrorOption(cause)
  return Effect.succeed(onFailure(Option.isSome(error) ? { _tag: "Error", error: error.value } : { _tag: "Defect" }))
}

const watch = <A, E, R, V, F>(
  stream: Stream.Stream<A, E, R>,
  h: StreamHandlers<A, E, V, F>
): Stream.Stream<V | F, never, R> =>
  stream.pipe(
    Stream.map(h.onValue),
    Stream.catchCause((cause) => Stream.fromEffect(recover(h.onFailure, cause)))
  )

const run = <A, E, R, V, F>(
  effect: Effect.Effect<A, E, R>,
  h: EffectHandlers<A, E, V, F>
): Effect.Effect<V | F, never, R> =>
  effect.pipe(
    Effect.map(h.onSuccess),
    Effect.catchCause((cause) => recover(h.onFailure, cause))
  )

type Stream_<K extends "syncStatus" | "notices" | "status"> = TabClient[K]

/** A registered query's live result (DESIGN §9.1 `subscribe`); keeps its last value across handover. */
export const subscribe = <CE, R, A extends Mutation.PureSchema, S extends Mutation.PureSchema, V, F>(
  client: ClientSource<CE, R>,
  query: Query.Query<string, A, S>,
  args: A["Type"],
  h: StreamHandlers<S["Type"], LeaderClient.SubscribeError | CE, V, F>
): Stream.Stream<V | F, never, R> =>
  watch(Stream.unwrap(Effect.map(Effect.scoped(client), (c) => c.subscribe(query, args))), h)

/** The leader's `SyncStatus`, current value then every change; survives handover. */
export const syncStatus = <CE, R, V, F>(
  client: ClientSource<CE, R>,
  h: StreamHandlers<Stream.Success<Stream_<"syncStatus">>, Stream.Error<Stream_<"syncStatus">> | CE, V, F>
): Stream.Stream<V | F, never, R> => watch(Stream.unwrap(Effect.map(Effect.scoped(client), (c) => c.syncStatus)), h)

/** The retained notices, current list then every change; decode them with `Mutation.decodeNotice`. */
export const notices = <CE, R, V, F>(
  client: ClientSource<CE, R>,
  h: StreamHandlers<ReadonlyArray<LeaderRpcs.StoredNotice>, Stream.Error<Stream_<"notices">> | CE, V, F>
): Stream.Stream<V | F, never, R> => watch(Stream.unwrap(Effect.map(Effect.scoped(client), (c) => c.notices)), h)

/** This tab's topology (`mode`, `isLeaderTab`, `leaderId`); `mode: "unsupported"` is a state to render. */
export const status = <CE, R, V, F>(
  client: ClientSource<CE, R>,
  h: StreamHandlers<LeaderClient.Status, CE, V, F>
): Stream.Stream<V | F, never, R> => watch(Stream.unwrap(Effect.map(Effect.scoped(client), (c) => c.status)), h)

/** The one write: resolves after the leader committed the optimistic apply and the outbox entry. */
export const mutate = <CE, R, A extends Mutation.PureSchema, E extends Mutation.PureSchema, V, F>(
  client: ClientSource<CE, R>,
  mutation: Mutation.Mutation<string, A, E>,
  args: A["Type"],
  h: EffectHandlers<LeaderRpcs.Enqueued, Effect.Error<ReturnType<TabClient["mutate"]>> | CE, V, F>
): Effect.Effect<V | F, never, R> =>
  run(
    Effect.flatMap(Effect.scoped(client), (c) => c.mutate(mutation, args)),
    h
  )

/** Wake the sync loop; success means the wake was accepted, never that the network succeeded. */
export const requestSync = <CE, R, V, F>(
  client: ClientSource<CE, R>,
  h: EffectHandlers<void, Effect.Error<TabClient["requestSync"]> | CE, V, F>
): Effect.Effect<V | F, never, R> =>
  run(
    Effect.flatMap(Effect.scoped(client), (c) => c.requestSync),
    h
  )

/** The only way a notice is removed (DESIGN §9.1). */
export const dismissNotices = <CE, R, V, F>(
  client: ClientSource<CE, R>,
  ids: ReadonlyArray<number>,
  h: EffectHandlers<void, Effect.Error<ReturnType<TabClient["dismissNotices"]>> | CE, V, F>
): Effect.Effect<V | F, never, R> =>
  run(
    Effect.flatMap(Effect.scoped(client), (c) => c.dismissNotices(ids)),
    h
  )
