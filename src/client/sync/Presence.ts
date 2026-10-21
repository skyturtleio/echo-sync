/**
 * The leader's presence (DESIGN §9.1 Presence, §12, §16 item 13). Each tab subscription is one upstream
 * `room` stream on the leader, kept up the way `followPokes` keeps the pokes stream up:
 *
 * - A `heartbeat` proves the connection alive and is never forwarded; only snapshots are.
 * - No element at all for `idleTimeout` ends the connection, and so does any failure, defect or clean end.
 *   The subscription then rejoins after a backoff (from `retryDelay`, doubling to `maxRetryDelay`, reset
 *   once a connection delivered a snapshot). The tab stream does not end.
 * - `track` only records the desired state, synchronously, and returns; it never waits on the network
 *   (DESIGN §9.1, 22741e2). The subscription owns delivery: every join carries the latest state, and a
 *   per-connection delivery fiber sends any newer state after the join's first snapshot, one send at a
 *   time, latest wins. Snapshots never wait for it. It lives in the connection's scope, so an idle
 *   timeout or reconnect interrupts a send that hangs; a failed send ends the connection, so the rejoin
 *   carries the state instead.
 * - Only an error `isFatal` classifies (the server's `Forbidden`: no grant in the room's scope) ends the
 *   subscription; an `Unauthorized` credential failure is rejoined like any other.
 *
 * One subscription per `(room, tabId)`: registering it is one synchronous step, and a newer one replaces
 * (and ends) an older one; its release only removes itself, never a successor. A rejoin with the same
 * key ends the older server stream (DESIGN §12); the older one is always ours and already abandoned
 * here, so that is expected. Presence never touches the replica or the outbox.
 */
import { Cause, Deferred, Duration, Effect, Exit, Queue, type Schema, Stream } from "effect"
import type { AccessScope, Protocol } from "echo-sync"

export interface Upstream<E> {
  readonly room: (payload: Join) => Stream.Stream<Protocol.RoomElement, E>
  readonly track: (payload: Join) => Effect.Effect<void, E>
}

export interface Join {
  readonly room: AccessScope.AccessScopeId
  readonly tabId: Protocol.TabId
  readonly state: Schema.Json
}

export interface Presence<F> {
  /**
   * This tab's entry in `room`, alive exactly while the returned stream runs; snapshots only. Fails only
   * with a fatal error.
   */
  readonly join: (payload: Join) => Stream.Stream<Protocol.RoomSnapshot, F>
  /** Replaces the state of `tabId`'s open subscription to `room`; a no-op without one. */
  readonly track: (payload: Join) => Effect.Effect<void>
}

interface Subscription {
  state: Schema.Json
  /** Bumped by every `track`; delivery compares it with the version it last sent (or joined with). */
  version: number
  /** Completed by `track`; replaced by the delivery fiber before each check. */
  wake: Deferred.Deferred<void>
  /** Completing it ends the current connection (the subscription then rejoins). */
  restart: Deferred.Deferred<void>
  /** Completed when a newer subscription for the same key replaces this one. */
  readonly replaced: Deferred.Deferred<void>
}

const key = (room: string, tabId: string) => JSON.stringify([room, tabId])

export const make = <E, F extends E>(options: {
  readonly upstream: Upstream<E>
  readonly isFatal: (error: E) => error is F
  /** No element (snapshot or heartbeat) for this long ends the connection. Default 45 s, as `followPokes`. */
  readonly idleTimeout?: Duration.Input | undefined
  readonly retryDelay?: Duration.Input
  readonly maxRetryDelay?: Duration.Input
}): Presence<F> => {
  const { upstream } = options
  const idleTimeout = options.idleTimeout ?? "45 seconds"
  const base = Duration.toMillis(Duration.fromInputUnsafe(options.retryDelay ?? "1 second"))
  const cap = Duration.toMillis(Duration.fromInputUnsafe(options.maxRetryDelay ?? "30 seconds"))
  const subscriptions = new Map<string, Subscription>()

  /** Sends every state newer than `sent` once `joined` completes, latest wins; ends on a failed send. */
  const deliver = (sub: Subscription, room: AccessScope.AccessScopeId, tabId: Protocol.TabId, sent: number) =>
    Effect.gen(function* () {
      while (true) {
        // Replaced before the check, so a `track` after the check always wakes the next wait.
        const wake = Deferred.makeUnsafe<void>()
        sub.wake = wake
        if (sub.version === sent) {
          yield* Deferred.await(wake)
          continue
        }
        const version = sub.version
        const failed = yield* upstream.track({ room, tabId, state: sub.state }).pipe(
          Effect.as(false),
          Effect.catch(() => Effect.succeed(true))
        )
        if (failed) return yield* Deferred.succeed(sub.restart, undefined)
        sent = version
      }
    })

  const join = ({ room, tabId, state }: Join): Stream.Stream<Protocol.RoomSnapshot, F> =>
    Stream.callback<Protocol.RoomSnapshot, F>((out) =>
      Effect.gen(function* () {
        const k = key(room, tabId)
        // Registration is one synchronous step, released on every exit, and only ever removes itself.
        const sub = yield* Effect.acquireRelease(
          Effect.sync(() => {
            const fresh: Subscription = {
              state,
              version: 0,
              wake: Deferred.makeUnsafe<void>(),
              restart: Deferred.makeUnsafe<void>(),
              replaced: Deferred.makeUnsafe<void>()
            }
            const previous = subscriptions.get(k)
            subscriptions.set(k, fresh)
            if (previous !== undefined) Deferred.doneUnsafe(previous.replaced, Effect.void)
            return fresh
          }),
          (registered) =>
            Effect.sync(() => {
              if (subscriptions.get(k) === registered) subscriptions.delete(k)
            })
        )
        let delay = base
        while (true) {
          sub.restart = Deferred.makeUnsafe<void>()
          const joinedVersion = sub.version
          const joined = Deferred.makeUnsafe<void>()
          let delivered = false
          const ended = yield* Effect.exit(
            Effect.scoped(
              Effect.gen(function* () {
                // Scoped to this connection: its end interrupts a send in flight.
                yield* Effect.forkScoped(
                  Effect.andThen(Deferred.await(joined), deliver(sub, room, tabId, joinedVersion))
                )
                yield* upstream.room({ room, tabId, state: sub.state }).pipe(
                  Stream.timeout(idleTimeout),
                  Stream.interruptWhen(Deferred.await(sub.restart)),
                  Stream.runForEach((element) =>
                    element._tag === "heartbeat"
                      ? Effect.void
                      : Effect.suspend(() => {
                          delivered = true
                          Deferred.doneUnsafe(joined, Effect.void)
                          return Queue.offer(out, element)
                        })
                  )
                )
              })
            ).pipe(Effect.raceFirst(Deferred.await(sub.replaced)))
          )
          if (Deferred.isDoneUnsafe(sub.replaced)) return yield* Queue.end(out)
          if (Exit.isFailure(ended)) {
            const fatal = ended.cause.reasons
              .filter(Cause.isFailReason)
              .map((reason) => reason.error)
              .find(options.isFatal)
            if (fatal !== undefined) return yield* Effect.fail(fatal)
          }
          if (delivered) delay = base
          yield* Effect.raceFirst(Effect.sleep(Duration.millis(delay)), Deferred.await(sub.replaced))
          if (Deferred.isDoneUnsafe(sub.replaced)) return yield* Queue.end(out)
          delay = Math.min(delay * 2, cap)
        }
      })
    )

  // Records the desired state in one synchronous step; the subscription's delivery fiber sends it.
  const track = ({ room, tabId, state }: Join): Effect.Effect<void> =>
    Effect.sync(() => {
      const sub = subscriptions.get(key(room, tabId))
      if (sub === undefined) return
      sub.state = state
      sub.version++
      Deferred.doneUnsafe(sub.wake, Effect.void)
    })

  return { join, track }
}
