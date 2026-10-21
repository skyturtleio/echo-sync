/**
 * The leader's presence (DESIGN §9.1 Presence, §12, §16 item 13). Each tab subscription is one upstream
 * `room` stream on the leader, kept up the way `followPokes` keeps the pokes stream up:
 *
 * - A `heartbeat` proves the connection alive and is never forwarded; only snapshots are.
 * - No element at all for `idleTimeout` ends the connection, and so does any failure, defect or clean end.
 *   The subscription then rejoins after a backoff (from `retryDelay`, doubling to `maxRetryDelay`, reset
 *   once a connection delivered a snapshot). The tab stream does not end.
 * - Every join carries the latest desired state: the one from the subscription's last `track`, not the
 *   state it opened with. `track`'s ack cannot confirm application (a `track` that races a join is a
 *   no-op on the server, or lands on an entry the join then replaces), so after the first snapshot of
 *   every connection, a state that changed since the join was read is sent again. `track` and that
 *   catch-up are serialized, so the server sees states in order.
 * - A failed `track` ends the connection, so the rejoin carries the state instead.
 * - Only an error `isFatal` classifies (the server's `Unauthorized`: no grant in the room's scope) ends
 *   the subscription.
 *
 * A rejoin with the same `(clientId, tabId)` key ends the older server stream (DESIGN §12); the older
 * one is always ours and already abandoned here, so that is expected. Presence never touches the
 * replica or the outbox.
 */
import { Cause, Deferred, Duration, Effect, Exit, Queue, Semaphore, type Schema, Stream } from "effect"
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
  /** Bumped by every `track`; a join compares it to see whether its state is still the latest. */
  version: number
  readonly lock: Semaphore.Semaphore
  /** Completing it ends the current connection (the subscription then rejoins). */
  restart: Deferred.Deferred<void>
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

  const send = (sub: Subscription, room: AccessScope.AccessScopeId, tabId: Protocol.TabId) =>
    upstream.track({ room, tabId, state: sub.state }).pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause) ? Effect.interrupt : Deferred.succeed(sub.restart, undefined)
      ),
      Effect.asVoid
    )

  const join = ({ room, tabId, state }: Join): Stream.Stream<Protocol.RoomSnapshot, F> =>
    Stream.callback<Protocol.RoomSnapshot, F>((out) =>
      Effect.gen(function* () {
        const sub: Subscription = {
          state,
          version: 0,
          lock: yield* Semaphore.make(1),
          restart: Deferred.makeUnsafe<void>()
        }
        const k = key(room, tabId)
        subscriptions.set(k, sub)
        yield* Effect.addFinalizer(() => Effect.sync(() => subscriptions.get(k) === sub && subscriptions.delete(k)))
        let delay = base
        while (true) {
          sub.restart = Deferred.makeUnsafe<void>()
          const joined = { state: sub.state, version: sub.version }
          let delivered = false
          const ended = yield* Effect.exit(
            upstream.room({ room, tabId, state: joined.state }).pipe(
              Stream.timeout(idleTimeout),
              Stream.interruptWhen(Deferred.await(sub.restart)),
              Stream.runForEach((element) =>
                element._tag === "heartbeat"
                  ? Effect.void
                  : Effect.gen(function* () {
                      if (!delivered) {
                        delivered = true
                        // The entry exists now: a state tracked since the join read it is applied from here.
                        yield* sub.lock.withPermit(
                          Effect.suspend(() => (sub.version === joined.version ? Effect.void : send(sub, room, tabId)))
                        )
                      }
                      yield* Queue.offer(out, element)
                    })
              )
            )
          )
          if (Exit.isFailure(ended)) {
            const fatal = ended.cause.reasons
              .filter(Cause.isFailReason)
              .map((reason) => reason.error)
              .find(options.isFatal)
            if (fatal !== undefined) return yield* Effect.fail(fatal)
          }
          if (delivered) delay = base
          yield* Effect.sleep(Duration.millis(delay))
          delay = Math.min(delay * 2, cap)
        }
      })
    )

  const track = ({ room, tabId, state }: Join): Effect.Effect<void> =>
    Effect.suspend(() => {
      const sub = subscriptions.get(key(room, tabId))
      if (sub === undefined) return Effect.void
      return sub.lock.withPermit(
        Effect.suspend(() => {
          sub.state = state
          sub.version++
          return send(sub, room, tabId)
        })
      )
    })

  return { join, track }
}
