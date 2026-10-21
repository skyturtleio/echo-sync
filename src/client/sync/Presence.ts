/**
 * The leader's presence (DESIGN §9.1 Presence, §12, §16 item 13). Each tab subscription is one upstream
 * `room` stream on the leader, kept up the way `followPokes` keeps the pokes stream up:
 *
 * - A `heartbeat` proves the connection alive and is never forwarded; only snapshots are.
 * - No element at all for `idleTimeout` ends the connection, and so does any failure, defect or clean end.
 *   The subscription then rejoins after a backoff (from `retryDelay`, doubling to `maxRetryDelay`, reset
 *   once a connection delivered a snapshot). The tab stream does not end.
 * - `track` only records the desired state, synchronously, and returns; it never waits on the network
 *   (DESIGN §9.1, 07a8f48). The subscription owns delivery: every join carries the latest state, and a
 *   supervised sender per connection sends any newer state after the join's first snapshot, one send at
 *   a time, latest wins. Each send has the `idleTimeout` deadline; one that fails, dies or times out is
 *   retried with backoff, never by ending the connection, so a healthy room stream never strands a state.
 *   Snapshots never wait for a send; the connection's end (idle timeout, reconnect, close) interrupts it.
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
  /**
   * Prepare-then-stream, like `SyncTransport.pokes`: the Effect resolves this join's credentials (DESIGN
   * §9.1) and the stream sends the request when run, so `idleTimeout` covers only the stream.
   */
  readonly room: (payload: Join) => Effect.Effect<Stream.Stream<Protocol.RoomElement, E>, E>
  readonly track: (payload: Join) => Effect.Effect<void, E>
}

export interface Join {
  readonly room: AccessScope.AccessScopeId
  readonly tabId: Protocol.TabId
  readonly state: Schema.Json
  /** The tab's revision of `state` (DESIGN §9.1, §12 revision fencing); the leader never assigns one. */
  readonly revision: number
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
  /** The highest revision accepted (the tab's); delivery compares it with the one it last sent or joined with. */
  revision: number
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

  /**
   * The connection's supervised sender (DESIGN §9.1, 07a8f48): sends every state newer than `sent`, one at
   * a time, latest wins. Each attempt has the `idleTimeout` deadline; an attempt that fails, dies or runs
   * out of time is retried with backoff, with the latest state. It never ends or restarts the connection:
   * a healthy room stream must not hold a stranded state back. Only the connection's scope ends it.
   */
  const deliver = (sub: Subscription, room: AccessScope.AccessScopeId, tabId: Protocol.TabId, sent: number) =>
    Effect.gen(function* () {
      let delay = base
      while (true) {
        // Replaced before the check, so a `track` after the check always wakes the next wait.
        const wake = Deferred.makeUnsafe<void>()
        sub.wake = wake
        if (sub.revision <= sent) {
          yield* Deferred.await(wake)
          continue
        }
        // Read together: the pair is what the server fences on. A timed-out attempt may still land later;
        // its lower revision makes it a no-op there, so no attempt is ever waited for.
        const { state, revision } = sub
        const attempt = yield* Effect.exit(
          upstream.track({ room, tabId, state, revision }).pipe(Effect.timeout(idleTimeout))
        )
        if (Exit.isSuccess(attempt)) {
          sent = revision
          delay = base
          continue
        }
        yield* Effect.logDebug(`echo-sync: presence track failed; retrying in ${delay} ms`, attempt.cause)
        yield* Effect.sleep(Duration.millis(delay))
        delay = Math.min(delay * 2, cap)
      }
    })

  const join = ({ room, tabId, state, revision }: Join): Stream.Stream<Protocol.RoomSnapshot, F> =>
    Stream.callback<Protocol.RoomSnapshot, F>((out) =>
      Effect.gen(function* () {
        const k = key(room, tabId)
        // Registration is one synchronous step, released on every exit, and only ever removes itself.
        const sub = yield* Effect.acquireRelease(
          Effect.sync(() => {
            const fresh: Subscription = {
              state,
              revision,
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
          const joinedRevision = sub.revision
          const joined = Deferred.makeUnsafe<void>()
          let delivered = false
          const ended = yield* Effect.exit(
            Effect.scoped(
              Effect.gen(function* () {
                // Scoped to this connection: its end interrupts a send in flight, and the next join's
                // sender starts from the state that join carried.
                yield* Effect.forkScoped(
                  Effect.andThen(Deferred.await(joined), deliver(sub, room, tabId, joinedRevision))
                )
                // Preparation (credentials) runs before the idle clock starts; a failure there is retried.
                const connection = yield* upstream.room({ room, tabId, state: sub.state, revision: joinedRevision })
                yield* connection.pipe(
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

  // Records the desired state in one synchronous step; the subscription's delivery fiber sends it. Only a
  // higher revision is accepted, so the leader never sends a lower revision after a higher one.
  const track = ({ room, tabId, state, revision }: Join): Effect.Effect<void> =>
    Effect.sync(() => {
      const sub = subscriptions.get(key(room, tabId))
      if (sub === undefined || revision <= sub.revision) return
      sub.state = state
      sub.revision = revision
      Deferred.doneUnsafe(sub.wake, Effect.void)
    })

  return { join, track }
}
