import { Deferred, type Duration, Effect, PubSub, Ref, type Scope, Semaphore, Stream } from "effect"
import { type AccessScope, Protocol } from "echo-sync"

/** An entry, the room it is in, and the revision of its state (SPEC §10 revision fencing). */
export interface Joined extends Protocol.PresenceEntry {
  readonly room: AccessScope.AccessScopeId
  readonly revision: number
}

/** One open `room` stream's entry. */
interface Member {
  readonly entry: Protocol.PresenceEntry
  /** The revision `entry.state` was installed with; a `track` must exceed it. */
  readonly revision: number
  /**
   * Ends this member's stream: succeeded when a later join with the same key replaces the entry,
   * failed `Forbidden` when its principal no longer holds the room's scope. Also its identity: an
   * ended stream's release must not remove its successor.
   */
  readonly ended: Deferred.Deferred<void, Protocol.Forbidden>
}

interface Room {
  readonly members: ReadonlyMap<string, Member>
  /** One tick per change; ticks carry nothing, so a slow observer's ticks coalesce. */
  readonly changes: PubSub.PubSub<void>
}

/**
 * The entry key. SPEC §10 keys entries by `(clientId, tabId)`; the principal is part of the key
 * too, so a caller naming another principal's client id can neither replace nor end that entry.
 */
const keyOf = (principalId: string, clientId: string, tabId: string) => JSON.stringify([principalId, clientId, tabId])

const entryOf = (joined: Joined): Protocol.PresenceEntry => ({
  principalId: joined.principalId,
  clientId: joined.clientId,
  tabId: joined.tabId,
  state: joined.state
})

const snapshotOf = (
  all: ReadonlyMap<AccessScope.AccessScopeId, Room>,
  room: AccessScope.AccessScopeId
): Protocol.RoomSnapshot => ({
  _tag: "snapshot",
  room,
  entries: [...(all.get(room)?.members.values() ?? [])].map((m) => m.entry).toSorted(byKey)
})

const heartbeat: Protocol.RoomElement = { _tag: "heartbeat" }

const byKey = (a: Protocol.PresenceEntry, b: Protocol.PresenceEntry) =>
  a.principalId < b.principalId
    ? -1
    : a.principalId > b.principalId
      ? 1
      : a.clientId < b.clientId
        ? -1
        : a.clientId > b.clientId
          ? 1
          : a.tabId < b.tabId
            ? -1
            : a.tabId > b.tabId
              ? 1
              : 0

/**
 * Process-local presence state (SPEC §10): a `Ref` of rooms plus a `PubSub` per room. A room
 * exists while it has an entry. Authorization is the caller's (the RPC handlers).
 *
 * Every `join` stream also carries a `heartbeat` every `heartbeatInterval` (DECISIONS 13).
 * Heartbeats come from the stream's own timer, merged beside the room's change subscription, so
 * a heartbeat can never slide out a pending change tick, and they read and change no room state.
 */
export const make = (options: { readonly heartbeatInterval: Duration.Input }) =>
  Effect.gen(function* () {
    const rooms = yield* Ref.make<ReadonlyMap<AccessScope.AccessScopeId, Room>>(new Map())
    /**
     * Installation gate: `reevaluate` holds it from reading the members through removing them, and
     * `enter` holds it only for the install itself (waiting for it is interruptible). So no entry is
     * installed between a check's capture and its removal: every entry of a revoked principal is
     * either captured, and removed in that one step, or installed after the removal, where its own
     * `admit` reads the grant after the check's. Never held across `admit` or any stream I/O.
     */
    const gate = yield* Semaphore.make(1)

    const snapshot = (room: AccessScope.AccessScopeId) => Effect.map(Ref.get(rooms), (all) => snapshotOf(all, room))

    /**
     * Adds the entry for the scope's lifetime. A later join with the same key replaces it and ends
     * the earlier stream (its `ended`), so no open stream is ever left without an entry: every
     * open stream keeps its room, and with it the room's `PubSub`, alive. Otherwise the replacing
     * stream could leave as the room's last member, the room would be recreated on a new `PubSub`,
     * and the replaced stream would observe the abandoned one forever.
     *
     * A join whose revision is lower than the stored entry's is a superseded attempt (SPEC §10
     * revision fencing): nothing is installed, the entry and its stream are untouched, and the result
     * is `undefined`.
     */
    const enter = (
      joined: Joined
    ): Effect.Effect<
      | { readonly changes: PubSub.PubSub<void>; readonly ended: Deferred.Deferred<void, Protocol.Forbidden> }
      | undefined,
      never,
      Scope.Scope
    > => {
      const key = keyOf(joined.principalId, joined.clientId, joined.tabId)
      const entry = entryOf(joined)
      const install = Effect.acquireRelease(
        Effect.gen(function* () {
          const fresh = yield* PubSub.sliding<void>(1)
          const ended = yield* Deferred.make<void, Protocol.Forbidden>()
          const installed = yield* Ref.modify(rooms, (all) => {
            const room = all.get(joined.room) ?? { members: new Map(), changes: fresh }
            const stored = room.members.get(key)
            if (stored !== undefined && joined.revision < stored.revision) return [undefined, all] as const
            const members = new Map(room.members).set(key, { entry, revision: joined.revision, ended })
            return [
              [room.changes, room.members.get(key)] as const,
              new Map(all).set(joined.room, { members, changes: room.changes })
            ] as const
          })
          if (installed === undefined) return undefined
          const [changes, previous] = installed
          if (previous !== undefined) yield* Deferred.succeed(previous.ended, undefined)
          return { changes, ended }
        }),
        (entered) => {
          if (entered === undefined) return Effect.void
          const { changes, ended } = entered
          return Ref.modify(rooms, (all) => {
            const room = all.get(joined.room)
            // A later stream with the same key took the entry over, or a revocation already removed
            // it: it is not this stream's to drop.
            if (room?.members.get(key)?.ended !== ended) return [false, all] as const
            const members = new Map(room.members)
            members.delete(key)
            const next = new Map(all)
            if (members.size === 0) next.delete(joined.room)
            else next.set(joined.room, { members, changes: room.changes })
            return [true, next] as const
          }).pipe(Effect.flatMap((left) => (left ? PubSub.publish(changes, undefined) : Effect.void)))
        }
      )
      // Only the install waits for the gate; the release (a removal) never conflicts with a check.
      return gate.withPermit(install)
    }

    return {
      /**
       * The entry is present exactly while the returned stream runs: added before its first
       * snapshot, removed when it ends for any reason (completion, failure, interruption,
       * revocation). The first snapshot already contains it; every later change produces another,
       * and a snapshot read after the entry's removal is never emitted to its stream. A superseded
       * join (revision lower than the stored entry's) ends at once without an element and changes
       * nothing.
       *
       * `admit` runs once the entry is installed, before anyone is told of it: `false` ends the
       * stream `Forbidden` and removes the entry. It closes the race with `reevaluate`: an entry
       * installed after a check (the gate) reads the grant after that check did. A revocation or
       * replacement while `admit` is pending ends the stream without waiting for it.
       */
      join: <E>(
        joined: Joined,
        admit: Effect.Effect<boolean, E> = Effect.succeed(true)
      ): Stream.Stream<Protocol.RoomElement, Protocol.Forbidden | E> =>
        Stream.unwrap(
          Effect.gen(function* () {
            const entered = yield* enter(joined)
            if (entered === undefined) return Stream.empty
            const { changes, ended } = entered
            const key = keyOf(joined.principalId, joined.clientId, joined.tabId)
            // Everything after the install runs inside `ended`'s boundary, `admit` included: a
            // revocation or replacement while `admit` is pending ends the stream at once (with
            // `Forbidden`, or empty), and a later outcome of `admit` can no longer surface.
            return Stream.unwrap(
              Effect.gen(function* () {
                const subscription = yield* PubSub.subscribe(changes)
                // `interruptWhen` only races a signal; one that already completed must win over
                // whatever `admit` does next: revoked ⇒ `Forbidden`, replaced ⇒ empty.
                const endedAlready = Effect.flatMap(Deferred.isDone(ended), (done) =>
                  done ? Effect.as(Deferred.await(ended), true) : Effect.succeed(false)
                )
                if (yield* endedAlready) return Stream.empty
                const admitted = yield* Effect.exit(admit)
                if (yield* endedAlready) return Stream.empty
                if (!(yield* admitted)) return yield* new Protocol.Forbidden({ room: joined.room })
                // Only while this stream's own entry is in the room: a snapshot read after its
                // removal is never emitted to it (one already read before may still be delivered).
                const own = Effect.map(Ref.get(rooms), (all) =>
                  all.get(joined.room)?.members.get(key)?.ended === ended ? [snapshotOf(all, joined.room)] : []
                )
                // Subscribed before announcing: the other streams see the join, and this stream
                // consumes the tick itself, so its first element is the join snapshot, emitted
                // before the merge (and with it the heartbeat timer) starts. A change after this
                // take ticks again.
                yield* PubSub.publish(changes, undefined)
                yield* PubSub.take(subscription)
                return Stream.concat(
                  Stream.flattenIterable(Stream.fromEffect(own)),
                  Stream.flattenIterable(Stream.mapEffect(Stream.fromSubscription(subscription), () => own)).pipe(
                    Stream.merge(Stream.fromEffectRepeat(Effect.as(Effect.sleep(options.heartbeatInterval), heartbeat)))
                  )
                )
              })
            ).pipe(Stream.interruptWhen(Deferred.await(ended)))
          })
        ),

      /**
       * Revocation (DECISIONS 13): asks `holds` which `(room, principal)` pairs with an open
       * stream still hold the room's scope, then, for each pair that does not, removes every entry
       * of that principal in that room in one step, ends each of those streams `Forbidden`, and
       * announces the removal once. The installation gate is held throughout, so no entry is
       * installed between the capture and the removal: a principal's entries all go in one step, and
       * an entry installed after the check (say, after a re-grant the check's read predates) is
       * judged by its own `admit`, never by this check. Only captured entries are removed.
       */
      reevaluate: <E>(
        holds: (
          pairs: ReadonlyArray<{ readonly room: AccessScope.AccessScopeId; readonly principalId: string }>
        ) => Effect.Effect<(room: AccessScope.AccessScopeId, principalId: string) => boolean, E>
      ) =>
        gate.withPermit(
          Effect.gen(function* () {
            const seen = yield* Ref.get(rooms)
            const pairs = [...seen].flatMap(([room, { members }]) =>
              [...new Set([...members.values()].map((m) => m.entry.principalId))].map((principalId) => ({
                room,
                principalId
              }))
            )
            if (pairs.length === 0) return
            const held = yield* holds(pairs)
            for (const { room, principalId } of pairs) {
              if (held(room, principalId)) continue
              const asked = new Set(
                [...(seen.get(room)?.members.values() ?? [])]
                  .filter((m) => m.entry.principalId === principalId)
                  .map((m) => m.ended)
              )
              yield* Effect.uninterruptible(
                Effect.gen(function* () {
                  const removed = yield* Ref.modify(rooms, (all) => {
                    const current = all.get(room)
                    if (current === undefined) return [undefined, all] as const
                    const members = new Map(current.members)
                    const gone: Array<Member> = []
                    for (const [key, member] of current.members) {
                      if (asked.has(member.ended)) {
                        members.delete(key)
                        gone.push(member)
                      }
                    }
                    if (gone.length === 0) return [undefined, all] as const
                    const next = new Map(all)
                    if (members.size === 0) next.delete(room)
                    else next.set(room, { members, changes: current.changes })
                    return [{ gone, changes: current.changes }, next] as const
                  })
                  if (removed === undefined) return
                  for (const member of removed.gone)
                    yield* Deferred.fail(member.ended, new Protocol.Forbidden({ room }))
                  yield* PubSub.publish(removed.changes, undefined)
                })
              )
            }
          })
        ),

      /**
       * Replaces the state of an existing entry; `false` (and no change) when there is none, or when its
       * revision is not greater than the entry's (a late request never overwrites a newer state). The
       * update and its announcement are one uninterruptible step: an interrupted `track` either
       * changed nothing or is seen by every observer.
       */
      track: (joined: Joined) =>
        Effect.uninterruptible(
          Effect.gen(function* () {
            const key = keyOf(joined.principalId, joined.clientId, joined.tabId)
            const entry = entryOf(joined)
            const changes = yield* Ref.modify(rooms, (all) => {
              const room = all.get(joined.room)
              const member = room?.members.get(key)
              if (room === undefined || member === undefined || joined.revision <= member.revision) {
                return [undefined, all] as const
              }
              const members = new Map(room.members).set(key, {
                entry,
                revision: joined.revision,
                ended: member.ended
              })
              return [room.changes, new Map(all).set(joined.room, { members, changes: room.changes })] as const
            })
            if (changes === undefined) return false
            yield* PubSub.publish(changes, undefined)
            return true
          })
        ),

      snapshot
    }
  })
