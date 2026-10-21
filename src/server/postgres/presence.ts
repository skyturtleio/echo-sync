import { Deferred, type Duration, Effect, PubSub, Ref, type Scope, Stream } from "effect"
import type { AccessScope, Protocol } from "echo-sync"

/** An entry and the room it is in. */
export interface Joined extends Protocol.PresenceEntry {
  readonly room: AccessScope.AccessScopeId
}

/** One open `room` stream's entry. */
interface Member {
  readonly entry: Protocol.PresenceEntry
  /**
   * Completed when a later join with the same key replaces this entry; it ends this member's
   * stream. Also its identity: a replaced stream's release must not remove its successor.
   */
  readonly replaced: Deferred.Deferred<void>
}

interface Room {
  readonly members: ReadonlyMap<string, Member>
  /** One tick per change; ticks carry nothing, so a slow observer's ticks coalesce. */
  readonly changes: PubSub.PubSub<void>
}

/**
 * The entry key. DESIGN §12 keys entries by `(clientId, tabId)`; the principal is part of the key
 * too, so a caller naming another principal's client id can neither replace nor end that entry.
 */
const keyOf = (principalId: string, clientId: string, tabId: string) => JSON.stringify([principalId, clientId, tabId])

const entryOf = (joined: Joined): Protocol.PresenceEntry => ({
  principalId: joined.principalId,
  clientId: joined.clientId,
  tabId: joined.tabId,
  state: joined.state
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
 * Process-local presence state (DESIGN §12): a `Ref` of rooms plus a `PubSub` per room. A room
 * exists while it has an entry. Authorization is the caller's (the RPC handlers).
 *
 * Every `join` stream also carries a `heartbeat` every `heartbeatInterval` (DESIGN §16 item 13).
 * Heartbeats come from the stream's own timer, merged beside the room's change subscription, so
 * a heartbeat can never slide out a pending change tick, and they read and change no room state.
 */
export const make = (options: { readonly heartbeatInterval: Duration.Input }) =>
  Effect.gen(function* () {
    const rooms = yield* Ref.make<ReadonlyMap<AccessScope.AccessScopeId, Room>>(new Map())

    const snapshot = (room: AccessScope.AccessScopeId) =>
      Effect.map(Ref.get(rooms), (all): Protocol.RoomSnapshot => ({
        _tag: "snapshot",
        room,
        entries: [...(all.get(room)?.members.values() ?? [])].map((m) => m.entry).toSorted(byKey)
      }))

    /**
     * Adds the entry for the scope's lifetime. A later join with the same key replaces it and ends
     * the earlier stream (its `replaced`), so no open stream is ever left without an entry: every
     * open stream keeps its room, and with it the room's `PubSub`, alive. Otherwise the replacing
     * stream could leave as the room's last member, the room would be recreated on a new `PubSub`,
     * and the replaced stream would observe the abandoned one forever.
     */
    const enter = (
      joined: Joined
    ): Effect.Effect<
      { readonly changes: PubSub.PubSub<void>; readonly replaced: Deferred.Deferred<void> },
      never,
      Scope.Scope
    > => {
      const key = keyOf(joined.principalId, joined.clientId, joined.tabId)
      const entry = entryOf(joined)
      return Effect.acquireRelease(
        Effect.gen(function* () {
          const fresh = yield* PubSub.sliding<void>(1)
          const replaced = yield* Deferred.make<void>()
          const [changes, previous] = yield* Ref.modify(rooms, (all) => {
            const room = all.get(joined.room) ?? { members: new Map(), changes: fresh }
            const members = new Map(room.members).set(key, { entry, replaced })
            return [
              [room.changes, room.members.get(key)] as const,
              new Map(all).set(joined.room, { members, changes: room.changes })
            ] as const
          })
          if (previous !== undefined) yield* Deferred.succeed(previous.replaced, undefined)
          return { changes, replaced }
        }),
        ({ changes, replaced }) =>
          Ref.modify(rooms, (all) => {
            const room = all.get(joined.room)
            // A later stream with the same key took the entry over: it is not this stream's to drop.
            if (room?.members.get(key)?.replaced !== replaced) return [false, all] as const
            const members = new Map(room.members)
            members.delete(key)
            const next = new Map(all)
            if (members.size === 0) next.delete(joined.room)
            else next.set(joined.room, { members, changes: room.changes })
            return [true, next] as const
          }).pipe(Effect.flatMap((left) => (left ? PubSub.publish(changes, undefined) : Effect.void)))
      )
    }

    return {
      /**
       * The entry is present exactly while the returned stream runs: added before its first
       * snapshot, removed when it ends for any reason (completion, failure, interruption). The
       * first snapshot already contains it; every later change produces another.
       */
      join: (joined: Joined): Stream.Stream<Protocol.RoomElement> =>
        Stream.unwrap(
          Effect.gen(function* () {
            const { changes, replaced } = yield* enter(joined)
            const subscription = yield* PubSub.subscribe(changes)
            // Subscribed before announcing: this tick is the join snapshot, for this stream and the others.
            yield* PubSub.publish(changes, undefined)
            return Stream.mapEffect(Stream.fromSubscription(subscription), () => snapshot(joined.room)).pipe(
              Stream.merge(Stream.fromEffectRepeat(Effect.as(Effect.sleep(options.heartbeatInterval), heartbeat))),
              Stream.interruptWhen(Deferred.await(replaced))
            )
          })
        ),

      /**
       * Replaces the state of an existing entry; `false` (and no change) when there is none. The
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
              if (room === undefined || member === undefined) return [undefined, all] as const
              const members = new Map(room.members).set(key, { entry, replaced: member.replaced })
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

export type Presence = Effect.Success<typeof make>
