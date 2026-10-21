import { Effect, PubSub, Ref, type Scope, Stream } from "effect"
import type { AccessScope, Protocol } from "echo-sync"

/** An entry and the room it is in. */
export interface Joined extends Protocol.PresenceEntry {
  readonly room: AccessScope.AccessScopeId
}

/** One open `room` stream's entry; `token` tells it apart from a later stream with the same key. */
interface Member {
  readonly entry: Protocol.PresenceEntry
  readonly token: symbol
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
 */
export const make = Effect.gen(function* () {
  const rooms = yield* Ref.make<ReadonlyMap<AccessScope.AccessScopeId, Room>>(new Map())

  const snapshot = (room: AccessScope.AccessScopeId) =>
    Effect.map(Ref.get(rooms), (all): Protocol.RoomSnapshot => ({
      room,
      entries: [...(all.get(room)?.members.values() ?? [])].map((m) => m.entry).toSorted(byKey)
    }))

  /** Adds the entry for the scope's lifetime; a later join with the same key replaces it. */
  const enter = (joined: Joined): Effect.Effect<PubSub.PubSub<void>, never, Scope.Scope> => {
    const key = keyOf(joined.principalId, joined.clientId, joined.tabId)
    const token = Symbol(key)
    const entry = entryOf(joined)
    return Effect.acquireRelease(
      Effect.gen(function* () {
        const fresh = yield* PubSub.sliding<void>(1)
        return yield* Ref.modify(rooms, (all) => {
          const room = all.get(joined.room) ?? { members: new Map(), changes: fresh }
          const members = new Map(room.members).set(key, { entry, token })
          return [room.changes, new Map(all).set(joined.room, { members, changes: room.changes })] as const
        })
      }),
      (changes) =>
        Ref.modify(rooms, (all) => {
          const room = all.get(joined.room)
          // A later stream with the same key took the entry over: it is not this stream's to drop.
          if (room?.members.get(key)?.token !== token) return [false, all] as const
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
    join: (joined: Joined): Stream.Stream<Protocol.RoomSnapshot> =>
      Stream.unwrap(
        Effect.gen(function* () {
          const changes = yield* enter(joined)
          const subscription = yield* PubSub.subscribe(changes)
          // Subscribed before announcing: this tick is the join snapshot, for this stream and the others.
          yield* PubSub.publish(changes, undefined)
          return Stream.mapEffect(Stream.fromSubscription(subscription), () => snapshot(joined.room))
        })
      ),

    /** Replaces the state of an existing entry; `false` (and no change) when there is none. */
    track: (joined: Joined) =>
      Effect.gen(function* () {
        const key = keyOf(joined.principalId, joined.clientId, joined.tabId)
        const entry = entryOf(joined)
        const changes = yield* Ref.modify(rooms, (all) => {
          const room = all.get(joined.room)
          const member = room?.members.get(key)
          if (room === undefined || member === undefined) return [undefined, all] as const
          const members = new Map(room.members).set(key, { entry, token: member.token })
          return [room.changes, new Map(all).set(joined.room, { members, changes: room.changes })] as const
        })
        if (changes === undefined) return false
        yield* PubSub.publish(changes, undefined)
        return true
      }),

    snapshot
  }
})

export type Presence = Effect.Success<typeof make>
