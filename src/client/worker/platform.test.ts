// Node implements the Web Locks API (navigator.locks), so the lock helpers run here unmodified.
import { expect, it } from "@effect/vitest"
import * as Arr from "effect/Array"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Scope from "effect/Scope"
import * as Str from "effect/String"
import * as Platform from "./platform.ts"
import { randomUUIDv4 } from "../Crypto.ts"

const pendingFor = (name: string) =>
  Effect.promise(() => navigator.locks.query()).pipe(
    Effect.map((snapshot) => Arr.filter(snapshot.pending ?? [], (lock) => lock.name === name).length)
  )

const waitUntil = (condition: Effect.Effect<boolean>) =>
  Effect.repeat(condition, { until: (done) => done, times: 1000 }).pipe(Effect.asVoid)

it.live("an interrupted lock wait leaves the queue and never acquires the lock afterwards", () =>
  Effect.gen(function* () {
    const name = `test:${yield* randomUUIDv4}`
    const holder = yield* Scope.make()
    yield* Platform.holdLock(name).pipe(Scope.provide(holder))

    const waiter = yield* Effect.forkChild(Effect.scoped(Effect.andThen(Platform.holdLock(name), Effect.never)))
    yield* waitUntil(Effect.map(pendingFor(name), (n) => n === 1))
    // Interruption must complete while the lock is still held by someone else.
    yield* Fiber.interrupt(waiter)

    yield* Scope.close(holder, Exit.void)
    // A cancelled request that stayed queued would now be granted and keep the lock taken.
    yield* waitUntil(Effect.map(pendingFor(name), (n) => n === 0))
    expect(yield* Effect.scoped(Platform.tryHoldLock(name))).toBe(true)
  })
)

it.live(
  "a lock wait that reaches its deadline as the holder lets go either holds the lock or leaves nothing held or queued",
  () =>
    Effect.gen(function* () {
      // The holder's release and the waiter's deadline race at every offset around the deadline; whichever wins,
      // a waiter that answers `false` must not be granted the lock later (INV-BRW-7 reset `closeTimeout`).
      for (let offset = -3; offset <= 3; offset++) {
        const name = `test:${yield* randomUUIDv4}`
        const holder = yield* Scope.make()
        yield* Platform.holdLock(name).pipe(Scope.provide(holder))
        const release = yield* Effect.forkChild(
          Effect.andThen(Effect.sleep(20 + offset), Scope.close(holder, Exit.void))
        )
        const waiter = yield* Scope.make()
        const held = yield* Platform.holdLockWithin(name, 20).pipe(Scope.provide(waiter))
        // Let a grant that lost the race arrive, if one is coming.
        yield* Effect.sleep("30 millis")
        expect(yield* Platform.isLockHeld(name)).toBe(held)
        expect(yield* pendingFor(name)).toBe(0)
        // A lock it did get is released with its scope (asynchronously, once the lock manager sees it).
        yield* Scope.close(waiter, Exit.void)
        yield* waitUntil(Effect.map(Effect.orDie(Platform.isLockHeld(name)), (h) => !h))
        yield* Fiber.join(release)
      }
    })
)

it.live("a name held only in shared mode does not count as held (a dead leader's watchers)", () =>
  Effect.gen(function* () {
    const name = `test:${yield* randomUUIDv4}`
    let drop: (() => void) | undefined
    // oxlint-disable-next-line effecttsgo/new-promise -- The actual Web Locks callback must return a pending Promise to hold the shared lock until drop().
    const shared = navigator.locks.request(name, { mode: "shared" }, () => new Promise<void>((r) => (drop = r)))
    yield* waitUntil(Effect.sync(() => drop !== undefined))
    expect(yield* Platform.isLockHeld(name)).toBe(false)
    drop?.()
    yield* Effect.promise(() => shared)
    const scope = yield* Scope.make()
    yield* Platform.holdLock(name).pipe(Scope.provide(scope))
    expect(yield* Platform.isLockHeld(name)).toBe(true)
    yield* Scope.close(scope, Exit.void)
  })
)

it.live("a Web Locks rejection surfaces as LockError instead of hanging", () =>
  Effect.gen(function* () {
    // Lock names starting with "-" are reserved; the API rejects the request.
    const error = yield* Effect.scoped(Platform.holdLock("-reserved")).pipe(Effect.flip)
    expect(error._tag).toBe("LockError")
  })
)

it.live("identities with all-dot segments are rejected", () =>
  Effect.sync(() => {
    expect(Platform.validateIdentity({ appId: ".", profile: "p" })).toBeDefined()
    expect(Platform.validateIdentity({ appId: "a", profile: ".." })).toBeDefined()
    expect(Platform.validateIdentity({ appId: "a.b", profile: "p-1" })).toBeUndefined()
  })
)

it.live(
  "an appId or profile over 255 UTF-8 bytes is refused even when the whole path fits; 255 bytes is accepted",
  () =>
    Effect.sync(() => {
      // 256 + 4 + 14 = 274 characters of path, far inside the pool's 502: only the per-name bound refuses.
      expect(Platform.validateIdentity({ appId: "app", profile: Str.repeat(256)("p") })).toContain(
        "profile is 256 bytes"
      )
      expect(Platform.validateIdentity({ appId: Str.repeat(256)("a"), profile: "p" })).toContain("appId is 256 bytes")
      expect(Platform.validateIdentity({ appId: "app", profile: Str.repeat(255)("p") })).toBeUndefined()
      expect(Platform.validateIdentity({ appId: Str.repeat(255)("a"), profile: "p" })).toBeUndefined()
    })
)

// Derived from the pinned AccessHandlePoolVFS: it stores "/" + SQLite's file name and throws once the
// UTF-8 path reaches HEADER_MAX_PATH_SIZE = 512 bytes; the longest file name is "<db>-journal".
const stored = (identity: Platform.ReplicaIdentity) => `/${Platform.dbName(identity)}-journal`
const fill = (n: number) => Str.repeat(n)("p")

it.live(
  "the longest identity whose journal path fits the pool's 512-byte path header is accepted; one more character is not",
  () =>
    Effect.gen(function* () {
      // Split across both segments: each is also bounded by maxSegmentBytes.
      const appId = Str.repeat(511 - stored({ appId: "", profile: fill(Platform.maxSegmentBytes) }).length)("a")
      const longest = { appId, profile: fill(Platform.maxSegmentBytes) }
      expect(new TextEncoder().encode(stored(longest)).length).toBe(511)
      expect(Platform.validateIdentity(longest)).toBeUndefined()
      const tooLong = { appId: `${appId}a`, profile: longest.profile }
      expect(Platform.validateIdentity(tooLong)).toContain(String(Platform.maxDbNameLength))
      // The trial's identity (a UUID profile) is well inside the bound.
      expect(Platform.validateIdentity({ appId: "heybabe-trial", profile: yield* randomUUIDv4 })).toBeUndefined()
    })
)
