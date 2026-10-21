// Node implements the Web Locks API (navigator.locks), so the lock helpers run here unmodified.
import { expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Scope from "effect/Scope"
import * as Platform from "./platform.ts"

const pendingFor = (name: string) =>
  Effect.promise(() => navigator.locks.query()).pipe(
    Effect.map((snapshot) => (snapshot.pending ?? []).filter((lock) => lock.name === name).length)
  )

const waitUntil = (condition: Effect.Effect<boolean>) =>
  Effect.repeat(condition, { until: (done) => done, times: 1000 }).pipe(Effect.asVoid)

it.live("an interrupted lock wait leaves the queue and never acquires the lock afterwards", () =>
  Effect.gen(function* () {
    const name = `test:${crypto.randomUUID()}`
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

it.live("a name held only in shared mode does not count as held (a dead leader's watchers)", () =>
  Effect.gen(function* () {
    const name = `test:${crypto.randomUUID()}`
    let drop: (() => void) | undefined
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
      expect(Platform.validateIdentity({ appId: "app", profile: "p".repeat(256) })).toContain("profile is 256 bytes")
      expect(Platform.validateIdentity({ appId: "a".repeat(256), profile: "p" })).toContain("appId is 256 bytes")
      expect(Platform.validateIdentity({ appId: "app", profile: "p".repeat(255) })).toBeUndefined()
      expect(Platform.validateIdentity({ appId: "a".repeat(255), profile: "p" })).toBeUndefined()
    })
)

// Derived from the pinned AccessHandlePoolVFS: it stores "/" + SQLite's file name and throws once the
// UTF-8 path reaches HEADER_MAX_PATH_SIZE = 512 bytes; the longest file name is "<db>-journal".
const stored = (identity: Platform.ReplicaIdentity) => `/${Platform.dbName(identity)}-journal`
const fill = (n: number) => "p".repeat(n)

it.live(
  "the longest identity whose journal path fits the pool's 512-byte path header is accepted; one more character is not",
  () =>
    Effect.sync(() => {
      // Split across both segments: each is also bounded by maxSegmentBytes.
      const appId = "a".repeat(511 - stored({ appId: "", profile: fill(Platform.maxSegmentBytes) }).length)
      const longest = { appId, profile: fill(Platform.maxSegmentBytes) }
      expect(new TextEncoder().encode(stored(longest)).length).toBe(511)
      expect(Platform.validateIdentity(longest)).toBeUndefined()
      const tooLong = { appId: `${appId}a`, profile: longest.profile }
      expect(Platform.validateIdentity(tooLong)).toContain(String(Platform.maxDbNameLength))
      // The trial's identity (a UUID profile) is well inside the bound.
      expect(Platform.validateIdentity({ appId: "heybabe-trial", profile: crypto.randomUUID() })).toBeUndefined()
    })
)
