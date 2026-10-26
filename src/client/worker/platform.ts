/**
 * Browser platform edges used by the single-owner topology: storage names, Web Locks, OPFS probing,
 * and capability detection. Everything here is usable from a window, a dedicated worker, or a
 * SharedWorker.
 */
/// <reference lib="webworker" />
import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto"
import * as Arr from "effect/Array"
import * as Crypto from "effect/Crypto"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Hex from "effect/encoding/Hex"
import * as Option from "effect/Option"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Str from "effect/String"

export interface ReplicaIdentity {
  readonly appId: string
  readonly profile: string
}

// No all-dot segments: "." and ".." are path traversal in the OPFS directory walk.
const segment = /^(?!\.+$)[A-Za-z0-9._-]+$/

/** SQLite path inside the OPFS access-handle pool (INV-BRW-1 naming). */
export const dbName = ({ appId, profile }: ReplicaIdentity): string => `echo-sync/${appId}/${profile}.db`

/**
 * The longest `dbName` the OPFS pool can hold, in bytes (pinned `@effect/wa-sqlite` 0.1.2,
 * `src/examples/AccessHandlePoolVFS.js`). The pool stores each file's path in a header of
 * `HEADER_MAX_PATH_SIZE = 512` bytes and refuses a path whose UTF-8 encoding fills it (`written >= 512`),
 * so a stored path is at most 511 bytes. The stored path is the URL pathname of SQLite's name: a leading
 * "/" plus the name (the identity charset needs no percent-encoding), and for our single persistent
 * database the longest name SQLite derives from it is its rollback journal, `<db>-journal` (8 more bytes).
 * 511 - 1 - 8 = 502. (A multi-database ATTACH transaction can use a longer super-journal name; the leader
 * never attaches a second database, so that case is unsupported here.)
 */
export const maxDbNameLength = 512 - 1 - 1 - "-journal".length

/**
 * The `mxPathname` the pool VFS is registered with. SQLite refuses to open a database whose full path
 * plus 8 (room for its journal suffix) exceeds the VFS's `mxPathname`; wa-sqlite's default of 64 would
 * reject a UUID-length profile, so the bound is raised to exactly what `maxDbNameLength` needs.
 */
export const vfsMaxPathname = maxDbNameLength + "-journal".length

/**
 * The longest OPFS entry name, in UTF-8 bytes, that every engine accepts: WebKit refuses a 256-byte
 * directory or file name (`UnknownError`) where Chromium accepts far longer ones (SPEC §12,
 * WebKit smoke). `appId` and `profile` are each a directory name in `poolDirectory`.
 */
export const maxSegmentBytes = 255

/**
 * Rejects ids that would collide or escape the `echo-sync/<appId>/<profile>.db` / `echo:<appId>:*`
 * namespaces, whose `appId` or `profile` is a longer OPFS name than `maxSegmentBytes`, or whose
 * database path the OPFS pool cannot hold (`maxDbNameLength`).
 */
export const validateIdentity = (identity: ReplicaIdentity): string | undefined => {
  if (!segment.test(identity.appId) || !segment.test(identity.profile)) {
    return `appId and profile must match ${segment.source} (got appId=${JSON.stringify(identity.appId)}, profile=${JSON.stringify(identity.profile)})`
  }
  for (const [name, value] of [
    ["appId", identity.appId],
    ["profile", identity.profile]
  ] as const) {
    const bytes = new TextEncoder().encode(value).length
    if (bytes > maxSegmentBytes) {
      return `${name} is ${bytes} bytes; an OPFS name holds at most ${maxSegmentBytes} (UTF-8)`
    }
  }
  const length = dbName(identity).length
  return length > maxDbNameLength
    ? `the database path echo-sync/<appId>/<profile>.db is ${length} characters; the OPFS pool holds at most ${maxDbNameLength}, so appId and profile together may have at most ${maxDbNameLength - dbName({ appId: "", profile: "" }).length}`
    : undefined
}

/**
 * OPFS directory of this replica's access-handle pool. One pool per replica: the pool VFS holds an
 * exclusive sync handle on every file in its directory, so sharing it would block other identities.
 */
export const poolDirectory = ({ appId, profile }: ReplicaIdentity): string => `echo-sync/${appId}/${profile}`

/**
 * Name of the identity's reset tombstone (INV-BRW-8), an engine-owned file in `echo-sync/<appId>` whose
 * presence means a reset that passed its pending decision condemned `poolDirectory(identity)`. It starts with
 * "~", which `validateIdentity` never accepts, so it is never a profile's directory. The profile is hashed
 * (SHA-256, hex) rather than embedded so the name fits `maxSegmentBytes` for every valid profile.
 */
export const tombstoneName = ({ profile }: ReplicaIdentity): Effect.Effect<string, BrowserStorageError> =>
  Effect.flatMap(Crypto.Crypto, (crypto) => crypto.digest("SHA-256", new TextEncoder().encode(profile))).pipe(
    Effect.map((digest) => `~reset-${Hex.encode(digest)}`),
    Effect.mapError(
      (error) =>
        new BrowserStorageError({
          operation: "hashing the profile for its reset tombstone",
          cause: error.cause ?? error
        })
    ),
    // Keep Web Crypto lazy: only tombstone operations need this service, not worker startup.
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This environment-free platform operation acquires Web Crypto only on execution, not at worker startup.
    Effect.provide(BrowserCrypto.layer)
  )

/** Exclusive lock held by exactly one leader worker per replica for its whole lifetime. */
/** Name of the router SharedWorker and of the BroadcastChannel its instances announce themselves on. */
export const routerName = ({ appId, profile }: ReplicaIdentity): string => `echo:${appId}:${profile}:router`

export const leaderLockName = ({ appId, profile }: ReplicaIdentity): string => `echo:${appId}:${profile}:leader`

/**
 * The BroadcastChannel `resetReplica` signals on (INV-BRW-7): every tab client of this identity that
 * receives it enters its terminal reset state and terminates its leader candidate.
 */
export const resetChannelName = ({ appId, profile }: ReplicaIdentity): string => `echo:${appId}:${profile}:reset`

/** Held exclusively by one specific leader incarnation; acquiring it in shared mode == "that leader is gone". */
export const leaderEpochLockName = ({ appId, profile }: ReplicaIdentity, leaderId: string): string =>
  `echo:${appId}:${profile}:leader:${leaderId}`

/** Held exclusively by a tab for its lifetime; acquiring it in shared mode == "that tab is gone". */
export const tabLockName = ({ appId, profile }: ReplicaIdentity, tabId: string): string =>
  `echo:${appId}:${profile}:tab:${tabId}`

/** `u` as one line of text: an error's name and message, anything else as a string. */
export const describe = (u: unknown): string => (u instanceof Error ? `${u.name}: ${u.message}` : String(u))

/**
 * A browser storage call (OPFS, SQLite on OPFS, Web Crypto) failed. `operation` says what was attempted, never
 * on which path (paths name the app and profile, which errors an app may forward must not carry); `cause` is what the browser threw, typically a `DOMException` whose `name` says why.
 */
export class BrowserStorageError extends Data.TaggedError("BrowserStorageError")<{
  readonly operation: string
  readonly cause: unknown
}> {
  override get message(): string {
    return `${this.operation} failed: ${describe(this.cause)}`
  }
}

/** Run a browser storage promise; a rejection is a `BrowserStorageError` naming `operation`. */
export const storageCall = <A>(operation: string, run: () => Promise<A>): Effect.Effect<A, BrowserStorageError> =>
  Effect.tryPromise({ try: run, catch: (cause) => new BrowserStorageError({ operation, cause }) })

/** The Web Locks API rejected a request for a reason other than our own cancellation. */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class LockError extends Schema.TaggedError<LockError>()("LockError", {
  // oxlint-disable-next-line effecttsgo/error-payload-shadows-name -- name is identity payload; _tag is the discriminant. Preserve wire/durable compatibility.
  name: Schema.String,
  message: Schema.String
}) {}

const lockError = (name: string, cause: unknown) => new LockError({ name, message: describe(cause) })

/**
 * Register ownership before requesting a lock. The native callback transfers its release function
 * directly into that ownership cell, never through a resumable Effect continuation. Failed or timed
 * out attempts close their cell even when the caller's scope remains open.
 */
const request = Effect.fnUntraced(function* (
  name: string,
  options: { readonly ifAvailable?: true; readonly withinMillis?: number }
) {
  let closed = false
  let release: (() => void) | undefined
  const close = () => {
    if (closed) return
    closed = true
    const owned = release
    release = undefined
    owned?.()
  }
  return yield* Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      yield* Effect.addFinalizer(() => Effect.sync(close))
      const acquire = Effect.callback<boolean, LockError>((resume, signal) => {
        // `signal` and `ifAvailable` cannot be combined; an ifAvailable request never queues anyway.
        const lockOptions: LockOptions = options.ifAvailable
          ? { mode: "exclusive", ifAvailable: true }
          : { mode: "exclusive", signal }
        navigator.locks
          .request(name, lockOptions, (lock) => {
            if (lock === null || closed || signal.aborted) {
              resume(Effect.succeed(false))
              return undefined
            }
            // oxlint-disable-next-line effecttsgo/new-promise -- Web Locks holds ownership until its callback's Promise settles; release resolves that exact Promise.
            const held = new Promise<void>((resolve) => {
              release = resolve
            })
            resume(Effect.succeed(true))
            return held
          })
          .catch((cause: unknown) => {
            if (!signal.aborted) resume(Effect.fail(lockError(name, cause)))
          })
      })
      const within = options.withinMillis
      const wait =
        within !== undefined && Number.isFinite(within)
          ? acquire.pipe(
              Effect.timeoutOption(Math.min(Math.max(within, 0), 2_147_483_647)),
              Effect.map(Option.getOrElse(() => false))
            )
          : acquire
      return yield* restore(wait)
    })
  ).pipe(
    // Observe interruption delivered when the mask exits, not just the native wait's success.
    Effect.onExit((exit) => (Exit.isSuccess(exit) && exit.value ? Effect.void : Effect.sync(close)))
  )
})

/**
 * Hold an exclusive Web Lock until the enclosing scope closes. Resolves once the lock is granted; the
 * wait is interruptible. The browser releases the lock automatically if the holding agent dies.
 */
export const holdLock = (name: string): Effect.Effect<void, LockError, Scope.Scope> => Effect.asVoid(request(name, {}))

/** Like `holdLock` but never waits: `true` and the lock is held if it was free, `false` otherwise. */
export const tryHoldLock = (name: string): Effect.Effect<boolean, LockError, Scope.Scope> =>
  request(name, { ifAvailable: true })

/**
 * Like `holdLock` but waits at most `millis` (non-finite: no deadline): `true` and the lock is held if it was
 * granted in time, `false` otherwise, and then the request has left the queue and nothing is held. A grant
 * whose callback runs after the deadline fired is released at once (see `request`).
 */
export const holdLockWithin = (name: string, millis: number): Effect.Effect<boolean, LockError, Scope.Scope> =>
  request(name, { withinMillis: millis })

/**
 * Completes when the exclusive holder of `name` releases it (or immediately if nobody holds it).
 * A shared-mode request, released at once, so any number of watchers are woken together.
 */
export const awaitLockReleased = (name: string): Effect.Effect<void, LockError> =>
  Effect.callback((resume, signal) => {
    navigator.locks
      .request(name, { mode: "shared", signal }, () => {
        // Returning from the native callback releases the lock even if resume yields or is interrupted.
        resume(Effect.void)
      })
      .catch((cause: unknown) => {
        if (!signal.aborted) resume(Effect.fail(lockError(name, cause)))
      })
  })

/**
 * Whether some agent currently holds `name` exclusively. Shared holders do not count: right after an
 * exclusive holder dies, `awaitLockReleased` watchers briefly hold the name in shared mode.
 */
export const isLockHeld = (name: string): Effect.Effect<boolean, LockError> =>
  Effect.tryPromise({
    try: () => navigator.locks.query(),
    catch: (cause) => lockError(name, cause)
  }).pipe(
    Effect.map((snapshot) => Arr.some(snapshot.held ?? [], (lock) => lock.name === name && lock.mode === "exclusive"))
  )

/**
 * Whether this agent can host the leader: Web Locks plus OPFS synchronous access handles (worker-only
 * API, so this is only meaningful inside a dedicated worker).
 */
export const leaderHostCapability = (): string | undefined => {
  if (typeof navigator === "undefined" || navigator.locks === undefined) return "Web Locks API unavailable"
  if (navigator.storage === undefined || typeof navigator.storage.getDirectory !== "function") {
    return "Origin Private File System unavailable"
  }
  if (typeof FileSystemFileHandle === "undefined" || !("createSyncAccessHandle" in FileSystemFileHandle.prototype)) {
    return "OPFS synchronous access handles unavailable"
  }
  return undefined
}

/** Capabilities a tab needs before it spawns its leader candidate. */
export const tabCapability = (): { readonly unsupported: string | undefined; readonly sharedWorker: boolean } => {
  const sharedWorker = typeof SharedWorker === "function"
  if (typeof navigator === "undefined" || navigator.locks === undefined) {
    return { unsupported: "Web Locks API unavailable", sharedWorker }
  }
  if (typeof Worker !== "function") return { unsupported: "Dedicated workers unavailable", sharedWorker }
  if (navigator.storage === undefined || typeof navigator.storage.getDirectory !== "function") {
    return { unsupported: "Origin Private File System unavailable", sharedWorker }
  }
  return { unsupported: undefined, sharedWorker }
}

/**
 * The access-handle pool VFS opens an exclusive synchronous access handle on every file in its
 * directory. After a leader's worker dies the browser may release those handles slightly after its
 * Web Locks, so this waits until every file in the pool can be opened; a freshly elected leader then
 * does not crash on a handle still held by its predecessor.
 */
export const awaitOpfsPoolFree = (directory: string): Effect.Effect<void, BrowserStorageError> =>
  // oxlint-disable-next-line effecttsgo/async-function -- storageCall adapts Promise-based OPFS traversal and normalizes browser failures.
  storageCall("waiting for the OPFS pool to be free", async () => {
    const root = await navigator.storage.getDirectory()
    let dir = root
    try {
      for (const part of Str.split(directory, "/")) dir = await dir.getDirectoryHandle(part)
    } catch {
      return
    }
    // `values()` is in the WHATWG spec and every OPFS engine, but not in TS's DOM lib yet.
    // oxlint-disable-next-line typescript/consistent-type-assertions -- platform edge, see above
    const iterable = dir as unknown as { values(): AsyncIterable<FileSystemHandle> }
    return iterable.values()
  }).pipe(
    Effect.flatMap((entries) => {
      if (entries === undefined) return Effect.void
      return Stream.fromAsyncIterable(
        entries,
        (cause) => new BrowserStorageError({ operation: "waiting for the OPFS pool to be free", cause })
      ).pipe(
        Stream.runForEach((handle) =>
          handle instanceof FileSystemFileHandle
            ? // oxlint-disable-next-line effecttsgo/async-function -- OPFS exposes access-handle acquisition as a Promise; storageCall maps rejection.
              storageCall("waiting for the OPFS pool to be free", async () => {
                const access = await handle.createSyncAccessHandle()
                access.close()
              })
            : Effect.void
        )
      )
    }),
    Effect.retry({
      // 20 + 40 + … + 5,120 ms = 10,220 ms before the final attempt.
      schedule: Schedule.exponential("20 millis").pipe(Schedule.upTo({ times: 9 })),
      while: ({ cause }) =>
        cause instanceof DOMException &&
        (cause.name === "NoModificationAllowedError" || cause.name === "InvalidStateError")
    })
  )
