/**
 * Browser platform edges used by the single-owner topology: storage names, Web Locks, OPFS probing,
 * and capability detection. Everything here is usable from a window, a dedicated worker, or a
 * SharedWorker.
 */
/// <reference lib="webworker" />
import * as Effect from "effect/Effect"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"

const noop = () => {}

export interface ReplicaIdentity {
  readonly appId: string
  readonly profile: string
}

// No all-dot segments: "." and ".." are path traversal in the OPFS directory walk.
const segment = /^(?!\.+$)[A-Za-z0-9._-]+$/

/** SQLite path inside the OPFS access-handle pool (DESIGN §9 naming). */
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
 * directory or file name (`UnknownError`) where Chromium accepts far longer ones (DESIGN §16 item 1,
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

/** Exclusive lock held by exactly one leader worker per replica for its whole lifetime. */
export const leaderLockName = ({ appId, profile }: ReplicaIdentity): string => `echo:${appId}:${profile}:leader`

/** Held exclusively by one specific leader incarnation; acquiring it in shared mode == "that leader is gone". */
export const leaderEpochLockName = ({ appId, profile }: ReplicaIdentity, leaderId: string): string =>
  `echo:${appId}:${profile}:leader:${leaderId}`

/** Held exclusively by a tab for its lifetime; acquiring it in shared mode == "that tab is gone". */
export const tabLockName = ({ appId, profile }: ReplicaIdentity, tabId: string): string =>
  `echo:${appId}:${profile}:tab:${tabId}`

/** The Web Locks API rejected a request for a reason other than our own cancellation. */
export class LockError extends Schema.TaggedError<LockError>()("LockError", {
  name: Schema.String,
  message: Schema.String
}) {}

const lockError = (name: string, cause: unknown) =>
  new LockError({ name, message: cause instanceof Error ? `${cause.name}: ${cause.message}` : String(cause) })

/**
 * Request `name` and resolve with a release function once granted (or `undefined` when `ifAvailable`
 * and the lock is taken). Interrupting the wait aborts the queued request; a grant that races with the
 * interruption is released immediately, so a cancelled waiter never ends up holding the lock.
 */
const request = (
  name: string,
  options: { readonly mode: LockMode; readonly ifAvailable?: true }
): Effect.Effect<(() => void) | undefined, LockError> =>
  Effect.callback<(() => void) | undefined, LockError>((resume, signal) => {
    // `signal` and `ifAvailable` cannot be combined; an ifAvailable request never queues anyway.
    const lockOptions: LockOptions = options.ifAvailable
      ? { mode: options.mode, ifAvailable: true }
      : { ...options, signal }
    navigator.locks
      .request(name, lockOptions, (lock) => {
        if (lock === null) {
          resume(Effect.succeed(undefined))
          return undefined
        }
        if (signal.aborted) return undefined
        let release = noop
        const held = new Promise<void>((resolve) => {
          release = resolve
        })
        resume(Effect.succeed(release))
        return held
      })
      .catch((cause: unknown) => {
        if (!signal.aborted) resume(Effect.fail(lockError(name, cause)))
      })
  })

/** Releases the granted lock when the enclosing scope closes; the wait itself stays interruptible. */
const holdUntilScopeCloses = (acquire: Effect.Effect<(() => void) | undefined, LockError>) =>
  Effect.uninterruptibleMask((restore) =>
    Effect.flatMap(restore(acquire), (release) =>
      release === undefined
        ? Effect.succeed(false)
        : Effect.as(
            Effect.addFinalizer(() => Effect.sync(release)),
            true
          )
    )
  )

/**
 * Hold an exclusive Web Lock until the enclosing scope closes. Resolves once the lock is granted; the
 * wait is interruptible. The browser releases the lock automatically if the holding agent dies.
 */
export const holdLock = (name: string): Effect.Effect<void, LockError, Scope.Scope> =>
  Effect.asVoid(holdUntilScopeCloses(request(name, { mode: "exclusive" })))

/** Like `holdLock` but never waits: `true` and the lock is held if it was free, `false` otherwise. */
export const tryHoldLock = (name: string): Effect.Effect<boolean, LockError, Scope.Scope> =>
  holdUntilScopeCloses(request(name, { mode: "exclusive", ifAvailable: true }))

/**
 * Completes when the exclusive holder of `name` releases it (or immediately if nobody holds it).
 * A shared-mode request, released at once, so any number of watchers are woken together.
 */
export const awaitLockReleased = (name: string): Effect.Effect<void, LockError> =>
  Effect.flatMap(request(name, { mode: "shared" }), (release) => Effect.sync(() => release?.()))

/**
 * Whether some agent currently holds `name` exclusively. Shared holders do not count: right after an
 * exclusive holder dies, `awaitLockReleased` watchers briefly hold the name in shared mode.
 */
export const isLockHeld = (name: string): Effect.Effect<boolean, LockError> =>
  Effect.tryPromise({
    try: () => navigator.locks.query(),
    catch: (cause) => lockError(name, cause)
  }).pipe(
    Effect.map((snapshot) => (snapshot.held ?? []).some((lock) => lock.name === name && lock.mode === "exclusive"))
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
export const awaitOpfsPoolFree = (directory: string): Effect.Effect<void, Error> =>
  Effect.tryPromise({
    try: async () => {
      const root = await navigator.storage.getDirectory()
      let dir = root
      try {
        for (const part of directory.split("/")) dir = await dir.getDirectoryHandle(part)
      } catch {
        return
      }
      // `entries()` is in the WHATWG spec and every engine that has OPFS, but not in TS's DOM lib yet.
      // oxlint-disable-next-line typescript/consistent-type-assertions -- platform edge, see above
      const iterable = dir as unknown as { values(): AsyncIterable<FileSystemHandle> }
      for await (const handle of iterable.values()) {
        if (handle instanceof FileSystemFileHandle) {
          const access = await handle.createSyncAccessHandle()
          access.close()
        }
      }
    },
    catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause)))
  }).pipe(
    Effect.retry({
      times: 100,
      schedule: Schedule.spaced("20 millis"),
      while: (e) => e.name === "NoModificationAllowedError" || e.name === "InvalidStateError"
    })
  )
