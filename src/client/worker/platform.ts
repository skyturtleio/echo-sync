/**
 * Browser platform edges used by the single-owner topology: storage names, Web Locks, OPFS probing,
 * and capability detection. Everything here is usable from a window, a dedicated worker, or a
 * SharedWorker.
 */
import * as Effect from "effect/Effect"
import * as Schedule from "effect/Schedule"
import type * as Scope from "effect/Scope"

const noop = () => {}

export interface ReplicaIdentity {
  readonly appId: string
  readonly profile: string
}

const segment = /^[A-Za-z0-9._-]+$/

/** Rejects ids that would collide or escape the `echo-sync/<appId>/<profile>.db` / `echo:<appId>:*` namespaces. */
export const validateIdentity = (identity: ReplicaIdentity): string | undefined =>
  segment.test(identity.appId) && segment.test(identity.profile)
    ? undefined
    : `appId and profile must match ${segment.source} (got appId=${JSON.stringify(identity.appId)}, profile=${JSON.stringify(identity.profile)})`

/** SQLite path inside the OPFS access-handle pool (DESIGN §9 naming). */
export const dbName = ({ appId, profile }: ReplicaIdentity): string => `echo-sync/${appId}/${profile}.db`

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

/**
 * Hold an exclusive Web Lock until the enclosing scope closes. Resolves once the lock is granted.
 * The browser releases it automatically if the holding agent (tab/worker) dies.
 */
export const holdLock = (name: string): Effect.Effect<void, never, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.callback<() => void>((resume) => {
      const abort = new AbortController()
      void navigator.locks
        .request(name, { mode: "exclusive", signal: abort.signal }, () => {
          let release = noop
          const held = new Promise<void>((resolve) => {
            release = resolve
          })
          resume(Effect.succeed(release))
          return held
        })
        .catch(() => {})
      return Effect.sync(() => abort.abort())
    }),
    (release) => Effect.sync(release)
  ).pipe(Effect.asVoid)

/**
 * Like `holdLock` but never waits: succeeds with `true` and holds the lock if it was free, `false` otherwise.
 */
export const tryHoldLock = (name: string): Effect.Effect<boolean, never, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.callback<(() => void) | undefined>((resume) => {
      void navigator.locks.request(name, { mode: "exclusive", ifAvailable: true }, (lock) => {
        if (lock === null) {
          resume(Effect.succeed(undefined))
          return undefined
        }
        let release = noop
        const held = new Promise<void>((resolve) => {
          release = resolve
        })
        resume(Effect.succeed(release))
        return held
      })
    }),
    (release) => Effect.sync(() => release?.())
  ).pipe(Effect.map((release) => release !== undefined))

/**
 * Completes when the exclusive holder of `name` releases it (or immediately if nobody holds it).
 * Uses a shared-mode request so any number of watchers are released together.
 */
export const awaitLockReleased = (name: string): Effect.Effect<void> =>
  Effect.callback<void>((resume) => {
    const abort = new AbortController()
    void navigator.locks
      .request(name, { mode: "shared", signal: abort.signal }, () => {
        resume(Effect.void)
        return undefined
      })
      .catch(() => {})
    return Effect.sync(() => abort.abort())
  })

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
