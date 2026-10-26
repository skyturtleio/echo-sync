const params = new URLSearchParams(location.search)
const appId = params.get("app") ?? ""
const profile = params.get("profile") ?? "default"
const leaderLock = `echo:${appId}:${profile}:leader`

let releaseLeader: (() => void) | undefined
let poolWorker: Worker | undefined

const holdLeader = () =>
  // oxlint-disable-next-line effecttsgo/new-promise -- Driver needs notification once the browser grants the raw leader lock, independently of engine execution.
  new Promise<void>((resolve) => {
    void navigator.locks.request(
      leaderLock,
      () =>
        // oxlint-disable-next-line effecttsgo/new-promise -- The lock callback's Promise intentionally lives until the driver calls releaseLeader.
        new Promise<void>((release) => {
          releaseLeader = release
          resolve()
        })
    )
  })

const holdPool = () =>
  // oxlint-disable-next-line effecttsgo/new-promise -- Browser driver waits for the separate holder worker's actual OPFS acquisition report.
  new Promise<{ readonly count: number }>((resolve, reject) => {
    poolWorker = new Worker(new URL("./opfs-holder.worker.ts", import.meta.url), { type: "module" })
    poolWorker.addEventListener(
      "message",
      (event: MessageEvent<{ readonly _tag: string; readonly count?: number; readonly message?: string }>) => {
        if (event.data._tag === "Held") return resolve({ count: event.data.count ?? 0 })
        if (event.data._tag === "Failed") reject(new Error(event.data.message))
      }
    )
    // oxlint-disable-next-line unicorn/require-post-message-target-origin -- dedicated workers have no target origin
    poolWorker.postMessage({ _tag: "Hold", directory: `echo-sync/${appId}/${profile}` })
  })

// oxlint-disable-next-line unicorn/require-post-message-target-origin -- dedicated workers have no target origin
const releasePool = () => poolWorker?.postMessage({ _tag: "Release" })

const holder = { holdLeader, holdPool, releaseLeader: () => releaseLeader?.(), releasePool }
Object.assign(window, { echo: holder, holder })
