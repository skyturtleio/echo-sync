/**
 * Router SharedWorker (DESIGN §9): one per replica identity. It never touches the database; it only
 * remembers which leader is current and hands every tab a fresh `MessagePort` to it.
 *
 * Usage, in an app-owned SharedWorker entry file:
 *
 * ```ts
 * import { runRouter } from "echo-sync/client/worker"
 * runRouter(self)
 * ```
 */
import * as Effect from "effect/Effect"
import { type FromRouter, isToRouter, type ToLeader } from "./messages.ts"
import * as Platform from "./platform.ts"

export interface SharedWorkerScope {
  addEventListener(type: "connect", listener: (event: MessageEvent) => void): void
}

export const runRouter = (self: SharedWorkerScope): void => {
  let identity: Platform.ReplicaIdentity | undefined
  let leader: { readonly leaderId: string; readonly port: MessagePort } | undefined
  const tabs = new Map<string, MessagePort>()

  const link = (tabId: string, tabPort: MessagePort) => {
    if (leader === undefined) return
    const channel = new MessageChannel()
    const connect: ToLeader = { _tag: "Connect", tabId, port: channel.port1 }
    leader.port.postMessage(connect, { transfer: [channel.port1] })
    const connected: FromRouter = { _tag: "Connected", leaderId: leader.leaderId, port: channel.port2 }
    tabPort.postMessage(connected, { transfer: [channel.port2] })
  }

  self.addEventListener("connect", (event) => {
    const tabPort = event.ports[0]
    if (tabPort === undefined) return
    tabPort.addEventListener("message", (message) => {
      const data: unknown = message.data
      if (!isToRouter(data)) return
      switch (data._tag) {
        case "Hello": {
          identity ??= { appId: data.appId, profile: data.profile }
          tabs.set(data.tabId, tabPort)
          const tabId = data.tabId
          Effect.runFork(
            Platform.awaitLockReleased(Platform.tabLockName(identity, tabId)).pipe(
              Effect.andThen(Effect.sync(() => tabs.get(tabId) === tabPort && tabs.delete(tabId)))
            )
          )
          link(tabId, tabPort)
          return
        }
        case "Leader": {
          if (identity === undefined) return
          leader?.port.close()
          leader = { leaderId: data.leaderId, port: data.port }
          const leaderId = data.leaderId
          Effect.runFork(
            Platform.awaitLockReleased(Platform.leaderEpochLockName(identity, leaderId)).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  if (leader?.leaderId !== leaderId) return
                  leader.port.close()
                  leader = undefined
                })
              )
            )
          )
          for (const [tabId, port] of tabs) link(tabId, port)
          return
        }
      }
    })
    tabPort.start()
  })
}
