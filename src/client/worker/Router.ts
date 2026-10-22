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
import * as Deferred from "effect/Deferred"
import * as Fiber from "effect/Fiber"
import * as Telemetry from "../../internal/Telemetry.ts"
import { type FromRouter, isToRouter, type RouterBroadcast, type ToLeader } from "./messages.ts"
import * as Platform from "./platform.ts"

export interface SharedWorkerScope {
  addEventListener(type: "connect", listener: (event: MessageEvent) => void): void
  removeEventListener?(type: "connect", listener: (event: MessageEvent) => void): void
}

/** Long-lived host-run router; interrupting it also ends its lock watchers. */
export const router = (self: SharedWorkerScope): Effect.Effect<void> =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope
    const context = yield* Effect.context<never>()
    let open = true
    const ports = new Set<MessagePort>()
    const listeners = new Map<MessagePort, (event: MessageEvent) => void>()
    const fork = (work: Effect.Effect<void>) => {
      if (!open) return undefined
      return Fiber.runIn(Effect.runForkWith(context)(work), scope)
    }
    let identity: Platform.ReplicaIdentity | undefined
    let leader: { readonly leaderId: string; readonly port: MessagePort } | undefined
    let replacement: Deferred.Deferred<void> | undefined
    const tabs = new Map<string, MessagePort>()
    /** The leader each tab was last linked to (by this instance, or as the tab reported in `Hello`). */
    const linked = new Map<string, string>()

    let announcements: Promise<void> = Promise.resolve()

    /**
     * Accept an announcement only from the current holder of its epoch lock. A dead leader's announcement
     * can arrive after its successor's (the announcing tab relays it); without this check it would replace
     * the live route and then be cleared, leaving no leader at all.
     */
    const accept = (
      replica: Platform.ReplicaIdentity,
      announced: { readonly leaderId: string; readonly port: MessagePort }
    ) =>
      Effect.gen(function* () {
        const epoch = Platform.leaderEpochLockName(replica, announced.leaderId)
        if (!(yield* Platform.isLockHeld(epoch))) {
          announced.port.close()
          return
        }
        const duplicate = leader?.leaderId === announced.leaderId
        const install = Effect.sync(() => {
          leader?.port.close()
          leader = announced
          for (const [tabId, port] of tabs) link(tabId, port)
          if (replacement !== undefined) Deferred.doneUnsafe(replacement, Effect.void)
          replacement = undefined
        })
        yield* duplicate
          ? install
          : Telemetry.observe(
              install.pipe(Effect.andThen(Telemetry.annotate({ "echo.result": "elected" }))),
              "echo.router.handover"
            )
        yield* Effect.forkIn(
          Platform.awaitLockReleased(epoch).pipe(
            Effect.andThen(
              Effect.suspend(() =>
                leader !== announced
                  ? Effect.void
                  : Telemetry.observe(
                      Effect.gen(function* () {
                        const next = Deferred.makeUnsafe<void>()
                        replacement = next
                        announced.port.close()
                        leader = undefined
                        yield* Telemetry.count("echo.leader.loss.total", "lost")
                        yield* Telemetry.count("echo.router.handover.total", "lost")
                        yield* Deferred.await(next)
                        yield* Telemetry.annotate({ "echo.result": "elected" })
                      }),
                      "echo.leader.handover"
                    )
              )
            ),
            Effect.catchCause((cause) => Telemetry.log("Error", "echo router: watching leader epoch failed", cause))
          ),
          scope
        )
      })

    const link = (tabId: string, tabPort: MessagePort) => {
      // A tab already connected to this leader keeps that connection: a second link would displace it.
      if (leader === undefined || linked.get(tabId) === leader.leaderId) return
      linked.set(tabId, leader.leaderId)
      const channel = new MessageChannel()
      const connect: ToLeader = { _tag: "Connect", tabId, port: channel.port1 }
      leader.port.postMessage(connect, { transfer: [channel.port1] })
      const connected: FromRouter = { _tag: "Connected", leaderId: leader.leaderId, port: channel.port2 }
      tabPort.postMessage(connected, { transfer: [channel.port2] })
    }

    const onConnect = (event: MessageEvent) => {
      if (!open) return
      const tabPort = event.ports[0]
      if (tabPort === undefined) return
      ports.add(tabPort)
      const onMessage = (message: MessageEvent) => {
        if (!open) return
        const data: unknown = message.data
        if (!isToRouter(data)) return
        switch (data._tag) {
          case "Hello": {
            if (identity === undefined) {
              identity = { appId: data.appId, profile: data.profile }
              // This instance starts empty: if a previous one died (WebKit ends a SharedWorker with its creating
              // tab), its tabs still hold ports to it. Tell every tab to reconnect and the leader to re-announce.
              const channel = new BroadcastChannel(Platform.routerName(identity))
              const started: RouterBroadcast = { _tag: "RouterStarted" }
              // oxlint-disable-next-line unicorn/require-post-message-target-origin -- BroadcastChannel has no target origin
              channel.postMessage(started)
              channel.close()
            }
            tabs.set(data.tabId, tabPort)
            const tabId = data.tabId
            if (data.connectedTo !== null && !linked.has(tabId)) linked.set(tabId, data.connectedTo)
            fork(
              Platform.awaitLockReleased(Platform.tabLockName(identity, tabId)).pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    if (tabs.get(tabId) !== tabPort) return
                    tabs.delete(tabId)
                    linked.delete(tabId)
                  })
                ),
                Effect.catchCause((cause) => Telemetry.log("Error", "echo router: watching tab failed", cause))
              )
            )
            link(tabId, tabPort)
            return
          }
          case "Leader": {
            if (identity === undefined) return
            const replica = identity
            const announced = { leaderId: data.leaderId, port: data.port }
            ports.add(data.port)
            // One announcement at a time, each checked against the live epoch lock when it is processed.
            announcements = announcements.then(() => {
              if (!open) return
              const fiber = fork(
                accept(replica, announced).pipe(
                  Effect.catchCause((cause) => Telemetry.log("Error", "echo router: leader announcement failed", cause))
                )
              )
              if (fiber !== undefined)
                return new Promise<void>((resolve) => {
                  fiber.addObserver(() => resolve())
                })
            })
            return
          }
        }
      }
      listeners.set(tabPort, onMessage)
      tabPort.addEventListener("message", onMessage)
      tabPort.start()
    }
    self.addEventListener("connect", onConnect)
    yield* Effect.never.pipe(
      Effect.ensuring(
        Effect.sync(() => {
          open = false
          self.removeEventListener?.("connect", onConnect)
          for (const [port, listener] of listeners) port.removeEventListener("message", listener)
          for (const port of ports) port.close()
        })
      )
    )
  }).pipe(Effect.scoped, Telemetry.boundary)

/** Default-runtime convenience runner. */
export const runRouter = (self: SharedWorkerScope): void => {
  Effect.runFork(router(self))
}
