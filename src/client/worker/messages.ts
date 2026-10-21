/**
 * Control-plane messages between a tab, its leader-candidate worker, and the router SharedWorker.
 * These only broker `MessagePort`s and election state; all data traffic is Effect RPC
 * (`LeaderRpcs`) over the brokered ports.
 */
import * as Schema from "effect/Schema"

const Port = Schema.instanceOf(MessagePort)

/** tab → its own leader-candidate worker */
export const ToLeader = Schema.Union([
  /** `principal`: who this replica acts as (optimistic role checks). One principal per profile. */
  Schema.TaggedStruct("Init", { appId: Schema.String, profile: Schema.String, principal: Schema.String }),
  /** Serve LeaderRpcs to `tabId` on `port` until that tab's lock is released. */
  Schema.TaggedStruct("Connect", { tabId: Schema.String, port: Port }),
  /** A port to the router; the router sends `Connect` messages on it. */
  Schema.TaggedStruct("RouterPort", { port: Port }),
  /**
   * Instead of `Init`: reset this identity's replica (DESIGN §9.1 `resetReplica`) and answer with one
   * `ResetOutcome`. The worker never becomes leader.
   */
  Schema.TaggedStruct("Reset", { appId: Schema.String, profile: Schema.String, discardPending: Schema.Boolean })
])
export type ToLeader = typeof ToLeader.Type
export const isToLeader = Schema.is(ToLeader)

export const ResetOutcome = Schema.Union([
  /** The pool directory is gone (or never existed). */
  Schema.TaggedStruct("Done", {}),
  /** A leader holds the identity's leader lock; nothing was touched. */
  Schema.TaggedStruct("ReplicaInUse", {}),
  /** The outbox has unreleased entries and `discardPending` was not set; nothing was deleted. */
  Schema.TaggedStruct("PendingWrites", { count: Schema.Int }),
  Schema.TaggedStruct("ResetFailed", { message: Schema.String })
])
export type ResetOutcome = typeof ResetOutcome.Type

/** leader-candidate worker → its tab */
export const FromLeader = Schema.Union([
  /** The leader lock is held by someone else; this worker is queued behind it. */
  Schema.TaggedStruct("Waiting", {}),
  /** This worker holds the leader lock and has opened the database. */
  Schema.TaggedStruct("Elected", { leaderId: Schema.String }),
  /** This worker won the election but could not open the database; it released the lock. */
  Schema.TaggedStruct("Failed", { message: Schema.String }),
  /** The answer to `Reset`; the worker has released every lock and closed the pool before sending it. */
  Schema.TaggedStruct("ResetOutcome", { outcome: ResetOutcome })
])
export type FromLeader = typeof FromLeader.Type
export const isFromLeader = Schema.is(FromLeader)

/** tab → router */
export const ToRouter = Schema.Union([
  /**
   * Sent on every (re)connect to a router instance. `connectedTo`: the leader this tab already has a live
   * connection to, if any; the router does not link the tab to that leader again (re-Hello is idempotent).
   */
  Schema.TaggedStruct("Hello", {
    appId: Schema.String,
    profile: Schema.String,
    tabId: Schema.String,
    connectedTo: Schema.NullOr(Schema.String)
  }),
  /** Sent by the tab whose worker was just elected: `port` reaches that worker. */
  Schema.TaggedStruct("Leader", { leaderId: Schema.String, port: Port })
])
export type ToRouter = typeof ToRouter.Type
export const isToRouter = Schema.is(ToRouter)

/** router → tab */
export const FromRouter = Schema.TaggedStruct("Connected", { leaderId: Schema.String, port: Port })
export type FromRouter = typeof FromRouter.Type
export const isFromRouter = Schema.is(FromRouter)

/**
 * router → every tab, on the BroadcastChannel named `Platform.routerName(identity)`: a router instance
 * received its first `Hello`. A browser may relaunch the SharedWorker empty (WebKit ends it with its creating
 * tab), so every tab reconnects and re-sends `Hello`, and the tab whose worker leads re-announces `Leader`.
 */
export const RouterBroadcast = Schema.TaggedStruct("RouterStarted", {})
export type RouterBroadcast = typeof RouterBroadcast.Type
export const isRouterBroadcast = Schema.is(RouterBroadcast)

/** Minimal shape shared by `MessagePort`, `Worker`, and a dedicated worker's global scope. */
export interface Endpoint {
  postMessage(message: unknown, options: { transfer: Array<Transferable> }): void
  addEventListener(type: "message", listener: (event: MessageEvent) => void): void
}
