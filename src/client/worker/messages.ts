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
  Schema.TaggedStruct("RouterPort", { port: Port })
])
export type ToLeader = typeof ToLeader.Type
export const isToLeader = Schema.is(ToLeader)

/** leader-candidate worker → its tab */
export const FromLeader = Schema.Union([
  /** The leader lock is held by someone else; this worker is queued behind it. */
  Schema.TaggedStruct("Waiting", {}),
  /** This worker holds the leader lock and has opened the database. */
  Schema.TaggedStruct("Elected", { leaderId: Schema.String }),
  /** This worker won the election but could not open the database; it released the lock. */
  Schema.TaggedStruct("Failed", { message: Schema.String })
])
export type FromLeader = typeof FromLeader.Type
export const isFromLeader = Schema.is(FromLeader)

/** tab → router */
export const ToRouter = Schema.Union([
  Schema.TaggedStruct("Hello", { appId: Schema.String, profile: Schema.String, tabId: Schema.String }),
  /** Sent by the tab whose worker was just elected: `port` reaches that worker. */
  Schema.TaggedStruct("Leader", { leaderId: Schema.String, port: Port })
])
export type ToRouter = typeof ToRouter.Type
export const isToRouter = Schema.is(ToRouter)

/** router → tab */
export const FromRouter = Schema.TaggedStruct("Connected", { leaderId: Schema.String, port: Port })
export type FromRouter = typeof FromRouter.Type
export const isFromRouter = Schema.is(FromRouter)

/** Minimal shape shared by `MessagePort`, `Worker`, and a dedicated worker's global scope. */
export interface Endpoint {
  postMessage(message: unknown, options: { transfer: Array<Transferable> }): void
  addEventListener(type: "message", listener: (event: MessageEvent) => void): void
}
