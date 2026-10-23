/**
 * What a tab client (`LeaderClient`) needs from the browser: capabilities, Web Locks, persistent storage
 * and an RPC connection to the leader over a `MessagePort`. The default is the browser itself; tests
 * provide their own, so lock grants and releases, the topology mode and the leader's answers are under
 * the test's control without replacing modules. Internal: not exported from `echo-sync/client`.
 */
import * as BrowserWorker from "@effect/platform-browser/BrowserWorker"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Scope from "effect/Scope"
import type * as Stream from "effect/Stream"
import { type Rpc, RpcClient, type RpcClientError, type RpcGroup, type RpcSchema } from "effect/rpc"
import type { WorkerError } from "effect/workers/WorkerError"
import { LeaderRpcs } from "./LeaderRpcs.ts"
import * as Platform from "./platform.ts"

type Rpcs = RpcGroup.Rpcs<typeof LeaderRpcs>

/** One leader's `LeaderRpcs`, each call as the tab uses it; a transport failure is `RpcClientError`. */
export type LeaderConnection = {
  readonly [Current in Rpcs as Current["_tag"]]: (
    input: Rpc.PayloadConstructor<Current>
  ) => Current extends Rpc.Rpc<
    infer _Tag,
    infer _Payload,
    infer Success,
    infer Error,
    infer _Middleware,
    infer _Requires
  >
    ? [Success] extends [RpcSchema.Stream<infer A, infer E>]
      ? Stream.Stream<A["Type"], E["Type"] | Error["Type"] | RpcClientError.RpcClientError>
      : Effect.Effect<Success["Type"], Error["Type"] | RpcClientError.RpcClientError>
    : never
}

export interface TabPlatform {
  readonly capability: () => { readonly unsupported: string | undefined; readonly sharedWorker: boolean }
  readonly holdLock: (name: string) => Effect.Effect<void, Platform.LockError, Scope.Scope>
  readonly isLockHeld: (name: string) => Effect.Effect<boolean, Platform.LockError>
  readonly awaitLockReleased: (name: string) => Effect.Effect<void, Platform.LockError>
  /** Best effort: ask the browser to keep this origin's storage (OPFS) under pressure. */
  readonly persistStorage: Effect.Effect<void>
  /** The RPC client to the leader on the other end of `port`, open for the enclosing scope. */
  readonly connect: (port: MessagePort) => Effect.Effect<LeaderConnection, WorkerError, Scope.Scope>
}

const browser: TabPlatform = {
  capability: Platform.tabCapability,
  holdLock: Platform.holdLock,
  isLockHeld: Platform.isLockHeld,
  awaitLockReleased: Platform.awaitLockReleased,
  persistStorage: Effect.sync(() => void navigator.storage.persist().catch(() => false)),
  connect: (port) =>
    Effect.gen(function* () {
      // Built in the caller's scope, so the protocol lives exactly as long as the connection.
      const protocol = yield* Layer.build(
        // One port, multiplexed: a request holds its pool lease until its Exit, so the default per-item
        // concurrency of 1 would let one open `subscribe` stream block every other request.
        RpcClient.layerProtocolWorker({ size: 1, concurrency: Number.MAX_SAFE_INTEGER }).pipe(
          Layer.provide(BrowserWorker.layer(() => port))
        )
      )
      return yield* RpcClient.make(LeaderRpcs).pipe(Effect.provide(protocol))
    })
}

export const TabPlatform = Context.Reference<TabPlatform>("echo-sync/client/worker/TabPlatform", {
  defaultValue: () => browser
})
