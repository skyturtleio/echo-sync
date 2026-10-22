/**
 * Typecheck-only FoldKit consumer of `echo-sync/foldkit` (DESIGN §16.17): the adapter's streams and effects
 * must fit FoldKit's own `Subscription`, `Command` and `ManagedResource` types, both with the client held by
 * a ManagedResource (NilBurger) and as the static `LeaderClient` service (HeyBabe). Not run.
 */
import { Effect, Schema } from "effect"
import { Mutation, Query } from "echo-sync"
import { LeaderClient } from "echo-sync/client"
import * as Echo from "echo-sync/foldkit"
import { Command, ManagedResource, Subscription } from "foldkit"
import { defineMessageUnion } from "foldkit/message"

const rename = Mutation.make("rename", { args: Schema.Struct({ title: Schema.String }), handler: () => Effect.void })
const count = Query.make("count", { args: Schema.Struct({}), success: Schema.Number, handler: () => Effect.succeed(0) })

const Message = defineMessageUnion({
  Counted: { n: Schema.Number },
  Synced: { status: LeaderClient.SyncStatus },
  Topology: { mode: Schema.Literals(["shared-worker", "single-tab", "unsupported"]) },
  NoticesChanged: { ids: Schema.Array(Schema.Int) },
  Saved: { seq: Schema.Number },
  Done: {},
  Failed: { code: Schema.String }
})
type Message = typeof Message.Type

const failed = <E extends { readonly _tag: string }>(failure: Echo.Failure<E>) =>
  Message.Failed({ code: failure._tag === "Error" ? failure.error._tag : "Defect" })

const Sync = ManagedResource.tag<Echo.TabClient>()("Sync")
type Services = ManagedResource.ServiceOf<typeof Sync>

export const subscriptions = Subscription.make<{}, Message, Services>()(() => ({
  count: Subscription.persistentEntry(
    Echo.subscribe(Sync.get, count, {}, { onValue: (n) => Message.Counted({ n }), onFailure: failed })
  ),
  sync: Subscription.persistentEntry(
    Echo.syncStatus(Sync.get, { onValue: (status) => Message.Synced({ status }), onFailure: failed })
  ),
  topology: Subscription.persistentEntry(
    Echo.status(Sync.get, { onValue: ({ mode }) => Message.Topology({ mode }), onFailure: failed })
  ),
  notices: Subscription.persistentEntry(
    Echo.notices(Sync.get, {
      onValue: (list) => Message.NoticesChanged({ ids: list.map(({ id }) => id) }),
      onFailure: failed
    })
  )
}))

export const Rename = Command.define("Rename", {
  args: { title: Schema.String },
  messages: [Message.Saved, Message.Failed],
  execute: ({ title }) =>
    Echo.mutate(Sync.get, rename, { title }, { onSuccess: ({ seq }) => Message.Saved({ seq }), onFailure: failed })
})

// The static-service form: the tab client's own service is a valid source.
export const RequestSync = Command.define("RequestSync", {
  messages: [Message.Done, Message.Failed],
  execute: Echo.requestSync(LeaderClient.LeaderClient, { onSuccess: () => Message.Done(), onFailure: failed })
})

export const Dismiss = Command.define("Dismiss", {
  args: { ids: Schema.Array(Schema.Int) },
  messages: [Message.Done, Message.Failed],
  execute: ({ ids }) => Echo.dismissNotices(Sync.get, ids, { onSuccess: () => Message.Done(), onFailure: failed })
})
