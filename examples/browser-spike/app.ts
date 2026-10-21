/**
 * The harness app, shared by the browser leader (leader.worker.ts) and the slice server
 * (examples/server/main.ts): one synced table and its mutations.
 *
 * - `t` writes nothing. The substrate scenarios use it to exercise the outbox, ownership and handover.
 * - `todos.add` inserts a todo into a scope. The server's copy adds a server-authoritative domain check
 *   the client cannot evaluate (titles containing "server-rejects"), so a rejection reaches the client
 *   only through the ledger, as a typed notice, and honours scenario gates (`serverHooks`).
 * - `todos.update` patches the fields it names (`title`, `note`) of an existing todo, with the same
 *   server-only title check; a missing row is the store's `RowNotFound` rejection.
 * - `todos.delete` deletes an existing todo; a missing row is `RowNotFound`.
 */
import { Deferred, Effect, Schema } from "effect"
import { AccessScope, Mutation, Registry, SyncTable } from "../../src/core/index.ts"

const TodoId = Schema.String.check(Schema.isNonEmpty()).pipe(Schema.brand("harness/TodoId"))

export const Todos = SyncTable.make("todos", {
  id: TodoId,
  accessScopeId: AccessScope.AccessScopeId,
  title: Schema.String,
  note: Schema.NullOr(Schema.String)
})

export const registry = Registry.make([Todos])

const AddArgs = Schema.Struct({ id: Schema.String, scope: Schema.String, title: Schema.String })
const UpdateArgs = Schema.Struct({
  id: Schema.String,
  title: Schema.optionalKey(Schema.String),
  note: Schema.optionalKey(Schema.String)
})
const DeleteArgs = Schema.Struct({ id: Schema.String })
const Blocked = Schema.Struct({ reason: Schema.Literal("blocked-title"), title: Schema.String })

const t = Mutation.make("t", { args: Schema.Json, handler: () => Effect.void })

/**
 * Server-side scenario hooks. A todo whose title contains `[gate:NAME]` parks its server handler, inside the
 * push transaction and before any write, until `/admin/open?gate=NAME`: a scenario can hold a push in flight
 * (or a rejection unanswered) for exactly as long as it asserts. `started` counts handler starts per todo id.
 */
const gates = new Map<string, Deferred.Deferred<void>>()
const gate = (name: string): Deferred.Deferred<void> => {
  const existing = gates.get(name)
  if (existing !== undefined) return existing
  const fresh = Deferred.makeUnsafe<void>()
  gates.set(name, fresh)
  return fresh
}
export const serverHooks = {
  started: new Map<string, number>(),
  /** The server-only "server-rejects" title check; `/admin/rejections?enabled=0` turns it off (a slice control). */
  rejecting: true,
  /** Opens a gate (now or before anyone waits on it); returns whether it was closed. */
  open: (name: string): boolean => Deferred.doneUnsafe(gate(name), Effect.void)
}
const gateIn = (title: string) => /\[gate:([^\]]+)\]/.exec(title)?.[1]

const addTodo = (side: "client" | "server") =>
  Mutation.make("todos.add", {
    args: AddArgs,
    error: Blocked,
    handler: (ctx, args) =>
      Effect.gen(function* () {
        if (side === "server") {
          serverHooks.started.set(args.id, (serverHooks.started.get(args.id) ?? 0) + 1)
          const name = gateIn(args.title)
          if (name !== undefined) yield* Deferred.await(gate(name))
          if (serverHooks.rejecting && args.title.includes("server-rejects")) {
            return yield* Effect.fail({ reason: "blocked-title" as const, title: args.title })
          }
        }
        yield* ctx.store.insert(Todos, {
          id: TodoId.make(args.id),
          accessScopeId: AccessScope.AccessScopeId.make(args.scope),
          title: args.title,
          note: null
        })
      })
  })

/** Server-side: counts the handler start for `id`, then parks at the title's gate, if any. */
const serverStart = (id: string, title: string | undefined) =>
  Effect.gen(function* () {
    serverHooks.started.set(id, (serverHooks.started.get(id) ?? 0) + 1)
    const name = title === undefined ? undefined : gateIn(title)
    if (name !== undefined) yield* Deferred.await(gate(name))
  })

const updateTodo = (side: "client" | "server") =>
  Mutation.make("todos.update", {
    args: UpdateArgs,
    error: Blocked,
    handler: (ctx, args) =>
      Effect.gen(function* () {
        if (side === "server") {
          yield* serverStart(args.id, args.title)
          if (serverHooks.rejecting && args.title?.includes("server-rejects") === true) {
            return yield* Effect.fail({ reason: "blocked-title" as const, title: args.title })
          }
        }
        yield* ctx.store.patch(Todos, TodoId.make(args.id), {
          ...(args.title === undefined ? {} : { title: args.title }),
          ...(args.note === undefined ? {} : { note: args.note })
        })
      })
  })

const deleteTodo = (side: "client" | "server") =>
  Mutation.make("todos.delete", {
    args: DeleteArgs,
    handler: (ctx, args) =>
      Effect.gen(function* () {
        if (side === "server") yield* serverStart(args.id, undefined)
        yield* ctx.store.delete(Todos, TodoId.make(args.id))
      })
  })

export const clientMutations = Mutation.makeRegistry([t, addTodo("client"), updateTodo("client"), deleteTodo("client")])
export const serverMutations = Mutation.makeRegistry([t, addTodo("server"), updateTodo("server"), deleteTodo("server")])

/** Dev-only credentials: the slice server trusts this header as the principal (no real auth). */
export const principalHeader = "x-echo-principal"
