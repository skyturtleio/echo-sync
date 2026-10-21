/**
 * The harness app, shared by the browser leader (leader.worker.ts) and the slice server
 * (examples/server/main.ts): one synced table and its mutations.
 *
 * - `t` writes nothing. The substrate scenarios use it to exercise the outbox, ownership and handover.
 * - `todos.add` inserts a todo into a scope. The server's copy adds a server-authoritative domain check
 *   the client cannot evaluate (titles starting with "server-rejects"), so a rejection reaches the client
 *   only through the ledger, as a typed notice. The server's copy also stalls titles starting with "slow"
 *   for 2 s inside the push transaction, so a scenario can close the leader while that push is in flight;
 *   `serverHooks.started` counts how often each mutation's server handler began.
 */
import { Effect, Schema } from "effect"
import { AccessScope, Mutation, Registry, SyncTable } from "../../src/core/index.ts"

const TodoId = Schema.String.check(Schema.isNonEmpty()).pipe(Schema.brand("harness/TodoId"))

export const Todos = SyncTable.make("todos", {
  id: TodoId,
  accessScopeId: AccessScope.AccessScopeId,
  title: Schema.String
})

export const registry = Registry.make([Todos])

const AddArgs = Schema.Struct({ id: Schema.String, scope: Schema.String, title: Schema.String })
const Blocked = Schema.Struct({ reason: Schema.Literal("blocked-title"), title: Schema.String })

const t = Mutation.make("t", { args: Schema.Json, handler: () => Effect.void })

export const serverHooks = { started: new Map<string, number>() }

const addTodo = (side: "client" | "server") =>
  Mutation.make("todos.add", {
    args: AddArgs,
    error: Blocked,
    handler: (ctx, args) =>
      Effect.gen(function* () {
        if (side === "server") {
          serverHooks.started.set(args.id, (serverHooks.started.get(args.id) ?? 0) + 1)
          if (args.title.startsWith("slow")) yield* Effect.sleep("2 seconds")
          if (args.title.startsWith("server-rejects")) {
            return yield* Effect.fail({ reason: "blocked-title" as const, title: args.title })
          }
        }
        yield* ctx.store.insert(Todos, {
          id: TodoId.make(args.id),
          accessScopeId: AccessScope.AccessScopeId.make(args.scope),
          title: args.title
        })
      })
  })

export const clientMutations = Mutation.makeRegistry([t, addTodo("client")])
export const serverMutations = Mutation.makeRegistry([t, addTodo("server")])

/** Dev-only credentials: the slice server trusts this header as the principal (no real auth). */
export const principalHeader = "x-echo-principal"
