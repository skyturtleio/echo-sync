# Getting started: the server side

This page covers how a consumer app wires the Echo Sync server: its tables, its mutations, its
app migrations, startup, and the request principal. The worked example is
[examples/server/main.ts](../examples/server/main.ts). It serves the harness app
(examples/browser-spike/app.ts) and is what scenarios.sh and slice.sh run against
(docs/vertical-slice.md). DESIGN.md is the contract, and docs/server-substrate.md describes how
the server is built.

The server needs **PostgreSQL 15+** (DESIGN §11) and runs on Bun. Import core from `echo-sync`
and the server from `echo-sync/server`.

```
┌───────────────────────────── Startup.layer ─────────────────────────────┐
│ migrate ─▶ migrateApp ─▶ attach (each app table) ─▶ Migrate.verify ─────┼─▶ /sync
│ engine     your            capture triggers         Drift: every        │   (only if
│ schema     migrations                               mismatch, no serve  │    verify passed)
└─────────────────────────────────────────────────────────────────────────┘
```

## 1. Tables: the registry

Each synced table is a `SyncTable`. Its columns are `id`, `accessScopeId` and your fields. The
registry is the list of them, and it is shared by client and server:

```ts
import { AccessScope, Registry, SyncTable } from "echo-sync"

export const Todos = SyncTable.make("todos", {
  id: TodoId,
  accessScopeId: AccessScope.AccessScopeId,
  title: Schema.String
})
export const registry = Registry.make([Todos]) // adds the engine's scope_grants table
```

DESIGN §5 lists the field types that map to columns. A table's Postgres DDL is
`table.postgres.createTable` / `createIndexes`, and you never write it by hand (see §3).

## 2. Mutations

A mutation is a named handler over `ctx.store` (DESIGN §6). It runs optimistically on the client and
authoritatively on the server. Build a registry of them with `Mutation.makeRegistry`. The server's copy
can add checks the client cannot evaluate. In the example, `todos.add` on the server rejects some
titles with a declared domain error, which the client receives as a ledger outcome.

`ctx.store` checks the actor's role on every write. A data row needs `writer`, and a `scope_grants`
row needs `owner`. Owners grant and revoke through ordinary mutations on `ScopeGrants.ScopeGrants`,
with `id: ScopeGrants.grantId(scope, principal)`.

**Creating a scope is not an engine API yet.** DESIGN §4's "scope creation" primitive does not exist
today. The server side of your app inserts the `echo_sync.access_scopes` row and the first
`owner` grant in SQL. The example's `/admin/grant` route shows the two statements.

## 3. App migrations

Your tables are created by your own migrations. They are numbered `NNN_name` modules, each with a
default export `Effect` over `SqlClient`, and `Startup` runs them in their own history table,
`echo_sync.app_migrations` (DESIGN §11). Generate them from the registry with core's planner. It
compares the registry against the committed snapshot `migrations/manifest.json` and emits only
safe DDL. Renames, drops and backfills become commented TODOs.

```ts
import { Migrate } from "echo-sync"
const previous = Migrate.decodeSnapshot(await Bun.file("migrations/manifest.json").text()) // or Migrate.emptySnapshot
const g = Migrate.generate({ previous, registry, existing: ["001_create_todos"], name: "add_due_date" })
await Bun.write(`migrations/${g.id}.ts`, g.source) // review it, then commit both files together
await Bun.write("migrations/manifest.json", g.snapshot)
```

Pass them to the server as a record keyed by id. The example has one, at
[examples/server/migrations/001_create_todos.ts](../examples/server/migrations/001_create_todos.ts):

```ts
import createTodos from "./migrations/001_create_todos.ts"
const appMigrations = { "001_create_todos": createTodos }
```

Migration modules are immutable once they have run against a database. Fix mistakes with a new
migration.
Keep `manifest.json` byte-identical to `g.snapshot`. Exclude it from your formatter: this repo's
`.oxfmtrc.json` ignores `**/migrations/manifest.json`.

## 4. Startup and serving

```ts
import { Startup } from "echo-sync/server"

const sync = Startup.layer({ registry, mutations: serverMutations, appMigrations, principalFor })
  .pipe(Layer.provide(PgClient.layer({ url, maxConnections: 40 })))
```

`Startup.layer` runs, in order:
1. The engine migrations.
2. Your app migrations.
3. `attach` for every app table, which installs the capture and truncate-guard triggers.
4. `Migrate.verify`.

Only after all four does it mount `Protocol.SyncRpcs` (HTTP POST + NDJSON) at `path`, default
`/sync`. Several instances may start concurrently, including against an empty database:
migrations and attach are serialized, and verify only reads.

`Startup.layer` must be built before you accept traffic. Otherwise a startup failure only appears
on the first request. The example builds it into a shared `MemoMap` and exits on failure, then
hands the same layer and `MemoMap` to `HttpRouter.toWebHandler`, which reuses that build:

```ts
const memoMap = Layer.makeMemoMapUnsafe()
const started = await Effect.runPromiseExit(
  Layer.buildWithMemoMap(Layer.provideMerge(sync, HttpRouter.layer), memoMap, Scope.makeUnsafe())
)
if (Exit.isFailure(started)) {
  console.error(Cause.pretty(started.cause))
  process.exit(1)
}
const web = HttpRouter.toWebHandler(sync, { memoMap })
Bun.serve({ fetch: (request) => web.handler(request) })
```

Other options are `pageLimit`, the number of pull rows per page (default 500), and `ledger`.
Log GC (`Postgres.collectGarbage(retention)`) is not scheduled by `Startup`. Run it from your own
job if you want a retention horizon (docs/server-substrate.md, GC).

## 5. Headers and the principal

`principalFor(headers)` is app-owned (DESIGN §4). It authenticates the request and returns the
`PrincipalId`, or fails with `Protocol.Unauthorized`. The server never takes the principal from the
payload, and every pull and push is filtered by that principal's grants (or `*`). On the client,
`LeaderOptions.syncHeaders(principal)` puts headers on every push, pull and pokes request.

The example trusts an `x-echo-principal` header. **That is a dev shim with no authentication.** A
real app verifies a session cookie or bearer token here.

## 6. Drift: what it means and how to fix it

`Migrate.verify` compares the live catalog with the registry. If they differ, startup fails with
one `Drift` that lists **every** mismatch, and the server does not serve (DESIGN §11). For the
example with `todos.title` dropped:

```
Drift: schema drift (refusing to serve):
  todos: declared column title is missing (every wire row must carry it, §11)
```

Drift means the database is not what the code that is starting expects. Fix the database or the
code. Never bypass the check.

| mismatch | usual cause | fix |
|---|---|---|
| `table does not exist` | the table is in the registry but no migration creates it | generate and commit its migration |
| `declared column … is missing`, or wrong type / collation / nullability | the migration was not generated, was edited after generation, or the database was changed by hand | generate a migration for the registry change, or repair the column with a new migration |
| a declared index or unique set is missing or different | the same | the same |
| `not attached`, or a missing / disabled / wrong-shape `echo_sync` trigger | the triggers were dropped or altered by hand | restart: startup re-attaches app tables, which recreates their triggers enabled. `scope_grants` is not re-attached at startup; restore it once with `Postgres.attach(ScopeGrants.ScopeGrants, { schema: "echo_sync" })` |
| `captured in N schemas` | two relations with the same name both carry the capture trigger | drop the triggers from the one that is not synced |
| `the unqualified name resolves through search_path to …` | `search_path` selects a different relation than the captured one | fix the server role's `search_path`; `Startup` attaches unqualified names |

Extra undeclared columns and extra indexes are allowed.
