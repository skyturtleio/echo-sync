# Getting started: wiring a server to the SPEC

> Consumer guide for SPEC §3, §4, §5, §8 and §13 (wiring a server). Non-normative: where this page disagrees with
> [`docs/SPEC.md`](SPEC.md), the SPEC wins and the disagreement is a SPEC Open Question.

This page covers how a consumer app wires the Echo Sync server: its tables, its mutations, its
app migrations, startup, and the request principal. The worked example is
[examples/server/main.ts](../examples/server/main.ts). It serves the harness app
(examples/browser-spike/app.ts) and is what scenarios.sh and slice.sh run against (run instructions
in `AGENTS.md`). [`SPEC.md`](SPEC.md) is the contract; its non-normative appendices explain
the implementation mechanisms.

The server needs **PostgreSQL 15+** (SPEC §13) and runs on Bun. Import core from `echo-sync`
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

SPEC §3 lists the field types that map to columns. A table's Postgres DDL is
`table.postgres.createTable` / `createIndexes`, and you never write it by hand (see §3).

### Local text search

Declare ordinary normalized text columns. The app recomputes them on every source edit,
including imports and server-side writes; the engine does not derive them automatically.

```ts
import { AccessScope, Query, Search, SyncTable } from "echo-sync"
import { Schema } from "effect"

const Aircraft = SyncTable.make("aircraft", {
  id: Schema.String.pipe(Schema.brand("AircraftId")),
  accessScopeId: AccessScope.AccessScopeId,
  designator: Schema.String,
  searchText: Search.Text
}, { indexes: { by_designator: ["designator"], by_search: ["searchText"] } })

// In the importer/mutation: store this alongside the source fields.
const searchable = (manufacturer: string, model: string, variant: string) =>
  Search.Text.make(Search.normalize([manufacturer, model, variant].join(" ")))

export const findAircraft = Query.make("aircraft.search", {
  args: Schema.String,
  success: Schema.Array(Aircraft.Row),
  handler: (db, input) => db.query(Aircraft).withIndex("by_designator")
    .contains("searchText", Search.normalize(input)).take(30)
})
// For indexed starts-with suggestions, in a Query.make handler:
// db.query(Aircraft).withIndex("by_search", q =>
//   q.prefix("searchText", Search.normalize(input))).take(30)
```

Register `findAircraft` in the leader's `Query.makeRegistry`; subscribe through the existing
typed client `subscribe(findAircraft, input)` API. Search subscriptions update after ordinary
row changes and grant revocation. For flights, combine remarks and crew into a search field
and select a scope/date index to return recent matching flights. For large airport catalogs,
use separate indexed normalized code/name fields for prefix suggestions.

This is literal search, not FTS: `%`, `_`, quotes and backslashes are data; multiple words
form one contiguous substring. Normalization lowercases and collapses whitespace, retaining
accents (no stemming, ranking, fuzzy matching or full Unicode case folding). Operators also
work on existing string-valued text columns, with no implicit normalization. NULL never matches;
empty input matches all non-null text. A second `contains` replaces the first. The selected
index defines ordering, not relevance, and scope/text filtering happens before `take`.
Contains may scan every candidate; a result limit is not a latency bound.

Adding these columns requires the normal PostgreSQL migration/backfill and replica rebuild.
Normalization changes require an explicit data backfill even if the storage shape is unchanged.

### Keyset pagination

Every declared index already ends with implicit `id`, including its physical SQLite/Postgres
index. Do not declare `id` yourself. For an existing `by_scope_date: ["accessScopeId", "date"]`
index, no schema/index migration is needed:

```ts
// In a registered Query.make handler; last is the previous page's last row.
db.query(Flights)
  .withIndex("by_scope_date", q => q.eq("accessScopeId", scope))
  .order("desc")
  .after({ accessScopeId: scope, date: last.date, id: last.id })
  .take(N)
// .after(last) also works. Omit .after(...) for the first page.
```

The cursor includes **all declared index fields plus id**, in decoded/domain form (use a
`Date` for a `Schema.Date` field). Descending order reverses both date and id. A page can split
a date tie group without skipping or repeating unchanged rows; the boundary row need not
still exist. NULL is smallest ascending, largest descending. Cursors compose with scalar
ranges, prefix and contains; all predicates and scope checks run before `take`.

`before(cursor)` selects strictly earlier keys in the final order, and `take` still returns
the first N in that order, not the closest N to the cursor. For the nearest previous page,
reverse order, use `after`, then reverse the returned array. Repeating either bound replaces
that side; after and before together intersect. Calling `order` later affects both bounds.

Pages are separate snapshots: inserts behind the boundary do not shift it; inserts ahead may
appear, and updates moving keys across it may repeat or omit rows across reads. Registered
page subscriptions (even empty pages) retain ordinary table/grant invalidation and refresh
after writes or revocation. Non-nullable keys use SQL tuple seeks; nullable keys use equivalent
null-aware predicates. Planner choices and selective text/scope filters can still add work.

## 2. Mutations

A mutation is a named handler over `ctx.store` (SPEC §5). It runs optimistically on the client and
authoritatively on the server. Build a registry of them with `Mutation.makeRegistry`. The server's copy
can add checks the client cannot evaluate. In the example, `todos.add` on the server rejects some
titles with a declared domain error, which the client receives as a ledger outcome.

`ctx.store` checks the actor's role on every write. A data row needs `writer`, and a `scope_grants`
row needs `owner`. Owners grant and revoke through ordinary mutations on `ScopeGrants.ScopeGrants`,
with `id: ScopeGrants.grantId(scope, principal)`.

Handler helpers that explicitly annotate their context must use `Mutation.HandlerContext`, not
the engine-facing `Mutation.MutationContext`. Handler store failures now carry
`StoreOperationFailure { reason: StoreError }`: recover a missing row with
`Effect.catchReason("StoreOperationFailure", "RowNotFound", ...)`, rather than
`Effect.catchTag("RowNotFound", ...)`. If inspecting with `Store.isOperationFailure`, rethrow
the envelope, not its `reason`, to preserve store provenance. Declared domain errors can use
engine error classes without being mistaken for infrastructure failures.

A handler creates a scope with `ctx.store.createScope(scopeId)` (SPEC §4). The scope id must be
fresh, and the actor becomes its first `owner`. A second creator gets
`ScopeForbidden { required: "owner" }`: "already exists" reads as "not yours". So does anyone
creating a scope whose grants were all revoked, because the server keeps every scope id it has
seen. On the client the claim is optimistic, and the server's rejection undoes it on the next
pull. The example's `/admin/grant` route still writes grants in SQL; that is an admin shortcut
for the scenarios, not the app path.

## 3. App migrations

Your tables are created by your own migrations. They are numbered `NNN_name` modules, each with a
default export `Effect` over `SqlClient`, and `Startup` runs them in their own history table,
`echo_sync.app_migrations` (SPEC §13). Generate them from the registry with core's planner. It
compares the registry against the committed snapshot `migrations/manifest.json` and emits only
safe DDL. Renames, drops and backfills become commented TODOs.

Server-only tables (for example `users`) use ordinary hand-written `NNN_name` modules
alongside generated SyncTable migrations, in the same `appMigrations` record and numbering
sequence. Include their ids in `existing` when generating; they do not belong in the sync
registry or its manifest.

```ts
import { Migrate } from "echo-sync"
import { Effect } from "effect"
const text = await Bun.file("migrations/manifest.json").text()
const g = await Effect.runPromise(Effect.gen(function* () {
  const previous = yield* Migrate.decodeSnapshot(text) // or Migrate.emptySnapshot for the first migration
  return yield* Migrate.generate({ previous, registry, existing: ["001_create_todos"], name: "add_due_date" })
}))
await Bun.write(`migrations/${g.id}.ts`, g.source) // review it, then commit both files together
await Bun.write("migrations/manifest.json", g.snapshot)
```

Snapshot decoding, encoding and generation are lazy Effects with recoverable `MigrationPlanError`
failures. `stage` identifies `snapshot-decode`, `snapshot-encode`, `intent` or `name`; snapshot
failures retain the original `SchemaError` in `cause`. The pure `plan` and `nextId` APIs return
`Result` rather than throwing for invalid intent or names. No database or filesystem service is
required; your script owns reading and writing the committed files.

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

When upgrading a pre-scope-index app, regenerate from that snapshot even if its declarations
did not change: the generated migration adds `table__echo-scope` on `(access_scope_id, id)`
for every synced app table. Commit and register that migration before starting the new server;
otherwise startup reports missing-index `Drift`. The snapshot records `scopeWalkIndex: true`,
but neither client hash changes and no replica rebuild is needed. Large tables may need a
reviewed concurrent build outside the migration transaction; see SPEC §13.2.1.

`generate` also preserves `history: [{ migration: "NNN_name", dataHash }]` and appends the
current registry hash, including on index-only migrations (repeated hashes are intentional).
The hash includes system tables such as `scope_grants`, exactly as the client's `Registry.make`
does; the snapshot's `tables` still holds only app manifests. Pre-gate snapshots without history
remain readable; no hashes are invented for earlier releases. History starts at your next
generated migration. Always use `generate`, not `snapshotOf`, to rewrite the committed file.
See [the server policy hook](SPEC.md#d4-client-policy-example) for deriving an
accepted floor from these entries. The default is serve: an additive rollout needs no policy.

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

Configure HTTP compression in the host stack rather than around `Startup.layer`. A host may use
Effect's `HttpMiddleware.compression()` for finite pull responses when the client advertises
`Accept-Encoding`; browser Fetch decompresses them before the RPC codec reads them, and Effect's
Fetch client follows whichever behavior its configured Fetch implementation provides. Never apply
compression indiscriminately to `/sync`: `pokes` and presence `room` are long-lived NDJSON streams,
and a host or proxy that does not flush compressed chunks promptly can delay their heartbeats.

Build `Startup.layer` and check the result before you accept traffic. `HttpRouter.toWebHandler`
starts building its layer immediately, but it gives you no result to await: a failed build only
shows up as the rejection of each request. The example builds the layer into a shared `MemoMap`
and exits on failure. It then hands the same layer and `MemoMap` to `toWebHandler`, which reuses
that build. The startup scope owns the build, so keep it and close it after `web.dispose()`:

```ts
const memoMap = Layer.makeMemoMapUnsafe()
const startupScope = Scope.makeUnsafe()
const started = await Effect.runPromiseExit(
  Layer.buildWithMemoMap(Layer.provideMerge(sync, HttpRouter.layer), memoMap, startupScope)
)
if (Exit.isFailure(started)) {
  console.error(Cause.pretty(started.cause))
  process.exit(1)
}
const web = HttpRouter.toWebHandler(sync, { memoMap })
Bun.serve({ fetch: (request) => web.handler(request) })

// On shutdown, every step runs even if an earlier one rejects:
// web.dispose().finally(() => Effect.runPromise(Scope.close(startupScope, Exit.void)))
```

Other options are `pageLimit`, the number of pull rows per page (default 500), and `ledger`.
Log GC (`Postgres.collectGarbage(retention)`) and ledger expiry (`Postgres.expireLedger({ retention })`,
default 7 days, at least 48 h) are not scheduled by `Startup`. Run them from your own job if you want
a retention horizon; without ledger expiry the ledger grows with every mutation
(SPEC §11; examples/server runs ledger expiry hourly).

## 5. Headers and the principal

`principalFor(headers)` is app-owned (SPEC §4). It authenticates the request and returns the
`PrincipalId`, or fails with `Protocol.Unauthorized`. The server never takes the principal from the
payload, and every pull and push is filtered by that principal's grants (or `*`). On the client,
`LeaderOptions.syncHeaders(principal)` returns an `Effect` of headers that the transport runs for every
request: each push, each pull, each stream (re)connect. Your app owns refresh: return the credential you
want on the wire now (cache and renew it as you like). A renewed token is used by the next request with no
leader restart; if a renewal fails, return what you have and the server's `Unauthorized` is retried.

The example trusts an `x-echo-principal` header. **That is a dev shim with no authentication.** A
real app verifies a session cookie or bearer token here.

## 6. Drift: what it means and how to fix it

`Migrate.verify` compares the live catalog with the registry. If they differ, startup fails with
one `Drift` that lists **every** mismatch, and the server does not serve (SPEC §13). For the
example with `todos.title` dropped:

```
Drift: schema drift (refusing to serve):
  todos: declared column title is missing (every wire row must carry it, SPEC §13)
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

## 7. Restore a Postgres backup without leaving clients on the old history

Stop all server instances and drain requests before restoring the whole database, including
`echo_sync`. If the backup predates engine migration 004, run the current engine migrations
while traffic remains blocked. Then run this one statement on the restored database:

```sql
UPDATE echo_sync.engine_state
SET epoch = gen_random_uuid()::text, require_epoch = true;
```

Check that it reports `UPDATE 1`, then restart the instances and reopen traffic. Use this step
after a Coolify database-backup restore too. Do not rotate during live traffic or disable
`require_epoch` afterwards. Normal server restarts do not require rotation.

Deploy the epoch-aware server before its clients. Legacy clients can sync during rollout;
after the restore fence, clients without the epoch field receive `ClientOutdated` and must
update. Epoch-aware clients replace their replica and resend pending mutations without
changing `clientId`. Mutations already acknowledged and released before the restore are lost
if they are absent from the backup; this mechanism cannot recover them. See SPEC §7.4 and §11.1.

## 8. Contributor type checking and editor setup

Install with `bun install --frozen-lockfile`. The lockfile pins TypeScript `7.1.0-beta`,
`@effect/tsgo` `0.59.0`, tsrs `0.20.0-ts7.1.0-beta`, Oxlint `1.89.0` and its type-aware
backend `oxlint-tsgolint` `7.0.2005`. tsrs targets the same TypeScript beta; it runs with
reference defaults, not the diagnostic-changing `--fast` preset.

- `bun run typecheck` checks the root project and all five example/driver projects with
  Effect's enhanced Go compiler. `bun run typecheck:tsrs` checks the same projects with its
  enhanced Rust compiler.
- `bun run lint` first preserves the original syntax-based correctness/suspicious checks,
  then runs `.oxlintrc.type-aware.json`: all 26 Effect correctness rules, all 20 anti-pattern
  rules and all 25 Effect-native rules, plus explicit promise and inferred-`any` safety rules.
  Both passes fail on warnings.
  The second profile turns off category expansion so it adds only the selected rules;
  Effect's style preset is not enabled.
- `bun run check` includes both compilers, both lint passes, registry/vector checks,
  formatting and unit tests. Run `bun run test:pg` with the disposable Postgres running for
  integration coverage; browser commands remain in `AGENTS.md`.

All enhanced tools run through `effect-tsgo tsc@…`, `effect-tsgo tsrs@…` or
`effect-tsgo oxlint`. There is deliberately no install/prepare patch hook: never run
`effect-tsgo patch` for this repo. Installed compiler and linter packages remain untouched.

In VS Code on macOS/Linux, install the workspace recommendations (**TypeScript 7** and **Oxc**),
trust the workspace and accept the workspace SDK when prompted. The editor extension host needs
Node `22.12+` (or a newer supported major) on its `PATH`; Bun alone cannot run these Node launchers.
The committed settings use
`.vscode/tsdk/tsgo` for the TypeScript server and `.vscode/oxlint` for type-aware/Effect
diagnostics. These small executable launchers are needed because the editors accept executable
paths rather than commands with arguments; they delegate to the installed Effect tooling and
never copy or modify binaries. Effect diagnostics come from Oxlint only (`diagnostics: false`
in the TypeScript plugin prevents duplicates); Effect quick fixes/refactors still use the
enhanced TypeScript server. The editor uses the selected type-aware profile; the CLI additionally
runs the full original syntax-category gate. Restart both servers after changing tool versions.

Other LSP clients can launch `bun run lsp` (TypeScript/Effect features) and
`bun run effect-tsgo oxlint --lsp` (lint diagnostics), with the repository as their working
directory. Oxlint's LSP takes its configuration through `initialize.initializationOptions`,
not the CLI `--config` flag: send an array containing `{ "workspaceUri": "file:///absolute/repo",
"options": { "configPath": "/absolute/repo/.oxlintrc.type-aware.json", "run": "onType" } }`.
Use the real repository URI/path and return the same options object for its
`workspace/configuration` requests for section `oxc_language_server`. TypeScript uses LSP
pull diagnostics (`textDocument/diagnostic`); Oxlint publishes diagnostics on document changes.
Do not also start a stock TypeScript server.
The VS Code SDK launcher is not a Windows `.exe`; Windows editors need their own supported
command-based LSP configuration. Effect's packaged tsrs/Oxlint integrations do not support musl.

No correctness or anti-pattern rule is globally disabled. Generic helpers preserve their actual
error channels rather than erasing them to `unknown`; telemetry classifiers cannot widen the
observed operation's error type. Schema-backed engine errors declare structural intent explicitly
with the second generic `{}`; local-only failures use `Data.TaggedError` rather than exposing
unused schema capabilities. Internal testing errors use nominal brands. These are typing and
responsibility choices, not execution provenance: store-origin failures still require the
stage-owned `StoreOperationFailure` envelope.

Error identity payloads no longer shadow `Error.name`: use `mutationName`, `queryName`,
`lockName`, or `NoticeMismatch.expectedMutationName` / `actualMutationName`. The five serialized
errors expose `*Codec` companions using native `Schema.encodeKeys` to retain encoded `name`;
engine RPC and durable unions already use them. Use those codecs for legacy standalone data,
not the renamed class's untransformed schema. Existing outcomes/notices and worker payloads need
no migration. The 15 local-only errors no longer expose Schema constructors/decoders; consumers
that used those incidental capabilities must supply their own boundary schema.

Line-local anti-pattern exceptions identify real composition roots: test/fixture runtimes,
administrative runners, per-tab RPC servers and browser telemetry roots. Encapsulated Web Crypto
provisioning stays lazy, so importing the engine does not acquire browser services. Callable
zero-argument RPC methods mirror the generated client contract; the ledger test's returned
Effect deliberately allocates a reusable concurrent barrier. These are documented exceptions,
not an invitation to disable a rule for a whole file or all tests.

Effect-native rules prefer Effect collections, configuration, clocks, schemas and owned async
work. Platform APIs remain at actual host boundaries: browser workers, Web Locks, OPFS, DOM
events and external browser drivers still have their native contracts. Line-local explanations
identify those boundaries and synchronous wire/source-inspection oracles; converting a native
host contract into a new engine abstraction purely for lint is not the goal.
Timestamp codecs compose Effect's native millisecond conversions with the existing portable
bounds; native Date interoperability is preserved without custom conversion or precision code.

The plugin explicitly allows only the unstable Effect module families already used by the
engine/harness (SQL, RPC, HTTP, workers, reactivity, Crypto and their platform packages). New
unstable module families and experimental APIs are still checked. JavaScript browser probes
remain syntax-linted; type-aware checks cover TypeScript projects. Other local exceptions
document runtime-reflection boundaries, untyped third-party matchers, generated imports, proxy
forwarding, the private driver protocol and promise cleanup callbacks; unsafe inference elsewhere
remains an error.
