# Echo Sync — Design Baseline

Status: baseline v0.3 (2026-10-21; v0.2 + substrate deltas: PullPage|PullDone with opaque continuation, replace-on-null, client_scopes state-diff membership, own OpfsSqlite host, handover semantics). This document consolidates three design threads and is
the single source of truth for implementation. Changes to this document require sign-off in
the coordinator thread. When implementation reveals a design error, propose the change,
do not silently diverge.

Project name: **Echo Sync**. Code namespace: `Echo`. Package name: `echo-sync` (consumed as
a git dependency, e.g. `"echo-sync": "github:skyturtleio/echo-sync#<sha>"`; no npm publish).
Postgres schema: `echo_sync`. Browser storage keys: `echo:<appId>:*`.

## 1. One-sentence model

**Send mutations up, send rows down.** Clients submit intent-shaped, typed mutations into a
durable local outbox and apply them optimistically to a local SQLite replica; the app's own
Postgres-backed Effect server applies each mutation authoritatively in one transaction; clients
pull authoritative row changes by cursor and rebase still-pending mutations on top.

Mutators are the conflict-resolution mechanism. No CRDTs, no IVM, no sidecar, no logical
replication, no ORM.

## 2. Goals and non-goals

Goals

- Effect v4 all the way through: one `SyncTable` Effect Schema per table drives Postgres DDL,
  SQLite DDL, row codecs, index definitions, the typed store API, and the sync rules.
- Embeds as Effect `Layer`s in the application's Bun server and browser worker. Deployment
  contract is plain Postgres via `DATABASE_URL` with permission to create the `echo_sync`
  schema. No extensions, no superuser, no replication slots.
- Offline-first: "saved" means the intent is durable in the local outbox; unsent work survives
  crashes, reloads, and replica replacement.
- Typed, reactive local reads; typed failures everywhere; no user-visible online/offline split.
- Structural sharing/security via first-class access scopes, enforced by the engine, not by
  ad-hoc `user_id` checks in every mutator.
- General enough for private user data, shared group data (HeyBabe couples), and global
  catalogs (SkyTime airports).

Non-goals (v1)

- Partial/predicate-based sync. Clients sync every scope they can read.
- CRDTs, collaborative text, IVM, query planner.
- Durable presence, blob/attachment sync.
- Per-tab SQLite replicas.
- Hosted service of any kind.

## 3. Architecture

```
Browser tab(s) ──RPC──▶ SharedWorker (router) ──MessagePort──▶ leader worker
                                                                 │  owns: SQLite WASM on OPFS
                                                                 │        (replica + outbox),
                                                                 │        sync loop, query runner
                                                                 ▼
                                                      Effect RPC over HTTP(S) at /sync
                                                                 │
                                                        app's Bun/Effect server
                                                                 │
                                                             Postgres
                                                  app tables + echo_sync.* + triggers
```

Durable path (the only path that carries application state):

```
outbox (SQLite) → push → echo_sync.mutations + echo_sync.log (Postgres) → pull → replica (SQLite)
```

Presence is a separate, in-memory, connection-scoped channel and never enters the durable path.

## 4. Access scopes

Every synced row belongs to exactly one **AccessScope** via a required `access_scope_id`
column. The v1 visibility invariant: **if a principal can read a scope, it sees every row and
every field in that scope.** Different visibility ⇒ different scope.

Engine tables (Postgres schema `echo_sync`):

| table | purpose |
|---|---|
| `access_scopes(id, created_at)` | scope identity (opaque id, app-generated) |
| `scope_grants` | a **system `SyncTable`** (`id`, `accessScopeId` = the granted scope, `principalId`, `role`, `grantedAt`); `role ∈ {reader, writer, owner}`; principal `*` = everyone (global catalogs). Replicates like any row so clients can run optimistic role checks. v1 disclosure policy: anyone who can read a scope sees who else is granted on it. Writes require `owner` on that scope; the only exception is scope creation (engine primitive: insert `access_scopes` row + first `owner` grant for the actor, allowed for any principal on a fresh id). `(accessScopeId, principalId)` is unique — derive `id` from it so duplicates are structurally impossible. |
| `clients(client_id, principal_id, cursor, last_seen_at, created_at)` | one row per client device/profile |
| `mutations(client_id, mutation_id, position, outcome, received_at)` | ledger: exactly-once + ack delivery |
| `client_scopes(client_id, position, scope_ids)` | the set of scopes a client **holds** as of each completed pull (written at `PullDone` under `nextCursor`, `ON CONFLICT DO NOTHING`; rows below the cursor a client presents are pruned on that pull). The wire cursor is a bare `Position`, and the held set cannot be rebuilt from the log (identity only, GC'd, revoke+re-grant cancels out), so it is recorded as an explicit fact. A presented cursor with no record ⇒ `ResnapshotRequired(unknownCursor)`, never trusted. |
| `log(position, table_name, row_id, access_scope_id, op, txid, committed_at)` | change identity, not payload. `table_name` is the **sync table name**, the identity shared with clients (SQLite has no schemas), unqualified; `attach` maps it to exactly one Postgres relation and refuses a second relation with the same name in another schema. |
| `migrations(...)` | `effect/sql/Migrator` history table (configured name) |

Principals are opaque strings supplied by the app (`principalFor(request)`). Authentication is
entirely app-owned. Authorization:

- Pull: engine filters log/rows to scopes the principal has any grant on.
- Mutation writes through `ctx.store`: engine checks `role ∈ {writer, owner}` for the row's
  scope (both the old and the new scope on a move). Mutators add domain checks on top.
- Grant management is itself done through mutations against the `scope_grants` system table via
  a small engine-provided API, so grant changes are captured in the log like any other change.

Store layering: the typed `Store` (`get/insert/patch/delete/query/scan`, codecs, scope-role
checks, touched-row/undo capture) is written once in `src/core` over a small per-dialect
`RowStore` (read / write / range / purgeScope / clearAll over wire-encoded rows, called inside a
transaction the substrate owns). Postgres, SQLite, and the in-memory reference implement
`RowStore` only. All three must produce identical range ordering (§10).

Scope move = `UPDATE ... SET access_scope_id = new`. The trigger emits a tombstone entry for
the old scope and an upsert entry for the new scope. Revocation stops future delivery and
purges the scope's rows on cooperative clients; it cannot retract already-downloaded data.

Scope grant added for a principal ⇒ the next pull must deliver the full current contents of
that scope at a consistent snapshot plus all later changes, with no gap. Revoked ⇒ pull
delivers a `scopePurged` instruction.

In v1 a client syncs all scopes its principal can read; explicit per-scope subscribe/unsubscribe
is deferred (the protocol must not preclude it).

## 5. SyncTable

```ts
const Todos = SyncTable.make("todos", {
  id: TodoId,                       // primary key, always `id`, Schema.String-based brand
  accessScopeId: AccessScopeId,     // required on every table
  projectId: Schema.NullOr(ProjectId),
  title: Schema.String,
  checklist: Checklist,             // nested ⇒ JSONB / TEXT(JSON) via the same Schema
  createdAt: Timestamp,
}, {
  indexes: { by_project: ["projectId", "createdAt"] },
})
```

Derived from one declaration: `Row` type, encoded (wire/DB) type, Postgres DDL, SQLite DDL,
index DDL for both, a canonical **manifest** (stable hash per table), store accessors, and
reactivity keys. Column naming on the wire and in SQL is `snake_case`; TS is `camelCase`.

Mapping rules are explicit and finite. Supported leaf types map to native columns (text,
integer, double, boolean, timestamptz/text-ISO, uuid/text); `NullOr` ⇒ nullable; anything else
that is `Schema.Struct`/`Array`/`Union`/etc. ⇒ JSON column decoded with the same Schema.
Unsupported or ambiguous mappings (e.g. a bare `Schema.Number` with no integer/finite
refinement where the dialect needs to know) **fail at declaration time with a clear error**;
nothing is guessed.

## 6. Mutations

```ts
const setCompleted = Mutation.make("todos.setCompleted", {
  args: Schema.Struct({ id: TodoId, completed: Schema.Boolean }),
  handler: (ctx, { id, completed }) => Effect.gen(function* () {
    const todo = yield* ctx.store.get(Todos, id)      // typed, scope-checked
    yield* ctx.store.patch(Todos, id, { completedAt: completed ? ctx.now : null })
  }),
})
```

- The same handler runs optimistically against SQLite (client) and authoritatively against
  Postgres (server). `ctx.store` is the only side-effecting capability; it is dialect-agnostic.
- `ctx` = `{ actor, now, random, store }`. `now` and `random` are deterministic inputs fixed by
  the engine (server uses its clock; client's optimistic result is transient). IDs should be
  generated in args, not inside handlers.
- Mutation identity: `{ clientId, mutationId }` (the idempotency key; `mutationId` is generated
  by the caller before enqueue so an enqueue retry across leader handover is idempotent, hence it
  is **not** ordered). Envelope: `{ clientId, mutationId, seq, name, args }` where `seq` is the
  client's outbox sequence: per-client monotone, gap-free, never reused. Clients push in `seq`
  order; `seq` is the only field the ledger-expiry floor (§16.5) compares against.
- **Version the mutation args, never the rows.** Rows are always at current server schema. Arg
  evolution uses Schema unions/transforms with upcast; old queued intents must still decode.
- No network, filesystem, secrets, raw SQL, or wall-clock reads inside handlers. Anything that
  needs network is an ordinary endpoint ("action"), not a mutation.

Server execution: one short Postgres transaction per mutation, in client order:
authorize → run handler (writes fire triggers → `echo_sync.log`) → write ledger row with
`position` → commit. Push is HTTP-batched (default 50, cap 100 / ~1 MB): **batch at the HTTP
level, commit at the mutation level.**

Outcomes: permanent failures (decode, forbidden, domain error) are recorded in the ledger as
processed-with-typed-error and the batch continues; transient failures (serialization `40001`
after bounded retries, connection) stop the batch at that mutation without recording it, so the
client retries from there and ordering is preserved. Retried mutations with a known identity
return the recorded outcome without re-executing. Every push outcome carries the ledger
`position` it was recorded at, so the client can tell whether its installed replica already
covers it (§8).

## 7. Log ordering invariant

Postgres sequences are not commit-ordered. Position 41 may commit after a client has already
advanced past 42 ⇒ permanently missed change. **Invariant: a client that has pulled through
cursor `c` has seen every change with `position ≤ c`.**

v1 mechanism: the trigger function `echo_sync_capture_changes()` itself takes
`pg_advisory_xact_lock(<echo key>)` before allocating a log position. Because the lock is
transaction-scoped and taken inside the trigger, **every writer participates** (mutators,
imports, scripts, cascades) without opt-in; positions are allocated in commit order. Ledger
positions are allocated under the same lock. Cost, accepted for v1: because the lock is
transaction-scoped, writers to synced tables are **serialized from their first captured row
until commit**, so a transaction that touches a synced table early and then does slow work
holds every other synced writer. Keep such transactions short. Scale-out path (xid-horizon /
commit-ordered cursors) is documented, not built.

`TRUNCATE` does not fire row triggers: unsupported on synced tables in v1 (statement-level
trigger raises). Retention: `echo_sync.log` is GC'd past a configurable horizon; a client whose
cursor is older than the horizon receives a typed `ResnapshotRequired` and re-snapshots.

## 8. Pull

- Cursor-based, scope-filtered, paginated. Each page is read in its own `REPEATABLE READ`
  snapshot (a snapshot cannot span HTTP requests without holding a transaction open, which a
  transaction pooler forbids). Request: `{ clientId, cursor, continuation: Option<PullContinuation> }`.
  Response is a tagged union so "more but nothing to continue with" and "done but no position"
  are unrepresentable: `PullPage { changes, acks, continuation }` (more to come) or
  `PullDone { changes, acks, nextCursor: Position, expiredThrough }` (page set complete; `expiredThrough` per §16.5). `PullContinuation` is
  opaque to the client (resent verbatim) but a Schema'd value on the server (row-walk progress,
  `head`, and the cursor it belongs to); a stale or invalid continuation fails with
  `ResnapshotRequired`. The continuation is **untrusted input**, not merely opaque: every value
  in it is validated against the request and the page's snapshot, not just decoded. In
  particular its `position`/`head` must lie in `[gcHorizon, max position visible in this
  snapshot]` — a **future position is `staleContinuation`**, never clamped or carried forward.
  Otherwise a forged future position becomes `nextCursor`, and because `client_scopes` keeps the
  first held set written for a cursor, two page sets ending at that same cursor with different
  scope sets leave a scope held-but-unrecorded, so its later revocation never purges. The
  "forgery only harms the forger" argument covers payload leakage (payloads are filtered by the
  principal's current grants) but **not** the one-cursor-one-held-set invariant, which must hold
  for every input. `changes` is an **ordered list** of upserts (row payload as of that
  page's snapshot), tombstones, and `scopePurged`, applied strictly in order (so a purge
  precedes a re-grant's upserts); `acks` are ledger entries for this client with
  `position ∈ (cursor, nextCursor]` across the set.
- **A page set is consistent only at `PullDone`.** A payload read at a later snapshot may
  already include a change whose log entries arrive in a later page, so applying pages
  incrementally can expose a torn transaction across rows. The client therefore stages pages
  (in a SQLite staging table, not memory) and applies the whole set, acks included, in one
  replica transaction on `PullDone`. The continuation is persisted with the staged pages so a reload resumes the set; if it cannot (stale → `ResnapshotRequired`), the staged pages are discarded, never partially applied. On `ResnapshotRequired` the client does **not** null its persisted cursor: it sets a durable resnapshot flag and keeps serving the installed replica, which remains authoritative at the old cursor (replica = replay(pending, server@cursor)) until the null-cursor set replaces it. Because positions are commit-ordered (§7), every
  change visible in any page's payload has a log entry `≤` the final `nextCursor`, so the
  installed state equals the server state at the last page's snapshot restricted to readable
  scopes. Known limitation: under sustained write load the server may keep returning `PullPage` for a long time;
  mitigate with a large page size, not by applying early. (Under this load `PullPage` keeps being returned; the client keeps staging.)
- `Position` is a branded `bigint` in memory (native comparison; `number` loses precision and
  string comparison is lexicographic) and a decimal string on the wire and in structured clone.
  `@effect/sql-pg` 4.0.5 already decodes `int8` to `bigint` (verified), so the server needs no
  extra decode. `PullContinuation` is declared in core as an opaque value (core cannot import
  server); the server owns its Schema and encodes to that opaque form. It must survive
  structured clone and storage in the client's staging table.
- Membership is a **state diff, not a log replay**: at each page the server compares the scopes
  the client holds (`client_scopes` at the cursor, §4) with the scopes readable in the page's
  snapshot. Held-but-unreadable ⇒ `scopePurged`; readable-but-not-held ⇒ start a row walk;
  outstanding walks continue; only when none remain does the page read log entries `> position`
  in readable scopes. A log entry whose row is gone or whose row's current scope is unreadable
  is delivered as a tombstone, so no payload ever leaves a readable scope. `ResnapshotRequired`
  carries a reason (`gcHorizon | unknownCursor | staleContinuation`); the client treats every
  reason the same way (discard staged pages, cursor `null`, replace the replica).
- **Invariant: a pending mutation leaves the client's optimistic replay set only when the
  authoritative replica state covering its outcome has been installed**, i.e. when the pull
  that delivers its ack is applied. An ack alone never removes an optimistic mutation. One
  corollary, not an exception: a push outcome whose ledger `position ≤` the client's current
  cursor is **already covered** by the installed replica (pull acks only `(cursor, nextCursor]`,
  so no later pull will ever ack it), and the client releases it on receipt of the push
  response, with the outcome as its notice. This is how a mutation that was duplicated into the
  outbox after its original was released (§9) terminates instead of staying pending forever.
  For this to work, the client's push watermark (`pushedThrough`, the highest `seq` it has sent)
  advances **only on push responses**, never on pull install: raising it to a released `seq` on
  install would skip a still-pending lower `seq` (such a duplicate) and leave it unsent forever.
- Initial sync = pull from `null` cursor. The log may be GC'd, so it cannot be log-driven:
  the server keyset-paginates current rows of all readable scopes by `(table, id)` with
  per-page snapshots, records the max position `head` at the first page, then continues with
  log entries `> head` until `PullDone`. Same staging rule applies. A newly granted scope is
  delivered the same way (row walk of that scope, then log) within the ordinary pull.
- **A page set that started from cursor `null` replaces the replica; it does not patch it.**
  After a resnapshot (log GC'd past the client's cursor, or a stale continuation) the server
  can only send what is readable *now*: revocations and deletes that happened while the client
  was behind have no surviving log entries, so no `scopePurged` or tombstone will ever arrive
  for rows the client still holds. Inside the install transaction the client therefore clears
  every replicated row (all `SyncTable`s, system tables included; not the outbox, not the
  staging table), installs the set, and replays pending mutations on top. For a first sync the
  clear is a no-op. `RowStore` gains a `clearAll` primitive for this; it is the only caller.
- Realtime: a poke stream (RPC stream) says "something may have changed"; it is a hint, never
  data. Pull is the only truth.

## 9. Client

- One **leader worker** (dedicated `Worker`) owns the single SQLite connection (AccessHandlePool
  VFS on OPFS — worker-only, single connection by construction), the outbox, the sync loop, and
  query execution. The connection is hosted by our own `src/client/worker/OpfsSqlite.ts`, not
  `@effect/sql-sqlite-wasm`'s `OpfsWorker.run`: that helper registers the VFS under the fixed name
  `"opfs"`, which is also its OPFS directory, and takes exclusive access handles on every file in
  it, so two identities (`appId`/`profile`) on one origin block each other (reproduced). Ours
  speaks the same port protocol (so the stock `SqliteClient.layer({ worker })` drives it) with a
  per-identity pool directory `echo-sync/<appId>/<profile>`. The protocol is internal to that
  package: re-verify `OpfsSqlite.ts` on every `@effect/sql-sqlite-wasm` bump.
- Leadership via Web Locks (`echo:<appId>:<profile>:leader`, plus per-incarnation and per-tab
  locks for death detection; never timeouts). Liveness invariant: a candidate that cannot serve (driver failed to open, OPFS unavailable) must release the leader and epoch locks so the next candidate is elected; every lock wait is cancellable. Tabs reach the leader through a `SharedWorker`
  router that holds the leader's control port and relinks a fresh `MessageChannel` per tab;
  tab↔leader traffic is Effect RPC over the worker transport. Handover: an in-flight request
  fails with `LeaderUnavailable{LeaderLost}` (the tab cannot know whether it committed; `enqueue`
  is idempotent on `mutationId` so retrying it is safe); an open subscription stream terminates
  with the same error and the caller re-subscribes (the new leader cannot know which result
  version was last seen, so a transparent resume would duplicate or skip). Without
  `SharedWorker` (capability-tested) the mode is `single-tab`: secondary tabs fail closed with
  `LeaderUnavailable{SecondaryTabWithoutRouter}` and take over only when the owner closes; a
  second DB owner is unreachable in every mode because opening the database sits behind the
  leader lock. Details and the support matrix: `docs/browser-substrate.md`.
- Local mutation: in one SQLite transaction, append outbox entry + record undo images + apply
  handler. Only after commit does the caller see "saved".
- On pull apply, in one SQLite transaction: undo pending optimistic effects (reverse order),
  apply server changes, drop acked mutations, re-run still-pending mutations. Rejected mutations
  are dropped and surfaced as typed notices (retained for inspection, not silently erased).
- Outbox idempotency must outlive release. `enqueue` is idempotent on `mutationId` so a retry
  after `LeaderLost` is safe, but the original may already have been pushed, acked, pulled and
  released in the retry window; a retry that then inserts a fresh row would replay the effect
  optimistically a second time and (see §8) get a position the pull window never acks. So
  release **marks** the outbox row (`released_at`) instead of deleting it; `enqueue` of a known
  id, pending or released, returns its original `seq` and inserts nothing; `pending()` excludes
  released rows; released rows are pruned after a retention window (default 24 h, far beyond any
  handover retry). The §8 position rule is the backstop if a released row has already been
  pruned.
- Replica is disposable; outbox is not. Schema manifest change ⇒ rebuild replica, keep outbox.
- `navigator.storage.persist()`, pending-writes warning on clear/sign-out, export of pending
  mutations.

## 10. Queries (client read API)

Convex-style typed data API, not a SQL builder:

```ts
const todosByProject = Query.make("todos.byProject", {
  args: Schema.Struct({ projectId: ProjectId }),
  success: Schema.Array(Todos.Row),
  handler: (db, { projectId }) =>
    db.query(Todos).withIndex("by_project", q => q.eq("projectId", projectId)).order("desc").take(50),
})
```

- `db.get(T, id)`, `db.query(T).withIndex(name, q => q.eq(...).gt(...)).order(...).take(n)/first()`,
  `db.scan(T)` for small tables. Joins/aggregates happen in ordinary typed code.
- Index ordering is pinned so Postgres, SQLite, and the in-memory reference agree exactly:
  order by the index fields, then `id`. `NULL` is the smallest value (`NULLS FIRST` ascending,
  `NULLS LAST` descending, written explicitly in Postgres index DDL and `ORDER BY` so the index
  serves the query and a backward scan gives `desc`). Range bounds (`gt/gte/lt/lte`) never match
  `NULL`; `eq(null)` means `IS NULL`. Text orders by Unicode code point: Postgres text columns
  are `COLLATE "C"`, SQLite uses `BINARY`, and the in-memory store must compare by code point —
  **not** JS `<` on strings, which is UTF-16 code-unit order and disagrees above U+FFFF.
  Timestamps must be order-preserving in every dialect (SQLite: integer epoch millis or
  fixed-width UTC ISO). Locale-aware ordering for display is done in app code, not the index.
- Reads record table-level dependencies; invalidation uses Effect's `Reactivity` service
  (`SqlClient.reactive`), keyed by table name. Local optimistic write or pull apply invalidates
  the touched tables; affected registered queries rerun and stream results to subscribers.
  Results in one batch are produced from one replica version so the UI never sees a mixed
  snapshot.
- `success` Schema is mandatory because results cross the worker boundary (structured clone).
- Raw SQL escape hatch with explicit result Schema and declared read tables.
- Full-text search is a later primitive; design must leave room. **The pinned
  `@effect/wa-sqlite` 0.1.2 build (SQLite 3.47.0, the only version in `@effect/sql-sqlite-wasm`
  4.0.5's peer range) does NOT include FTS5** — verified in Chromium: `CREATE VIRTUAL TABLE …
  USING fts5` fails with "no such module: fts5", `pragma_compile_options` lacks
  `ENABLE_FTS5`, and none of the shipped `.wasm` files contain the string. JSON functions
  (`json_extract`) do work. Options when search is built: a custom wa-sqlite build with FTS5
  (requires our own `OpfsWorker` equivalent or a package override, since `OpfsWorker`
  hard-imports `@effect/wa-sqlite/dist/wa-sqlite.mjs`), or search without FTS (e.g. `LIKE` over
  indexed columns / trigram-ish helper columns). Tracked as open item 6.

## 11. Schema evolution

- Source: `SyncTable` manifests. Migration plan = previous committed manifest + new manifest +
  explicit intent ⇒ generated, reviewable TypeScript migration module (Effect + `sql\`\``),
  run by `effect/sql/Migrator` (numbered `NNN_name.ts`, default export `Effect`).
- Startup verifier checks live catalog (tables, columns, types, indexes, triggers) against the
  registered manifests and refuses to serve on drift (keeps the previous container running).
- Renames, lossy type changes, drops, backfills are never inferred; the generator emits what is
  safe plus a commented TODO for manual steps. NOT NULL evolution: expand → compatible writers →
  chunked idempotent backfill (outside Migrator's transaction) → validate → enforce.
- Adding an index must not force a replica re-download.

## 12. Transport

- Shared contracts module: `RpcGroup`s with Schema payload/success/error.
  `SyncRpcs`: `push`, `pull`, `pokes` (stream). `PresenceRpcs`: `track`, `room` (stream).
- Server: `RpcServer.layerProtocolHttp({ path: "/sync" })` initially; WebSocket is a `Layer`
  substitution. Handlers never know their transport. Use `Layer.provide`, not `provideMerge`,
  to keep each protocol private.
- Client: `RpcClient` with HTTP protocol via `@effect/platform-browser` fetch.

## 13. Effect usage rules

- `effect@4.0.5` exact-pinned, plus `@effect/sql-pg`, `@effect/sql-sqlite-wasm`,
  `@effect/platform-bun`, `@effect/platform-browser` at the same exact version.
  Runtime dependencies are Effect packages only.
- Import paths (v4): `effect/sql`, `effect/rpc`, `effect/http`, `effect/workers`,
  `effect/testing`, stable `effect/Schema`. `effect/sql`, `effect/rpc`, `effect/http`,
  `effect/workers` are **unstable** modules; accepted deliberately, re-verified on every bump.
  Do not assume v3 APIs. **Read the pinned source in `node_modules/effect/src` before using an
  API**; training data is wrong about v4 often enough that this is mandatory.
- Services via `Context.Service`/`Layer`; resources via `Scope`; streams via `Stream`; retries via
  `Schedule`; typed errors via `Schema.TaggedError`; no `any`, no non-null assertions, no
  `as` casts except at the platform edge with a comment.
- Prefer existing Effect facilities (`Reactivity`, `Migrator`, `RpcGroup`, `SqlSchema`) over
  re-implementation.

## 14. Testing philosophy

Test invariants, not functions. Every test must name the failure it would catch. Expected
kinds of tests:

- Property/simulation: outbox+rebase state machine (random interleavings of local mutations,
  pulls, acks, rejects, reloads) preserves: no lost intent, no double-applied intent, replica
  equals server state after quiescence.
- Real Postgres (docker) concurrency: log ordering invariant under concurrent writers with the
  trigger lock; the same test demonstrates the gap when the lock is removed.
- Ledger exactly-once under retry with lost responses.
- Scope move/grant/revoke delivery (tombstone to old scope readers, upsert to new, snapshot on
  grant, purge on revoke, no gaps).
- Codec round-trips through both dialects for every supported leaf type; declaration-time
  rejection of unsupported mappings.
- Browser (Chromium via agent-browser): OPFS persistence across reload, single DB owner across
  tabs, outbox durable before "saved".

Not wanted: one test per function, tests of private helpers, snapshot tests of generated SQL
strings without a behavioral assertion, tests whose expected values were copied from the
implementation.

Use `@effect/vitest`, `effect/testing` (`TestClock`), and `Arbitrary` where they fit.

## 15. Prior art (for orientation only)

Replicache/Zero (mutations-up/rows-down, rebase), Convex (typed ctx, indexed reads, registered
queries), LiveStore (leader worker, Web Locks, rerun-on-change), PowerSync/Syncular (rejected:
sidecar / invasive storage), Phoenix Presence (connection-scoped presence).

## 16. Open items tracked by the coordinator

1. Leader handover + stream behavior — resolved in §9 / `docs/browser-substrate.md`; only Chromium is verified by test, other browsers are documentation-only (support matrix is a user decision when targets are known).
2. Grant-added snapshot delivery — resolved in §8 (state diff + row walk then log), covered by the server substrate's grant / random grant-revoke tests.
3. Global catalog snapshot format (leaning NDJSON+gzip) — deferred past v1 vertical slice.
4. Protocol conformance test suite for a future non-TS client.
5. Ledger retention, separate from log GC. **Confirmed hole** (reconcile simulation: with ledger GC and no handling, 34/40 seeds double-apply or never release). Mechanism, in the protocol from v1 so it needs no later wire change; the v1 server runs no expiry job (its floor stays 0 and `collectGarbage` never touches the ledger, `clients`, or `client_scopes`):
   - Server keeps a per-client **floor** = highest `seq` (§6) whose ledger row was expired. Push answers any `seq ≤ floor` with the terminal outcome `Rejected{ClientExpired}` instead of executing it. Because clients push in `seq` order and the ledger records every processed mutation, every `seq ≤ floor` was processed; a never-pushed mutation always has `seq >` floor and is accepted normally.
   - `PullDone` carries `expiredThrough` (the floor). Inside the install transaction, a pending mutation with `seq ≤ expiredThrough` and no ack leaves the replay set with a notice whose meaning is "processed, outcome unknown, effect (if any) is in the installed replica" — not "rejected". Needed in addition to the push rule: a client that pushed and lost the ack would otherwise hold the intent forever, since a resnapshot acks only surviving ledger rows.
   - `Rejected{ClientExpired}` still carries a ledger `position` (§6 says every outcome does): the server records, next to the floor, the position of the highest expired ledger row and returns that. The client reports it as an `OutcomeUnknown` notice.
   - **Expiry depends on §9's outbox rule (1).** The §8 push-position backstop (rule 2) does not survive expiry: a duplicate enqueued after its original was released gets a fresh `seq` above the floor, and once the original's ledger row is gone the server cannot recognise it and executes it again (shown by the reconcile simulation). Only a released-row memory in the outbox rules such a duplicate out. Any expiry policy must therefore expire a client's ledger rows only after the released-row retention window (§9, default 24 h) has passed since those rows were acked, and never rows newer than the client's last presented cursor. When to raise the floor beyond that is the remaining open question.
6. Local full-text search: the pinned wa-sqlite build lacks FTS5 (§10); decide custom build vs. non-FTS search when the search primitive is scheduled.
