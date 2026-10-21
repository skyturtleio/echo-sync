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
  catalogs (SkyTime airports). **App-agnostic (user ruling):** the same engine serves at least
  three app shapes — HeyBabe (couples sharing todos/notes), a Fasten-style personal records
  app, and the SkyTime flight-time logbook — so nothing in `src/` may assume one app's tables,
  identity shape, UI library, or domain rules. Consumer trials (the HeyBabe port) exist to
  surface *generic* gaps; a trial finding becomes a ruling only when stated as an engine
  property any of those apps would need.

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
| `scope_grants` | a **system `SyncTable`** (`id`, `accessScopeId` = the granted scope, `principalId`, `role`, `grantedAt`); `role ∈ {reader, writer, owner}`; principal `*` = everyone (global catalogs). Replicates like any row so clients can run optimistic role checks. v1 disclosure policy: anyone who can read a scope sees who else is granted on it. Writes require `owner` on that scope; the only exception is scope creation (engine primitive: insert `access_scopes` row + first `owner` grant for the actor, allowed for any principal on a fresh id). `(accessScopeId, principalId)` is unique — derive `id` from it so duplicates are structurally impossible. `id = grantId(accessScopeId, principalId)` is a server-enforced invariant: `echo_sync.scope_grants` carries a CHECK that recomputes it, so a grant written around the store (raw SQL) can never make an id lookup and a column lookup disagree. The length prefix in `grantId` is the **code-point** count of the scope id (`[...scopeId].length`, never UTF-16 `.length`), which is exactly Postgres `char_length` in a UTF8 database; core and the CHECK therefore derive the same id for every scope id, and no scope id is rejected for its characters. The engine requires a **UTF8 server encoding** (in `SQL_ASCII` `char_length` counts bytes); the engine migration that adds the CHECK fails loudly on any other encoding. The derivation is part of the client/server contract: a client bundle that derives ids differently cannot authorize optimistically against canonical grants (`roleFromGrants` looks up by id), so a change to `grantId` is a coordinated client upgrade, never a mixed-version deployment. |
| `clients(client_id, principal_id, cursor, last_seen_at, created_at)` | one row per client device/profile |
| `mutations(client_id, mutation_id, position, outcome, received_at)` | ledger: exactly-once + ack delivery |
| `client_scopes(client_id, position, scope_ids)` | the set of scopes a client **holds** as of each completed pull (written at `PullDone` under `nextCursor`, `ON CONFLICT DO NOTHING`; rows below the cursor a client presents are pruned on that pull). The wire cursor is a bare `Position`, and the held set cannot be rebuilt from the log (identity only, GC'd, revoke+re-grant cancels out), so it is recorded as an explicit fact. A presented cursor with no record ⇒ `ResnapshotRequired(unknownCursor)`, never trusted. |
| `log(position, table_name, row_id, access_scope_id, op, txid, committed_at)` | change identity, not payload. `table_name` is the **sync table name**, the identity shared with clients (SQLite has no schemas), unqualified; `attach` maps it to exactly one Postgres relation and refuses a second relation with the same name in another schema. `attach` owns its transaction (READ COMMITTED) and refuses to run inside an ambient one, like the ledger: its uniqueness check must see concurrent commits, which a caller's REPEATABLE READ snapshot hides. Schema and table identifiers are quoted the same way for the catalog lookup and the trigger DDL, so `schema: "Public"` and `public` are different relations and the second attach of one name is refused, never silently captured twice. The server `RowStore` resolves a sync table to its attached relation only; a table with no attach mapping is a defect (the trigger is missing, so a write would never reach the log), never a fallback to `public`. |
| `migrations(...)` | `effect/sql/Migrator` history table (configured name) |

Principals are opaque strings supplied by the app (`principalFor(request)`). Authentication is
entirely app-owned. Authorization:

- Pull: engine filters log/rows to scopes the principal has any grant on.
- Mutation writes through `ctx.store`: engine checks `role ∈ {writer, owner}` for the row's
  scope (both the old and the new scope on a move). Mutators add domain checks on top.
- Grant management is itself done through mutations against the `scope_grants` system table via
  a small engine-provided API, so grant changes are captured in the log like any other change.
- **Scope creation** is `ctx.store.createScope(scopeId)` inside a mutation (never reachable from
  a query's `ReadStore`). It claims the id (`StoreOptions.claimScope`) and, only if the claim
  succeeds, writes the first grant `{ id: grantId(scopeId, actor), role: owner }` **for the
  actor** through the normal write path (undo image, log, touched rows) with the owner check
  skipped; a handler cannot name another principal. Server claim = `INSERT INTO access_scopes …
  ON CONFLICT DO NOTHING RETURNING` inside the mutation's transaction and savepoint, so the
  access row and the grant commit or roll back together and two concurrent creators get exactly
  one `Applied` and one `Rejected{ScopeForbidden{required: owner}}` (the existing rejection; the
  wire does not change). A claim that fails for any other reason (the id exists, including a
  scope whose grants were all revoked: the `access_scopes` row is kept, so an orphaned scope can
  never be hijacked) is the same `ScopeForbidden`. Client claim is optimistic: fresh iff the
  replica holds no `scope_grants` row for that scope; the server is authoritative and a
  rejection undoes the optimistic grant like any other row. The actor `*` can never create a
  scope (that would make everyone its owner).

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

Field admissibility, enforced by `SyncTable.make` (a rejected field is a construction error,
not a runtime surprise): a field's encoded form must always carry the key (no encoded-side
defaults or optional keys; `NullOr` is the only way to say "absent"), so §11's key-presence rule
is true for every table; a field schema must have no service requirements (`R = never`), since
row codecs run at storage and wire edges with no context; and a transformed field (e.g. a
branded or refined timestamp) must round-trip through every codec, nullable or not. The
manifest hashes the **storage shape only**: `dataHash` covers columns, column kinds and
nullability; the index set and unique sets hash separately as `indexHash`, which never triggers
a rebuild (§11).
Refinements and brands are validation, not storage: changing one does not change the manifest
and does not rebuild the replica, because a rebuild would refetch the same rows; tightening a
refinement against existing data is a §11 backfill (expand → backfill → enforce), never a hash
change.
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
  client's outbox sequence: per-client monotone, gap-free, never reused, **starting at 1** (the
  server's expiry floor defaults to 0, so `seq = 0` would be born expired). `seq` is allocated by
  the durable outbox (§9), never from in-memory engine state: a pull failure, `ResnapshotRequired`
  or reload must not reset the counter. Clients push in `seq` order; `seq` is the only field the
  ledger-expiry floor (§16.5) compares against.
- **Version the mutation args, never the rows.** Rows are always at current server schema. Arg
  evolution uses Schema unions/transforms with upcast; old queued intents must still decode.
- No network, filesystem, secrets, raw SQL, or wall-clock reads inside handlers. Anything that
  needs network is an ordinary endpoint ("action"), not a mutation.

Server execution: one short Postgres transaction per mutation, in client order:
authorize → run handler (writes fire triggers → `echo_sync.log`) → write ledger row with
`position` → commit. Push is HTTP-batched (default 50, cap 100 / ~1 MB): **batch at the HTTP
level, commit at the mutation level.** The mutation transaction runs at **SERIALIZABLE**, set as
the first statement of the outer transaction (before the client registration and ledger lookup),
so two clients' read-modify-write handlers on the same row cannot both commit a lost update at
READ COMMITTED; the `40001` retry below re-runs the whole attempt, registration and ledger
lookup included. `attach` is the one place that deliberately runs READ COMMITTED (§4).

Outcomes: permanent failures (decode, forbidden, domain error) are recorded in the ledger as
processed-with-typed-error and the batch continues; transient failures (serialization `40001`
after bounded retries, connection) stop the batch at that mutation without recording it, so the
client retries from there and ordering is preserved. Retried mutations with a known identity
return the recorded outcome without re-executing. Every push outcome carries the ledger
`position` it was recorded at, so the client can tell whether its installed replica already
covers it (§8).

**Outcome classification is over the whole `Cause`, with precedence defect > transient >
rejection.** A handler can fail with a compound cause (parallel branches, finalizers). Any
defect anywhere in the cause makes the mutation die with the full cause: never retried, never
ledgered, never reported transient. Otherwise, any transient failure (`40001`, connection)
anywhere in the cause means the transaction could not have committed, so nothing it computed
is trustworthy, including a rejection decided on its snapshot: the mutation is retried
(bounded) and, if retries are exhausted, stops the batch as transient with nothing ledgered. A
rejection is ledgered only when it is the sole kind of failure in the cause, i.e. it came from
a transaction that would have committed had the handler succeeded. The same rule applies to
`Mutation.run` in core (`StoreUnavailable` outranks `MutationRejection`).

**Request-level push errors are reserved for input an honest client cannot produce.** Push
fails as a whole, with nothing executed or ledgered, only on `Unauthorized` and
`MalformedEnvelope` (e.g. `seq < 1`; the `Seq` schema already refuses this at RPC decode, so
the server guard is defense in depth against corrupt or forged input). Any per-envelope check
an honest client *could* fail (args decode, authorization, domain error, size) must instead be
a ledgered `Rejected{…}` so the batch continues and the outbox drains. The client treats
`MalformedEnvelope`, and a push payload decode failure, as a fatal integrity error: it stops
pushing, surfaces the error, and offers export of pending mutations (§9). It never releases the
entry (that would open a `seq` gap, which §16.5 forbids) and never retries automatically.
On the wire a payload decode failure has no failure tag: the pinned `RpcServer` answers it with
a defect, exactly as it answers a handler defect (which this section says is never retried or
ledgered). So the client rule is: **any defect in a push response is fatal** for the leader's
lifetime; a halt is in-memory, so a reload or leader handover pushes once more and either
succeeds (server fixed) or halts again. A batch that fails the client-side `PushRequest` check
before sending is fatal the same way, and nothing is sent. Corollary for the server: every
infrastructure failure (connection, pool, `40001` after retries) must surface as a transient
*failure*, never leak as a defect, or clients halt until reload. On `push` that failure is the
short result list (the batch stops at the first unprocessed entry). `pull` and `pokes` carry a
typed transient member, `Protocol.ServerUnavailable`, for the same purpose: the server maps
`SqlError`/`StoreUnavailable` to it **over the whole cause**, `Fail` or `Die` alike (the pinned
`SqlClient` rollback dies on a lost connection, so `Die(SqlError)` is a real production shape);
a cause that also carries any non-SQL defect stays a defect (§6 precedence, defect > transient).
So a defect from the server always means a bug. The client
treats a pull defect as transient anyway (below) as a safety net, not as the contract. The
`pokes` stream ending for any reason (failure, defect, clean end) is reconnected by the client
with backoff; it is a hint stream and nothing is lost **because of the subscribe-then-poke
guarantee**: the server emits one poke on every `pokes` stream immediately after its `LISTEN` is
confirmed, before any notification, and the client pulls on every poke. **The server holds one
`LISTEN` per process, not one per stream** (coordinator ruling, HeyBabe trial): the pinned
`PgClient.listen` reserves a pool connection for the life of its scope, so a `LISTEN` per `pokes`
stream exhausts the pool at `poolSize` connected clients and then blocks every push and pull.
A server-scoped listener (built with the handlers, started before the server listens) owns the
single `LISTEN` and fans notifications out through a `PubSub`; a `pokes` stream subscribes to
that `PubSub` first and emits its initial poke only after the subscription exists and the shared
`LISTEN` is confirmed, which preserves subscribe-then-poke per stream. Pokes carry no payload,
so a subscriber may coalesce (a sliding subscription of capacity 1 is enough). When the shared
`LISTEN` connection is lost, every subscribed `pokes` stream ends (typed `ServerFailure`) and the
listener re-establishes `LISTEN` with backoff; clients reconnect and the initial poke's pull
covers the gap, exactly as for a single lost stream today.
**Liveness (user ruling, §16 item 9a):** the `pokes` stream element is a union `Poke | Heartbeat`
(`Schema.TaggedStruct("poke")` / `("heartbeat")`; the initial element is a `Poke`). The server
emits a `Heartbeat` on every stream after `heartbeatInterval` (server option, default 15 s)
without any element, from the shared listener's clock. The client treats every element as proof
of liveness, wakes the loop only on `Poke` (a heartbeat must never trigger a pull), and ends the
stream itself when `idleTimeout` (client option, default 45 s, must exceed the server interval)
passes with no element; the normal reconnect-with-backoff then applies, and the new stream's
initial poke covers anything missed. The `idleTimeout` clock starts when the HTTP request is
sent and bounds the wait for the first element as well as the gaps between elements; local
preparation of a (re)connect (notably `syncHeaders`, §9.1 Credentials) runs before the clock
starts, stays interruptible, and is never counted as stream silence. Nothing else changes: pokes stay hints, no resync timer. A change committed between the
client's last pull and the `LISTEN` is therefore covered by the pull that initial poke triggers,
and a change committed after it produces a notification. The client must not rely on a pull
issued *before* the stream is established to close a reconnect gap, and v1 adds no periodic
resync timer. Transient push failures
(`RpcClientError`, `Unauthorized`, interruption) and every pull failure (including a pull defect
or an undecodable page, mapped to a transient pull error) are retried with backoff. v1 uses one
backoff per sync cycle (push then pull), not one per operation: a transient push failure also
delays the next pull. A halted push does not count as a failure and does not delay pull.
Transport: `SyncRpcs` over HTTP POST with NDJSON (`RpcServer.layerHttp({ protocol: "http" })`
+ `RpcSerialization.layerNdjson` on the server; `RpcClient` over the platform fetch on the
client).

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
- **Unknown table in a pull response is skipped, not a failure.** The manifest is never
  exchanged on the wire: the server checks its registry against the database, the client
  rebuilds its replica when its *own* registry changes (§9). So during a deploy that adds a
  synced table, a running client with the old registry legitimately receives `upsert`/`tombstone`
  changes for a table it does not know. It cannot store or query them, and nothing is lost: its
  next app version has a new manifest, which rebuilds the replica from a `null` cursor and
  fetches those rows. The engine resolves `Change.table` through `Registry.get` and drops
  unknown names with a log line, inside the same install transaction. A defect here would kill
  sync for every stale client until the app updated. (Rows of a *known* table at a newer server
  schema are the separate additive-evolution question, open item 7.)
- **A known table's row is never installed raw.** `Protocol.Change` carries the row as generic
  JSON; the install path decodes every `upsert` payload through `table.wire` and writes the
  canonical re-encoding, so the replica only ever holds rows the table codec produced (the §11
  excess-key drop and missing-key failure happen here). A row that fails to decode fails the
  whole page-set install: the transaction rolls back, nothing from the set is installed, the
  cursor does not move, and the failure is a transient pull failure per §6. Decoding, not an
  `id` type check, is what makes the row's key trustworthy.
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
  per-identity pool directory `echo-sync/<appId>/<profile>`. **Path capacity ruling (HeyBabe
  trial):** `wa-sqlite`'s `VFS.Base` declares `mxPathname = 64`, while `AccessHandlePoolVFS`
  itself stores paths up to its 512-byte file header. A UUID-length profile makes
  `echo-sync/<appId>/<profile>.db` plus SQLite's `-journal` suffix exceed 64 and `sqlite3_open_v2`
  fails with an opaque `LeaderFailed`. The host must raise the registered VFS's `mxPathname` to
  the pool header's capacity (derive the bound from the pinned sources, leaving room for the
  journal suffix), keep the existing `dbName` layout so existing pools stay readable, and
  `validateIdentity` must reject an identity whose database path would not fit with a message
  that names the limit. Any UUID-length `profile` with a reasonable `appId` must be accepted.
  The protocol is internal to that
  package: re-verify `OpfsSqlite.ts` on every `@effect/sql-sqlite-wasm` bump.
- Storage transactions on that connection are admitted through a permit taken interruptibly
  before the connection lease, so a transaction queued behind a long install can be cancelled
  while it waits. The permit guarantees **serialized, cancellable admission and nothing more**:
  no fairness, so a fiber that releases and immediately re-acquires may barge ahead of a waiter.
  Ordering between one tab's mutations comes from the caller sequencing them (each `mutate`
  completes before the next is issued), never from the lease.
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
  The re-run is unconditional and belongs to the install path (engine `rebase`), including after
  a schema rebuild (§11 `dataHash` change): the rebuild empties the replica, nulls the cursor and
  empties each pending entry's undo (it keeps the outbox, released marks, `pushedThrough` and
  notices: those describe the outbox, not the replica), so until the null-cursor set installs, pending effects are
  deliberately invisible (replaying onto an empty replica would run handlers against missing
  rows and the install discards the result anyway). On that install, `rebase` restores nothing
  (undo is empty), installs the set, then replays every pending entry the set does not settle in
  `seq` order and records fresh undo for each. Invariant: after any install, replica =
  replay(pending, server@cursor) with every pending entry's undo recaptured; skipping the replay
  after a rebuild leaves pending effects invisible until push.
- Outbox idempotency must outlive release. `enqueue` is idempotent on `mutationId` so a retry
  after `LeaderLost` is safe, but the original may already have been pushed, acked, pulled and
  released in the retry window; a retry that then inserts a fresh row would replay the effect
  optimistically a second time and (see §8) get a position the pull window never acks. So
  release **marks** the outbox row (`released_at`) instead of deleting it; `enqueue` of a known
  id, pending or released, returns its original `seq` and inserts nothing; `pending()` excludes
  released rows; released rows are pruned after a retention window (default 24 h, far beyond any
  handover retry). The §8 position rule is the backstop if a released row has already been
  pruned. This rule is part of the engine's client storage port, not only the browser outbox:
  the sync engine (§8) must consult the outbox for a known id, pending or released, **before**
  executing an intent optimistically; the in-memory simulation's store must honour the same
  contract or it cannot catch the double-execution it exists to catch.
- Replica is disposable; outbox is not. Schema manifest change ⇒ rebuild replica, keep outbox.
- `navigator.storage.persist()`, pending-writes warning on clear/sign-out, export of pending
  mutations.

### 9.1 Tab-side API (the contract every UI adapter builds on)

`src/client` owns one typed tab-side surface. The library is UI-framework agnostic: any
adapter (FoldKit, React, or other) lives in the consuming app, is a thin translation of this
surface, and adds no sync semantics of its own. Rulings (coordinator):

- **`mutate(mutation, args)`** is the only write API. It encodes `args` with the mutation's
  schema, **generates `mutationId` itself** (one id per logical call, fixed before the first
  `enqueue` attempt, so a retry after `LeaderLost` reuses it and lands on the §9 idempotency
  rule), retries `LeaderUnavailable{LeaderLost}` and nothing else, and completes only after the
  leader's commit. Apps never mint `mutationId`; an explicit id is accepted only for replay and
  tests. The caller still sequences its own mutations (await each before issuing the next).
- **`subscribe(query, args)`** takes a registered query (§10.1), not SQL, and is a `Stream` that
  survives handover: on `LeaderLost` it re-subscribes to the new leader and **keeps the last
  emission** until the first fresh one arrives (no flash-to-empty). Every emission is decoded
  with the query's `success` Schema on the tab. Any other error ends the stream.
- **Notices** are a `Stream` of the retained list (current value, then every change; reactive
  on `echo_notices`), plus **`dismissNotices(ids)`**. "Retained for inspection, not silently
  erased" (§9) means exactly: the engine never removes a notice on its own, there is no cap,
  and only an explicit dismissal deletes one. A notice carries the full envelope, so an
  undismissed `Rejected`/`OutcomeUnknown` is also the user's recovery record for that intent.
- **`status`** is a `Stream` (mode, `isLeaderTab`, `leaderId`), current value then changes.
  `mode: "unsupported"` and `SecondaryTabWithoutRouter` are states the adapter must render, not
  errors it may swallow.
- **`syncStatus`** (ruling from the HeyBabe consumer trial) is a `Stream` of
  `{ push, lastError, pendingMutations, lastSyncedAt, phase, nextRetryAt }`, current value then
  every change, and it **survives handover like `subscribe`**: on `LeaderLost` it re-subscribes
  to the new leader and keeps the last emission until the first fresh one. `pendingMutations` is
  the number of unreleased outbox entries (pushed-but-not-installed included) and changes on
  every local append and release, not only on network cycles; a consumer must never need
  `exportPending` just to count. `lastSyncedAt` is epoch ms of the last cycle whose push and
  `PullDone` install both completed, `null` until the first such cycle in this leader (not
  persisted; a fresh leader starts at `null`). `phase` is
  `"idle" | "pushing" | "pulling" | "backoff"`; `nextRetryAt` is epoch ms while
  `phase === "backoff"`, otherwise `null`. There is no `connected` boolean: a completed cycle
  proves nothing about current connectivity, and v1 adds none (see §16 item 9).
  **`lastError` shape (coordinator ruling, HeyBabe fifth-pin finding):** `lastError` is
  `null | { kind, message }`, not a bare string, because an app must distinguish a failed
  credential refresh ("sign in again") from an unreachable server ("you are offline") without
  parsing text. `kind` is a closed union: `"credentials"` (`syncHeaders` raised, nothing sent),
  `"unauthorized"` (the server rejected the credential), `"unavailable"` (`ServerUnavailable`),
  `"network"` (the request failed before a server response: transport, HTTP, decode of a
  response that never arrived), `"server"` (any other server-produced transient failure, the
  stopped-batch case included). Amendment after the fd77a29 review: preserving arbitrary
  inner text and an absolute no-secrets guarantee cannot both hold, and the guarantee wins.
  `message` is a **library-controlled constant** chosen by walking the cause chain to the
  innermost *recognized* stage or tag (so an outer `RpcClientError` never hides a wrapped
  `CredentialsUnavailable`); it never copies a raw error message, error name, unrecognized
  tag, string cause, URL, or `Cause.pretty` output into public status, because any of those
  can carry a token, an echoed header, or a request body. The same rule applies to every
  string in `syncStatus`, the `Halted` reason included. Diagnostic detail belongs in the
  leader's log (`Effect.log*` with the cause), never in `syncStatus`. Kind clarifications:
  `"network"` means no usable RPC result, which includes a body-read, empty, or incomplete
  response after an HTTP response arrived; `"server"` is the closed union's fallback for any
  other transient failure, a local `StoreUnavailable` included (an app treats it as "retrying",
  nothing finer). Cleared (`null`) by the next clean cycle, as before. Consumer-visible shape
  change; lands before any app ships on `syncStatus`.
- **`requestSync`**: `Effect<void, SyncDisabled | LeaderUnavailable>` on the tab client. It
  wakes the sync loop (coalesced with the existing wake queue) and completes once the wake is
  accepted. It never implies network success, never clears `Halted`, and retries `LeaderLost`
  like `mutate`. Its outcome is observable only through `syncStatus`.
- **Closing the tab client's scope** terminates a locally owned leader worker and nothing else.
  The outbox is durable (§9), so a push in flight at that moment resolves on the next leader
  through the §9 idempotency rule and the §8 backstop; the engine neither drains nor freezes.
  An app that wants a "pending writes" exit gate builds it from `pendingMutations` /
  `exportPending` and its own approval UI; there is no global freeze primitive.
- **`resetReplica(identity, options)`** (user ruling, §16 item 11a) is a static function of
  `src/client` (not a tab-client method: it must run when no client is alive), returning
  `Effect<void, ReplicaInUse | PendingWrites | ResetFailed>`. It acquires the identity's leader
  lock with `ifAvailable` and fails `ReplicaInUse` if a leader holds it; the app is responsible
  for closing its tab clients first (its own cross-tab sign-out tombstone). Under the lock it
  opens the outbox through the same worker-hosted VFS path the leader uses (reuse the leader
  worker startup in a reset mode; no second OPFS host), counts unreleased entries, and fails
  `PendingWrites { count }` on any unreleased entry; an outbox that cannot be read (the database
  does not open, is corrupt, or has a schema this build cannot read) fails closed with
  `ResetFailed`. With `options.discardPending === true` the reset never opens the database: it
  skips the count entirely, so a corrupt replica or a partially removed one can still be reset
  (coordinator ruling, ws/reset-replica review). Both paths keep identity validation, the leader
  lock, success on an absent directory, waiting for the pool's access handles to be free as the
  leader does, and removal of only this profile's directory. Then it closes whatever it opened
  and removes the OPFS directory `echo-sync/<appId>/<profile>` recursively, releasing the lock
  last. A candidate that becomes leader afterwards finds no pool and creates a fresh, empty
  replica, which is the correct state for a signed-out identity. Notices are part of the replica
  and go with it; the outbox is the only guarded content. The config carries the app's
  `spawnLeader` factory (the same one passed to the tab client), since the reset must start the
  app's own leader worker entry.
- **Presence** (user ruling, §16 item 12a): `presence(room, state)` is a `Stream<RoomSnapshot>`
  on the tab client; this tab's entry exists exactly while the stream is open, and it survives
  handover like `subscribe` (re-joined on the new leader). `track(room, state)` replaces this
  tab's state in a room it has joined (no-op otherwise). Both are leader-owned like the sync
  loop. Presence never touches the replica or the outbox (§3).
  Two rulings from the ws/presence-client review (coordinator): (1) one `presence` stream per
  room per tab; a second `presence(room, …)` while one is open fails `PresenceAlreadyOpen`
  (§12 allows one server entry per `(clientId, tabId)` key, and two tab subscriptions would
  fight over it); an app that needs several observers of one room multiplexes the one stream.
  (2) `Unauthorized` on a `room` stream is a credential failure like everywhere else and is
  retried per §9.1 Credentials, never fatal. A principal with no grant in the room's scope is a
  different fact: the server fails `room` with `Forbidden { room }` (new `Protocol` error, added
  to the `room` RPC's error union; `track` is unchanged), and `Forbidden` is the only error that
  ends a presence stream. `Forbidden` is wire-additive and lands with the presence client, before
  any app consumes presence.
  Track semantics (coordinator ruling, ws/presence-client review, amended after re-review):
  `track` completes once **this tab** has recorded `state` as its desired state for the room, in
  one synchronous step that never waits on the leader or the network; interrupting the caller
  after that step does not retract the state. Delivery is then two hops of the same shape, each
  owned by the open `presence` stream and each a supervised *sender*: the tab's room cell
  delivers the latest desired state to the leader, and the leader delivers it to the server.
  A sender is serialized per room, latest-wins (an undelivered older state may be skipped), never
  holds a lock across I/O, and has a finite deadline per attempt (`idleTimeout`). A sender that
  fails, dies, or exceeds its deadline is restarted with the ordinary backoff and retries the
  latest state; `LeaderLost` restarts the tab's sender against the new leader. Sender health is
  independent of the stream: a stranded send must never wait for an unrelated stream failure or
  rejoin to be retried, and a sender failure never ends the stream. Every (re)join and handover
  re-sends the latest state. A `track` call never blocks snapshot delivery or reconnect, and a
  `track` whose stream has closed is dropped. The tab API therefore guarantees only that the
  latest desired state is eventually visible while the stream is open.
  Latest-wins is strict and enforced at the receiver (§12 revision fencing): the tab owns one
  `revision` counter per `(room, tab)` for the **life of the tab client** (amendment after the
  87a85e8 review: it lives outside any per-stream cell and is never reset when a `presence`
  stream closes and a new one opens; a fresh tab id starts at 0). The counter is consumed by
  every tab-side event that the server may see out of order: the first open of a room, each
  accepted `track`, every later re-open, and every handover rejoin (`LeaderLost`) each take the
  next value, and a join carries the latest desired state with that value. Leader-initiated
  rejoins (idle timeout, `Unauthorized` retry, sender restart) re-send the leader's latest
  `(state, revision)` unchanged; a leader never generates a revision. The leader's entry keeps
  the highest revision it has accepted and never sends a lower one after a higher one. The
  `(state, revision)` pair a join or sender uses is captured in one synchronous step, never a
  revision read before and a state read after an intervening effect. A `track` or `room`
  attempt that timed out or was interrupted may still reach the server later; the §12 fence
  makes that harmless, so no sender waits for a cancelled attempt before sending a newer state,
  and no client-side reconciliation against snapshots is needed. Accepted residue: two attempts
  carrying the *same* revision (a leader rejoin retried with no intervening `track`) may replace
  each other once, which costs one extra rejoin and never strands or regresses state.
  As landed (5b122f3, `src/client/worker/LeaderClient.ts`): `presence(room: AccessScopeId,
  state: Json): Stream<RoomSnapshot, PresenceError>` with `PresenceError = SyncDisabled |
  Forbidden | PresenceAlreadyOpen | LeaderUnavailable` (`LeaderLost` is absorbed by the rejoin;
  the other `LeaderUnavailable` reasons fail the stream); `track(room, state): Effect<void>` has
  no error channel. Revisions never appear in the tab API. Deadlines: the leader's per-send
  deadline and the room stream's idle timeout (after header preparation) are both
  `LeaderOptions.idleTimeout` (45 s); the tab's sender uses `tabApi(calls, tuning)`'s
  `TabApiOptions { presenceSendTimeout = 45 s, presenceRetryDelay = 1 s doubling to 30 s }`,
  which the production `LeaderClient` layer does not expose, so the tab deadline is fixed for
  now (§16 item 14).
- **Typed notice decoding** (ruling from the HeyBabe trial): a retained notice carries the
  envelope's `args` and a `DomainError.error` as `Schema.Json`, so a mutation's static types stop
  at the notice. The stored notice shape, `Notice = Rejected { envelope, error } |
  OutcomeUnknown { envelope }`, is defined in `src/core/Mutation.ts` (it uses only core schemas;
  client storage and the engine reference it rather than redeclaring it). `Mutation.decodeNotice(mutation, notice)`
  takes that `Notice` directly: when `notice.envelope.name === mutation.name` it decodes `args`
  with the mutation's `args` schema and, for a `Rejected` whose error is a `DomainError` of this
  mutation, decodes the payload with its `error` schema, and returns the notice with those fields
  typed (`OutcomeUnknown` keeps only typed args; other rejections pass through unchanged). A
  different mutation name, a `DomainError` naming another mutation, or any decode failure is a
  typed `NoticeMismatch`, never a defect. Pure (no services), core-only, usable on the tab and
  the server. (Corrected from an earlier `{ envelope, outcome }` shape that no consumer holds.)
- **Credentials** (coordinator ruling, §16 item 10): `LeaderOptions.syncHeaders` is
  `(principal) => Effect<Readonly<Record<string, string>>>` (error channel `never`), evaluated
  by the transport for every outgoing request: each push, each pull, and each (re)connect of a
  stream (`pokes`, and `room` once the presence client exists). The app owns refresh: it returns
  the credential it wants on the wire now, caching and renewing as it sees fit; if a renewal
  fails it returns what it has and the §6 `Unauthorized` retry path applies. The engine never
  caches headers across requests, so a renewed token takes effect on the next request without a
  leader restart. `Unauthorized` stays retried and never fatal; the app observes a persistent
  `Unauthorized` through `syncStatus` and decides on sign-out itself.
  Failure classification (coordinator ruling, ws/credential-refresh review): `syncHeaders` runs
  before the request leaves the leader, so anything it raises (a defect, since its error channel
  is `never`) is a local acquisition failure, classified like a network failure: the request is
  not sent, the loop retries with the ordinary backoff, and `syncStatus` reports it as a
  transient error. It is never a server defect, never `PushIntegrityError`, and never halts the
  push for the leader's lifetime; interruption passes through unchanged. Server-side defect
  fatality applies only to responses the server actually produced. For streams, the
  `idleTimeout` clock starts after `syncHeaders` has produced the headers (§6 Liveness).

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

- `db.get(T, id)`, `db.query(T).withIndex(name, q => q.eq(...).gt(...)).order(...).take(n)` / `.first` /
  `.collect` (`first` and `collect` are Effect-valued properties, not methods),
  `db.scan(T)` for small tables. Joins/aggregates happen in ordinary typed code.
- `take(n)` requires a non-negative safe integer (`Number.isSafeInteger(n) && n >= 0`); anything
  else (`NaN`, `±Infinity`, negatives, fractions, `2**53`) is a programmer error and is a defect in
  every store, never silently clamped, truncated, or treated as unlimited. `take(0)` is `[]`.
  `collect` is unlimited by construction (plan `limit = None`), not a sentinel passed through `take`.
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

### 10.1 Query registry and execution (coordinator rulings)

- **`Query.make` lives in `src/core` and mirrors `Mutation.make`**: `name`, `args` Schema,
  mandatory `success` Schema, and `handler: (db: EchoStore, args) => Effect<Success, ReadError>`.
  `Query.makeRegistry(queries)` is given to the leader next to the mutation registry. The
  `EchoStore` is the one that already exists (`src/core/Store.ts`, `Store.make` over the
  client's `SqliteRowStore`, with the same `roleFor` the engine uses for optimistic
  execution), so queries and mutation handlers read through one typed API and one ordering
  definition. No new query engine.
- **Tabs send `(name, encoded args)`**, never SQL. The leader decodes `args` with the query's
  schema (a decode failure is the tab's bug: a typed error, not `SqlFailure`), runs the handler
  in one read transaction, and sends the **encoded** result; the tab decodes with `success`.
- **Dependencies are recorded, not declared.** The store records every table a run touches
  (`get`, `query`, `scan`); the live subscription is reactive on exactly the tables read by the
  **latest** run, re-derived after each run. This is correct because handlers are deterministic
  functions of their args and their reads (same purity rule as mutation handlers, §5): a table a
  run did not read cannot change that run's result, and a run that newly reads a table widens
  the set before the next change can be missed. Result + dependency set come from one read
  transaction, so an emission is never a mixed snapshot.
- **Registered queries are read-only by construction.** A query handler receives `ReadStore`,
  the read projection of `EchoStore` (`get`, `query`, `scan`; no `insert`/`patch`/`delete`),
  and `Query.run` hands it an object that carries only those three members, so a write from a
  query is a compile error and has no runtime path either. The leader additionally runs each
  registered-query transaction under `PRAGMA query_only` (toggled off again inside the same
  admitted permit, before COMMIT/ROLLBACK) as defense in depth: any write that reaches SQLite
  fails and the transaction rolls back, leaving the replica and the next mutation unaffected.
- **Raw SQL is a harness tool, both directions.** The declared-tables `LiveQuery` subscription
  (`subscribeRaw`) and raw **`exec`** exist only when the leader is started with
  `allowRawExec: true`; otherwise both RPCs fail with the same typed error before touching the
  connection. `PRAGMA query_only` around raw statements is kept as defense in depth but is
  **not** a security boundary: the caller's text can itself contain `PRAGMA query_only = OFF` or
  several statements, so the only protection against untrusted raw SQL is not exposing the RPC.
  The vertical-slice harness sets the flag; apps and their UI adapters never do, and an
  adapter exposes neither RPC.
- `take(n)` pushdown through the scope filter landed (open item 8, resolved) inside `Store.make`,
  behind the same `EchoStore` interface; `Query.make` is unaffected.

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
- **Generator and verifier contract (rulings):**
  - *Previous committed manifest* is a checked-in snapshot file next to the app's migrations
    (default `migrations/manifest.json`): canonical JSON of the registry's `TableManifest`s. The
    generator reads it, emits `NNN_name.ts`, and rewrites the snapshot in the same commit, so
    "committed" means committed and review sees the manifest diff. It is never read from a
    database. `TableManifest` carries the unique sets (`unique`, already part of `indexHash`), so
    the snapshot needs nothing beyond the manifests.
  - App migrations run in their **own history table** (default `echo_sync.app_migrations`),
    numbered from `001`, via `PgMigrator.make({})({ loader: fromRecord(...), table })` after the
    engine's `migrate` (which keeps `echo_sync.migrations` for `001_engine`). Never share the
    engine's table.
  - Intent: `{ defaults?: { "table.column": sqlExpr }, renames?: { tables?, columns? },
    acknowledgeDrops?: [...] }`. A new NOT NULL column with a default emits
    `ADD COLUMN … NOT NULL DEFAULT <expr>` **followed by `ALTER COLUMN … DROP DEFAULT`** in the
    same module: the default backfills existing rows once; new rows always carry every declared
    column explicitly (§5 forbids encoded-side defaults), and the live catalog must not keep a
    default the manifest does not know. Without a default: nullable `ADD COLUMN` plus the §11
    TODO sequence. Rename and drop intents only make the commented TODO more specific; they
    never emit DDL.
  - "Drops" that are never inferred means **columns and tables** (data). Index add, drop and
    change (`DROP INDEX` + `CREATE INDEX` under the same `table__index` name, plain, inside the
    Migrator transaction) are inferred; `indexHash` changes rebuild nothing. `NOT NULL → nullable`
    emits `DROP NOT NULL` (loss-free) with a generated comment that `dataHash` changes (replica
    rebuild) and that a client still at the old manifest fails to decode a `null`, so apps ship
    clients before writing nulls. Unique sets added or removed are TODO only (adding can fail on
    data; removing needs the auto-generated constraint name).
  - Verifier policy: extra undeclared live columns and extra indexes are allowed; a declared
    column missing, of the wrong type (text without `COLLATE "C"` included) or nullability,
    a declared index or unique set missing or with different key columns / order / `NULLS`
    placement, a primary key not on `id`, a missing or non-functional `echo_sync` trigger
    (either of capture / truncate guard absent, disabled, or with the wrong timing, events,
    `WHEN` clause or column list — check `tgenabled`, `tgtype`, `tgqual`, `tgattr` on every
    relation carrying the function, not the first), or a registry table that is not attached,
    is drift. **More than one relation carrying the capture trigger for one sync table name is
    drift by itself**, however clean each relation is: the name is the unqualified sync identity
    (§3) and `attach` refuses that state, so the verifier reports it rather than checking each
    relation and passing. Likewise, **the relation the unqualified name resolves to through
    `search_path` must be the captured one**: when `to_regclass(name)` resolves to a relation that
    carries no capture trigger while another schema's same-name relation does, the verifier
    reports it (app SQL through `search_path` would write rows sync never sees, and `attach`
    would have refused with `TableNameConflict`); the fix is the app's `search_path` or an
    explicit `schema` on attach, never a silent pass. A declared index or unique set is compared against a **valid, ready, live**
    index (`indisvalid`, `indisready`, `indislive`) whose **key** columns (`indnkeyatts`, not
    `INCLUDE` payload) match in order, collation and `NULLS` placement; a unique set's expected
    shape is the one `SyncTable`'s DDL generates (`NULLS DISTINCT`, default placement), which
    differs from query indexes (`NULLS FIRST`). Reading `pg_index.indnullsnotdistinct` makes
    **PostgreSQL 15 the server's minimum**; tests run on 17. Scope: registry tables including
    `scope_grants`; the engine's own tables are not checked. Compare the structured catalog (`pg_index.indkey`,
    `indoption`, `indcollation`, `pg_attribute`), never a string match against generated DDL.
    The result is one typed `Drift` error listing every mismatch; the server runs it after
    `migrate` and before serving.
  - Any generated DDL step that references a table or column whose rename, drop or kind
    change is itself a TODO is emitted as a TODO too, in dependency order after it, never as
    executable DDL against a relation the module cannot assume exists. The generator resolves
    the previous manifest through table renames before diffing columns, so a combined
    table-and-column rename produces one table TODO and one column TODO, not an intent error.
    A changed index is **one step** (`DROP INDEX` then `CREATE INDEX` of the same name): if
    either half references a blocked table or column, both halves are the same TODO, never an
    executable `CREATE` whose `DROP` was deferred (the name is still taken; `42P07`). After a
    table-rename TODO, every later step for that table addresses the table and its indexes by
    their **new** names, because the rename TODO itself instructs renaming each
    `old__<index>` to `new__<index>` (the verifier finds declared indexes by name) and the
    later steps run by hand after it. An index whose column is dropped is dropped with the
    column by PostgreSQL: the generator notes that on the drop TODO and emits no separate
    `DROP INDEX` for it; only a replacement `CREATE`, as a TODO after the drop.
- **Additive column evolution (observed on pinned Effect 4.0.5 `Schema.Struct`):** wire and
  storage row codecs **drop excess keys on decode** and **fail on a missing key**. Never set
  `onExcessProperty: "error"` on row codecs. Consequences, which are the v1 rule: (1) a new
  column must be nullable or server-defaulted; (2) an older client keeps syncing a known table
  at the newer server schema (it decodes the fields it knows; its optimistic `patch` re-encodes
  a row without the new column into its *own* replica only, and the next pull overwrites it —
  the server is authoritative and always runs the newest schema, and mutations carry args, not
  rows); (3) a newer client against an older server is a decode failure the §9 rebuild cannot
  repair, so **deploy order is server first, then clients**; the startup verifier (above) makes
  the server refuse to serve a mismatched database, never a stale client. A manifest/version
  gate on the wire is the later fix for (3). Key presence is the server's obligation: a row on
  the wire carries **every declared column, `null` included** (a missing key fails decode on
  every client). That holds only if every declared column exists in the live table, which the
  startup verifier enforces.
- **Pull payloads are built from the declared columns through core's codec, never from the
  live relation.** `to_jsonb(t)` / `row_to_json` are forbidden for synced rows: they emit
  `timestamptz` as an ISO string where the wire is epoch ms (§10), and they ship any column the
  app added to the table but did not declare in the `SyncTable` to every scope reader (a
  disclosure leak; the client dropping it on decode is too late). The server selects the
  declared columns only and decodes through `table.postgres.row`, whose decoded side is the
  wire row, so there is one definition of the encoding. `attach` therefore takes the
  `SyncTable`, not a bare name.

## 12. Transport

- Shared contracts module: `RpcGroup`s with Schema payload/success/error.
  `SyncRpcs`: `push`, `pull`, `pokes` (stream). `PresenceRpcs` (user ruling, §16 item 12a):
  `room({ clientId, room, tabId, state }): Stream<RoomSnapshot>` joins and observes; the entry
  keyed by `(clientId, tabId)` lives exactly while that stream is open and is dropped when it
  ends for any reason. `track({ clientId, room, tabId, state })` replaces that entry's state
  (no-op without an entry). The payload carries `clientId` because it is part of the key
  (ruling, ws/presence-server); the server's internal key also includes the principal, so
  naming another principal's client id can neither replace nor end that principal's entry. A
  rejoin with the same key replaces the entry and ends the older `room` stream (ruling,
  ws/presence-server review): an entry has exactly one observing stream, so a stream never
  outlives its entry on an abandoned room. `track` applies its in-memory update and its change
  tick atomically with respect to interruption, so a state that is installed is always
  announced. **Revision fencing (coordinator ruling, ws/presence-client review):** the payload
  of `room` and `track` carries `revision` (a non-negative integer assigned by the tab, see
  §9.1 Presence); the entry stores the revision it was installed with, and `track` is a no-op
  when its `revision` is not greater than the stored one. A join whose `revision` is **greater
  than or equal to** the stored one installs the carried `(state, revision)`, replacing the
  entry and ending the older stream as above; a join whose `revision` is **lower** is a
  superseded attempt (amendment after the 87a85e8 review): the server installs nothing, leaves
  the entry and its stream untouched, and ends the superseded stream without emitting an
  element (the only case in which a `room` stream carries no snapshot; no live client observes
  it, because a client issues a new join only after abandoning the old attempt). Fencing is
  therefore by revision for both joins and tracks, so neither a late `track` nor a late `room`
  request from an abandoned attempt, an old leader, or a closed-and-reopened subscription can
  displace a newer entry. A cancelled or timed-out request is never a remote completion
  barrier the client must wait for, and the room converges on the highest revision the tab has
  sent. Required field, lands with the presence client before any app consumes presence. `room` is an `AccessScopeId`: the principal must hold a grant in that scope
  (`roleFor`, same check as reads) or the call fails `Unauthorized`. `state` is `Schema.Json`;
  `RoomSnapshot` is `{ room, entries: ReadonlyArray<{ principalId, clientId, tabId, state }> }`,
  emitted on join and on every change (coalescing allowed). Server state is in-memory and
  process-local (a `Ref` of rooms plus a `PubSub` per room); multi-instance presence is out of
  scope for v1. The durable path is untouched, and presence shares no tables with sync.
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

Conformance suites (`src/core/testing/Conformance.ts`, run against every `RowStore` /
`SyncStorage` implementation) derive their expected values from the fixtures they write, never
from the subject's own reads: "before reopen equals after reopen" is not an assertion, because a
subject that drops every write satisfies it. A no-op subject must fail every conformance check,
and that stub is kept as a test so the rule stays true. The suite is core-only (no runtime
import of `src/client` or `src/server`) and scopes each case separately. `SyncStorage.nextSeq`
**allocates**: two calls in one transaction return distinct seqs whether or not an append
happens in between, and a rolled-back allocation is given back. A store that merely peeks at
its counter violates the port, and the conformance suite keeps a check that calls `nextSeq`
twice before appending so that peeking fails.

**Test-harness placement (coordinator ruling):** `src/core/testing` holds only harnesses that
need nothing from the client or the server at runtime: `Conformance`, `MemoryStore`, `SimApp`,
`RefServer`, and the mutation-execution helpers they share (`runHandler`, `restore`,
`roleFromGrants`, which live in core and are re-exported by the client engine; `initialMeta`
stays engine-owned because core has no runtime use for it).
The reconciliation engine (`Engine.make`, `applyPageSet`) is client code and stays in
`src/client/sync`; harnesses that drive it (`Simulation`, `MemorySyncStorage`, their tests)
live in `src/client/sync/testing`. A lint rule forbids runtime imports of `src/client` or
`src/server` from `src/core`; type-only imports are allowed.

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
5. Ledger retention, separate from log GC. **Confirmed hole** (reconcile simulation, 40 seeds × 400 steps, naive ledger GC by position with no floor/`expiredThrough` handling: 37/40 seeds fail, 14 double-apply and 23 never release; corrected from an earlier 34/40 measured on older simulation code. With the floor and `expiredThrough` handling and no late retries of pruned ids, 40/40 pass; with late retries of pruned ids enabled, 13/40 still double-apply, which is the case the retention floor below exists for). Mechanism, in the protocol from v1 so it needs no later wire change; the v1 server runs no expiry job (its floor stays 0 and `collectGarbage` never touches the ledger, `clients`, or `client_scopes`):
   - Server keeps a per-client **floor** = highest `seq` (§6) whose ledger row was expired. Push answers any `seq ≤ floor` with the terminal outcome `Rejected{ClientExpired}` instead of executing it. Because clients push in `seq` order and the ledger records every processed mutation, every `seq ≤ floor` was processed; a never-pushed mutation always has `seq >` floor and is accepted normally.
   - `PullDone` carries `expiredThrough` (the floor). Inside the install transaction, a pending mutation with `seq ≤ expiredThrough` and no ack leaves the replay set with a notice whose meaning is "processed, outcome unknown, effect (if any) is in the installed replica" — not "rejected". Needed in addition to the push rule: a client that pushed and lost the ack would otherwise hold the intent forever, since a resnapshot acks only surviving ledger rows.
   - `Rejected{ClientExpired}` still carries a ledger `position` (§6 says every outcome does): the server records, next to the floor, the position of the highest expired ledger row and returns that. The client reports it as an `OutcomeUnknown` notice.
   - **Expiry depends on §9's outbox rule (1).** The §8 push-position backstop (rule 2) does not survive expiry: a duplicate enqueued after its original was released gets a fresh `seq` above the floor, and once the original's ledger row is gone the server cannot recognise it and executes it again (shown by the reconcile simulation). Only a released-row memory in the outbox rules such a duplicate out. Any expiry policy must therefore expire a client's ledger rows only after the released-row retention window (§9, default 24 h) has passed since those rows were acked, and never rows newer than the client's last presented cursor. **Policy resolved (coordinator ruling, user: working engine first; ws/ledger-expiry):**
   - The ledger row records when it was acked: in the pull install transaction, after the cursor is accepted, the server stamps `acked_at = now()` on this client's ledger rows with `position ≤` the presented cursor that have no `acked_at` yet (one indexed `UPDATE` per pull, usually touching few rows). A row with `acked_at IS NULL` is never expired, whatever its age.
   - `expireLedger({ retention })` is a server-exported job in the same shape as `collectGarbage` (the app's server schedules it; `examples/server` runs it on an interval). Default `retention` 7 days; a value below 48 h (twice the §9 released-row window, margin for a paused tab's clock) is refused at construction. For each client it deletes the ledger rows with `acked_at ≤ now − retention` and raises that client's floor to the highest `seq` deleted, recording the highest deleted `position` next to it. Floor and position are monotone and set in the same transaction as the delete.
   - The deleted set is always a `seq` prefix of the client's rows: one client pushes in `seq` order, so positions and ack times are monotone in `seq`. The job asserts this (no surviving row with `seq <` the new floor) and fails loudly instead of raising the floor if it ever does not hold.
   - Floors are permanent. No job, including any future forgetting of idle clients (`clients`, `client_scopes`), may delete a client's floor: a forgotten floor turns a lost-ack resubmission into a double apply. Forgetting idle clients stays out of scope for v1.
   - **Hole found in review of ws/ledger-expiry 432c7ad (reviewer + oracle; reproduced with a real SQLite outbox + PG over a ten-day TestClock advance: first seq 1, retry seq 2, body applied twice):** the 24 h / 48 h / 7 d chain assumed every `enqueue` retry arrives inside the released-row window, but `mutate` retries `LeaderLost` indefinitely with the same id, and a tab can be suspended for any length of time between a committed `enqueue` whose reply it lost and its retry. After the released row is pruned and the ledger row expired, the retry gets a fresh `seq` above the floor and the server no longer recognises the id. No retention value closes this; a client-side retry bound would need a clock the server cannot trust and a wire field. **Ruled (coordinator, ws/ledger-expiry): the ledger keeps every mutation's identity forever; expiry drops only its outcome.** `expireLedger` sets `outcome = NULL` (and clears `acked_at`) on rows with `acked_at ≤ now − retention` instead of deleting them; `(client_id, mutation_id, seq, position)` stay. A push whose `(client_id, mutation_id)` hits a row with a null outcome is answered `Rejected{ClientExpired}` at that row's `position` and is not executed, **whatever `seq` the retry presents**; the floor, `expiredThrough` and the `OutcomeUnknown` notice stay as specified for the lost-ack pull case. The seq-prefix assertion now applies to the set whose outcomes are cleared. Cost accepted for v1: ~50–80 bytes per mutation retained indefinitely (the outcome payload is what is reclaimed); forgetting idle clients and their identity rows remains out of scope, same as floors. Server-only; no wire or client change. The reviewer's ten-day lost-reply sequence becomes a committed E2E invariant (retry with a fresh `seq` is answered `Rejected{ClientExpired}` at the original position, server body executed once, client ends with an `OutcomeUnknown` notice and a drained outbox), and the reconcile simulation's "late retries of pruned ids" row must pass 40/40 once its server model keeps identities the same way.
   - Also from that review (fixed in cb16ef0): the pull head includes the global maximum recorded expired position, so a cursor never regresses after the newest ledger row expires (its position can exceed the log's maximum after log GC) and another client's continuation at that position survives; a cursor non-regression and a continuation-across-expiry test cover it.
   - Invariants to test (PG): an acked row older than `retention` expires and a resubmission of it answers `Rejected{ClientExpired}` with the recorded position, not a second execution; an acked row younger than `retention` and an unacked row of any age survive; the floor after a job is a `seq` prefix and `PullDone.expiredThrough` reports it; a client that lost the ack and pulls after expiry gets `OutcomeUnknown` for that mutation and releases it; a job run with no eligible rows changes nothing; the reconcile simulation seeds that double-applied under naive ledger GC pass under this policy.
6. Local full-text search: the pinned wa-sqlite build lacks FTS5 (§10); decide custom build vs. non-FTS search when the search primitive is scheduled.
7. Additive schema evolution across a deploy — **resolved in §11** (codecs drop excess keys, fail on missing; new columns nullable/defaulted; deploy server first). Remaining: a wire manifest/version gate so a newer client against an older server degrades cleanly instead of failing row decode; needed before clients can ship ahead of the server.
8. Query `limit` pushdown through the scope filter — **resolved** (main 41b878a): `IndexRangePlan` carries `readableBy` (principals) and `limit`; every `RowStore` applies readability as a `scope_grants` semi-join on `(access_scope_id, principal_id)` and stops after `limit` readable rows; `Store.planResult` is the shared reference. Covered by the `Conformance.rowStore` invariant run by Memory, SQLite and PG. Integration rule still owed by `ws/query` at landing: a registered-query run must record `scope_grants` as read whenever the plan carries `readableBy`, so a revoke re-runs the query.
9. Pokes liveness — **resolved (user: a)**, see §6 Liveness: `Poke | Heartbeat` stream element, server `heartbeatInterval` (default 15 s), client `idleTimeout` (default 45 s) that ends and reconnects the stream; heartbeats never wake the loop. The harness-only proxy workaround goes away once the client timeout exists.
10. Credential refresh — **resolved (user: a)**, see §9.1 Credentials: `syncHeaders` returns an Effect and is evaluated per outgoing request (push, pull, every stream (re)connect); the app owns refresh; `Unauthorized` stays retried and never fatal. Scheduled (ws/credential-refresh).
11. Replica reset on sign-out — **resolved (user: a)**, see §9.1 `resetReplica`: engine-owned, guarded by the leader lock (`ReplicaInUse`) and by the outbox (`PendingWrites { count }` unless `discardPending`), deletes only the identity's OPFS directory.
12. Presence — **resolved (user: a)**, implement as designed: §12 `PresenceRpcs` (`room` stream joins and observes, `track` updates, entries keyed by `(clientId, tabId)`, scope-grant authorization, process-local in-memory state) and §9.1 `presence` / `track` on the tab client, leader-owned and handover-surviving. Multi-instance presence is a later item.
13. Presence liveness and revocation (v1 gaps declared by ws/presence-server). **Liveness resolved (coordinator ruling, before the tab client exists so the wire never changes):** the `room` stream element becomes `RoomSnapshot | Heartbeat` (the same `Heartbeat` tagged struct as `pokes`), the server emits a heartbeat every `heartbeatInterval` on every open `room` stream, and the tab client's `presence` applies the same `idleTimeout` as `followPokes` (end and rejoin a silent stream; a rejoin re-sends the latest desired state since `track`'s ack cannot confirm application). A heartbeat is never a snapshot and never changes room state. **Revocation resolved (coordinator ruling, user: d; ws/presence-revocation):** enforced at the server, event-driven, no wire change. Grant changes are mutations on `scope_grants` captured in the log and announced on the shared LISTEN (§6), so the presence state subscribes to committed `scope_grants` changes: for each affected `(scope, principal)` it re-evaluates that principal's open `room` streams in that scope (a principal holds a scope while any `scope_grants` row for it or for `*` exists, the same predicate `room` uses on join). A principal that no longer holds the scope has every entry of its streams in that room removed in one step (the remaining members get one snapshot without them) and each such stream fails `Forbidden { room }`, the same error a join without a grant gets, so the tab client already ends the stream and no `Protocol` change is needed; a later `track` for a removed entry is the existing no-op. Re-granting does nothing automatically; the app opens `presence` again. Staleness is bounded by LISTEN delivery; a notification missed across a server restart is repaired by the next rejoin, which re-checks the grant as today, and the process-local state carries nothing across restarts anyway. Multi-instance fan-out remains out of scope with §12. Tests state the invariants: a revoked principal's stream ends `Forbidden` and its entry disappears from the other members' next snapshot without them doing anything; a revoke of one of two grants (or a `*` grant remaining) ends nothing; a grant to an unrelated principal or scope ends nothing; a revoke committed while the stream is joining cannot leave an entry behind (join and revoke race).
14. Presence tab-side tuning (declared with ws/presence-client 5b122f3): `TabApiOptions` (`presenceSendTimeout`, `presenceRetryDelay`) exists on `tabApi` but the `LeaderClient` layer does not thread it through its config, so apps get the 45 s / 1 s defaults. Thread it through only when an app needs a different tab deadline. Not scheduled.
