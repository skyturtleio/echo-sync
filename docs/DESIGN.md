# Echo Sync — Design Baseline

Status: baseline v0.3 (2026-10-21; v0.2 + substrate deltas: PullPage|PullDone with opaque continuation, replace-on-null, client_scopes state-diff membership, own OpfsSqlite host, handover semantics). This document consolidates the initial design discussions and is
the single source of truth for implementation. Changes to this document are deliberate: made by the
owner or with the owner's sign-off, and recorded in §16 (problem and ruling) with the code that
implements them. When implementation reveals a design error, propose the change, do not silently
diverge.

Attribution: §16 and the inline rulings name who decided during the initial build: the owner
("user"), the coordinator (the agent that supervised the build and owned this document), the reviewer
(the independent review agent) and the oracle (a second model consulted before commits); "dial" was
an agent's effort setting. These are historical attributions. None of those roles is part of the
current process, which `AGENTS.md` describes.

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
  identity shape, UI library, or domain rules. Consumer ports exist to surface *generic* gaps; a
  port finding becomes a ruling only when stated as an engine property any of those apps would
  need. **Production consumers:** NilBurger (first cutover 2026-10-22) and HeyBabe (cutover from
  Syncular 2026-10-23, heybabe-effect 41cf96d on engine pin dcd21c7; the import migration dropped
  every Syncular relation in the same transaction, recovery point is the Coolify scheduled backup
  of 03:00 UTC that day). HeyBabe is the first production consumer of presence and of scope
  self-revocation (§16.27, §16.29).

Non-goals (v1)

- Partial/predicate-based sync. Clients sync every scope they can read.
- CRDTs, collaborative text, IVM, query planner.
- Durable presence, blob/attachment sync.
- Per-tab SQLite replicas.
- Hosted service of any kind.
- Multiple server instances (user ruling, 2026-10-21: solo-developer engine, one Bun process per
  deployment). Presence fan-out, GC and ledger expiry are process-local by design, not as a gap.
  `effect/cluster` was assessed and not adopted: it coordinates server processes; the browser
  cannot be a runner, and the outbox + ledger (§6, §8, §9, §16.5) already are this engine's
  at-least-once-with-dedup mechanism.

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
| `scope_grants` | a **system `SyncTable`** (`id`, `accessScopeId` = the granted scope, `principalId`, `role`, `grantedAt`); `role ∈ {reader, writer, owner}`; principal `*` = everyone (global catalogs). Replicates like any row so clients can run optimistic role checks. v1 disclosure policy: anyone who can read a scope sees who else is granted on it. Writes require `owner` on that scope; the exceptions are deleting one's own grant row (§16.27: `delete` of the `scope_grants` row whose `principalId` is the actor needs no role; `*` is never an actor) and scope creation (engine primitive: insert `access_scopes` row + first `owner` grant for the actor, allowed for any principal on a fresh id). `(accessScopeId, principalId)` is unique — derive `id` from it so duplicates are structurally impossible. `id = grantId(accessScopeId, principalId)` is a server-enforced invariant: `echo_sync.scope_grants` carries a CHECK that recomputes it, so a grant written around the store (raw SQL) can never make an id lookup and a column lookup disagree. The length prefix in `grantId` is the **code-point** count of the scope id (`[...scopeId].length`, never UTF-16 `.length`), which is exactly Postgres `char_length` in a UTF8 database; core and the CHECK therefore derive the same id for every scope id, and no scope id is rejected for its characters. The engine requires a **UTF8 server encoding** (in `SQL_ASCII` `char_length` counts bytes); the engine migration that adds the CHECK fails loudly on any other encoding. The derivation is part of the client/server contract: a client bundle that derives ids differently cannot authorize optimistically against canonical grants (`roleFromGrants` looks up by id), so a change to `grantId` is a coordinated client upgrade, never a mixed-version deployment. |
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

**Request-level push errors are authentication, compatibility policy, or malformed input.** Push
fails as a whole, with nothing executed or ledgered, on `Unauthorized`, `ClientOutdated` (§11), or
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
(`RpcClientError`, `Unauthorized`, interruption) and pull failures other than `ClientOutdated`
(including a pull defect or an undecodable page) are retried with backoff. `ClientOutdated`
stops both directions for the leader's lifetime (§11), without retry or rebuild. A known-table
row missing a declared top-level wire key reports `server-outdated`, with backoff and no rebuild.
v1 uses one
backoff per sync cycle (push then pull), not one per operation: a transient push failure also
delays the next pull. A halted push does not count as a failure and does not delay pull.
Transport: `SyncRpcs` over HTTP POST with NDJSON (`RpcServer.layerHttp({ protocol: "http" })`
+ `RpcSerialization.layerNdjson` on the server; `RpcClient` over the platform fetch on the
client). The HTTP transport is one composition of exported server seams, not the only one
(main 57a0008, ws/server-pipeline): push is `principalFor(headers)` → `executeBatch(options,
actor, request)` → `pushFailures`; pull is `principalFor` → `pull(registry, request)` →
`serverFailures`; pokes applies the same failure mapping to its stream. The combinators carry the
failure classification above (infrastructure ⇒ transient stop or `ServerUnavailable`,
`ClientForbidden` ⇒ `Unauthorized`, other defects stay defects) so any transport that composes
them gets identical behavior; `docs/server-substrate.md` “Composing a transport” is the contract.

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

Tested (main b6acbf5, ws/ordering-control-determinism): a deterministic Deferred-sequenced
interleaving (A inserts and holds, B inserts and commits, reader pulls, A commits, reader syncs)
is the primary check, with the pull barriered on B being committed or queued on the advisory lock
in `pg_locks`; its twin without the lock asserts exactly A is missed, so each test discriminates
the trigger; the 40-writer random race stays as the stress case. The held writers are owned by
the helper's scope so a failure mid-interleaving cannot deadlock pool teardown.

`TRUNCATE` does not fire row triggers: unsupported on synced tables in v1 (statement-level
trigger raises). Retention: `echo_sync.log` is GC'd past a configurable horizon; a client whose
cursor is older than the horizon receives a typed `ResnapshotRequired` and re-snapshots.

## 8. Pull

- Cursor-based, scope-filtered, paginated. Each page is read in its own `REPEATABLE READ`
  snapshot (a snapshot cannot span HTTP requests without holding a transaction open, which a
  transaction pooler forbids). Request: `{ clientId, clientVersion?, principal?, cursor, continuation: Option<PullContinuation> }`
  (`clientVersion` is always sent by gate-aware clients, §11; `principal` is the replica's, an
  assertion checked against the authenticated principal, §16.21).
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
  A second corollary (coordinator ruling, ws/ledger-expiry): a push outcome
  `Rejected{ClientExpired}` is released on receipt **whatever its `position`**, with an
  `OutcomeUnknown` notice. Its ledger identity has no live outcome and is never acked by any
  pull, snapshot pulls included (a null-cursor pull must not carry a client's whole expired
  history), so waiting for `position ≤ cursor` can wait forever: after a replica rebuild the
  cursor is null and a late duplicate's original `position` lies above it. The outcome is
  terminal by definition ("processed, outcome unknown, effect (if any) is in the installed
  replica"), so releasing before the install carries no risk of losing an effect; it only
  removes the duplicate's optimistic copy, which the next install replaces with the truth.
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
- **Unknown table in a pull response is skipped, not a failure.** The client's combined
  registry `dataHash` and engine `wireVersion` travel on every push and pull (§11), not the
  full manifest. The server checks its registry against the database, the client
  rebuilds its replica when its *own* registry changes (§9). So during a deploy that adds a
  synced table, a running client with the old registry legitimately receives `upsert`/`tombstone`
  changes for a table it does not know. It cannot store or query them, and nothing is lost: its
  next app version has a new manifest, which rebuilds the replica from a `null` cursor and
  fetches those rows. The engine resolves `Change.table` through `Registry.get` and drops
  unknown names with a log line, inside the same install transaction. A defect here would kill
  sync for every stale client until the app updated. Known-table additive columns are dropped
  by the old client's codec (§11); differing hashes alone never refuse a client.
- **A known table's row is never installed raw.** `Protocol.Change` carries the row as generic
  JSON; the install path decodes every `upsert` payload through `table.wire` and writes the
  canonical re-encoding, so the replica only ever holds rows the table codec produced (the §11
  excess-key drop and missing-key failure happen here). A row that fails to decode fails the
  whole page-set install: the transaction rolls back, nothing from the set is installed, the
  cursor does not move, and the failure is retried with backoff per §6. On decode failure an
  absent declared top-level column key is `ServerOutdated` (`null` counts as present), surfaced
  as `lastError.kind: "server-outdated"`; invalid present values remain `MalformedChange`.
  Before staging a `PullPage`, the client decodes every known-table upsert through the same
  classification, without installing rows or changing the page's wire form. A failed page is
  never persisted; prior valid staging, cursor and resnapshot flag remain unchanged, so the
  next attempt uses the last valid continuation and re-fetches that page after backoff.
  Install-time decoding remains in place, including for the final `PullDone`.
  Neither triggers a rebuild, cursor reset, resnapshot flag or staging clear. Decoding, not an
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
adapter (FoldKit, React, or other) lives in the consuming app or in the library's
`echo-sync/foldkit` entry point (§16.17), is a thin translation of this
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
  stopped-batch case included), `"outdated"` (`ClientOutdated`, both directions stopped for this
  leader's lifetime), `"server-outdated"` (a known-table row lacks a declared top-level wire
  column; retry with backoff, never rebuild). An outdated leader stays `phase: "idle"`,
  `nextRetryAt: null`; wakes from pokes, `requestSync` and local writes cannot resume it.
  Local `mutate`, reads, export and reactive pending counts continue. The app owns the update
  prompt and service-worker reload; a fresh leader tries again, keeping the durable outbox.
  Amendment after the fd77a29 review: preserving arbitrary
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
  accepted. It never implies network success, never clears `Halted` or `outdated`, and retries `LeaderLost`
  like `mutate`. Its outcome is observable only through `syncStatus`.
- **Closing the tab client's scope** terminates a locally owned leader worker and nothing else.
  The outbox is durable (§9), so a push in flight at that moment resolves on the next leader
  through the §9 idempotency rule and the §8 backstop; the engine neither drains nor freezes.
  An app that wants a "pending writes" exit gate builds it from `pendingMutations` /
  `exportPending` and its own approval UI; there is no global freeze primitive.
- **`resetReplica(identity, options)`** (user ruling, §16 item 11a) is a static function of
  `src/client` (not a tab-client method: it must run when no client is alive), returning
  `Effect<void, ReplicaInUse | PendingWrites | ResetFailed>`. It closes the identity's tab clients
  itself (§16.24, superseding the original "the app is responsible for closing its tab clients
  first"): it broadcasts a reset signal that puts every client of the identity into a terminal reset
  state, then waits at most `closeTimeout` (finite, default 5 s) for the identity's leader lock and
  fails `ReplicaInUse` if a leader still holds it, touching nothing. Under the lock it
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
  also accepted directly on `LeaderClientConfig` by `make`/`layer`; omission preserves these
  defaults (§16 item 14, resolved in 88bd4a6).
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
    database. `TableManifest` carries the unique sets (`unique`, already part of `indexHash`).
    The snapshot also keeps ordered `history: [{ migration: "NNN_name", dataHash }]`, appended
    by `generate` at every migration, index-only migrations included. This is the byte-identical
    combined `Registry.make` hash the client sends, including engine tables such as
    `scope_grants`, not a hash of the app-table-only snapshot. Old version-1 snapshots without
    history still decode; history starts at the next generated migration, never inferred for
    earlier releases. Commit the generated migration and snapshot together; `snapshotOf`
    alone describes tables for planning, not a replacement for history-preserving `generate`.
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
  repair, so **deploy order remains server first, then clients**; the startup verifier (above)
  makes the server refuse to serve a mismatched database. The wire gate below makes skew
  degrade cleanly, not make missing data syncable. Key presence is the server's obligation: a row on
  the wire carries **every declared column, `null` included** (a missing key fails decode on
  every client). That holds only if every declared column exists in the live table, which the
  startup verifier enforces.
- **Two-direction wire version gate (§16.7):** every new client's push and pull page carries
  `clientVersion: { dataHash: registry.dataHash, wireVersion: Protocol.wireVersion }` (initial
  engine version 1). Both schemas accept an optional key for legacy pre-gate clients; absence
  is unknown/legacy, never the server's own hash. Wire versions decode as non-negative integers,
  not a literal equal to this server's version. Effect 4.0.5 RPC uses these payload schemas;
  an old server drops extra metadata, and a new server accepts absent metadata.
  `SyncServerOptions.clientPolicy?: (version | undefined) => Effect<void, ClientOutdated>`
  defaults to **serve**, not equality. It runs after `principalFor`, before catalog reads,
  registration, replay/idempotency lookup, ledger writes or cursor work, on every push and pull.
  Exported `executeBatch` and `pull` apply the same policy for other transports. Metadata is
  untrusted compatibility advice, never authentication. Pokes and presence have no policy gate.
  Refusal is a typed request error, never a mutation rejection or integrity error; the client
  stops as in §9.1, keeping replica and outbox. A pre-gate binary cannot understand the new error
  or stop rule retroactively; default serve preserves it, but an app refusing legacy clients
  can only prevent server work, not fix their old retry/status behavior.
- **Manifest floor governs table shape only** (columns, kinds, nullability). An app can accept
  the set of history hashes from migration number NNN onwards. A non-additive table-shape change
  (dropping, renaming or retyping a declared column, or NOT NULL → nullable followed by writing
  nulls) requires raising the floor past every incompatible old manifest. Otherwise a dropped
  or renamed column can make an old client misreport `server-outdated`: missing-key detection
  cannot determine which side is newer. Hashes are identities, not ordered versions; repeated
  hashes cannot distinguish releases with the same table shape.
  Mutation evolution stays under §6: version args with unions/upcasts so old queued intents
  still decode, and never remove a mutation name while any client may still hold it queued.
  If an app does, old intents land as retained `UnknownMutation` rejections, not as `outdated`;
  incompatible args similarly produce retained `ArgsDecodeFailed` rejections. The notice is
  the user's recovery record (§9.1). A mutation-registry hash as a second floor signal is a
  possible future item if a real app needs it, not part of this gate.
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
  `SyncRpcs`: `push`, `pull`, `pokes` (stream). Every sync and presence payload carries an
  optional `principal`, the replica's own; the server refuses the request `Unauthorized`, before
  any client binding, grant, ledger or presence work, unless it equals the authenticated
  principal, and never uses it as the actor (§16.21). `PresenceRpcs` (user ruling, §16 item 12a):
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
- **Consumer flag contract (§16.28):** the package ships TypeScript source, so every consumer
  typechecks it under its own `compilerOptions`. The library must therefore be clean under every
  TypeScript strictness flag, and its own `tsconfig.json` enables all of them (`strict`,
  `exactOptionalPropertyTypes`, `noUncheckedIndexedAccess`, `noImplicitReturns`,
  `noPropertyAccessFromIndexSignature`, `noImplicitOverride`, `noFallthroughCasesInSwitch`,
  `noUnusedLocals`, `noUnusedParameters`, `allowUnreachableCode: false`,
  `allowUnusedLabels: false`, `noUncheckedSideEffectImports`) so a consumer can never be the first to find a
  violation. Apps never relax a flag to consume echo-sync.

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

## 16. Open items and rulings

1. Leader handover + stream behavior — resolved in §9 / `docs/browser-substrate.md`. Chromium is verified by test. **WebKit smoke (ws/webkit-smoke, Playwright WebKit 27.2 on Linux GTK, persistent context):** scenarios 28/32, slice 11/12; Web Locks, OPFS sync access handles, wa-sqlite AccessHandlePool, BroadcastChannel/MessagePort, streamed pokes and presence rooms, offline/conflict slices, reset and the no-SharedWorker fallback all work, with no COOP/COEP. Two findings, both ruled fix-in-library (coordinator):
   - **Router relaunch.** WebKit terminates the SharedWorker when the tab that created it closes, even with other tabs connected (isolated repro `examples/browser-spike/webkit/sharedworker-lifetime.sh`); the successor wins the leader lock but the relaunched router has no routes, so no tab is served. Ruling: the router must be rebuildable from its clients. A router instance announces itself on first connect (BroadcastChannel); on that announcement or on a leader-epoch end every tab constructs a `new SharedWorker` (which relaunches a dead router) and re-sends `Hello`; the tab whose worker holds the live epoch re-announces `Leader` (`accept()` already fences stale epochs). The recovery is generic (any router death, any browser), not WebKit-sniffed; Chromium behavior must be unchanged. Safari itself is **unconfirmed**: observed only under Playwright WebKit on Linux; must be checked on a real iPhone/macOS Safari. **Resolved (main b52afc7, ws/router-relaunch).** Implemented as ruled (`RouterStarted` once per router instance, `Hello.connectedTo`, reconnect on broadcast or on a connected leader's epoch end, re-announce through `accept()`), plus three findings from review, all in `src/client/worker/LeaderClient.ts`: a newly elected owner reconnects (new `SharedWorker`, `Hello`, re-announce) rather than announcing on a possibly dead router port; and a **receive-side epoch fence**: `Connected` messages from all of a tab's retained router ports are processed one at a time, and one is installed only for a non-active epoch whose exclusive epoch lock is still held (`Platform.isLockHeld`), because a message posted earlier on one port can be delivered after a message posted later on another (reproduced natively on Chromium), so arrival order across router incarnations proves nothing; a duplicate for the active epoch or a dead epoch closes its port and displaces nothing. `Hello.connectedTo` stays non-authoritative on the router: the fence keeps `linked` consistent by construction. Router ports per tab are deliberately uncapped, bounded by events not traffic (2 + router relaunches + epoch ends seen; accepted for v1, no `Hello` acknowledgement). Five Chromium CDP checks (`examples/browser-spike/cdp/kill-router.ts`, fourth `tsc` project) fail on the pre-branch worker code; Chromium 37/37, slice 12/12. **WebKit now 30/32, slice 12/12** (S5 passes). The two residuals are one limitation and its knock-on: a closed tab's leader worker keeps running an in-flight long wasm query and holds the leader/epoch locks for ~50 s, so the successor waits; observed under Playwright WebKit 27.2 on Linux GTK, cause not identified, possibly related to WebKit bug 289686 (marked FIXED Aug 2026, so not established), query-free run passes 3/3; needs a real Safari device.
   - **OPFS name bound** — **resolved (main 5194076, ws/opfs-name-bound).** WebKit caps each OPFS name at 255 UTF-8 bytes (Chromium 480); identity validation only bounded the total, so a 256–488-byte profile passed validation and failed as `LeaderFailed{UnknownError}`. Ruling: identity validation also refuses any `appId` or `profile` over 255 UTF-8 bytes with `Unsupported`; the combined bound stays.
   iPhone Safari caveats are documentation-only (best-effort origin eviction deletes replica and outbox; private browsing has no OPFS; background kill of a leader or router-creator tab needs a device): `docs/browser-substrate.md` “WebKit smoke”.
2. Grant-added snapshot delivery — resolved in §8 (state diff + row walk then log), covered by the server substrate's grant / random grant-revoke tests.
3. Global catalog snapshot format (leaning NDJSON+gzip) — deferred past v1 vertical slice.
4. Protocol conformance test suite for a future non-TS client — **deferred indefinitely (owner, 2026-10-22): TypeScript is the only client language for the foreseeable future**; revisit only if a non-TS client is planned.
5. Ledger retention, separate from log GC — **resolved (main 3352c0c, ws/ledger-expiry; identities kept forever, outcomes expire; `Rejected{ClientExpired}` released on receipt; known id answered before the floor; pull head includes expired positions; simulation with late retries of pruned ids 40/40, 120/120 at `SIM_SEEDS=120`; `examples/server` runs `expireLedger` hourly).** History: **Confirmed hole** (reconcile simulation, 40 seeds × 400 steps, naive ledger GC by position with no floor/`expiredThrough` handling: 37/40 seeds fail, 14 double-apply and 23 never release; corrected from an earlier 34/40 measured on older simulation code. With the floor and `expiredThrough` handling and no late retries of pruned ids, 40/40 pass; with late retries of pruned ids enabled, 13/40 still double-apply, which is the case the retention floor below exists for). Mechanism, in the protocol from v1 so it needs no later wire change; the v1 server runs no expiry job (its floor stays 0 and `collectGarbage` never touches the ledger, `clients`, or `client_scopes`):
   - Server keeps a per-client **floor** = highest `seq` (§6) whose ledger row was expired. Push answers any `seq ≤ floor` with the terminal outcome `Rejected{ClientExpired}` instead of executing it. Because clients push in `seq` order and the ledger records every processed mutation, every `seq ≤ floor` was processed; a never-pushed mutation always has `seq >` floor and is accepted normally.
   - `PullDone` carries `expiredThrough` (the floor). Inside the install transaction, a pending mutation with `seq ≤ expiredThrough` and no ack leaves the replay set with a notice whose meaning is "processed, outcome unknown, effect (if any) is in the installed replica" — not "rejected". Needed in addition to the push rule: a client that pushed and lost the ack would otherwise hold the intent forever, since a resnapshot acks only surviving ledger rows.
   - `Rejected{ClientExpired}` still carries a ledger `position` (§6 says every outcome does): the server records, next to the floor, the position of the highest expired ledger row and returns that. The client reports it as an `OutcomeUnknown` notice.
   - **Expiry depends on §9's outbox rule (1).** The §8 push-position backstop (rule 2) does not survive expiry: a duplicate enqueued after its original was released gets a fresh `seq` above the floor, and once the original's ledger row is gone the server cannot recognise it and executes it again (shown by the reconcile simulation). Only a released-row memory in the outbox rules such a duplicate out. Any expiry policy must therefore expire a client's ledger rows only after the released-row retention window (§9, default 24 h) has passed since those rows were acked, and never rows newer than the client's last presented cursor. **Policy resolved (coordinator ruling, user: working engine first; ws/ledger-expiry):**
   - The ledger row records when it was acked: in the pull install transaction, after the cursor is accepted, the server stamps `acked_at = now()` on this client's ledger rows with `position ≤` the presented cursor that have no `acked_at` yet (one indexed `UPDATE` per pull, usually touching few rows). A row with `acked_at IS NULL` is never expired, whatever its age.
   - `expireLedger({ retention })` is a server-exported job in the same shape as `collectGarbage` (the app's server schedules it; `examples/server` runs it on an interval). Default `retention` 7 days; a value below 48 h (twice the §9 released-row window, margin for a paused tab's clock) fails the job's Effect with a typed `LedgerRetentionTooShort` before any SQL (ws/ledger-expiry-effect-error; the earlier synchronous throw at construction was replaced because a config-derived retention needs a `catchTag`-able error channel). For each client it clears the outcomes of the ledger rows with `acked_at ≤ now − retention` (identities stay forever, per the ruling above; an earlier draft deleted the rows) and raises that client's floor to the highest `seq` cleared, recording the highest cleared `position` next to it. Floor and position are monotone and set in the same transaction as the clear.
   - The deleted set is always a `seq` prefix of the client's rows: one client pushes in `seq` order, so positions and ack times are monotone in `seq`. The job asserts this (no surviving row with `seq <` the new floor) and fails loudly instead of raising the floor if it ever does not hold.
   - Floors are permanent. No job, including any future forgetting of idle clients (`clients`, `client_scopes`), may delete a client's floor: a forgotten floor turns a lost-ack resubmission into a double apply. Forgetting idle clients stays out of scope for v1.
   - **Hole found in review of ws/ledger-expiry 432c7ad (reviewer + oracle; reproduced with a real SQLite outbox + PG over a ten-day TestClock advance: first seq 1, retry seq 2, body applied twice):** the 24 h / 48 h / 7 d chain assumed every `enqueue` retry arrives inside the released-row window, but `mutate` retries `LeaderLost` indefinitely with the same id, and a tab can be suspended for any length of time between a committed `enqueue` whose reply it lost and its retry. After the released row is pruned and the ledger row expired, the retry gets a fresh `seq` above the floor and the server no longer recognises the id. No retention value closes this; a client-side retry bound would need a clock the server cannot trust and a wire field. **Ruled (coordinator, ws/ledger-expiry): the ledger keeps every mutation's identity forever; expiry drops only its outcome.** `expireLedger` sets `outcome = NULL` (and clears `acked_at`) on rows with `acked_at ≤ now − retention` instead of deleting them; `(client_id, mutation_id, seq, position)` stay. A push whose `(client_id, mutation_id)` hits a row with a null outcome is answered `Rejected{ClientExpired}` at that row's `position` and is not executed, **whatever `seq` the retry presents**; the floor, `expiredThrough` and the `OutcomeUnknown` notice stay as specified for the lost-ack pull case. The seq-prefix assertion now applies to the set whose outcomes are cleared. Cost accepted for v1: ~50–80 bytes per mutation retained indefinitely (the outcome payload is what is reclaimed); forgetting idle clients and their identity rows remains out of scope, same as floors. Server-only; no wire or client change. The reviewer's ten-day lost-reply sequence becomes a committed E2E invariant (retry with a fresh `seq` is answered `Rejected{ClientExpired}` at the original position, server body executed once, client ends with an `OutcomeUnknown` notice and a drained outbox), and the reconcile simulation's "late retries of pruned ids" row must pass 40/40 once its server model keeps identities the same way.
   - Implementing the identity ruling (ws/ledger-expiry 94c0b50) showed the client half: with identities kept, the simulation's "late retries" row goes from 13 double-apply to 0, but 3/40 seeds never release the duplicate, because an expired identity is never acked and the §8 release rule waits for `position ≤ cursor`, which a replica rebuild (null cursor) makes unreachable. The same happens on the real stack (`dataHash` rebuild, then a late tab retry). **Ruled:** the §8 second corollary — `Rejected{ClientExpired}` is released on receipt regardless of position, with the `OutcomeUnknown` notice — made in `src/client/sync/Engine.ts` on the same branch (ownership exception granted to the server thread for this one file), covered by an Engine-level invariant (a `ClientExpired` result above the cursor is released with its notice; fails without the change) and by the simulation gate with late retries on (40/40; 120/120 with `SIM_SEEDS=120`). Acking expired identities in snapshot pulls was rejected: it would carry a client's entire expired history on every null-cursor pull.
   - Also from that review (fixed in cb16ef0): the pull head includes the global maximum recorded expired position, so a cursor never regresses after the newest ledger row expires (its position can exceed the log's maximum after log GC) and another client's continuation at that position survives; a cursor non-regression and a continuation-across-expiry test cover it.
   - Invariants to test (PG): an acked row older than `retention` expires and a resubmission of it answers `Rejected{ClientExpired}` with the recorded position, not a second execution; an acked row younger than `retention` and an unacked row of any age survive; the floor after a job is a `seq` prefix and `PullDone.expiredThrough` reports it; a client that lost the ack and pulls after expiry gets `OutcomeUnknown` for that mutation and releases it; a job run with no eligible rows changes nothing; the reconcile simulation seeds that double-applied under naive ledger GC pass under this policy.
6. Local full-text search: the pinned wa-sqlite build lacks FTS5 (§10); decide custom build vs. non-FTS search when the search primitive is scheduled. **Owner (2026-10-22): not needed by NilBurger; required for the SkyTime flight logbook port**, so the decision is due before that port starts, not at v1.
7. Additive schema evolution and the two-direction wire version gate — **resolved (ws/version-gate, implementation cd325dd)**, see §8, §9.1 and §11. Default serve preserves additive evolution; app policy can stop an outdated client without losing its outbox, and missing declared keys back off as `server-outdated` without rebuilding. Generated manifest history supplies the table-shape floor. **Deployed 2026-10-22: NilBurger ea21be0 on engine pin 96b25eb (§16.21 account binding, §16.24 coordinated reset, §16.26 reset tombstone), Coolify healthy, `/api/health` verified. All app-side engine workarounds are gone: no lifetime locks, account epochs, do-not-respawn, confirmatory reset, persisted recovery quarantine or expected-account sync header; sign-out is `resetReplica` plus app auth propagation (§16.25), with persisted auth state written only by reducer-accepted transitions and quota-safe revocation ordering. Known limitation: 7c8b7a2 tabs that have not reloaded keep the pre-§16.21 first-registration window until reload.**
   **Scope ruling (coordinator, user pick G; scheduled, medium dial):** the gate covers both
   directions, because a PWA user can dismiss the reload prompt and run an old client against a
   newer server indefinitely. Implemented after coordinator approval: (a) every `pull` and `push` carries the client's registry
   `dataHash` and the engine wire version; (b) the server decides through an app-supplied
   policy whose default is *serve* (additive evolution already works, §11); a refused client
   gets a typed `ClientOutdated`, which the client treats as **not transient**: no retry loop, no
   rebuild, outbox and replica kept, `syncStatus.lastError.kind: "outdated"` (closed union
   grows), and the app owns the "update required" prompt and the service-worker reload;
   (c) a row of a known table that fails decode on a **missing declared key** is "server behind"
   (§11: the server must send every declared column), reported as `lastError.kind:
   "server-outdated"` with backoff and never a rebuild, which removes the only way a newer
   client could corrupt its replica against an older server. A `PullPage` is decoded under the
   client registry before staging: an incompatible page is never persisted; prior valid staging
   and its continuation are kept so the same page is re-fetched after backoff and can converge
   when the server catches up. Pre-fix poisoned staging needs no recovery path because no
   production replica existed when this landed, and a registry rebuild already discards staging.
   **Production note:** NilBurger cut over on engine pin 3cb3492, one commit before this gate, so
   its first production clients are pre-gate (no `clientVersion` on the wire, cannot decode
   `ClientOutdated`, pre-fix staging). Its upgrade to 604dc2e must deploy server-first under the
   default serve policy (or treat an absent `clientVersion` explicitly as legacy) and precede any
   non-additive table change. **Done:** NilBurger deployed engine pin 7c8b7a2 (gate + observability) server-first under the default policy on 2026-10-22 (NilBurger e763e03); a refusing policy or non-additive change still waits until every deployed client is on that pin.
   (d) the generator keeps a history
   of committed `dataHash`es in `migrations/manifest.json` so an app can express its floor as
   "every manifest since NNN" without hand-copying hashes. Invariants: an old client against an
   additive newer server still syncs (fails if the gate is too strict); a client below the floor
   is refused with `ClientOutdated`, stops retrying, and its outbox survives the forced reload;
   a newer client against an older server shows `server-outdated` and never rebuilds.
8. Query `limit` pushdown through the scope filter — **resolved** (main 41b878a): `IndexRangePlan` carries `readableBy` (principals) and `limit`; every `RowStore` applies readability as a `scope_grants` semi-join on `(access_scope_id, principal_id)` and stops after `limit` readable rows; `Store.planResult` is the shared reference. Covered by the `Conformance.rowStore` invariant run by Memory, SQLite and PG. Integration rule still owed by `ws/query` at landing: a registered-query run must record `scope_grants` as read whenever the plan carries `readableBy`, so a revoke re-runs the query.
9. Pokes liveness — **resolved (user: a)**, see §6 Liveness: `Poke | Heartbeat` stream element, server `heartbeatInterval` (default 15 s), client `idleTimeout` (default 45 s) that ends and reconnects the stream; heartbeats never wake the loop. The harness-only proxy workaround goes away once the client timeout exists.
10. Credential refresh — **resolved (user: a)**, see §9.1 Credentials: `syncHeaders` returns an Effect and is evaluated per outgoing request (push, pull, every stream (re)connect); the app owns refresh; `Unauthorized` stays retried and never fatal. Scheduled (ws/credential-refresh).
11. Replica reset on sign-out — **resolved (user: a)**, see §9.1 `resetReplica`: engine-owned, guarded by the leader lock (`ReplicaInUse`) and by the outbox (`PendingWrites { count }` unless `discardPending`), deletes only the identity's OPFS directory.
12. Presence — **resolved (user: a)**, implement as designed: §12 `PresenceRpcs` (`room` stream joins and observes, `track` updates, entries keyed by `(clientId, tabId)`, scope-grant authorization, process-local in-memory state) and §9.1 `presence` / `track` on the tab client, leader-owned and handover-surviving. Multi-instance presence is a non-goal (§2).
13. Presence liveness and revocation (v1 gaps declared by ws/presence-server). **Liveness resolved (coordinator ruling, before the tab client exists so the wire never changes):** the `room` stream element becomes `RoomSnapshot | Heartbeat` (the same `Heartbeat` tagged struct as `pokes`), the server emits a heartbeat every `heartbeatInterval` on every open `room` stream, and the tab client's `presence` applies the same `idleTimeout` as `followPokes` (end and rejoin a silent stream; a rejoin re-sends the latest desired state since `track`'s ack cannot confirm application). A heartbeat is never a snapshot and never changes room state. **Revocation resolved (coordinator ruling, user: d; ws/presence-revocation):** enforced at the server, event-driven, no wire change. Grant changes are mutations on `scope_grants` captured in the log and announced on the shared LISTEN (§6), so the presence state subscribes to committed `scope_grants` changes: for each affected `(scope, principal)` it re-evaluates that principal's open `room` streams in that scope (a principal holds a scope while any `scope_grants` row for it or for `*` exists, the same predicate `room` uses on join). A principal that no longer holds the scope has every entry of its streams in that room removed in one step (the remaining members get one snapshot without them) and each such stream fails `Forbidden { room }`, the same error a join without a grant gets, so the tab client already ends the stream and no `Protocol` change is needed; a later `track` for a removed entry is the existing no-op. Re-granting does nothing automatically; the app opens `presence` again. Staleness is bounded by LISTEN delivery; a notification missed across a server restart is repaired by the next rejoin, which re-checks the grant as today, and the process-local state carries nothing across restarts anyway. Multi-instance fan-out remains out of scope with §12. Tests state the invariants: a revoked principal's stream ends `Forbidden` and its entry disappears from the other members' next snapshot without them doing anything; a revoke of one of two grants (or a `*` grant remaining) ends nothing; a grant to an unrelated principal or scope ends nothing; a revoke committed while the stream is joining cannot leave an entry behind (join and revoke race).
14. Presence tab-side tuning (declared with ws/presence-client 5b122f3) — **resolved** (main 88bd4a6, ws/tab-api-options): `LeaderClientConfig` extends the unchanged `TabApiOptions` (`presenceSendTimeout`, `presenceRetryDelay`) and `make`/`layer` forward it to `tabApi`; omission keeps the 45 s / 1 s-doubling-to-30 s defaults. Covered by a public-layer invariant (a configured short deadline governs the tab-side retry when the leader does not answer; fails on f6de0a9).
15. Constraint violations in a mutation body (scheduled, ws/constraint-rejection; motivated by the NilBurger port, whose `fasting_cycles` rules are Postgres constraints). **Problem:** `ledger.ts` classifies every `SqlError` as transient, so a mutation that can never succeed (unique or check violation) is retried forever and, because the outbox pushes in `seq` order, every mutation behind it is stuck with no notice. **Ruling (coordinator):** the pinned `SqlError` reasons carry `isRetryable`; use it. A `ConstraintError` reason (check, foreign key, not-null) in a cause with no defect and no retryable failure is a ledgered `Rejected{ConstraintViolated{constraint, message}}` at a recorded position, and the batch continues. A `UniqueViolation` first goes through the existing bounded conflict retry (under SERIALIZABLE a racing insert can surface as a unique violation that a retry would not reproduce); if it persists after the retries it is the same rejection, not a transient. `ConstraintViolated` is a new member of core's `MutationRejection` union, additive on the wire, surfaced by the client through the existing `Rejected` notice path unchanged. Defect > transient > rejection precedence (§6) is untouched: a conflict anywhere in the cause still wins over a constraint rejection. App handlers remain expected to check domain rules first and fail with a typed `DomainError`; the constraint path is the safety net, not the UX. Invariants: a second insert of a unique key from one client is `Rejected{ConstraintViolated}` and a third mutation behind it is `Applied` (fails on main, where the batch stops transient); a serialization failure still retries and is never ledgered as a rejection; a unique violation that a retry resolves is `Applied`.
    **Resolved** (main 3e07aef): permanent constraint rejections are ledgered after bounded unique retries without blocking later mutations.
    **Addendum (coordinator, after the author's scope finding):** handlers run on client and server through `Store` (§4, §5), never raw SQL, and the server `RowStore` maps every `SqlError` to `StoreUnavailable`, so a raw-`SqlError` ledger path alone never reaches an app. The constraint path therefore goes through the store contract: `ConstraintViolated{kind: "unique" | "other", constraint: string | null, message}` is a permanent **store** failure, a member of core's `StoreRejection` (hence `StoreError` and `MutationRejection`), raised by the server `RowStore` for the `UniqueViolation` and `ConstraintError` reasons (every other `SqlError` stays `StoreUnavailable`); the write methods of `RowStore`/`Store` carry it in their error channel; `Mutation.make`'s whole-cause classification is unchanged (a `StoreUnavailable` anywhere still wins). The client `SqliteRowStore` is unchanged: the replica's tables carry only the primary key and `write` is an upsert. The ledger treats a body rejection `ConstraintViolated{kind: "unique"}` as a conflict for the bounded retry and ledgers it only when the retries are exhausted; `kind: "other"` is ledgered at once. The raw-`SqlError` path already implemented stays for bodies that bypass `Store`. Committed tests go through `Mutation.make` + `Store`, since that is the path apps use; the fails-on-main control is `MutationTransient` from the store path.
    **Addendum resolved** (main 3e07aef): Store-backed constraint failures preserve whole-cause precedence and round-trip through retained client rejection notices.
16. `principalFor` receives body-forgeable headers (found by the reviewer in the NilBurger port; scheduled, ws/principal-http-headers). **Problem:** `layerHttp` hands `principalFor` the RPC handler's `headers`, which the pinned `effect/rpc` HTTP protocol builds as `requestHeaders.concat(message.headers)` (RpcServer.ts, `httpEffect`): every NDJSON request message may add or override any header, and the server reads the body regardless of its media type. Reading a secret credential (session cookie, bearer token) from that bag is safe, since a forger does not hold the secret; deciding origin, `Sec-Fetch-Site`, media-type or any other request policy from it is not, and an app that puts its CSRF check in `principalFor` is bypassable. **Ruling (coordinator):** the principal and every request policy are decided from the real HTTP request, never from body-supplied headers. Preferred mechanism: if the RPC handler fiber can reach `HttpServerRequest` (verify against the pinned source), `principalFor` receives that request's headers and the merged bag is never exposed; otherwise `layerHttp` gains `authorizeRequest?: (request: HttpServerRequest) => Effect<void, Unauthorized>`, run as middleware on the sync route before the RPC decoder, and `principalFor`'s contract states that its headers are the merged bag fit only for secret credentials. Either way `docs/server-substrate.md` and `examples/server` show origin policy at the HTTP boundary. Invariant: a push whose request message carries `cookie` and `origin` headers that differ from the HTTP request's is authorized (or refused) by the HTTP values only; fails on main, where the body's values win.
    **Resolved** (ws/principal-http-headers): all five RPCs pass only real `HttpServerRequest.headers` to `principalFor`, with Effect 4.0.5 request-context propagation verified for buffered and streaming responses and a raw-NDJSON spoofing regression that fails on the prior main. **Addendum (reviewer finding in the NilBurger port):** request policy must never be gated by a raw pathname predicate on a separate middleware: the pinned router (FindMyWay defaults) resolves `/SYNC`, `/%73ync`, `/sync;x` and `//sync` to the same route while a `pathname === "/sync"` check returns false, so the policy is skipped. Decide origin / fetch-site / media type inside `principalFor` (route-independent, runs first in every handler on the real headers, the `examples/server` pattern) or attach the middleware to the route itself through `HttpRouter`, never to a path string.
17. FoldKit adapter as a library export — **resolved (ws/foldkit, reviewed head 23c207b; trigger met: NilBurger on 7c8b7a2 renders update-required)**; originally scheduled after NilBurger's production cutover (user pick O). **Timing (user pick E):** starts after NilBurger runs the version gate (604dc2e or later) and renders update-required/server-behind; §16.19 ships with it. The library stays UI-framework agnostic (§9.1): the adapter is a separate entry point (`echo-sync/foldkit`), depends only on the `src/client` tab-side API, adds no sync semantics, and was planned to take FoldKit as an optional peer dependency; **amended at extraction (coordinator):** the adapter's runtime and public types reference only Effect `Stream`/`Effect` contracts, so FoldKit is **not** a peer dependency at all (declaring one would be a false claim and the "runtime dependencies are Effect only" rule keeps no exception); FoldKit is a devDependency solely for the `examples/foldkit` tsc project, and `src/client`, `src/core`, `src/server` never import it (enforced by lint). Source of truth is the NilBurger adapter once it has survived production, diffed against the HeyBabe trial adapter; a medium-dial thread extracts the common surface so HeyBabe, SkyTime and future FoldKit apps stop re-implementing the plumbing. Not before cutover: the extracted adapter must be the one that proved out, and it must not compete with the cutover for review.
    **Resolved** (ws/foldkit, reviewed head 23c207b): `echo-sync/foldkit` (`src/foldkit/index.ts`) turns `subscribe`, `syncStatus`, `notices` and `status` into `Stream<message, never, R>` and `mutate`, `requestSync` and `dismissNotices` into `Effect<message, never, R>` over a client source (a `ManagedResource`'s `.get` or the `LeaderClient` service), extracted from NilBurger's production adapter (divergences from the HeyBabe trial resolved in NilBurger's favour). Each upstream emission is one message, value unchanged; a typed error is `{ _tag: "Error", error }`, any defect a carrier-free, unlogged `{ _tag: "Defect" }`, interruption yields no message, and `onFailure` is a stream's terminal element with no adapter retry. A stream runs to completion, finalizers included, before its end is classified, and reads at most one message ahead of its consumer (lossless backpressure). FoldKit stays a devDependency for `examples/foldkit` only, never a peer, lint-enforced. §16.19 landed with it.
18. HTTP-level 401/403 is classified as `network` by the client (found by the NilBurger port while implementing session refresh; scheduled, ws/http-status-unauthorized). **Problem:** the pinned `RpcClient.makeProtocolHttp` never inspects the HTTP status; a refusal emitted before the RPC decoder (app middleware, a reverse proxy) with an empty body becomes an `RpcClientError` protocol defect, so `syncStatus.lastError.kind` says `"network"` and the app cannot route the user to sign-in. **Rulings (coordinator):** (1) the wire contract for an authentication denial is the typed `Protocol.Unauthorized` failed by `principalFor`, which runs first in every handler before any ledger row, cursor read or mutation execution; app middleware on the sync route decides policy from the real request (§16.16), performs session refresh and emits response cookies, and hands the validated principal or the denial to `principalFor` through a request-scoped service, never through RPC metadata. Refusing at the HTTP layer is reserved for attacker-facing policy (origin, media type), where the client classifying it as `network` is acceptable. (2) The client additionally classifies an HTTP response with status 401 or 403 as `"unauthorized"`: `Transport.layerProtocolHttp` fails those responses with an `HttpClientError` whose reason is `StatusCodeError` before the RPC body is read, and `SyncLoop`'s `kindOf` maps that status to `"unauthorized"` (the §9.1 message stays the library constant; nothing from the response is copied). `Unauthorized` stays retried and never fatal. Invariant: a `/sync` mount whose middleware answers 401 with an empty body yields `lastError.kind: "unauthorized"` and keeps the outbox; fails on main with `"network"`.
    **Resolved** (main d4c337c, ws/http-status-unauthorized): HTTP 401/403 before RPC decoding report `unauthorized` with the constant message, retry with backoff without halting, and preserve the pending outbox, while other statuses keep their classification.
19. `SyncStatus` is exported as a type only (found by the NilBurger port while building its FoldKit model; scheduled with §16.17). **Problem:** `SyncLoop.ts` defines `SyncStatus`, `SyncError` and `SyncErrorKind` as runtime `Schema` values, but `LeaderClient.ts` re-exports them with `export type` and `src/client/index.ts` does not expose `SyncLoop`, so an app that needs the runtime schema (a FoldKit model, a persisted status snapshot, a test fixture) must introspect `LeaderRpcs.requests`, select the `syncStatus` streaming RPC and take its `successSchema`. That introspection is public API and keeps working, but it is RPC plumbing an app should not need. **Ruling (coordinator):** `LeaderClient` re-exports the three as values (`export { SyncError, SyncErrorKind, SyncStatus } from "../sync/SyncLoop.ts"`), which also exports their types; no new schema, no wire change, no behaviour change. Done in the §16.17 extraction thread, since the adapter is the consumer and the closed `SyncErrorKind` union is still growing (§16.7 adds `outdated` and `server-outdated`); the adapter must not duplicate the union. Not a test target beyond the existing example tsc projects.
    **Resolved** (ws/foldkit, with §16.17): `LeaderClient` re-exports `SyncError`, `SyncErrorKind` and `SyncStatus` as values; the adapter consumes them and declares no copy of the union.
20. Observability (requested by the owner after the NilBurger cutover; scheduled, ws/observability, medium dial; design proposal approved before code). **Problem:** the engine emits nothing a host can export: no spans, no metrics, and its only signals are `Effect.log*` calls that attach raw causes (header values, URLs, SQLite messages, bodies can appear in those causes) and go to whatever logger the worker's default runtime has. The leader and router entry points (`runLeaderWorker`, `runRouter`) call `Effect.runFork` internally, so a host cannot provide a `Tracer`, loggers, a minimum log level or metric attributes to the engine's fibers without forking the library. **Rulings (coordinator):**
    (a) **Backend-neutral, host-owned.** The engine instruments with the pinned `effect` primitives only (`Effect.withSpan`, `Effect.annotateCurrentSpan`, `Metric.counter`/`gauge`/`histogram`/`timer`); it never depends on a vendor SDK, an exporter, or any app. The host provides the `Tracer.Tracer` reference, `Logger.CurrentLoggers`, `References.MinimumLogLevel`, `Metric.CurrentMetricAttributes` and sampling; unconfigured, the engine is inert: the default no-op tracer and the default logger behaviour are unchanged, and no instrumentation changes any sync, commit, retry or handover decision. A failing or slow tracer/logger/metric sink must not change those decisions either (instrumentation is fire-and-forget with respect to the engine's own effects; a sink failure is the host's defect, never a sync error).
    (b) **Runtime seam.** The worker entries expose the program as an Effect that the host runs: `Leader.leaderWorker(self, options): Effect<void, never, R>` and `Router.router(self): Effect<void, never, R>` (names to be proposed), where `R` is what a host may provide (nothing required); `runLeaderWorker`/`runRouter` stay as the convenience runners that fork the same Effect with the default runtime. A host that wants telemetry provides references with `Effect.provideService` / `Effect.provide(Layer)` around that Effect and forks it itself; every fiber the engine forks (sync loop, pokes follower, presence, tab servers, lock watchers, handover) inherits that context because it is forked from the same program, and the proposal must state which fibers are forked detached and how they inherit. The server side needs no new seam: `Startup.layer` is already a Layer the host composes; the proposal states which server effects get spans and how the HTTP request's incoming trace context is or is not trusted (host decision, documented; the engine never parses `traceparent` itself).
    (c) **Signals and semantics.** Spans (names fixed, `echo.` prefix) for: local mutation acceptance/rejection (`enqueue`: saved vs `LocallyRejected`, idempotent re-enqueue is a distinct outcome), push (one span per batch; outcome is the classified kind: applied count, stopped, halted, outdated, transient kind), pull (one span per set from first request to install or failure; page count; outcome resnapshot/outdated/server-outdated/malformed/transient), durable install (a child of pull that opens after `PullDone` arrives and succeeds only after the storage transaction commits; it is independent of push: an install while push is halted is observed as success), rebase (child of install: pending count replayed, released count, notices by tag), poke stream connect/reconnect (reason: idle timeout, server end, failure kind), and leader election/handover (elected, lost). Metrics: counters per outcome of each of the above, a gauge for pending outbox entries, histograms for push batch size, pull pages per set, install and rebase duration. The proposal must specify for every span what a retry, an interruption (handover, teardown) and a no-op cycle (nothing to push, `PullDone` with no changes) record, and that a span never ends with a failure exit for a classified transient: the engine handles the failure inside the span and records the classification as attributes, so the tracer only ever sees defects as failures.
    (d) **Telemetry safety.** Attribute values are from fixed closed sets or counts/durations only. Never: principal, profile, client, leader, tab or mutation ids; cursors or continuations; mutation names, args or envelopes; row data; header values, URLs, cookies; free-text messages or raw causes. The existing `Effect.log*` calls with raw causes are reclassified: a fixed-text log line at the level the message warrants plus the cause **only under a `echo.diagnostics` annotation the host can filter** (or at `Debug` level), with the safe message as the span/metric signal; the proposal lists each call site. **Defects are not an exception (NilBurger finding: a defect's cause and stack can carry secrets too).** Every engine span goes through one sanitizing boundary: the span wraps the effect's `Exit` (`Effect.exit`), records the outcome from a closed set (`ok`, the classified transient kinds, `interrupted`, `defect`) as attributes, ends the span with a success exit, and only then re-raises the original exit to the caller, so the engine's own failure and interruption semantics are unchanged while no tracer ever receives a `Cause`. The raw cause is available to the host only through the separate diagnostics log channel above, which is disabled for routine export: a host exporter that forwards engine logs must drop entries carrying the `echo.diagnostics` annotation, and `docs/` says so. Secret-canary tests cover a defect (`Effect.die` carrying the canary) as well as typed failures.
    (e) **Tests (invariants, §14):** an unconfigured engine runs every existing suite unchanged (inert); a recording tracer and logger installed through the seam observe the fixed span names and attributes for one push/pull/install cycle, with secret canaries (a header value, a mutation arg, a principal id, a cursor, and a defect message) planted and asserted absent from every recorded span name, attribute, event, status, metric tag and every log entry not annotated `echo.diagnostics` (the annotated entries are the only place a canary may appear, which the test asserts positively so the channel is known to exist); a tracer whose `span.end` throws or a logger that fails does not change the sync outcome, the commit, the retry schedule or the halt; teardown of the host-run program ends every engine fiber (no leaked lock watcher or stream); install is observed and succeeds while push is halted. Browser suites rerun unchanged.
    **Proposal amendments (coordinator with oracle, binding for ws/observability):** (1) The sanitizing boundary is an engine-owned `Span` proxy installed through an engine-owned wrapping `Tracer` around all engine-owned execution; the proxy's `end` forwards `Exit.void` and approved attributes only, so every host-visible engine span, including defects and interruptions, ends `Success(undefined)` with the outcome as closed-set attributes; `Telemetry.observe` classifies with `Effect.onExit` inside `Effect.withSpan`, no saved-exit cell and no extra interruptibility mask; the original value, cause and interruption propagate unchanged. (2) No telemetry queue or dispatcher fiber: delegate synchronously through the proxy and guarded logger wrappers, each host call caught separately (span creation failure falls back to a local non-exporting span; attribute/end failure drops that operation); the engine never awaits export or flush; host callbacks must be bounded and nonblocking, and immunity to a blocked event loop is outside the guarantee (tests assert unchanged commit/halt/retry decisions, not timing under a blocked loop). (3) The pinned `sql/Statement`, `rpc/RpcServer`, `rpc/RpcClient` and `http/HttpClient` create their own spans (query text, URLs, headers, raw exits), which would reach a host-provided tracer; the wrapping tracer forwards only an exact declared name set (not a prefix) with validated attribute values, forwards no events, links or annotations (the default `tracerLogger` writes `Cause.pretty` into span events), resolves a forwarded span's parent by walking its own parent chain to the nearest forwarded ancestor, preserving any root break or explicit `ExternalSpan` met on that chain (a suppressed root span's descendants start a new trace; they never reconnect to the boundary's captured host parent, which applies only when the chain reaches the boundary), while forwarded spans keep the exported target's span/trace identity and sampling, and does not delegate the optional `Tracer.context` hook. On the server the boundary wraps the RPC route's execution before `RpcServer`'s automatic span, since `handlerContext` lets the request fiber's tracer win over the handler layer's; unrelated host routes are untouched. (3b) Telemetry never changes a sync decision: `SyncLoop`'s halt/retry/outdated decision for a push is `classify` over the whole final cause of the push, exactly as before this phase, including typed fatal failures contributed outside the batch (finalizers); the `echo.push` outcome is the same `classify` applied to the batch's own cause, and a cached batch decision may re-raise a captured policy defect but must never suppress scanning the final cause. `isFatalPush` is a pure predicate and may be evaluated more than once per failure. (4) Callback-originated fibers use `runForkWith(context captured after the boundary is installed)` plus `Fiber.runIn`/`forkIn` on the long-lived program scope; the router's detached epoch watcher becomes `forkIn(routerScope)` (never a child of one announcement acceptance, which would break dead-leader detection); closing the program disables listeners, rejects late callbacks before any work, and closes owned ports; the same applies to `LeaderClient.make`'s fork helper and serialized router acceptance. (5) No `echo.leader.tenure` and no `echo.server.presence.room`/`track` spans in this phase: leadership is election/loss counters plus an `echo.leader.handover` span observed by the surviving tab/router epoch watcher (a terminated worker cannot end its own span); the poke span measures one connection attempt to its first protocol element or establishment failure, and later stream termination records the reconnect-reason counter. (6) Log reclassification approved; hosts filter with `fiber.getRef(References.CurrentLogAnnotations)["echo.diagnostics"]`, and the inventory must also remove interpolated dynamic text (`Cause.pretty` in `followPokes`, the table name in `applyChange`). Reviewer target: the completeness of the boundary on a real server request and worker sync with `Logger.tracerLogger` enabled and canaries in typed failures and defects, not the helper alone.
    (f) **Out of scope:** exporters, OTLP, backend provisioning, sampling policy, retention, privacy review of the host, and anything in NilBurger (two app-only threads own browser reporting and server ingestion). The NilBurger pin is not changed by this item; the proposal is returned to the owner before any pin change.
    **Resolved:** Observability landed per the amended contract in [the reviewed implementation](https://github.com/skyturtleio/echo-sync/commit/437992ca4738e1f8a9ea15ef0e2b2bc1762391d6), “Keep final push causes authoritative and preserve suppressed root breaks”.
21. First-registration account binding (found by the reviewer in the NilBurger port; user pick C, ws/first-registration-binding; **resolved**: landed as "Refuse requests whose replica principal is not the authenticated one" and "Assert the replica principal on every sync and presence request", reviewed at dbfc558, with the ruling below implemented as written and previous-Protocol wire compatibility covered by a frozen pre-change fixture in `Transport.test.ts`). **Problem:** a replica is created for principal A, but the engine never asserts that the HTTP session performing that replica's *first* registration belongs to A. If the browser's session switches to B between replica creation and the first successful sync, A's unknown `clientId` is bound to B on the server and B's snapshot is installed in A's local profile. Already-registered clients are protected by the existing binding check; only first registration is exposed. NilBurger closed it app-side (an expected-account header compared against the authenticated session before provisioning). **Direction when scheduled:** the engine carries the assertion itself: the client sends a stable, non-secret expected-principal identifier with registration, and the server refuses provisioning on mismatch before any grant or snapshot work; it is a consistency assertion, never a credential, and must not weaken `principalFor` (§16.16). Tests: session switch before first registration installs nothing and binds nothing; a registered A client with pending writes is unaffected. **Coordinator ruling (binding for ws/first-registration-binding):** (1) The identifier is the replica's own `principal` (`LeaderClientConfig.principal`, decoded as `AccessScope.PrincipalId` at `Init`; it is the same opaque app string `principalFor` returns, DESIGN §4), carried as an additive `Schema.optionalKey` field `principal` on `PushRequest`, `PullRequest`, the `pokes` payload and both presence request payloads, following the `clientVersion` precedent so a deployed pre-change server ignores it and a pre-change client omits it. No new header: `syncHeaders` stays credentials-only. (2) The server compares `request.principal` with the authenticated `principalFor(headers)` result before `registerClient`/`lockClient`, before any grant lookup and before any ledger or presence work, on every request, not only the first; a mismatch fails with the existing `Protocol.Unauthorized` (fixed message, no principal text) and writes nothing. Equality is an assertion; the body value is never used as the actor. An absent field behaves exactly as today. (3) Client classification is unchanged: the response is `Unauthorized`, so `lastError.kind` is `unauthorized` and the app's credential path runs (§9.1); the engine does not reset or re-home the replica. (4) Tests (invariants): a request whose authenticated actor differs from the replica principal binds no client row, installs no page and writes no ledger/presence state in PG; an already-registered client with pending writes is unaffected when the session switches (pending preserved, push refused, no data of the other principal installed); a request without the field from an old client still binds on first sight; wire compatibility with the previous `Protocol` is covered by the existing conformance fixtures. Reviewer target: every server entry point that calls `registerClient` or `lockClient` performs the comparison first.
22. Cloudflare/edge server composition (owner question, CAFE stack; **unscheduled**, recorded so the seam is not forgotten). The engine is runtime-agnostic at the client and the wire; the server composition assumes one long-lived process: the pokes fan-out is a process-scoped shared `LISTEN` plus in-memory `PubSub` (§6) and presence is process-local state (§12, multi-instance a v1 non-goal). Push and pull handlers port to a Workers-style runtime over a TCP proxy to Postgres; the pokes hub and presence do not, and their Cloudflare-native shape is a Durable Object that the push handler notifies after commit and that owns the streams. Direction when scheduled: make the server's notifier/presence seam an explicit service so a second composition drops in additively (§12: the HTTP transport is one composition of exported server seams); the wire contract (`Poke | Heartbeat`, subscribe-then-poke) does not change. Not before the owner wants to leave a long-lived process host.
23. Tab-side resubscription drops defects (found by ws/foldkit while testing the adapter's defect translation; **scheduled, ws/resubscribe-cause, medium dial**). **Problem:** `LeaderClient.resubscribing` recovers `LeaderLost` with `Stream.catchIf`; the pinned `Stream.catchIf` delegates to `Channel.catchFilter`, which re-fails a non-matching error with `fail(error)`, not the original cause, so a `Fail`+`Die` cause from `subscribe`, `notices` or `syncStatus` reaches the tab as `Fail` only and the defect (a bug signal, §6 precedence defect > transient) is lost before any consumer sees it. `Effect.catchFilter` preserves the cause; only the Stream/Channel variant does not. **Ruling (coordinator):** `resubscribing` recovers only a cause whose failures are all `LeaderLost` and that carries no defect; every other cause passes through **unchanged** (use the cause-level combinator, verified against the pinned `Stream`/`Cause` source, no reconstruction of the cause). A `LeaderLost` accompanied by a defect is therefore not resubscribed: it ends the stream with the full cause, since the defect says the leader died of a bug and silent resubscription would hide it. No wire change, no `LeaderRpcs` change. **Invariants:** a `Fail(X)`+`Die` cause from a leader stream reaches the tab API's consumer with the `Die` present (fails on main: `Die` absent); a plain `LeaderLost` still resubscribes and the consumer sees no error; `LeaderLost`+`Die` ends the stream with both present. Reviewer target: `mutate`'s `LeaderLost` retry (`Effect`-level) is unaffected, and no other `Stream.catchIf`/`catchTag` in `src/client` has the same loss.
    **Resolved** (ws/resubscribe-cause, reviewed head 9063734): `resubscribing` recovers only a defect-free all-`LeaderLost` cause via `Stream.catchCauseIf`, and the same loss upstream in `streamWithLeader`'s `RpcClientError` mapping is fixed with `Cause.map`, covered by a public-layer invariant over `notices` and `syncStatus` that fails if either site is reverted.
24. Engine-owned coordinated reset (owner ruling after the NilBurger sign-out port; **resolved (ws/coordinated-reset, reviewed head 6c39e89, landed 0370c01); supersedes the §9.1 `resetReplica` sentence "the app is responsible for closing its tab clients first"**). **Problem:** `resetReplica` fails `ReplicaInUse` whenever any leader for the identity is alive, and nothing in the engine closes the identity's tab clients, stops queued leader candidates, or prevents a tab from electing a fresh leader (which re-creates the replica and, with a still-valid session, re-pulls the account's data) between the deletion and the app's sign-out completing. NilBurger had to build app-side lifetime locks, persistent account epochs, a BroadcastChannel fence, a do-not-respawn rule and a second confirmatory reset to get a correct sign-out; a greenfield app would have to build the same plumbing again. The engine already owns the cross-tab channel, the leader lock, the candidates and the tab clients, so it owns the coordination. **Ruling (binding for ws/coordinated-reset):** (1) `resetReplica(identity, options)` keeps its signature, error union and `discardPending` semantics, and additionally *closes the identity's clients itself*: it broadcasts a reset signal on an engine-owned `BroadcastChannel` for the identity (`echo:`-prefixed, like every engine name), then waits a bounded time (option `closeTimeout`, default a few seconds) for the leader lock; a lock still held when the wait expires is `ReplicaInUse` as today (a tab that does not honour the signal: an old build, a frozen tab), and nothing is touched. The caller's own tab client is closed by the same signal; the app no longer closes anything first. (2) Every `LeaderClient` for the identity that receives the signal, including one whose worker is a queued candidate and one that is the leader's tab, enters a **terminal reset state** at once: it terminates its own leader worker (which releases the leader, epoch and queued locks), displaces its connection, publishes the state on `status`, and from then on every request fails with `LeaderUnavailable { reason: "Reset" }`, which `mutate` does not retry and `resubscribing` does not resubscribe (§16.23's handover predicate stays `LeaderLost`-only); open streams end with that error. The state is terminal for that client; the app's scope closes it whenever it likes, and using the replica again (sign-in, or "keep my data" after a refused reset) means building a new `LeaderClient`, which is the only way a leader for the identity is ever started after the signal: no engine-owned fiber, lock callback, router announcement or reconnect path spawns or elects a leader for a client in reset state. A client built after the signal is unaffected by it (the signal is not sticky; it is an event, not a tombstone). (3) Order under the lock is unchanged: pending count (unless `discardPending`), `PendingWrites { count }` fails closed with nothing deleted, then removal of only `echo-sync/<appId>/<profile>`. After a `PendingWrites` refusal the clients stay in reset state and the lock is free, so the app's prompt resolves to either `resetReplica({ discardPending: true })` (succeeds without `ReplicaInUse`, since nothing is alive) or a fresh `LeaderClient` to keep the data; `syncStatus.pendingMutations` remains the same number an app may show before asking. (4) The router needs no new message: the leader's epoch lock release already clears its route. Single-tab mode (no SharedWorker) coordinates the same way, since `BroadcastChannel` is independent of the router; `unsupported` mode has nothing to close. No wire change, no server change. **Invariants (Chromium scenarios, DESIGN §14):** with two tabs holding clients and the leader in either, `resetReplica` from one tab without any app-side preparation ends with the directory gone, both clients in reset state with `mutate` failing `Reset` (not `LeaderLost`, not retried), and no new leader elected until a tab builds a new client (fails on main: `ReplicaInUse`, or a queued candidate re-creates the replica); a `PendingWrites` refusal leaves the clients in reset state, deletes nothing, and a following `discardPending` reset succeeds without `ReplicaInUse`; a signal for another profile of the same app changes nothing; a client built after the signal elects a leader on a fresh, empty replica; a tab that ignores the signal (test harness holds the leader lock) yields `ReplicaInUse` after `closeTimeout` with nothing deleted. Reviewer target: every path that starts or elects a leader worker in `LeaderClient` checks the reset state first, the terminating tab releases every lock it held or queued (verify with `navigator.locks.query`), and a reset signal can never close a client of a different identity.
    **Resolved** (ws/coordinated-reset, reviewed head 6c39e89): `resetReplica` broadcasts `ResetSignal` on `echo:<appId>:<profile>:reset` after spawning its worker, which waits for the leader lock in one request bounded by `closeTimeout` (finite, default 5 s; a non-finite value is `ResetFailed` before anything is spawned or signalled; a grant after the deadline is released at once). Every client of the identity enters the terminal state (`Status.reset`, `LeaderUnavailable{Reset}`, candidate terminated, tab lock and lock waits released, the client's scope close joins that release). Covered by 19 Chromium checks in `scenarios.sh` (3/17 of the original set pass on the prior main), a deadline-race invariant in `platform.test.ts`, and a gated scope-close regression in `LeaderClient.test.ts`.
25. Sign-out that keeps the replica (boundary question from the NilBurger upgrade to §16.24; **resolved, coordinator ruling: no engine change**). **Question:** §16.24 coordinates *deletion*; nothing coordinates a cross-tab *close without deletion*, so a "keep my data" sign-out leaves other tabs' clients alive on readable data until each tab's app closes its own scope. Should the engine add `closeReplica` first? **Ruling:** no. The engine owns cross-tab coordination only where an engine-owned resource forces it: the OPFS directory cannot be removed while a leader holds it, hence §16.24. A replica that is *kept* has no such resource conflict; which tabs may still show it is the app's authentication state, which every multi-tab app must propagate anyway (cookie/`storage` change, an app-owned channel, or the next `401`), with or without a sync engine. The engine's contract stays §9.1: a client lives exactly as long as its scope, "closing the tab client's scope terminates a locally owned leader worker and nothing else", there is no global freeze or close primitive, and credentials own authorization. The engine fails safe in the gap: a revoked session makes push/pull/stream `Unauthorized` (retried, never fatal, §16 item 10), queued local writes upload only once the same account signs in again, and a different account cannot sync a stale replica (§16.21 account binding). **Consequence for apps (NilBurger now, every app later):** when a tab learns the user signed out, it closes its client scope and shows signed-out UI; how it learns is app auth plumbing and may be a plain app-owned `BroadcastChannel` or `storage` event, but that plumbing must do no engine work: no locks, no leader or candidate handling, no `echo:`-prefixed name, no `ResetSignal`. The §16.24 directive to delete app-side lifetime locks, account epochs, do-not-respawn and the confirmatory reset stands; those existed to make *deletion* safe. **Reversibility:** an additive `closeReplica(identity, { closeTimeout })` (same signal and bounded lock wait as `resetReplica`, no deletion) can be added later without changing any existing contract if a real app need appears; none is known.
26. Reset durability across a crash (found by the NilBurger upgrade review; **resolved (ws/reset-tombstone, reviewed head 2615eb7)**). **Problem:** the reset worker removes `echo-sync/<appId>/<profile>` with one recursive `removeEntry`. A crash, tab close or interruption during that call leaves a partial directory, and `docs/browser-substrate.md` says what a leader makes of such a directory "is not established". NilBurger filled the gap app-side with a persisted (`localStorage`, cross-tab) recovery quarantine that stops a tab from reopening a replica whose removal may have begun, and that marker is now the subject of its own overwrite races. An intent to remove data that outlives a crash is durable state about the replica, and the engine owns the replica, so the engine owns the intent. **Ruling (binding for ws/reset-tombstone):** (1) Under the leader lock, after the pending check passed or `discardPending` was given, and before touching the directory, the reset worker writes an engine-owned tombstone entry in `echo-sync/<appId>` whose name `validateIdentity` can never accept as a profile (so it collides with no replica), then removes the profile directory, then removes the tombstone. A crash at any point leaves either an intact replica (no tombstone) or a tombstone (removal intended, possibly partial). (2) Every leader, after winning the leader lock and before opening its pool, checks for its identity's tombstone; if present it completes the removal (directory if any, then tombstone) and starts on a fresh replica with a new `clientId`, transparently to the tab. The reset worker does the same when it finds a tombstone at start: it completes the removal and answers `Done` without a pending count, since that data was already condemned by a reset that passed the check. (3) Nothing else changes: signature, error union, `discardPending`, `closeTimeout`, §16.24 signalling, wire and server. The substrate doc's "not established" sentence is replaced by this contract. **Invariants (Chromium scenarios, DESIGN §14):** a reset whose worker is terminated after the tombstone is written and before removal finished is followed by a new `LeaderClient` that starts on a fresh, empty replica with a new `clientId`, and afterwards neither directory nor tombstone exists; a worker terminated before the tombstone is written leaves the replica intact with its `clientId` and outbox unchanged; a tombstone of another profile of the same app changes nothing for this identity; a second `resetReplica` after an interrupted one answers `Done` with nothing left behind. Reviewer target: the tombstone is only ever written under the leader lock after the pending decision, and the leader's completion path runs before any pool or database open. **Consequence for apps:** no persisted app-side reset or recovery marker is needed or permitted (§16.25 rule on engine work); NilBurger removes its quarantine when it upgrades to the landing.
    **Resolved** (ws/reset-tombstone, reviewed head 2615eb7): the tombstone is the file `echo-sync/<appId>/~reset-<sha256(profile) hex>` (the `~` is outside `validateIdentity`'s charset; 71 bytes). The reset worker writes it under the leader lock after the pending decision, then removes the directory, then the tombstone. A leader completes a tombstoned removal after taking its leader and epoch locks and before `openDatabase`; a leader whose completion fails cannot serve (locks released, worker ends, the condemned replica never opened). A reset that finds the tombstone completes it and answers `Done` without opening or counting; a reset whose completion fails answers `ResetFailed`. Qualification of ruling (1): a crash after both removals completed, before the worker reports `Done`, leaves neither replica nor tombstone. The invariant "afterwards neither directory nor tombstone exists" is checked as: no tombstone and none of the condemned pool files remain (the new leader's fresh pool directory is expected). Covered by four Chromium checks in `scenarios.sh` ("reset durability across a crash", harness crash points before the tombstone write and mid-removal); all four fail on the prior main. Evidence covers worker termination and partial removal, not power loss or browser-process durability. `docs/browser-substrate.md` "Resetting a replica" states the contract.
27. Self-revocation of a grant (found by the HeyBabe port, ws/echo-sync; **resolved (main 4bb1bb3, ws/self-revoke, reviewed head 4bb1bb3)**: `Store.delete` skips the owner check only for the `scope_grants` row whose `principal_id` is the actor and the actor is not `*`; the three invariants below plus a denied self-reinsert after leaving are covered in `Store.test.ts` and `grants.test.ts`, and fail on the prior main). **Problem:** every `scope_grants` write requires `owner` on the scope (`Store.ts` `requiredRole`), scope creation being the only exception, so a principal holding `reader` or `writer` cannot remove its own grant. Leaving any shared group (a HeyBabe couple, a household, a team) then has to be a server action although it needs no network and nothing but the leaver's own consent, and a leaver keeps reading the partner's `couple:<c>:ro:<partner>` scope forever. Relinquishing one's own access is part of the access model, so this is an engine gap, not app policy. **Ruling:** `ctx.store.delete(ScopeGrants, id)` is permitted whatever role the actor holds when the stored grant's `principalId` equals the actor (the actor then necessarily holds that grant; `*` can never equal an actor, so the `*` row still needs `owner`). Insert and patch of `scope_grants` stay `owner`-only, including on the actor's own row; the only other exception stays `createScope`. One core `Store` rule serves client and server, so the optimistic delete and the authoritative one agree, and the revocation is delivered as today (tombstone in the scope's log to the remaining readers, `scopePurged` to the leaver's clients on their next pull, §4). A handler that relinquishes a grant must do its other writes in that scope first; a later write in the same handler is `ScopeForbidden` like any write without a grant. Deleting the last `owner` grant orphans the scope exactly as an owner revoking itself does today; the engine does not prevent it. No wire change. **Invariants (§14):** a `writer` or `reader` deleting its own grant is `Applied`, its client receives `scopePurged` for that scope on the next pull, and another member's replica keeps the rows and sees the grant tombstone (fails on main with `ScopeForbidden{required: owner}`); a `reader` deleting another principal's grant, or the `*` grant, is still `ScopeForbidden{required: owner}`; a handler that writes to the scope after relinquishing its grant is rejected. **Consequence for apps:** leaving a group is an ordinary client mutation (HeyBabe `couples.leave`: move own rows out of the couple scopes, then delete own grants last); no server action and no app-side workaround.
28. Consumer strictness flags (found by the HeyBabe port: `noImplicitReturns` fails inside echo-sync at 80c7c1a, TS7030 in `src/client/worker/LeaderClient.ts:798` and `Router.ts:166`; **resolved (ws/consumer-strict-flags, reviewed head 9d06662)**). Measured on main: `noImplicitReturns` 3 errors (those two plus `src/client/sync/testing/Simulation.ts:496`), `noPropertyAccessFromIndexSignature` 2 errors (`Simulation.ts:130, :480`); every other strictness flag is already enabled or clean. **Ruling:** fix the five sites and enable both flags in the library `tsconfig.json`; the rule is the §13 consumer flag contract. No app relaxes a flag; HeyBabe clears the error by bumping its pin. **Resolved** (ws/consumer-strict-flags, reviewed head 9d06662): the five sites are fixed with no behavior change (`LeaderClient.ts` and `Router.ts` promise-chain callbacks return `undefined` explicitly on the guard and no-fiber paths; `Simulation.ts` reads index-signature rows with bracket access and `record`, used only through `Effect.tap`, no longer returns `push`'s length). The library `tsconfig.json` enables the two ruled flags plus `allowUnreachableCode: false`, `allowUnusedLabels: false` and `noUncheckedSideEffectImports`, which were already clean on TS 7.0.2 (each confirmed enforced by a probe); the §13 list names all of them. Left off: `isolatedDeclarations` (a declaration-emit constraint, not a strictness check; the package ships source and emits no declarations) and `skipLibCheck: false` (its errors are only in third-party and TypeScript lib `.d.ts` files, never in echo-sync source).
29. Presence departure lag behind proxies (found by the HeyBabe port, the first app to use presence; **resolved (user: a)**, no engine change). A presence entry lives exactly while its `room` HTTP stream is open (§3, §9.1). Directly against the server an aborted stream clears the entry in 2 ms (probed). Through a development reverse proxy the client abort never reached the server, and the partner's cue cleared only when the heartbeat/idle bound ended the stream (~45 s). Whether the production proxy (Coolify) behaves the same is unknown; NilBurger does not use presence, so production presence is untested until HeyBabe ships. **Ruling:** accept the current bound. Presence is advisory, and a departed member may remain announced for up to the heartbeat/`idleTimeout` window (≤45 s by default) behind a proxy that does not propagate client aborts. Apps must not treat a cue as a lock or as proof of liveness. Rejected for now, revisit with real-device numbers after the HeyBabe deploy: (b) a best-effort client `leave` on `pagehide` (fixes tab close only, not crashes or lost connectivity); (c) a server-side lease that expires an entry unless the client re-tracks within N s (covers every case, but adds a second liveness mechanism beside the stream heartbeat).
30. Sync failures are classified once, by type (assessment of Effect idioms against Alchemy and Foldkit, ws/sync-failure-reasons). **Problem:** `Engine` was generic over its transport error (`Engine<TE>`, `Transport<E>`), so nothing downstream could name a failure by type. `SyncLoop` (status `kind`, `Halted` reason), `Transport.isFatalPush` (`error: unknown`) and `Telemetry.kind` each walked `reason`/`cause` chains and compared `_tag` strings, `Engine.pull` narrowed `ResnapshotRequired` with `instanceof`, and `Telemetry.pushOutcome` was a context reference that let `SyncLoop` inject its classifier into the `echo.push` span. A renamed or new error silently became `server`; the compiler could not see it. **Ruling:** the transport is the only module that sees RPC/HTTP errors. It wraps every push failure in `SyncFailure.PushTransportError { reason: PushReason }` and every pull and pokes failure in `PullTransportError { reason: PullReason }` (the Effect v4 reason pattern, as `RpcClientError`/`HttpClientError` use), lifting the innermost evidence once: a `CredentialsUnavailable` inside the HTTP `TransportError`, or an HTTP 401/403, becomes that reason. The two reason unions are separate because the RPCs differ: only a push can be `MalformedEnvelope` or `PushIntegrityError`, only a pull can be `ServerUnavailable` or `PullProtocolError`, so a pull cannot carry a halting reason by type. `ResnapshotRequired` passes through unwrapped for the engine (`catchTag`). `Engine` and `Transport` are no longer generic; `Engine.push` fails with `SyncFailure.PushFailure`, `pull` with `PullFailure`. Each direction has one exhaustive `Match.valueTags` over the failure and its reason, applied to a whole cause by one function: `classifyPush` returns the status kind, the halt reason (DESIGN §6: `MalformedEnvelope`, `PushIntegrityError`, or a defect anywhere) and the telemetry outcome, with the existing precedence (defect > integrity > first failure); `classifyPull` returns the kind and outcome of the first failure (a defect is a transient `server`, never a halt). `SyncLoop.make` loses `isFatalPush` (the fatal rule is the reason's meaning, not a host policy) and `pushOutcome` is removed. `Telemetry.observe`/`observeStream` take the operation's failure classifier as a required argument and the `_tag`-string fallback is gone: sync spans use `classifyPush`/`classifyPull`, the server's push/pull/pokes handlers one exhaustive match over their protocol errors, `echo.enqueue` maps `LocallyRejected` to `rejected`, and operations whose typed failures are all `server` (rebase, startup, election, handover) say so with `Telemetry.serverFailure`. `MalformedChange`, `ServerOutdated`, `PushIntegrityError`, `PullProtocolError`, `CredentialsUnavailable` and `SyncErrorKind` move to `SyncFailure` (`LeaderClient` still re-exports `SyncErrorKind`). On the tab side, with no behavior change: `LeaderClient` reads the browser (locks, capability, and the leader RPC client as the explicit `LeaderConnection` interface) through the `TabPlatform` `Context.Reference`, whose default is the browser, so its tests use a typed fake instead of module mocks; router acceptance in `LeaderClient` and announcements in `Router` are taken in arrival order by one scope-owned fiber over a `Queue` instead of a Promise chain; and the request route (wait for a leader, fail in-flight calls with the displaced connection's `LeaderLost`, fail `Reset` for good after a reset) is `LeaderRoute`, tested directly, while `LeaderClient` keeps the topology and decides when to publish, displace or reset. No wire, status, halt, retry or telemetry vocabulary change; presence errors are unchanged. One telemetry label is corrected: an `RpcClientError` for a malformed RPC response, which the old chain walk could label `server` (or by a tag inside the body), is now `network`, as its status kind already was. **Invariants (§14):** the existing push-fatality table over a real RPC server (Transport.test.ts), the halt/retry/outdated SyncLoop tests, the whole-final-cause and batch-outcome observability tests, the §16.21 session-switch and §11 version-gate e2e tests, and the deterministic simulation all pass; they changed only in how they construct or inspect failures, except that the observability cases exercising a host-supplied `isFatalPush` (a custom fatal tag, a non-fatal `MalformedEnvelope`, a throwing policy) were removed with that option. A new reason does not compile until its direction's match handles it.
31. Errors keep their cause and say where (assessment of Effect idioms, ws/error-causes). **Problem:** engine-stored data that failed to decode died with a bare `SchemaError` (`Effect.orDie`), naming neither the table nor the row, and the leader's browser storage calls (OPFS, SQLite on OPFS, Web Crypto) failed with a bare `Error` coerced by `new Error(String(cause))`, so the tab's `LeaderFailed` carried `Cause`'s debug string. **Ruling:** every decode of data the engine stored (both row stores, the typed `Store`, the client outbox, staged pages, undo images, notices and meta, the server ledger and pull acks) goes through `Store.decodeStored(schema, table, stored, id?)`, whose defect is `Errors.CorruptStorage { table, id, cause }`, still a defect, never a typed failure; its message names the table, row and the schema error. Encoding a handler's value stays `orDie`. Browser storage calls go through `platform.storageCall(operation, run)`, failing `BrowserStorageError { operation, cause }`; `operation` names what was attempted and never a path, since paths carry the app id and profile and an app may forward the message. The tab's `LeaderFailed` and tab-lock messages carry the failing error's name and message (`Cause.squash`), the full cause staying in the diagnostics log. **Invariants (§14):** a corrupt SQLite row read through the row store dies with `CorruptStorage` naming table, row and column (SqliteRowStore.test.ts); a lone OPFS startup failure reaches the tab as `LeaderFailed` naming the operation and the browser's reason and not the app path (scenarios.sh).
