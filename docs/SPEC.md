# Echo Sync — Specification

This document is both the design and the contract of the engine. It is derived from the code at
the commit it ships with; where the code and this document disagree, the code is the truth and the
disagreement is a bug in this document. Report it; do not silently diverge in either direction.

Every chapter has the same shape: an **Overview**, an **Invariant digest** (each invariant has a
stable ID `INV-<AREA>-<n>`; IDs are never renumbered or reused, a withdrawn invariant keeps its ID
and is marked withdrawn), **Details**, and **Open Questions** (places where the intended contract
could not be told from the implementation, or where the implementation and an older document
disagree). §17 collects every invariant with the test that covers it, or marks it untested. §18
collects every limit and default. Dated rationale notes sit beneath the rules they explain;
§17 also lists retired rules so they are not reintroduced.

## 0. Conventions, versioning and the three questions answered first

### 0.1 Conventions

The key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be interpreted as in RFC 2119.
A MUST that the current implementation violates is recorded as an Open Question, not as a MUST.

Names in `code font` are identifiers in the source: tables (`echo_sync.mutations`), schema tags
(`ResnapshotRequired`), options (`pageLimit`) and functions (`next_position()`). Source paths are
relative to the repository root. `SPEC §x` citations refer to this document. Notes marked
**Rationale (YYYY-MM-DD):** record the ruling's date, reason and rejected alternative where
there was one; the rule remains the primary statement. Appendices C–E are non-normative
implementation and verification notes, not additional contracts.

### 0.2 Versioning

- **Wire version.** `Protocol.wireVersion` is `1`. Every `push` and `pull` request carries
  `clientVersion: { dataHash, wireVersion }` (optional on the wire only for clients that predate
  the field). The server MUST NOT infer compatibility from the field's absence. Compatibility is
  decided by the app-supplied `clientPolicy`; the engine's default is to serve every client.
- **Consumers pin a commit.** The package is consumed as a git dependency and ships TypeScript
  source; there is no npm release. A consumer picks up a change only by bumping its pinned
  commit SHA. There is therefore no semantic version; the unit of compatibility is the pair
  (server pin, client pin), and the server MUST be deployed first for any change that adds a
  wire field, because old clients keep running against the new server until they reload.
- **Breaking change.** A change is breaking when a client and a server on different pins can no
  longer exchange `push`, `pull`, `pokes`, `room` or `track` messages that decode, when a stored
  form changes meaning (`echo_meta`, `echo_outbox`, `echo_staging`, `echo_notices`, the
  engine's Postgres tables), when a `dataHash`-relevant part of a `SyncTable` changes (§3), or when
  a MUST in this document changes. A breaking change MUST bump `wireVersion` when it concerns the
  wire, MUST ship a migration when it concerns the Postgres schema, and MUST update the rule
  and its dated rationale in this document in the same branch. Additive fields marked optional on the wire
  (`clientVersion`, `principal`) are not breaking.
- **Error vocabulary is closed.** The tags in §15 and the literal unions they carry
  (`ResnapshotRequired.reason`, `SyncErrorKind`, `LeaderUnavailable.reason`, telemetry names and
  outcomes) are part of the contract; adding a member is a design change recorded in a dated
  rationale note next to the affected rule.

**Rationale (2026-10-22):** Two-direction compatibility gates were chosen because an old PWA
can keep running indefinitely after dismissing reload, while a newer client can corrupt its
replica against an older server; §10 stops refused clients without discarding intent and §9
backs off on incompatible server rows rather than rebuilding. The app chooses the table-shape
floor from generated hash history (§13), rather than the engine assuming the latest hash is
the only compatible one; a refusing policy or non-additive change must wait for deployed
clients to understand the gate (Appendix D.4).

### 0.3 The three questions answered first

These are the places where a review could not tell the intended contract from the
implementation. Each states what the code does today; the full rules live in the cited sections.

**Q1 — Can a forged pull continuation walk a scope the actor no longer holds?** No. The
continuation is an unsigned JSON string (`pull.ts` `Continuation`, encoded with
`Schema.fromJsonString`), so a client can forge any well-formed value, but the server never takes
authorization from it. On every page the readable set is recomputed from `scope_grants` for the
authenticated principal (own grants and `*`); a held or walking scope that is not in that set is
emitted as `scopePurged` and dropped from the walk; log entries are filtered by that set; and each
upsert payload is re-read from the current row and replaced by a tombstone when the row's current
scope is not readable. A forged continuation can only harm the forger (skip a walk or log range
it should have received, which then leaves its replica incomplete until a resnapshot); at
`PullDone` the held set recorded under `nextCursor` is the readable set of the snapshot, never
what the continuation claimed. What a forgery cannot do is obtain rows outside the actor's
grants, or make the server record a held set no snapshot produced. See §7.3, INV-PULL-5,
INV-PULL-6; tested by `gc.test.ts` "a continuation replayed by another client, principal, or
cursor, or garbled, is refused" and "a well-formed forged continuation cannot record a held set no
snapshot produced".

**Q2 — What does pull do when a client's cursor is ahead of head?** There is no explicit check
that `cursor ≤ head` for a bare cursor (one is enforced for a resumed continuation's `position`).
The case is reached only after the database is restored to an earlier backup (head is monotone
within one database timeline: `GREATEST(gc_horizon, max(log.position), max(mutations.position),
max(clients.expired_position))`). In that case the client's cursor names a `PullDone` the restored
database never completed, so its `echo_sync.client_scopes` row is absent and the server answers
`ResnapshotRequired{unknownCursor}`; the client clears its staging, sets `resnapshot`, and pulls
from a null cursor, which replaces the replica. If the row did exist with `cursor > head` (no
known path produces this), `PullDone.nextCursor` would be `head`, moving the cursor backwards and
re-delivering acks in `(cursor, head]`, which is the empty range, so no ack would be
re-delivered; the client would then re-install from a position below rows it has already
installed. There is no restore or epoch fence: the recovery point of a deployed app is its
database backup, and the engine's recovery mechanism after a restore is the resnapshot the
missing `client_scopes` row forces. Pending outbox entries survive a resnapshot and are replayed
and pushed again; the server treats them as new mutations unless their ledger row survived the
restore. See §7.4 and Open Question OQ-PULL-1.

**Q3 — What is the rebase cost model with respect to outbox size and page-set size?** Every
install and every push that covers an entry is a *full* rebase (`Engine.ts` `rebase`): undo the
optimistic effect of **every** pending outbox entry in reverse `seq` order, install the base
(for an install: every change of every staged page, each upsert decoded through the table's
wire codec and re-encoded before the write; for a push-covered release: nothing), release the
entries the base settles, then re-run the handler of **every** remaining pending entry and record
its fresh undo images. With `P` pending entries, `U` the total number of undo images they hold, `C`
the number of changes in the page set and `H(m)` the cost of mutation `m`'s handler, one install
costs `O(U) + O(C) + Σ_{m unreleased} H(m)` store operations, and `P` does not shrink the cost of a
page set nor `C` the cost of the outbox: the terms add. A pushed batch that is answered with
positions at or below the installed cursor triggers the same undo-all/replay-all with `C = 0`.
Every upsert of a known table is decoded once at staging (to refuse a malformed page before it is
persisted) and decoded again, then encoded, at install: two decodes and one encode per upsert
per install. All of this runs inside one SQLite transaction under the storage's single permit, so
no query runs and no mutation is accepted while it runs. See §9.4, §16 and INV-CLI-3.

## 1. Scope, non-goals and admissions

### Overview

Echo Sync replicates rows of declared tables from one PostgreSQL database to one SQLite replica
per browser profile, and ships mutations the other way through a per-client outbox. The server is
authoritative; the client applies mutations optimistically and rebases them on every
authoritative install. Readability is decided per row by the row's access scope and the
principal's grants; grants are themselves a synced table.

### Non-goals

The engine does not and, at this pin, will not:

- run more than one server process for pokes or presence (`LISTEN` fan-out and presence state
  are process-local; a second instance sees neither);
- serve a client whose principal differs from the one its replica was first registered under
  (INV-MUT-10: the binding is permanent per `clientId`);
- order pulls across clients, provide a global catalog snapshot format, or offer a conformance
  suite for a non-TypeScript client;
- forget idle clients (`echo_sync.clients`, `client_scopes`, ledger identities and floors are
  kept forever: a forgotten floor turns a lost-ack resubmission into a double apply);
- fence a database restore (Q2);
- run more than one synced-write transaction at a time on the server (§6);
- offer full-text search in the replica (the pinned wa-sqlite build has no FTS5);
- ship a UI adapter other than FoldKit (`echo-sync/foldkit`), or carry any UI-framework
  knowledge in `src/core`, `src/client` or `src/server`.

**Rationale (2026-10-21):** A packaged global catalog snapshot (NDJSON+gzip was considered)
was deferred past the v1 vertical slice in favor of row-by-row scope walks (§7).

**Rationale (2026-10-22):** A non-TypeScript protocol conformance suite was deferred
indefinitely because TypeScript is the only planned client language; revisit only when a
non-TypeScript client is planned.

**Rationale (2026-10-21):** Local full-text search was deferred because the pinned wa-sqlite
build lacks FTS5; a custom build versus non-FTS search must be decided before a consumer
requiring full-text search is ported (requirement confirmed 2026-10-22).

**Rationale (2026-10-22):** Edge/serverless composition remains unscheduled: the wire and
client are runtime-agnostic, but the process-scoped shared `LISTEN`/`PubSub` and in-memory
presence assume a long-lived server (§16), not interchangeable per-request instances.

**Rationale (2026-10-22):** FoldKit is a separate library entry point so adapters can reuse
the tab API's streams and effects without duplicating sync semantics; keeping the framework
as a devDependency preserves the framework-free engine boundary.

### Admissions

- Pull pages of one set are read from several `REPEATABLE READ` snapshots; §7.6 argues why the
  result still installs as the state at `nextCursor`.
- Presence departure is detected only when the server sees the HTTP stream end; behind a proxy
  that swallows client aborts this takes up to the client's `idleTimeout` plus the server
  heartbeat (about 45 s).
- The browser substrate depends on Web Locks, OPFS synchronous access handles and
  (for multi-tab service) `SharedWorker`; without them the client fails closed with
  `Unsupported` (INV-BRW-9).
- WebKit terminates a `SharedWorker` with the tab that created it; the router is rebuilt from
  its clients (§12.4).

## 2. Model and identity

### Overview

Every durable fact in the system is named by a small set of identifiers defined in
`src/core/Ids.ts` and `src/core/AccessScope.ts`. Positions are the server's single commit-ordered
counter; cursors are positions a client has installed; sequence numbers are the client's own
order of intents.

### Invariant digest

- INV-ID-1: A `Position` MUST be a non-negative integer of arbitrary size; on the wire it is a
  decimal string (`Schema.BigIntFromString`), never a JSON number, so values beyond 2^53 are
  exact.
- INV-ID-2: A `Seq` MUST be an integer ≥ 1; `seq < 1` on the wire is `MalformedEnvelope` for the
  whole push (§8).
- INV-ID-3: A `SeqFloor` MUST be an integer ≥ 0 and fit a JavaScript safe integer
  (`PullDone.expiredThrough` is built with `Number(...)`).
- INV-ID-4: A `ClientId` and a `MutationId` MUST be non-empty strings; a `MutationId` MUST be
  unique per client and is the idempotency key of a mutation on both sides.
- INV-ID-5: A `ClientId` is bound to exactly one principal, the one that first registered it;
  a request for it under another principal MUST fail `Unauthorized`.

### Details

| Name | Definition | Where it lives |
|---|---|---|
| `Position` | bigint ≥ 0, branded; decimal string on the wire | `Ids.ts`; `echo_sync.log.position`, `mutations.position`, `client_scopes.position` |
| `Cursor` | `Position \| null`; `null` = the client holds nothing | `echo_meta` key `sync_meta` (client), `echo_sync.clients.cursor` (server, last presented) |
| `ClientId` | non-empty string; the client generates `crypto.randomUUID()` once and stores it in `echo_meta` | `echo_sync.clients.client_id` |
| `MutationId` | non-empty string, chosen by the caller of `enqueue`, `crypto.randomUUID()` by `mutate` | `echo_outbox.mutation_id`, `echo_sync.mutations.mutation_id` |
| `Seq` | integer ≥ 1, SQLite `AUTOINCREMENT` in `echo_outbox`, never reused | `echo_outbox.seq`, `echo_sync.mutations.seq` |
| `SeqFloor` | integer ≥ 0, the highest expired `seq` of a client | `echo_sync.clients.expired_through` |
| `PrincipalId` | non-empty string; `*` is the wildcard grantee, refused as the actor of `createScope` and unable to relinquish the `*` grant; the engine does not otherwise reject `*` as an authenticated principal (a `principalFor` returning `*` is served) | `scope_grants.principal_id`, `clients.principal_id` |
| `AccessScopeId` | non-empty string | `access_scopes.id`, every synced row's `access_scope_id` |
| `TabId` | non-empty branded string; presence entry key with the client id | wire only |

**Expiry floors.** Each client has two floors in `echo_sync.clients`: `expired_through` (a
`SeqFloor`, the highest `seq` whose ledger row's outcome was reclaimed) and `expired_position`
(the position of the highest reclaimed row). Both start at 0, only `expireLedger` raises them,
and they are never lowered or deleted (§11).

**The principal assertion.** `push`, `pull`, `pokes`, `room` and `track` carry an optional
`principal`. The server MUST compare it with the authenticated principal and fail `Unauthorized`
on mismatch; it MUST NOT use it as the actor. Its absence (pre-assertion clients) is served.

### Open Questions

None.

## 3. Tables and codecs

### Overview

A `SyncTable` (`src/core/SyncTable.ts`) is declared once from an Effect `Schema.Struct` and
yields the wire codec, the Postgres DDL, the SQLite DDL, the manifest and the two hashes. Every
synced table has a text primary key `id` and a non-null text `accessScopeId`. The engine's own
synced table is `scope_grants` (§4).

### Invariant digest

- INV-TBL-1: Every column kind MUST map to exactly one Postgres type, one SQLite type and one
  wire form (table below); a field that cannot be mapped unambiguously MUST be refused at
  declaration, never guessed.
- INV-TBL-2: Text MUST compare by Unicode code point in every store: Postgres `COLLATE "C"`,
  SQLite `BINARY`, and the in-memory reference comparator (`Store.compareText`). Index order is
  one definition across dialects: declared key columns ascending with NULL first, then `id`.
- INV-TBL-3: `dataHash` MUST change exactly when the storage shape changes (table name, column
  set, column kind, nullability) and MUST NOT change for indexes, unique sets, documentation
  annotations, field order, refinements, brands or nested JSON schema changes. `indexHash` covers
  the index set and the unique sets, so a unique-set change does not rebuild the replica.
- INV-TBL-4: A stored or pulled row MUST be the canonical wire re-encoding of the decoded value:
  excess keys dropped, values normalized; every store round-trips every leaf type at its
  boundaries, `null`, and the empty string unchanged.
- INV-TBL-5: Names MUST satisfy: table `/^[a-z][a-z0-9_]*$/` and ≤ 48 characters; column snake
  case ≤ 63 characters and round-tripping camelCase ↔ snake_case; index name such that
  `${table}__${index}` ≤ 63 characters; no index on a JSON column; no two registry tables with
  one sync name (`scope_grants` included).

### Details

| kind | accepted field (classified on its encoded side) | wire | Postgres | SQLite |
|---|---|---|---|---|
| text | `String` (any checks/brands), string literals, template literals | string | `text COLLATE "C"` | `TEXT` (BINARY) |
| integer | `Number` with `isInt`, integer literals | number | `bigint` | `INTEGER` |
| double | `Number` with `isFinite` | number | `double precision` | `REAL` |
| boolean | `Boolean`, boolean literals | boolean | `boolean` | `INTEGER CHECK (c IN (0, 1))` |
| timestamp | `Schema.Date`, `Schema.DateTimeUtc` | epoch ms number | `timestamptz` | `INTEGER` (epoch ms) |
| json | `Struct`, `Array`, `Tuple`, `Record`, `Suspend`, a `Union` with a non-literal member or mixing literal kinds (a `Union` of same-kind literals is that kind) | JSON value | `jsonb` | `TEXT` (JSON) |

`NullOr(x)` makes a column nullable. Rejected at declaration: bare `Schema.Number`, `BigInt`,
`Symbol`, `Undefined`/optional keys at the top level, `Unknown`/`Any`, `Enum`, other declarations
(`Uint8Array`, …), an encoded-side default under `NullOr`, a non-integer number literal, a field
that is only `Null`, a nullable `id` or `accessScopeId`, an index that is empty, names `id`, or
repeats a field. Optional keys inside a JSON column are allowed. Timestamps are stored as epoch
milliseconds and MUST lie in `[−210 866 803 200 000, 8 640 000 000 000 000]` (the `timestamptz`
lower bound to the JS `Date` upper bound). An integer column crosses the wire as a JS `number`
(`Number(bigint)`), so a value beyond 2^53 written by a script is not exact on the wire.

**Hashes.** `dataHash` and `indexHash` are FNV-1a 64 over the canonical JSON of the manifest's
data part (name, sorted columns with kind and nullability) and index part (indexes, unique
sets). The
registry's combined `dataHash` is the client's `clientVersion.dataHash`; a change of it rebuilds
the replica (§9.6).

**Indexes.** `CREATE INDEX "${table}__${index}" ON "${table}" (k1 ASC NULLS FIRST, …, id ASC)` in
Postgres; the same without `NULLS FIRST` in SQLite, where NULL already sorts first. Declared
`unique` sets become table-level `UNIQUE` constraints in both dialects. The startup verifier
(§13) checks the live catalog against this.

**The comparator.** `Store.planOrder` and `Store.planMatches` are the reference semantics every
`RowStore` must match; `Conformance.rowStore` checks `range` against them with seeded random
plans, and `scan` orders by `id` code point.

### Open Questions

None.

## 4. Access scopes and grants

### Overview

Every row belongs to one access scope. A principal reads a row when it holds any grant in the
row's scope; it writes when its strongest grant (own or `*`) in that scope is at least `writer`
(or `owner` for `scope_grants` rows). Grants are rows of the synced table `scope_grants`, so they
reach clients through pull like any other row, and a mutation may change them.

### Invariant digest

- INV-ACL-1: Roles are the closed set `reader < writer < owner`. The effective role of a
  principal in a scope is the strongest of its own grant and the `*` grant.
- INV-ACL-2: A write MUST be authorized against the stored row's scope (the row the database
  holds, not the caller's claim): insert needs the required role in the new row's scope; patch
  needs it in the stored row's scope and, when the patch moves the row, in the target scope as
  well; delete needs it in the stored row's scope.
- INV-ACL-3: The required role is `owner` for `scope_grants` and `writer` for every other table.
- INV-ACL-4: A principal MAY delete its own grant (`principal_id` = actor) without `owner`; the
  actor `*` MUST NOT. After relinquishing, a write in that scope is `ScopeForbidden`.

  **Rationale (2026-10-23):** Self-revocation was allowed so members can leave a scope without
  being its owner; this exception grants no right to reinsert the grant after leaving.

- INV-ACL-5: Two grants for one `(scope, principal)` MUST be unrepresentable: `scope_grants.id`
  MUST equal `grantId(scope, principal)` = `${codePointLength(scope)}:${scope}:${principal}`
  (Postgres `CHECK`, migration 002; requires a UTF-8 database), and a patch MUST NOT change the
  pair.
- INV-ACL-6: `createScope(id)` MUST write exactly the actor's `owner` grant on a fresh id, MUST
  fail `ScopeForbidden{owner}` with nothing written when the id exists (`access_scopes` row,
  claimed with `INSERT … ON CONFLICT DO NOTHING`), and MUST refuse the actor `*` before any
  claim. A scope whose grants were all revoked is never fresh again.
- INV-ACL-7: A query under `readableBy` MUST return the first `n` rows the actor can read in
  index order (readability pushed under the limit as a `scope_grants` semi-join on
  `(access_scope_id, principal_id)`), not the readable rows among the first `n`.

  **Rationale (2026-10-21):** Applying the limit after an in-memory readability filter read
  unbounded rows; pushing readability and the limit into every row store bounds the query
  while retaining `Store.planResult` as the shared conformance reference.

### Details

Tables (`src/server/postgres/migrations/001`): `echo_sync.access_scopes(id PK)`,
`echo_sync.scope_grants(id PK, access_scope_id FK → access_scopes ON DELETE CASCADE,
principal_id, role CHECK IN (reader, writer, owner), UNIQUE (access_scope_id, principal_id))`.
The wire table is `scope_grants` with fields `id`, `accessScopeId`, `principalId`, `role`,
`grantedAt` (a timestamp; `createScope` stamps it with the handler's `ctx.now`, which on the client
is the outbox entry's `createdAt` and on the server is the server clock at execution, so the
optimistic value is replaced by the server's at install).

A grant's `accessScopeId` is the scope it grants and also the scope the grant row lives in, so a
grantee pulls the grant row itself. Revoking a grant removes the row; the next pull emits
`scopePurged` for the scope (§7.3), and a presence stream in the room ends with `Forbidden`
(§10.3).

`Store.make` (`src/core/Store.ts`) is the only write path for handlers on both sides; it
captures undo images (`UndoImage`) for every write so a failed handler, and later a rebase,
restore the exact prior rows. A domain error declared by the mutation becomes a `DomainError`
rejection with the encoded payload; an undeclared one is a defect.

### Open Questions

- OQ-ACL-1: `*` is refused only as the actor of `createScope` and as the subject of a
  self-revocation. Nothing in `assertPrincipal`, `registerClient` or the RPC layer refuses `*`
  as the authenticated principal, so an app whose `principalFor` returns `*` is served with every
  row readable. The retired design text called `*` "never an actor"; whether the engine should
  refuse it at the request boundary is open.

## 5. Mutations and the ledger

### Overview

A mutation is a named, schema-typed intent (`Mutation.make` in `src/core/Mutation.ts`) with one
handler that runs against a `Store` on the client (optimistically) and on the server
(authoritatively). The server records every processed mutation once in the ledger
`echo_sync.mutations`, keyed by `(client_id, mutation_id)`, with its outcome and the position it
was recorded at. The ledger is what makes a push idempotent and what a pull acks from.

### Invariant digest

- INV-MUT-1: `(clientId, mutationId)` is the idempotency key. A mutation MUST be executed at
  most once on the server: a known id MUST be answered from the ledger without running the body,
  including under concurrent submission.
- INV-MUT-2: Outcomes are the closed set `Applied | Rejected{error}` with `error` one of
  `UnknownMutation | ArgsDecodeFailed | RowNotFound | RowAlreadyExists | ScopeForbidden |
  InvalidRow | DomainError | ConstraintViolated | ClientExpired`. Every outcome carries a
  `Position`.
- INV-MUT-3: Classification is over the whole cause of a handler run: a defect anywhere is a
  defect (the request dies; nothing is ledgered); otherwise a `StoreUnavailable` or a `SqlError`
  other than an integrity violation (INV-MUT-6) anywhere is transient (`MutationTransient`; nothing is ledgered); otherwise the first domain
  failure in cause order is the rejection, and every domain failure in the cause MUST encode or
  the run dies. An error the mutation did not declare is a defect, never a `DomainError`.
- INV-MUT-4: A rejected mutation MUST leave none of its writes behind (body in a savepoint) and
  MUST be ledgered with its outcome and a position.
- INV-MUT-5: Retry on conflict: an attempt whose cause holds no defect and holds a conflict
  anywhere (SQLSTATE `40001`, a deadlock, a unique violation, or a `StoreUnavailable` prefixed
  `SerializationError:`/`DeadlockError:`, or `ConstraintViolated{unique}`) is retried under
  `Schedule.exponential("10 millis")` up to 4 times (5 attempts). The body MUST NOT be rerun once
  a permanent unique violation is being recorded.
- INV-MUT-6: A unique violation that survives the retries is a permanent
  `Rejected{ConstraintViolated{unique, constraint}}`, recorded in a fresh transaction with the
  same identity check and the same conflict retry but with the body replaced by the rejection;
  any other integrity violation (SQLSTATE `23xxx` except `23505`: `CHECK`, `NOT NULL`, foreign
  key, exclusion) is `ConstraintViolated{other, constraint: null}` at once, without retries.

  **Rationale (2026-10-22):** Treating every SQL error as transient made permanent constraint
  failures retry forever and block all later queued mutations; Store classifies them for
  ledgered rejection and retained notices instead, preserving whole-cause precedence.
  Unique violations retain bounded retries because they can be races; other integrity
  violations reject immediately.

- INV-MUT-7: `seq < 1` is `MalformedEnvelope` for the whole push: checked for every envelope
  before any body runs; nothing executed, nothing ledgered, not answered as `ClientExpired`.
- INV-MUT-8: The seeded random available to a handler is a function of `(clientId, mutationId)`
  only, so a rebase replays identically; on the client `ctx.now` is the outbox entry's
  `createdAt` (stable across rebases); on the server it is `DateTime.now` at execution, since
  `createdAt` is not on the wire.
- INV-MUT-9: A registry MUST refuse two mutations (or two queries) with one name at
  construction.
- INV-MUT-10: A client id registered to one principal MUST be refused to another
  (`ClientForbidden` → `Unauthorized`).

  **Rationale (2026-10-22):** Permanent first-registration binding prevents a request
  authenticated as another principal from driving an existing replica; every sync and
  presence request checks it, with the replica-principal assertion optional on the wire for
  compatibility with earlier peers (§7.1).

### Details

**Ledger row** (`migrations/001`, `003`): `mutations(client_id, mutation_id) PK, seq bigint,
position bigint UNIQUE, outcome jsonb NULL, received_at, acked_at NULL`, with indexes on
`(client_id, position)` and the expiry scan. `outcome IS NULL` means the row's outcome was
reclaimed by `expireLedger` (§11) while its identity was kept.

**`runMutation` (`src/server/postgres/ledger.ts`)**, one transaction per mutation, in order:

1. `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`. `runMutation` MUST own its transaction;
   calling it inside an ambient `withTransaction` dies.
2. `lockClient`: `INSERT INTO clients … ON CONFLICT DO NOTHING`, then
   `SELECT principal_id, expired_through, expired_position … FOR UPDATE`. A different
   `principal_id` is `ClientForbidden`.
3. `SELECT outcome, position FROM mutations WHERE (client_id, mutation_id)`:
   - row with `outcome IS NULL` → `Rejected{ClientExpired}` at the row's own position, whatever
     `seq` the attempt carries;
   - row with an outcome → replay that outcome and position;
   - no row and `seq ≤ expired_through` → `Rejected{ClientExpired}` at `expired_position`, body
     not run;
   - otherwise run the body in a savepoint (`sql.withTransaction` nested).
4. `INSERT INTO mutations (…, position, outcome) VALUES (…, echo_sync.next_position(), …)
   RETURNING position`. The ledger position is allocated after the body's log entries, so a
   mutation's ledger position is greater than the positions of the log entries it caused.
5. Commit. The advisory lock taken by `next_position()` is released here (§6).

**Batches** (`runBatch`): every envelope is checked (step of INV-MUT-7) before any runs; then one
transaction per mutation in order; the first `MutationTransient` stops the batch and the
response is the processed prefix. A defect dies and the push ends as a server defect (§8).

**Statements per pushed mutation (happy path, no conflict, k row writes):** `BEGIN`, `SET
TRANSACTION`, `INSERT clients`, `SELECT clients FOR UPDATE`, `SELECT mutations`, `SAVEPOINT`, the handler's
statements (one `SELECT` on `scope_grants` per authorization, one read and one write per row;
each write fires the capture trigger, which runs `next_position()` and inserts one log row, two
for a scope or id move), `RELEASE SAVEPOINT` (`ROLLBACK TO SAVEPOINT` on a rejected body),
`INSERT mutations … RETURNING`, `COMMIT`. Per push request, in addition: the principal lookup and
one catalog query (`resolveTables`).

### Open Questions

- OQ-MUT-1: `runBatch` returns the processed prefix on the first `MutationTransient`. The
  client pushes the remainder on its next cycle after backoff; a persistently transient mutation
  (for example one whose handler always hits `StoreUnavailable`) blocks every later entry of
  that client indefinitely. The code has no bound on this; whether a bound is wanted is open.
- OQ-MUT-2: `createdAt` is not on the wire, so a handler's `ctx.now` is the outbox entry's
  `createdAt` on the client and the server clock on the server (INV-MUT-8). Any value derived
  from it (`scope_grants.grantedAt`, an app's own timestamps) differs between the optimistic
  write and the authoritative row until install. The retired design text implied the two agree;
  whether `createdAt` should travel with the envelope is open.

## 6. Positions and the ordering invariant

### Overview

One sequence, `echo_sync.position_seq`, numbers every log entry and every ledger row. The
numbers are not what orders them; the advisory lock inside `next_position()` is.

### Invariant digest

- INV-POS-1: `next_position()` MUST take `pg_advisory_xact_lock(7305797848413072995)` before
  `nextval`. The lock is transaction-scoped, so it is held until the allocating transaction
  commits or rolls back. Consequently positions MUST be allocated and committed in one order:
  a reader that sees position `p` committed sees every position `< p` committed.
- INV-POS-2: Every write to a captured table MUST produce a log entry: the capture trigger is
  `AFTER ROW INSERT|UPDATE|DELETE`, and `TRUNCATE` is refused by a `BEFORE STATEMENT` trigger.
  Writes from outside any mutation (scripts, cascades) are captured like any other.
- INV-POS-3: A log entry carries identity only (`table_name, row_id, access_scope_id, op`, plus
  `txid` and `committed_at` defaults that GC keys on), never the payload. A change of `id` or `access_scope_id` is logged as `delete` of the old identity
  and `upsert` of the new.
- INV-POS-4: `next_position()` MUST `pg_notify('echo_sync', '')` on every allocation, so a
  committed change produces a poke after commit (§10.1).
- INV-POS-5: Gaps in positions are allowed (rolled-back allocations); position `0` is the empty
  cursor's meaning only (`client_scopes` at a null cursor does not exist) and no row is at 0.

### Details

The consequence of INV-POS-1 is a throughput ceiling: at most one synced-write transaction is
between its first `next_position()` call and its commit at any time, across the whole database.
Every mutation that writes a captured row, and every script write, serializes on that lock.
Read-only transactions (pull, presence, GC's reads) do not take it. Log GC and ledger expiry do
not allocate positions.

Head, as pull computes it (§7.2), is `GREATEST(gc_horizon, max(log.position),
max(mutations.position), max(clients.expired_position))` in a `REPEATABLE READ` snapshot. Because
of INV-POS-1 that snapshot cannot contain position `p` without containing every committed
position below `p`, so "everything ≤ head" is a prefix of commit order.

### Open Questions

None.

## 7. Pull

### Overview

A pull delivers to one client the changes between the cursor it presents and the server's head,
as a page set: zero or more `PullPage`s followed by one `PullDone`. Membership (which scopes the
client holds) is a diff of states, not a replay of grant events; rows of newly readable scopes
are walked from current state, rows of unreadable scopes are purged, and log entries cover the
rest. Payloads are always the current row.

### Invariant digest

- INV-PULL-1: The server MUST authenticate the principal from the request context, compare the
  wire `principal` assertion when present, and bind `clientId` on first sight (`registerClient`:
  `INSERT … ON CONFLICT (client_id) DO UPDATE SET last_seen_at = now() RETURNING principal_id`;
  a different principal is `Unauthorized`).
- INV-PULL-2: A page set MUST install as the authoritative state restricted to the readable
  scopes at `nextCursor` = the head of the `PullDone` snapshot (§7.6), when the client applies
  all pages and the done in one transaction (INV-CLI-2).
- INV-PULL-3: `ResnapshotRequired{gcHorizon}` MUST be returned when the base position (the
  cursor, or the resumed continuation's position) is below `engine_state.gc_horizon`;
  `{unknownCursor}` when a non-null cursor has no `client_scopes` row;
  `{staleContinuation}` when the continuation does not decode, names another `clientId`,
  `principalId` or `cursor`, or carries a `position` outside `[cursor, head]`.
- INV-PULL-4: Membership diff on every page: every held scope not readable now → `scopePurged`;
  every walking scope not readable now → `scopePurged` and dropped; every readable scope that is
  neither held nor walking → a new walk appended in code-point order of scope id.
- INV-PULL-5: Readability MUST be recomputed per page from `scope_grants` for the authenticated
  principal and `*`; nothing in the continuation grants read access (Q1).
- INV-PULL-6: At `PullDone`, the held set recorded under `nextCursor` MUST be the readable set
  of that snapshot, never the continuation's claim.
- INV-PULL-7: Order within a page set: all row walks (scopes in code-point order; within a scope,
  tables by name, rows by `id COLLATE "C"`, keyset-continued by `after = (table, rowId)`) complete
  before any log entry is read; log entries are read in position order, `> position` and in
  readable scopes, with `LIMIT budget + 1` to detect the end.
- INV-PULL-8: The page budget (`limit`, default `pageLimit` 500) counts upserts and
  tombstones only; `scopePurged` entries are free. A walk page never exceeds the remaining
  budget. `pageLimit ≥ 1` is an unvalidated precondition: with `0` no walk and no log read runs
  and every page is a `PullPage` at the same position, forever.
- INV-PULL-9: Payloads MUST be read from the current row through the table's codec, restricted to
  declared columns; a log entry whose row is gone, whose current `access_scope_id` is not
  readable, or whose table is no longer attached and registered (`resolveTables`), MUST be
  delivered as a tombstone, never as a payload.
- INV-PULL-10: Acks delivered by a page are the client's ledger rows with
  `position ∈ (ackFloor, nextPosition]` and `outcome IS NOT NULL`, where `ackFloor` is the
  resumed position, else the cursor, else −1. An expired row (outcome `NULL`) is never acked.
- INV-PULL-11: After the read snapshot, in this order and each idempotent: delete
  `client_scopes` rows with `position < cursor`; set `clients.cursor = cursor`; stamp
  `acked_at = now()` on ledger rows with `position ≤ cursor`, `acked_at IS NULL` and an outcome;
  on `PullDone`, insert `client_scopes(nextCursor, readable set) ON CONFLICT DO NOTHING`. A crash
  between any two leaves state a retry converges from.
- INV-PULL-12: Head is monotone within one database timeline (§6); expiring the newest ledger
  row MUST NOT move head back (it is included through `expired_position`).
- INV-PULL-13: A null cursor is a full snapshot: the client holds nothing, every readable scope is
  walked, and the log position starts at the head of the first page.

### Details

#### 7.1 Request and policy

`PullRequest{clientId, clientVersion?, principal?, cursor, continuation?}`. The server runs
`assertPrincipal` (INV-PULL-1), the app's `clientPolicy` (`ClientOutdated`), `registerClient`,
then decodes the continuation (failure → `staleContinuation`; identity mismatch →
`staleContinuation`).

#### 7.2 The snapshot

One `REPEATABLE READ READ ONLY` transaction per page. First statement: `gc_horizon`, `head`
(§6), `expired_through` of this client. Checks, in order: resumed `position ∈ [cursor, head]`
and ≥ 0 (else stale); `base = resumed.position ?? cursor`, `base < gc_horizon` → `gcHorizon`;
cursor non-null → `client_scopes` row at `(clientId, cursor)` must exist (else `unknownCursor`),
its `scope_ids` is the held set; a resumed continuation then overrides held (`scopes`) and
`walking`. Then readable = `SELECT DISTINCT access_scope_id FROM scope_grants WHERE principal_id
IN (principal, '*')`, and the table list from the catalog (`resolveTables`: registry ∩ captured,
sorted by name). `position = base ?? head`.

#### 7.3 Membership and walks

INV-PULL-4 then INV-PULL-7. `walkPage(tables, scope, after, budget)` reads, per table in name
order starting at `after.table`, `SELECT … WHERE access_scope_id = scope AND id > after.rowId
ORDER BY id COLLATE "C" LIMIT remaining + 1` (the extra row is the lookahead that decides
whether the table is exhausted; at most `remaining` are delivered). When a walk completes its scope moves from `walking`
to `scopes` (held). Only when `walking` is empty is the log read.

**Rationale (2026-10-21):** Newly granted scopes are walked before the log because a principal
gaining a scope mid-stream needs its current rows, not only changes committed after the grant.

#### 7.4 Cursor ahead of head

See Q2. No explicit `cursor ≤ head` check exists; the `client_scopes` lookup is what refuses a
cursor from a different database timeline.

#### 7.5 The continuation

`{clientId, principalId, cursor, position, scopes, walking: [{scopeId, after: {table, rowId} |
null}]}` as a JSON string, unsigned. The server trusts from it only what it re-validates
(identity, position bounds, readability of every scope); the rest (`scopes`, `walking.after`,
`position`) is progress the client may corrupt only to its own detriment.

#### 7.6 Why several snapshots still install as `server@nextCursor`

Each page is read in its own `REPEATABLE READ` snapshot `S_1, …, S_n`, with heads `h_1 ≤ … ≤
h_n` and `nextCursor = h_n`. Claim: after applying all pages in order on top of a replica equal
to `server@cursor` restricted to the readable scopes at `cursor`, the replica equals
`server@h_n` restricted to the readable scopes at `h_n`.

1. **Row walks are current-state reads.** A walked row of scope `s` is its state in the page's
   snapshot. Any later change to that row (position `q`, `q > position_i` where `position_i` is
   the log position the continuation carried when the walk ran) is in the log, and the log is read
   only after every walk completed, from `position` upward, so the change is delivered again as a
   log entry with the then-current payload. Row walks start at `position = base ?? head_1`, and
   every change to a walked scope committed after `S_i` has a position `> h_i ≥ position_i`
   (INV-POS-1), so none is skipped.
2. **Log entries carry current payloads.** A log entry at position `p` is delivered with the row
   as of the page's snapshot `S_j`, which is at least as new as `p`. If the row changed again after
   `S_j`, that change has a position `> h_j` and is delivered by a later page or the next set.
   Applying the newer payload earlier than its own log position is harmless: the final state of
   the row after the set is its state in the last snapshot that mentioned it.
3. **Tombstones are safe.** A log entry whose row is deleted or moved out of readability in
   `S_j` is a tombstone; the delete or move has a position `> p`, and the replica ends without the
   row, which is the state at `h_n` (a later re-insert has a position `> h_j` and is delivered
   later).
4. **Membership is a diff of states.** Each page diffs the held set against readability in its
   own snapshot; a grant revoked between pages purges on the next page, a grant added starts a
   walk on the next page, so at `PullDone` the held set is exactly readability in `S_n`, which
   is what INV-PULL-6 records.
5. **Head is a prefix.** By INV-POS-1, every position `≤ h_n` was committed before `S_n`, so no
   change `≤ h_n` is missing from the union of the pages, and the next set starts at `h_n`.
6. **Atomic install.** The client applies pages only at `PullDone`, in one transaction
   (INV-CLI-2), so no reader sees an intermediate union.

The argument needs the log to be complete for every write (INV-POS-2) and payloads to be
current (INV-PULL-9); it does not need the pages to share a snapshot. It also shows why a
page-at-a-time install is wrong: a walk page and a later log page may disagree about a row, and
only the last mention wins.

#### 7.7 Statements per pull page

`BEGIN`; `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`; state query
(`gc_horizon`, head, `expired_through`); `client_scopes` lookup (only when `cursor` is non-null);
readable grants; catalog (`resolveTables`); one `SELECT` per table per walk page touched; one log
`SELECT` (only when no walk is in progress and budget remains); one `SELECT` per table that has
upsert entries, for payloads; one ack `SELECT`; `COMMIT`; after the transaction, three writes when
`cursor` is non-null (`DELETE client_scopes`, `UPDATE clients`, `UPDATE mutations`) and one
`INSERT client_scopes` on `PullDone`. Plus, per request, the principal lookup, `clientPolicy` and
`registerClient`.

### Open Questions

- OQ-PULL-1 (Q2): there is no explicit `cursor ≤ head` fence; recovery after a database restore
  relies on the restored `client_scopes` table lacking the row. A row that survived a restore
  with `cursor > head` would regress the cursor silently. Whether an explicit
  `ResnapshotRequired{unknownCursor}` for `cursor > head` should be added is open; the
  `gc.test.ts` test "a cursor the server never completed a page set at is refused rather than
  trusted" covers the missing-row path only.
- OQ-PULL-3: `pageLimit` is not validated. A value of `0` makes every pull answer a `PullPage`
  at the same position forever (INV-PULL-8). Whether the server should refuse it at construction
  is open.

## 8. Push

### Overview

A push sends the client's pending outbox entries in `seq` order and receives the processed
prefix's outcomes with positions. Push never installs data; the pull after it acks.

### Invariant digest

- INV-PUSH-1: `PushRequest.mutations` MUST be non-empty and at most 100 entries; the client's
  batch is `min(pushBatchSize ?? 50, 100)` entries with `seq > pushedThrough`.
- INV-PUSH-2: The server MUST answer with the processed prefix in order; fewer results than
  mutations means a transient stop at the first missing one, never an error, and the client
  retries from there.
- INV-PUSH-3: A `SqlError`, failed or died, anywhere in authentication or the batch (lost
  connection, pool exhausted) MUST reach the client as a short or empty `results` list, never as
  a defect the client would halt on; any other defect, from `principalFor` or a handler, is a
  server defect and halts the client's push (INV-LOOP-3).
- INV-PUSH-4: Request-level refusals are `Unauthorized`, `MalformedEnvelope`, `ClientOutdated`;
  `MalformedEnvelope` executes and records nothing.
- INV-PUSH-5: Headers used by `principalFor` MUST be the real HTTP request headers, never
  per-message RPC headers a body could forge.

  **Rationale (2026-10-22):** The pinned RPC HTTP protocol concatenates body-supplied message
  headers after request headers, allowing NDJSON to override authentication headers; only
  `HttpServerRequest.headers` is trusted, and request policy must not depend on a raw pathname
  predicate that can miss the RPC route.

- INV-PUSH-6: After a response, `pushedThrough` advances to the highest answered `seq`; a result
  whose position is `≤` the installed cursor, or whose outcome is `Rejected{ClientExpired}`, is
  covered already (no pull will ack it) and is released at once through a rebase with no
  install; every other result waits for its ack in a pull.

### Details

`Engine.push` sends batches in a loop, each batch the first `batchSize` pending entries with
`seq > pushedThrough`, until no such entry remains or a batch is stopped (fewer results than
mutations); a stopped batch ends the push for this cycle and the loop backs off (§10). The
client validates the batch against `PushRequest` before sending (`PushIntegrityError
{invalidPayload}` halts: the outbox holds something the wire refuses). `syncHeaders` is
evaluated once per request; a failure there is `CredentialsUnavailable` (nothing sent).

### Open Questions

None.

## 9. Client engine: outbox, staging, install, rebase

### Overview

`src/client/sync/Engine.ts` owns one replica (`SqliteSyncStorage` over wa-sqlite in OPFS, or
`MemorySyncStorage` in tests) and the three operations on it: `mutate` (optimistic apply and
durable append), `push`, and `pull` (stage pages, install at `PullDone`). Every change to the
replay set goes through one function, `rebase`.

### Invariant digest

- INV-CLI-1: `replica = replay(pending, server@cursor restricted to readable scopes)` after
  every step, where `pending` is the unreleased outbox in `seq` order. This single equation
  rules out flicker, double optimistic application, torn page sets and stale rows (it is the
  simulation's main check).
- INV-CLI-2: A page set MUST be installed in one storage transaction at `PullDone`, never page
  by page; a set that started from a null cursor MUST replace the replica (`clearAll` first).
- INV-CLI-3: Rebase is undo-all, install, settle, replay-all: every pending entry's undo images
  are restored in reverse `seq` order; the base is installed; each pending entry is released if
  the base settles it (its ack is in the set, or `seq ≤ expiredThrough`), else its handler is
  re-run and its fresh undo images stored. A replay that is rejected locally keeps the entry
  queued with no optimistic effect (empty undo); only the server's verdict removes it.
- INV-CLI-4: `mutate` is one storage transaction: lookup by `mutationId` (a known id, pending or
  released, returns the original `seq` and runs nothing), `nextSeq`, run the handler with
  `createdAt = now`, store undo images, append. A `LocallyRejected` run saves nothing. The
  transaction commits before `enqueue` resolves.
- INV-CLI-5: `seq` is allocated by SQLite `AUTOINCREMENT` and never reused, including after a
  release, a prune, a reopen or a rolled-back allocation.
- INV-CLI-6: The outbox never holds an entry whose ledger position is `≤` the installed cursor
  (a push result at such a position, or with outcome `Rejected{ClientExpired}` whatever its
  position, is released at once, INV-PUSH-6).
- INV-CLI-7: `pushedThrough` advances only on push responses, never at install: an entry below
  a released one may need to be re-sent so a push response's position releases it.
- INV-CLI-8: Released rows are kept as markers (`released_at`) for `defaultReleasedRetentionMillis`
  = 24 h, so an `enqueue` retried after its original was released adds no intent; the leader
  prunes them at open.
- INV-CLI-9: A `PullPage` is decoded under the client registry before it is staged: every upsert
  of a known table must decode through the table's wire codec; a row missing a declared column
  is `ServerOutdated{table}` (back off, keep prior staging and its continuation, never rebuild),
  any other failure is `MalformedChange{table}` (transient). Changes of unknown tables are
  skipped at install with a debug log.
- INV-CLI-10: `ResnapshotRequired` on a request that was not the initial one (non-null cursor
  or a continuation) clears staging, sets `meta.resnapshot`, and continues the loop from a null
  cursor; the replica and its cursor keep serving reads until the snapshot installs. On an
  initial request it is a pull failure.
- INV-CLI-11: A stored row that no longer decodes, or stored engine data that does not decode,
  is `CorruptStorage` (a defect naming the table and row id), never a bare schema error.
- INV-CLI-12: A registry whose `dataHash` or table DDL differs from the stored one rebuilds the
  replica in one transaction: replica tables dropped and recreated, cursor set to null, staging
  cleared, pending undo images emptied; the outbox, `pushedThrough` and notices survive. An
  `indexHash`-only change recreates indexes and keeps rows and cursor. A rebuild interrupted at
  any scheduler boundary leaves the old replica or the new one, never a mix.
- INV-CLI-13: An install MUST skip a change whose table is absent from the registry at Debug,
  without preventing the rest of the page set from installing or its cursor from advancing. A
  later `dataHash` rebuild for a registry that gains that table MUST reset the cursor so the
  replacement snapshot delivers its rows.

### Details

#### 9.1 Storage

SQLite tables, all `STRICT` except the outbox: `echo_meta(key, value)` with keys `sync_meta`
(one JSON value `{cursor, pushedThrough, resnapshot}`), `client_id`, `engine_schema_version`,
`replica_tables`, `replica_data_hash`, `replica_table_ddl`, `replica_index_hash`;
`echo_outbox(seq AUTOINCREMENT PK, client_id, mutation_id UNIQUE, name, args, created_at,
released_at, undo TEXT NOT NULL DEFAULT '[]')` (`undo` is engine schema step 2; a stored
`engine_schema_version` above the engine's own step count is a defect, not a rebuild), `echo_staged(ord, page)` (the
`PullPage` JSON as received), `echo_notices(id, notice)`. The replica tables are the registry's
SQLite DDL. The storage has one permit: every transaction, including a long install, excludes
every other; a `mutate` queued behind an install can be interrupted while it waits and its
handler never runs.

#### 9.2 Meta

`{cursor: Cursor, pushedThrough: number, resnapshot: boolean}`. The pull request's cursor is
`null` when `resnapshot` is set, else `cursor`; its continuation is the last staged page's.

#### 9.3 Install

`applyPageSet(pages, done)`: `install` = (`clearAll` if the set started from null) then
`applyChange` for every change of every page in order; `scopePurged` → `purgeScope`,
`tombstone` → delete, `upsert` → decode through `table.wire`, encode again, write the canonical
form. Acks of the whole set settle entries by `mutationId` (`Applied` → no notice; `Rejected` →
`Rejected` notice; `Rejected{ClientExpired}` → `OutcomeUnknown` notice); an unacked entry with
`seq ≤ done.expiredThrough` settles as `OutcomeUnknown`. Then `cursor = nextCursor`,
`resnapshot = false`, staging cleared, all in the same transaction.

#### 9.4 Cost model

See Q3. Formally, for one rebase with pending set `P` (before release), undo images `U = Σ_{m∈P}
|undo(m)|`, base changes `C`, released set `R ⊆ P`:

| step | store operations |
|---|---|
| undo all | `U` writes (restore each pre-image) |
| install | `C` changes; per upsert: 1 wire decode, 1 wire encode, 1 SQLite-row encode, 1 write; per tombstone: 1 write; per purge: 1 statement per table |
| settle | `|P|` map lookups; `|R|` releases; one notice insert per rejected/unknown |
| replay all | `Σ_{m∈P∖R} H(m)` handler operations + `|P∖R|` undo writes |

Push-covered rebase: `C = 0`, same undo and replay terms. Staging adds one decode per upsert of a
known table per page received (INV-CLI-9), so an upsert is decoded twice (staging, install) and
encoded twice (wire canonical form in `Engine.ts`, then the table's `sqlite.row` codec in
`SqliteRowStore.write`) between arrival and the replica. Every undo restore is also one
`sqlite.row` encode. Reading the pending outbox (`storage.pending`) decodes each entry's `undo`
JSON, so the read itself is `|P|` decodes.

#### 9.5 Notices

`Notice = Rejected{envelope, error} | OutcomeUnknown{envelope}`. The engine only appends;
`dismissNotices(ids)` is the only removal and ignores unknown ids. `decodeNotice` types a stored
notice by its mutation: a `DomainError` payload round-trips to the declared error type; a notice
of another mutation, or one its schemas cannot read, is `NoticeMismatch`, never a value or a
defect.

#### 9.6 Rebuild

INV-CLI-12. After a rebuild the next pull is from a null cursor (the installed set replays
pending on top and recaptures undo).

### Open Questions

- OQ-CLI-1: A full rebase per install (Q3) has no incremental path. Whether an install should
  skip undo/replay for entries whose rows the set does not touch is a design question; nothing in
  the code or tests constrains it beyond INV-CLI-1.

## 10. Sync loop, pokes and presence

### Overview

`SyncLoop` runs push-then-pull cycles, woken by pokes, mutations and `requestSync`, with
exponential backoff on transient failure. `followPokes` keeps one server stream open. Presence is
a leader-owned, server-fenced room membership with process-local state on the server.

### Invariant digest

- INV-LOOP-1: A cycle is push (if push is `Active`) then pull. Backoff after a cycle with a
  transient failure starts at `retryDelay` (1 s), doubles to `maxRetryDelay` (30 s) and resets on
  a clean cycle; a wake during backoff runs the next cycle at once. Wakes are coalesced (sliding
  queue of 1).
- INV-LOOP-2: `lastSyncedAt` is set only by a cycle in which push was `Active`, ran without a
  failure or a stopped batch, and whose pull installed a `PullDone`; it does not advance while
  push is `Halted`, even though pulls keep installing.
- INV-LOOP-3: Push halts (`push: Halted{message}`, in memory for this leader) on
  `MalformedEnvelope`, `PushIntegrityError{invalidPayload | serverDefect}`, or any defect in the
  push cause; pull keeps running; `requestSync` never clears a halt; `exportPending` stays
  available.
- INV-LOOP-4: `ClientOutdated` on push or pull is terminal for this leader (`lastError.kind:
  "outdated"`, phase `idle`, no retry, outbox and replica kept); every other failure is transient
  with its kind: `credentials` (`syncHeaders` raised, nothing sent), `unauthorized` (`Unauthorized`
  or HTTP 401/403 before RPC decoding), `unavailable` (`ServerUnavailable`), `network` (no usable
  RPC result), `server-outdated` (`ServerOutdated`), `server` (everything else: a stopped batch,
  a pull defect, `StoreUnavailable`, `ConstraintViolated`, `MalformedChange`,
  `ResnapshotRequired` on an initial request).

  **Rationale (2026-10-22):** HTTP 401/403 refusals before RPC decoding are `unauthorized`
  rather than network/protocol defects because the pinned HTTP RPC client ignores status and
  an empty middleware or proxy refusal otherwise looks like a malformed response; retrying
  preserves queued intent without halting push, and other statuses keep their classification.

- INV-LOOP-5: Classification is over the whole cause: for push, a defect beats a halting failure
  beats the first failure; for pull, a defect is transient `server`. When push and pull both fail
  transiently in one cycle, `lastError` is push's, unless pull's kind is `server-outdated`, which
  overrides.

  **Rationale (2026-10-23):** One typed `SyncFailure` union is classified once so status kind,
  halt reason and telemetry agree; separate walks of transport `reason` strings in the engine,
  loop and telemetry were rejected.

- INV-LOOP-6: `SyncStatus` strings carry no secret: every `lastError.message` is drawn from a
  closed set of library constants (one per kind, plus the stopped-batch text for kind `server`);
  the `Halted` message is library text.
- INV-LOOP-7: Pokes: every stream element that is a `Poke` wakes the loop; a `Heartbeat` never
  does. The stream is reconnected after any end (failure, defect, clean end, or no element for
  `idleTimeout` = 45 s) with backoff 1 s → 30 s, reset once a connection delivered a poke.
  Reconnecting does not wake the loop; the server's first poke on every stream, emitted right
  after `LISTEN` is in place, covers anything missed while disconnected.
- INV-LOOP-8: `syncHeaders` is evaluated once per outgoing request (push, pull, every stream
  (re)connect); its evaluation is not counted against `idleTimeout`.

  **Rationale (2026-10-21):** Credential refresh belongs to the app and runs through an Effect
  per request rather than frozen transport-construction headers; `Unauthorized` is retried
  with backoff, never fatal, so refreshed credentials can recover the connection.

- INV-LOOP-9: `SyncStatus = {push: Active | Halted{message}, lastError: {kind, message} | null,
  pendingMutations, lastSyncedAt, phase: idle | pushing | pulling | backoff, nextRetryAt}`;
  `status` and `changes` wait for the first real pending count (no seed 0 leaks).

  **Rationale (2026-10-22):** `LeaderClient` exports `SyncError`, `SyncErrorKind` and
  `SyncStatus` as runtime schemas, not only types, so an adapter need not declare its own copy
  of the union.

### Details

#### 10.1 Pokes on the server

One shared `LISTEN echo_sync` per process, reconnected with backoff `min(50 ms · 2^attempt, 5 s)`;
each `pokes` stream emits one `Poke` immediately after `LISTEN` is confirmed, then one per
`NOTIFY` (a `NOTIFY` is issued by every `next_position()` call and delivered at commit), and a
`Heartbeat` every `heartbeatInterval` (15 s). A `LISTEN` failure before the initial poke emits
nothing; a lost `LISTEN` connection ends the stream with `ServerUnavailable`. The server's
`heartbeatInterval` MUST stay below the clients' `idleTimeout`.

Liveness bound: a connection that died silently (a proxy that keeps the response open) is
noticed after `idleTimeout` (45 s) and reconnected after the backoff; the change it would have
announced is covered by the new stream's first poke.

**Rationale (2026-10-21):** A silently dead stream behind a proxy left clients waiting
forever; periodic heartbeats and an idle deadline were chosen to detect it without making
heartbeat traffic wake the sync loop.

#### 10.2 Presence on the server (`src/server/postgres/presence.ts`)

Process-local state keyed by `(principalId, clientId, tabId)` per room (a room is an
`AccessScopeId`). `room(join)` is a stream: the first element is always the join snapshot, then
a snapshot after every change and a `Heartbeat` every `heartbeatInterval`; a heartbeat never
displaces a change. The entry lives exactly while the stream runs (completion, consumer failure,
interruption and HTTP end each remove it). Revision fencing: a join with a revision lower than the
stored entry's is superseded (the late stream ends empty); a `track` applies only with a revision
above the stored one. Snapshot entries are sorted by `(principalId, clientId, tabId)`. A principal
without a grant in the room's scope gets `Forbidden{room}` and never appears; the watcher
re-checks every `(room, principal)` on each poke and ends the streams of a principal whose grant
is gone (with `Forbidden`), restarting after 1 s on failure. A revoke committed while a join is in
flight cannot leave an entry behind.

#### 10.3 Presence on the client

The tab keeps the latest `(state, revision)` per joined room; one revision counter per room for
the life of the tab client (closing and reopening never reuses a revision). The leader owns the
upstream stream (`Presence.ts`): `idleTimeout` 45 s, rejoin after backoff 1 s → 30 s with the
latest tracked state, a supervised sender latest-wins with the `idleTimeout` deadline per send.
`track` records the state synchronously, before any IPC, and is a no-op unless the tab's
`presence` stream for the room is open; a second open of one room on one tab is
`PresenceAlreadyOpen`. `presence` survives leader handover (rejoin on the new leader with the
latest state); only `Forbidden` ends it. Tab→leader send deadline `presenceSendTimeout` 45 s,
retry `presenceRetryDelay` 1 s doubling to 30 s.

Departure lag: behind a proxy that does not forward the client's abort, a partner sees the
departure only after the stream's idle timeout and heartbeat window, about 45 s.

**Rationale (2026-10-23):** Departure lag was accepted rather than adding an explicit leave
RPC: a direct server abort removed the entry in milliseconds, but a development reverse proxy
swallowed the abort and left removal to the heartbeat/idle bound.

### Open Questions

- OQ-LOOP-1: The halt is in memory per leader (INV-LOOP-3); a new leader (reload) retries the
  same batch and halts again. Whether a halt should be durable is open; the code and the retired
  design agree on in-memory.

## 11. Retention and GC floors

### Overview

Two independent jobs, both server-exported and app-scheduled: `collectGarbage(retention)`
deletes old log entries and raises `gc_horizon`; `expireLedger({retention})` reclaims the
outcomes of old acked ledger rows and raises each client's floors. Identities are kept forever.

### Invariant digest

- INV-GC-1: `collectGarbage` deletes every log entry with `position ≤ max(position WHERE
  committed_at < now − retention)`, a position prefix (so the horizon is gap-free even if
  `committed_at` were not monotone in position), and sets `engine_state.gc_horizon` to that
  position, in one transaction. It MUST NOT delete ledger rows or `client_scopes` rows.
- INV-GC-2: A client whose base position is below `gc_horizon` is told `gcHorizon` and
  resnapshots; a client at head is unaffected. A page set interrupted mid-walk or mid-log whose
  position GC passed MUST resnapshot instead of resuming.
- INV-GC-3: `expireLedger` retention MUST be ≥ 48 h (`LedgerRetentionTooShort` before any SQL);
  default 7 days. Only rows with `acked_at ≤ now − retention` and an outcome are eligible; a row
  with `acked_at IS NULL` is never expired, whatever its age (rows from before migration 003 are
  unstamped until a pull acks them).
- INV-GC-4: Expiry clears `outcome` and `acked_at`, keeps the identity, and raises
  `clients.expired_through` to the highest expired `seq` and `expired_position` to the highest
  expired position, forward only. The expired set MUST be a `seq` prefix of the client's rows:
  if a row with an outcome survives at `seq ≤` the new floor the job dies and rolls back.
- INV-GC-5: Floors are permanent: no job deletes a client's floor or identities.
- INV-GC-6: Expiring the newest ledger row MUST NOT move head back (INV-PULL-12).

### Details

Why 48 h: twice the client's released-row window (24 h) with margin for a suspended tab's
clock; the chain that makes a late duplicate safe is: a duplicate `enqueue` inside 24 h is
answered by the released-row marker; a duplicate after that gets a fresh `seq`, and the server
answers it from the kept identity (`ClientExpired` at its own position) whatever its `seq`; an
unknown id at or below the floor is answered at `expired_position`.

**Rationale (2026-10-21):** Expiring outcomes bounds their growth without forgetting mutation
identities: deleting identities was rejected because a tab can retry `LeaderLost` with the
same id after suspension past the released-marker window, re-enqueue at a fresh seq above the
floor and double-apply. Ack-based retention, permanent floors and identity checks preserve
that late-retry safety independently of log GC (§5, §9).

### Open Questions

None.

## 12. Browser substrate

### Overview

`src/client/worker` makes one browser profile's replica a single-writer database: one leader
worker owns the SQLite file; tabs talk to it through a router `SharedWorker` that brokers
`MessagePort`s; Web Locks decide who leads and detect loss; a coordinated reset deletes the
replica.

### Invariant digest

- INV-BRW-1: Identity: `appId` and `profile` MUST match `^(?!\.+$)[A-Za-z0-9._-]+$`, each ≤ 255
  UTF-8 bytes, combined database name ≤ 502; otherwise `Unsupported`. Names derived:
  database `echo-sync/<appId>/<profile>.db`, locks `echo:<appId>:<profile>:leader`,
  `…:leader:<leaderId>` (epoch), `…:tab:<tabId>`, `…:router`, reset channel `…:reset`, tombstone
  `echo-sync/<appId>/~reset-<sha256hex(profile)>`.

  **Rationale (2026-10-21):** WebKit's 255-byte OPFS segment limit requires bounding each
  identity segment, not only the combined path; otherwise accepted 256–488 byte profiles
  failed late with `LeaderFailed{UnknownError}` instead of `Unsupported`.

- INV-BRW-2: At most one leader per identity holds the database: election is the leader Web
  Lock (`tryHoldLock` then `holdLock`; `Waiting` is reported only when the first try fails,
  `Elected` once the database is open and the leader serves, `Failed` on any startup failure);
  the leader then takes its epoch lock, completes any interrupted reset (tombstone), and opens
  the pool.
- INV-BRW-3: Leader loss is detected only through the epoch lock's release, never by waiting on
  a dead port. The router accepts a leader announcement only while the epoch lock is held.
- INV-BRW-4: Handover: in-flight requests fail `LeaderUnavailable{LeaderLost}`; requests issued
  while no leader is connected wait for the next one; `mutate` fixes its `mutationId` before the
  first attempt and retries `LeaderLost` with it (so a mutation committed on the lost leader
  lands on INV-CLI-4: one outbox row); `subscribe`, `notices`, `syncStatus` and `presence`
  resubscribe on a defect-free all-`LeaderLost` cause and emit nothing in between;
  `subscribeRaw` ends. A cause with a defect beside `LeaderLost` is not a handover and passes
  through.

  **Rationale (2026-10-22):** Cause-preserving recovery was chosen because `Stream.catchIf`
  re-fails nonmatching errors bare and could drop a defect beside `LeaderLost`; resubscription
  uses `Stream.catchCauseIf` and RPC error mapping uses `Cause.map` instead.

- INV-BRW-5: The router is rebuildable from its clients: a router announces itself
  (`RouterStarted`) on its first `Hello`, every tab re-links, and a stale leader announcement is
  ignored (INV-BRW-3).

  **Rationale (2026-10-21):** WebKit terminates the creating tab's SharedWorker even with
  other tabs connected; rebuilding routes from clients prevents the relaunched, empty router
  from leaving every tab unserved.

- INV-BRW-6: Registered queries run in one read transaction with `PRAGMA query_only`; every
  emission is one snapshot; a query re-emits after a write to a table its latest run read and
  not after one it stopped reading; raw `exec`/`subscribeRaw` are refused without
  `allowRawExec` before any SQL runs.
- INV-BRW-7: Reset (`resetReplica`): broadcast `ResetSignal` on the reset channel, every client
  of the identity enters the terminal `Reset` state (every request and stream fails
  `LeaderUnavailable{Reset}`, `status.reset = true`), then `Reset`; the reset worker waits at most
  `closeTimeout` (default 5 s, MUST be finite else `ResetFailed`) for the leader lock
  (`ReplicaInUse` on timeout); with the lock: a tombstone present → done; `discardPending` false
  and unreleased entries → `PendingWrites{count}`; else write the tombstone, remove the
  directory, remove the tombstone.

  **Rationale (2026-10-22):** Engine-owned coordinated close replaces requiring apps to close
  tab clients first: otherwise any live leader made reset fail `ReplicaInUse`, with nothing
  closing other tabs; a finite lock deadline still fails closed when a client ignores the signal.

  **Rationale (2026-10-22):** Coordination is for deletion only; an engine `closeAll` for
  sign-out that keeps the replica was rejected, so the app owns cross-tab scope closure when
  retaining readable data.

- INV-BRW-8: A leader that finds the tombstone completes the removal before opening the
  database (durability across a crash mid-reset).

  **Rationale (2026-10-22):** Recursive removal can crash after leaving a partial replica;
  recording deletion under the leader lock before removal lets the next leader finish it
  before opening, and the `~` tombstone prefix cannot collide with a valid profile.

- INV-BRW-9: Modes: `shared-worker` (every tab served through the router), `single-tab` (no
  `SharedWorker`; only the tab whose own worker leads is served, others fail
  `SecondaryTabWithoutRouter`), `unsupported` (a capability missing; everything fails
  `Unsupported`). Never a second database owner.
- INV-BRW-10: Closing the tab client never abandons a transferred port queued for acceptance or
  waiting for its epoch check; leader and router teardown close fresh transfers.

### Details

#### 12.1 State machine (tab client)

```diagram
┌────────────┐ spawn leader, connect router ┌──────────────┐  leader announced  ┌─────────────┐
│  starting  │─────────────────────────────▶│  no leader   │───────────────────▶│  connected  │
└────────────┘                              │ (requests    │◀───────────────────│ (requests   │
      │ capability missing                  │  wait)       │  epoch lock lost   │  served)    │
      ▼                                     └──────┬───────┘  (LeaderLost)      └──────┬──────┘
┌────────────┐                                     │ ResetSignal                       │ ResetSignal
│ unsupported│                                     ▼                                   ▼
└────────────┘                              ┌──────────────────────────────────────────────┐
                                            │ reset (terminal: LeaderUnavailable{Reset})   │
                                            └──────────────────────────────────────────────┘
```

A fourth, non-terminal state is **unavailable**: the route publishes `Unavailable{error}` and
every request fails at once with that error until a later leader announcement publishes
`Connected` again (only `reset` is terminal). It is entered from: `Waiting` received while no
router exists (`SecondaryTabWithoutRouter`; superseded if this tab's own worker wins and sends
`Elected`); `Failed` received before any connection existed (`LeaderFailed`; a `Failed` after a
connection is ignored); and failure to take the tab lock (`Unsupported`, message `tab lock: …`).

#### 12.2 Leader lifecycle

`leaderWorker(self, options)`: validate identity → election (INV-BRW-2) → epoch lock →
`completeInterruptedReset` → `awaitOpfsPoolFree` (100 × 20 ms) → open pool → `migrate` (engine
steps, registry rebuild check) → `pruneReleased` → serve `LeaderRpcs` and run the sync loop
(when a `syncUrl` is given) and the pokes follower. A leader without `syncUrl` is a local-only
replica: `syncStatus`, `requestSync`, `presence` and `track` fail `SyncDisabled`.

#### 12.3 `LeaderRpcs`

`exec`, `enqueue`, `subscribe`, `subscribeRaw`, `leaderInfo`, `syncStatus`, `requestSync`,
`presence`, `track`, `notices`, `dismissNotices`, `exportPending`; errors per §15. The tab API
(`tabApi`) adds id generation, `LeaderLost` retry, resubscription and codec work; a UI adapter
is a translation of the tab API and adds no sync semantics.

#### 12.4 Router

`router(self)`: one instance per identity; routes a tab's port to the current leader; keeps a
link per tab; broadcasts `RouterStarted` on its first `Hello` so a relaunched router (WebKit
kills the `SharedWorker` with its creating tab) is repopulated by the tabs.

### Open Questions

- OQ-BRW-1: The `awaitOpfsPoolFree` retry inside `openConnection` (after election and the epoch
  lock; 100 × 20 ms = 2 s) is a bound on how
  long a dead leader's access handles may linger; what happens after it elapses is a
  `LeaderFailed`, which tabs surface as `LeaderUnavailable{LeaderFailed}` and do not retry.
  Whether a longer bound or a retry is wanted is open.

## 13. Schema evolution and startup verification

### Overview

A `SyncTable` declaration is the single source of the storage shape on both sides: Postgres DDL,
SQLite DDL, the wire codec, `dataHash` and `indexHash` (§3). The server never alters a table on
its own. The app's migrations are generated from the committed manifest snapshot and reviewed;
at startup the server runs the engine's migrations, the app's migrations, attaches change
capture to every registry table, and compares the live catalog with the registry. On any
mismatch it refuses to serve. The client rebuilds its replica from a `dataHash` change (§9.6).

### Invariant digest

- INV-SCH-1: The generator (`Migrate.plan`) MUST emit DDL only for safe changes: create table,
  add a nullable column, add a `NOT NULL` column with an intent default (added with the default,
  which is then dropped), `NOT NULL` → nullable (`DROP NOT NULL`), index add, drop or change. A
  new `NOT NULL` column without an intent default is added nullable plus a `TODO(migration)` for
  the backfill and `SET NOT NULL`; nullable → `NOT NULL` on an existing column is a TODO only.
  Renames, column and table drops, lossy kind changes, unique-set changes and backfills MUST
  become commented `TODO(migration)` steps, never DDL, whatever the intent says.
- INV-SCH-2: A step that depends on a table or column whose rename, drop or kind change is a
  TODO MUST itself be a TODO; an intent naming something the diff does not contain MUST be
  refused (`MigrationPlanError`), never ignored.
- INV-SCH-3: Migrations are `NNN_name`, numbered after the highest existing one; the snapshot
  (`manifest.json`, `version: 1`) is the app tables' manifests sorted by name and carries a
  `history` of `(migration, dataHash)` appended at every generation; `scope_grants` is the
  engine's and is never planned.
- INV-SCH-4: `verify` MUST compare the live catalog with every registry table (`scope_grants`
  included) and fail with one `Drift` listing every mismatch; the server MUST NOT serve on
  `Drift` (the RPC layer is built only after `prepare` succeeds).
- INV-SCH-5: Exactly one relation per sync name may be captured: `attach` refuses a second
  schema (`TableNameConflict`), including names differing only in case, and concurrent attaches
  of one name from two schemas cannot both succeed; `verify` reports a double capture and a
  `search_path` that resolves the name to an uncaptured relation as `Drift`.
- INV-SCH-6: `prepare` MUST be safe to run concurrently from several instances, including first
  starts on an empty database: the schema bootstrap takes advisory lock `7305797848413072997`,
  attaches serialize on `7305797848413072996`, the migrators lock their history tables, `verify`
  only reads; the app migration runs exactly once.
- INV-SCH-7: `attach` MUST own its transaction (refused inside any ambient transaction, whatever
  its isolation level) and MUST install the capture trigger and the truncate guard together or not at
  all.
- INV-SCH-8: Extra live columns are tolerated, so a server whose registry predates an additive
  migration MUST serve push and pull against the expanded database. A server whose registry
  declares a missing added column MUST fail startup with `Drift` until that migration runs.

### Details

#### 13.1 Startup order

`Startup.prepare`: `migrate` (engine migrations `001_engine`, `002_grant_id_check`,
`003_ledger_acked_at` in `echo_sync.migrations`) → `migrateApp` (the app's record of
`NNN_name` → Effect, in `echo_sync.app_migrations`) → for every registry table except
`scope_grants` that exists (`to_regclass(quote_ident(name))`, i.e. resolved through
`search_path`): `attach`, with `TableNameConflict` left for `verify` → `verify`. A table the app
migrations never created is not attached; `verify` reports it as `table does not exist` in the
same `Drift`. `Startup.layer` mounts the RPC router only after `prepare` succeeds.

#### 13.2 What `verify` checks

Per registry table, from `pg_attribute`, `pg_index` (`indkey`, `indoption`, `indcollation`,
`indnkeyatts`) and `pg_trigger`, never DDL text:

- capture `AFTER ROW INSERT|UPDATE|DELETE` and truncate guard `BEFORE STATEMENT TRUNCATE` present,
  enabled, without `WHEN` or column list;
- every declared column present with `format_type` `text | bigint | double precision | boolean |
  timestamp with time zone | jsonb` by kind, text collation `"C"`, declared nullability;
- primary key exactly `(id)`;
- every declared index `table__index`: plain btree (no predicate, no expression), key columns in
  order, `ASC NULLS FIRST` on declared fields, `ASC` (`NULLS LAST`) on the trailing `id`, text
  keys collated `"C"`; only valid, ready, live indexes count and only their key columns
  (`indnkeyatts`) are compared;
- every declared unique set backed by a unique index on exactly those columns, `ASC NULLS LAST`,
  `NULLS DISTINCT`.

Extra live columns and indexes are allowed. The engine's own tables are not checked.

#### 13.3 Migration 002 and 003 preconditions

`002_grant_id_check` installs the `scope_grants.id = grantId(scope, principal)` `CHECK`
(INV-ACL-5); it fails loudly on an existing non-derived grant and on a non-UTF-8 database (the
check counts code points, which only holds in UTF-8). `003_ledger_acked_at` adds `acked_at`;
rows written before it are unstamped until a pull acks them (INV-GC-3).

#### 13.4 Client side

The leader's `migrate` (`SqliteSyncStorage`) creates the engine tables, migrates the outbox
forward (the `undo` column is added to a database created by the first outbox DDL, pending rows
kept), and compares `replica_data_hash` / `replica_table_ddl` / `replica_index_hash` with the
registry (INV-CLI-12).

## 14. Telemetry

### Overview

Observability is host-owned: the engine emits spans, metrics and logs into whatever tracer,
metric registry and loggers the host provides, through a sanitizing boundary
(`src/internal/Telemetry.ts`) that admits only closed vocabularies. Nothing the host can see
carries an id, a cursor, a payload, a header or an error message from the engine's internals.

**Rationale (2026-10-22):** Host-owned telemetry through pinned Effect primitives was chosen
instead of shipping exporters, OTLP or sampling; worker entries expose Effects the host can
run in its own telemetry context. Raw-cause logs can expose headers, URLs and bodies, so fixed
routine lines and a sanitizing tracer with `Exit.void` replace unguarded export.

### Invariant digest

- INV-TEL-1: Spans the host may see are created only by `Telemetry.observe` / `observeStream`,
  with a name from the closed set `Telemetry.Name` (§14.1). Every other span the engine opens
  (`Effect.fn`, SQL client spans, RPC spans) is suppressed inside `Telemetry.boundary` while
  keeping the exported trace and span ids of the nearest visible ancestor, so sampling and
  parentage propagate.
- INV-TEL-2: Attributes are from closed sets only: `echo.outcome`, `echo.result`, `echo.reason`
  with a value from `Telemetry.Outcome`; `echo.resnapshot` boolean; the count keys of §14.1 with
  a non-negative safe integer. Any other attribute, any span event and any link is dropped. No
  attribute carries a `clientId`, cursor, mutation argument, header or message.
- INV-TEL-3: A host-visible span always ends with `Exit.void` (Success), whatever the work's
  exit; the outcome is in `echo.outcome`. A throwing host tracer, metric or logger never changes
  the work's result, its saved intent, a retry deadline, a halt or an install (`guard`).
- INV-TEL-4: The outcome of a span is computed over the whole exit: `ok` on success, else
  `interrupted` if the cause has an interrupt, else `defect` if it has a die, else the
  classifier's verdict over the typed failures (for push: `halted` beats the first failure; the
  server's classifier is the constant `server`). A recorded `echo.result` on a successful exit
  (`"saved"`, `"duplicate"`, …) is the outcome.
- INV-TEL-5: Log lines the host receives are from the closed `LogMessage` set, with their cause
  removed; the cause is logged separately at Debug under the annotation `echo.diagnostics: true`
  (the only place a host can opt into seeing engine causes). A line whose cause is an interrupt
  is logged at Debug whatever its level.

### Details

#### 14.1 Vocabularies

Span names (`Telemetry.Name`): `echo.enqueue`, `echo.push`, `echo.pull`, `echo.install`,
`echo.rebase`, `echo.pokes`, `echo.leader.election`, `echo.leader.handover`,
`echo.router.handover`, `echo.server.startup`, `echo.server.push`, `echo.server.pull`,
`echo.server.pokes`. `echo.install` and `echo.rebase` are *timed*: they also record the timer
`<name>.duration` (boundaries 0.5, 1, 5, 10, 50, 100, 500, 1000 ms) tagged with the outcome.

Outcomes (`Telemetry.Outcome`): `ok, credentials, unauthorized, unavailable, network, server,
outdated, server-outdated, malformed, resnapshot, interrupted, defect, rejected, forbidden,
halted, stopped, saved, duplicate, elected, lost, idle-timeout, server-end, page, done`.

Count attribute keys: `echo.batch_size, echo.processed, echo.applied, echo.rejected, echo.pages,
echo.resnapshot_count, echo.pending, echo.replayed, echo.released, echo.notices_rejected,
echo.notices_unknown, echo.pokes, echo.heartbeats, echo.changes, echo.acks`.

Metrics: counters `<Name>.total`, `echo.leader.loss.total`, `echo.pokes.reconnect.total`, each
tagged `echo.outcome`; gauge `echo.outbox.pending`; histograms `echo.push.batch_size`
(boundaries 0, 1, 10, 25, 50, 100) and `echo.pull.pages` (1, 2, 5, 10, 25, 50, 100, 500).

Log messages (`Telemetry.LogMessage`): `echo-sync: diagnostics`, `echo-sync: unknown table
change skipped`, `echo-sync: push halted by an integrity error`, `echo-sync: push failed;
retrying`, `echo-sync: pull failed; retrying`, `echo-sync: pending count failed; restarting`,
`echo-sync: pokes stream ended; reconnecting`, `echo-sync: presence track failed; retrying`,
`echo leader candidate cannot serve`, `echo: connecting to leader failed`, `echo: accepting a
router connection failed`, `echo: watching this tab's leader epoch failed`, `echo: watching
leader epoch failed`, `echo router: watching leader epoch failed`, `echo router: watching tab
failed`, `echo router: leader announcement failed`, `presence revocation watch restarting`,
`echo leader completed the removal of a replica condemned by an interrupted reset`.

**Rationale (2026-10-24):** Names, outcomes, counters and log messages use closed compiler-checked
literal types instead of `string` parameters beside runtime allow-lists: misspelled outcomes
previously fell back to `server` and missing log lines disappeared silently.

#### 14.2 Boundary

`boundary(work)` replaces the fiber's `Tracer` with one that opens a host span only for a
`Telemetry.Name` (with the host's annotations and links stripped) and a `SafeSpan` for everything
else, and wraps every current logger so it drops any message outside `LogMessage` unless the
`echo.diagnostics` annotation is set. An external parent span provided by the host (an RPC
request span, for instance) is shielded behind an `engine boundary` `SafeSpan` so engine
annotations never land on it. `observeStream` applies the same to a stream, with `server-end` as
the outcome of a clean end.

### Open Questions

- OQ-TEL-1: The closed vocabularies are enforced at the boundary but not asserted against a
  committed list: adding an outcome or count key is a one-line change in `Telemetry.ts` that no
  test or registry notices. §0.2 makes it a design change; tooling to check it is future work.

## 15. Error catalog

### Overview

Every error the engine can produce is a tagged class in a closed union; the tag is part of the
contract. The table names where each is raised, whether a client retries it, and what the
client does. "Retry" is from the sync loop's point of view (§10); "design change" means adding a
member updates the rule and its dated rationale here (§0.2).

### Invariant digest

- INV-ERR-1: Error tags, and the literal unions they carry (`ResnapshotRequired.reason`,
  `PushIntegrityError.reason`, `ConstraintViolated.kind`, `LeaderUnavailable.reason`,
  `SyncErrorKind`), are closed sets. The transport classifies by tag, never by message text.
- INV-ERR-2: An error that wraps a cause keeps it (`CorruptStorage.cause`,
  `BrowserStorageError.cause`, `LockError`) and its message names what was attempted and where:
  `CorruptStorage` names the table and row id (INV-CLI-11), `LeaderLost` the leader id,
  `Unsupported` the offending identity segment, storage failures the driver's message. A message
  never carries a header value, cookie, URL or credential. Error messages are for the app's
  diagnostics, not for `SyncStatus` (INV-LOOP-6), which is the only engine string meant for a
  screen.

  **Rationale (2026-10-23):** Typed wrappers retain the original cause and location because
  a bare `SchemaError` did not identify the stored table or row, while
  `new Error(String(cause))` discarded the cause of browser storage failures.

### Details

#### 15.1 Wire errors (`src/core/Protocol.ts`), encodable, cross the RPC boundary

| tag | fields | raised by | client action |
|---|---|---|---|
| `ResnapshotRequired` | `reason: gcHorizon \| unknownCursor \| staleContinuation` | `pull` | non-initial request: clear staging, `resnapshot`, pull from null (INV-CLI-10); initial: transient pull failure |
| `Unauthorized` | `message` | `push`, `pull`, `pokes`, `room`, `track` (principal assertion, `ClientForbidden`, app `principalFor`); also HTTP 401/403 | transient, kind `unauthorized`; presence rejoins |
| `ClientOutdated` | `message` | app `clientPolicy` on `push`/`pull` | terminal for this leader, kind `outdated` |
| `MalformedEnvelope` | `clientId, mutationId, message` | `push` (`seq < 1`) | push halts |
| `ServerUnavailable` | `message` | `pull` (database failure, lost connection), `pokes` (lost `LISTEN`), `room` (grant lookup failed), declared on `track` | transient, kind `unavailable` |
| `Forbidden` | `room` | `room`, `track` (no grant in the room's scope) | presence subscription ends |

#### 15.2 Mutation outcomes (`Mutation.MutationRejection`), ledgered and acked

| tag | fields | meaning |
|---|---|---|
| `UnknownMutation` | `name` | no mutation of that name in the server registry |
| `ArgsDecodeFailed` | `name, message` | args do not decode with the mutation's args schema |
| `RowNotFound` | `table, id` | `patch`/`delete` of a missing row |
| `RowAlreadyExists` | `table, id` | `insert` of an existing id |
| `ScopeForbidden` | `scopeId, required, held` | actor lacks `required` in the row's scope (INV-ACL-2) or the scope id is taken (INV-ACL-6) |
| `InvalidRow` | `table, message` | a row, patch or index bound does not satisfy the table schema |
| `DomainError` | `mutation, error: Json` | a declared domain error, encoded with the mutation's `error` schema |
| `ConstraintViolated` | `kind: unique \| other, constraint, message` | an authoritative database constraint (INV-MUT-6) |
| `ClientExpired` | `mutationId` | answered at or below the client's expiry floor (§11); the effect, if any, is installed; outcome unknown |

#### 15.3 Core store and registry errors (`src/core/Errors.ts`, `Query.ts`, `Mutation.ts`)

| tag | fields | kind | notes |
|---|---|---|---|
| `StoreUnavailable` | `message` | typed, transient | never ledgered; stops the batch (`MutationTransient` on the server) |
| `CorruptStorage` | `table, id, cause` | defect | stored data no longer decodes with its schema (INV-CLI-11) |
| `UnknownQuery` | `name` | typed | tab `query` of an unregistered name |
| `QueryArgsDecodeFailed` | `name, message` | typed | |
| `NoticeMismatch` | `mutation, name, message` | typed | `decodeNotice` against the wrong mutation |
| `DuplicateMutation` / `DuplicateQuery` | `name` | construction `Error` | registry refuses two of one name (INV-MUT-9) |

#### 15.4 Server-only (`src/server`)

| tag | fields | where | effect |
|---|---|---|---|
| `MutationTransient` | `clientId, mutationId, message` | `runMutation` | batch stops; processed prefix returned |
| `ClientForbidden` | `clientId` | `lockClient` | surfaced as `Unauthorized` |
| `LedgerRetentionTooShort` | `retentionMillis, minimumMillis` | `expireLedger` | job fails before any SQL |
| `Drift` | `mismatches: [{table, problem}]` | `verify` | server does not serve |
| `TableNameConflict` | `table, existingSchema` | `attach` | left to `verify` by `prepare` |
| `MigrationPlanError` | `message` | `Migrate.plan` | generation refused |

#### 15.5 Client sync (`src/client/sync`)

| tag | fields | meaning | loop verdict |
|---|---|---|---|
| `CredentialsUnavailable` | `message` | `syncHeaders` raised; nothing sent | kind `credentials`, retry |
| `PushIntegrityError` | `reason: invalidPayload \| serverDefect, message` | batch not a valid request / server defect | push halts |
| `PullProtocolError` | `message` | pull ended in a defect | kind `server`, retry |
| `MalformedChange` | `table, message` | known-table change does not decode | kind `server`, outcome `malformed`, retry, staging cleared by the failed install |
| `ServerOutdated` | `table` | known-table payload lacks a declared column | kind `server-outdated`, retry, staging kept |
| `PushTransportError` | `reason: PushReason` | transport wrapper | per reason (§10) |
| `PullTransportError` | `reason: PullReason` | transport wrapper | per reason (§10) |
| `LocallyRejected` | `name, error: MutationRejection` | `mutate`'s handler rejected against the replica | surfaced to the tab; nothing saved |

`SyncErrorKind` = `credentials | unauthorized | unavailable | network | server | outdated |
server-outdated`.

#### 15.6 Browser substrate (`src/client/worker`)

| tag | fields | meaning |
|---|---|---|
| `SqlFailure` | `message` | a SQLite statement failed in the leader; transaction rolled back |
| `LeaderUnavailable` | `reason: LeaderLost \| LeaderFailed \| Unsupported \| SecondaryTabWithoutRouter \| Reset, message` | §12; `LeaderLost` is retried by `mutate` and resubscribed by streams, the others are not |
| `RawExecDisabled` | — | `exec`/`subscribeRaw` without `allowRawExec` |
| `SyncDisabled` | — | leader without `syncUrl` |
| `PresenceAlreadyOpen` | `room` | second `presence` open for one room in one tab |
| `ReplicaInUse` | — | reset: a leader still held the lock after `closeTimeout` |
| `PendingWrites` | `count` | reset without `discardPending` over unreleased entries |
| `ResetFailed` | `message` | reset could not run (unsupported browser, invalid identity, non-finite `closeTimeout`, OPFS error, a throwing `spawnLeader`) |
| `BrowserStorageError` | `operation, cause` | OPFS / Web Crypto call failed; message names the operation, never the path |
| `LockError` | `name, message` | Web Locks rejected for a reason other than our own cancellation |

### Open Questions

- OQ-ERR-1: `Unauthorized` is produced by four different conditions (app `principalFor`,
  principal assertion mismatch, `ClientForbidden`, HTTP 401/403 mapped by the transport) and
  carries only a `message`. An app cannot tell "sign in again" from "this replica belongs to
  another account" without reading text. Whether a `reason` literal is wanted is open.

## 16. Performance budget

### Overview

This chapter states each per-operation cost as the code has it today, in store statements and
codec passes, so later work can turn them into counters and numbers. Nothing here is a target; a
change that moves one of these costs is a design change (§0.2) because consumers size their
batches, page limits and retention against them.

### Invariant digest

- INV-PERF-1: One synced-write transaction commits at a time per database: every
  `next_position()` holds advisory lock `7305797848413072995` until its transaction ends
  (INV-POS-1), so the throughput ceiling of all mutations that write captured rows is one
  transaction per lock hold time, across every server instance.
- INV-PERF-2: Per pushed mutation (happy path, `k` row writes, `a` authorizations):
  `SET TRANSACTION`, `INSERT clients`, `SELECT clients FOR UPDATE`, `SELECT mutations`,
  `SAVEPOINT`, `a` × `SELECT scope_grants`, `k` × (1 read + 1 write + 1 trigger-side
  `next_position()` + 1 log `INSERT`, 2 log inserts for a scope or id move), `RELEASE SAVEPOINT`,
  `INSERT mutations … RETURNING`, `COMMIT`: 9 + a + 4k statements. Plus per push request: the
  principal lookup and `resolveTables`. A conflict retry repeats the whole transaction (up to 5
  attempts).
- INV-PERF-3: Per pull page (§7.7): state, `client_scopes`, grants, catalog, one `SELECT` per
  table per walk page, one log `SELECT`, one payload `SELECT` per table with log entries, one ack
  `SELECT`, then up to four writes outside the snapshot: 8 + t_walk + t_log statements with `t`
  the tables touched. Plus per request: principal lookup, `clientPolicy`, `registerClient`.
- INV-PERF-4: Per install (§9.4, Q3): one SQLite transaction holding the storage's single permit
  for `U` undo writes + `C` change writes + `Σ H(m)` replay operations + `|P∖R|` undo writes;
  every upsert of a known table is decoded twice (staging and install) and encoded twice (wire,
  then `sqlite.row`); one `clearAll` statement per replica table when the set started from null.
- INV-PERF-5: Per push batch on the client (`Engine.push` loops batches until the pending set
  is drained or a batch is stopped): one storage transaction reading `meta` and the whole
  pending outbox (`|P|` undo decodes), one RPC with at most `min(pushBatchSize ?? 50, 100)`
  entries, and, only when the response holds at least one result, one storage transaction
  advancing `pushedThrough` that also runs a full rebase with `C = 0` (INV-PERF-4 without the
  install term) when any result is covered (INV-PUSH-6). A response with zero results (stopped
  at the first entry) advances nothing.
- INV-PERF-6: Pokes are one `LISTEN` per server process shared by every subscriber, plus a
  heartbeat tick per `heartbeatInterval` per stream; a poke fans out to every stream in the
  process only (§10). A commit costs one `pg_notify` per position allocated (INV-POS-4), i.e.
  one per log row and one per ledger row, coalesced by Postgres within a transaction.

### Details

Known hot spots the code does not yet measure:

- The capture trigger calls `next_position()` once per log row, each taking the advisory lock
  (already held by the transaction after the first call, so re-entrant but still a function
  call and a `pg_notify`).
- `resolveTables` runs a catalog query per push request and per pull page.
- Payload reads during the log phase are one `SELECT … WHERE id = ANY(...)` per table, not one
  per entry.
- Install decodes staged pages from JSON (`echo_staged.page`) before decoding each upsert:
  the JSON double decode noted in Q3.
- The replay term `Σ H(m)` runs each pending handler against the SQLite replica with the same
  authorization reads a server run would do.

### Open Questions

- OQ-PERF-1: No counter today reports statements per operation; the only timed spans are
  `echo.install` and `echo.rebase`. Turning INV-PERF-2..4 into asserted counts is future work.

## 17. Invariant registry

Every invariant with the test that covers it. "Covered" names a test whose failure would be
caused by a violation of the invariant (a test name from `*.test.ts`, a Conformance check, a
Simulation invariant, or a `check` in `examples/browser-spike/scenarios.sh`). "Partial" means a
test covers one clause and the rest is untested. "Untested" means no test would fail if the
invariant were broken. IDs are never reused.

Rules for the harnesses that produce "covered" (verified against `src/core/testing/Conformance.ts`,
`src/client/sync/testing/MemorySyncStorage.test.ts` and `.oxlintrc.json`):

- A conformance check derives its expected values from the fixtures it writes, never from the
  subject's own reads. `MemorySyncStorage.test` runs `Conformance.syncStorage` against "a subject
  whose every write is a no-op" and expects every check to fail; that stub stays so the rule stays
  true.
- `SyncStorage.nextSeq` allocates: two calls in one transaction, before any append, return
  distinct seqs, and a rolled-back allocation is given back (Conformance "nextSeq allocates: …").
- `src/core/testing` holds only harnesses that need nothing from the client or the server at
  runtime (`Conformance`, `MemoryStore`, `SimApp`, `RefServer`); a lint rule forbids runtime
  imports of `src/client` or `src/server` from `src/core` (type-only imports allowed).
  Harnesses that drive the client engine (`Simulation`, `MemorySyncStorage`) live in
  `src/client/sync/testing`.
- The package compiles under every TypeScript strictness flag its own `tsconfig.json` enables
  (`strict`, `exactOptionalPropertyTypes`, `noUncheckedIndexedAccess`, `noImplicitReturns`,
  `noPropertyAccessFromIndexSignature`, `noImplicitOverride`, `noUncheckedSideEffectImports`, …),
  because consumers typecheck the shipped source under their own options.

  **Rationale (2026-10-23):** These flags are supported because consumers compile the shipped
  source; requiring consumers to relax `noImplicitReturns` or
  `noPropertyAccessFromIndexSignature` for a git dependency was rejected.

### Retired rules

- **Retired (2026-10-22):** Apps must close all tab clients before `resetReplica` (2026-10-21); engine-owned coordinated close supersedes this prerequisite (INV-BRW-7, §12).

### Coverage

| ID | status | covering test(s) |
|---|---|---|
| INV-ID-1 | covered | `Mutation.test` "the wire rejects seq < 1 and keeps Position exact beyond 2^53"; `SqliteSyncStorage.test` "staged pages and meta round-trip with bigint positions" |
| INV-ID-2 | covered | `Mutation.test` (same); `ledger.test` "seq < 1 is a malformed envelope: not executed, not recorded, not answered as clientExpired" |
| INV-ID-3 | untested | — |
| INV-ID-4 | covered | `Outbox.test` "an enqueue retried after its original was released returns the original seq and is not pending"; `ledger.test` "a resubmitted envelope (lost response) returns the recorded outcome and position without re-executing" |
| INV-ID-5 | covered | `ledger.test` "a client id registered to one principal is refused to another"; `session-switch.e2e` "a fresh replica first synced under another principal's session binds no client, ledgers nothing and installs none of its data" |
| INV-TBL-1 | covered | `SyncTable.test` "rejects a bare Schema.Number instead of guessing integer or double", "rejects leaves with no portable column (bigint, binary, enum, optional key)", "SQLite stores and returns every supported leaf type unchanged", "Postgres stores and returns every supported leaf type unchanged" |
| INV-TBL-2 | covered | `Store.test` "the reference orders text by code point, NULL first ascending, id as tiebreak"; `SqliteRowStore.test` "range returns exactly core's reference rows in core's reference order (seeded random plans)"; Conformance "range returns exactly Store.planMatches rows in Store.planOrder…" |
| INV-TBL-3 | covered | `SyncTable.test` "does not change when only indexes change", "does not change for documentation annotations or field order", "ignores refinements, brands and nested JSON schemas", "changes when the storage shape changes (nullability, column kind, column set, table name)" |
| INV-TBL-4 | covered | `Simulation.test` "a pulled row is stored in canonical wire form (excess columns dropped, values normalized)"; `scopes.test` "pulled rows are the wire form: epoch-ms timestamps, and no column the SyncTable does not declare" |
| INV-TBL-5 | covered | `SyncTable.test` "rejects field names that do not survive camelCase ↔ snake_case", "rejects indexes on JSON columns and on unknown fields", "a registry rejects two tables with the same sync name, including the system grants table" |
| INV-ACL-1 | covered | `Store.test` "queries return the first n rows the actor can read (own or * grant), skipping unreadable ones in index order", "writes need writer on the row" |
| INV-ACL-2 | covered | `Store.test` "writes need writer on the row"; `scopes.test` "a scope move deletes the row for old-scope readers and delivers it to new-scope readers" |
| INV-ACL-3 | covered | `Store.test` "a non-owner can delete only its own grant, and a write after relinquishing it is ScopeForbidden" |
| INV-ACL-4 | covered | `Store.test` (same); `grants.test` "a reader or writer deleting its own grant is Applied: its client is purged of the scope, the owner keeps the rows and sees the tombstone" |
| INV-ACL-5 | covered | `Store.test` "scope_grants DDL makes duplicate (scope, principal) grants unrepresentable in storage", "two grants for one (scope, principal) cannot both be written, nor can a patch change the pair"; `grants.test` "a grant written around the store with an id that is not grantId(scope, principal) is rejected" |
| INV-ACL-6 | covered | Conformance "createScope writes exactly the actor's owner grant on a fresh id; an existing id is ScopeForbidden{owner} with nothing written; the actor * is refused before any claim"; `scopeCreate.test` "two concurrent creators of one scope id get exactly one Applied and one ScopeForbidden{owner}, never two owners", "a scope whose grants were all revoked is never fresh again; a rejected mutation's claim rolls back with it" |
| INV-ACL-7 | covered | Conformance "a limited range with a readable-scope constraint returns the first n readable rows in index order, not the readable rows among the first n"; `Conformance.test` "the scoped-limit check catches stores that do not push the scope filter under the limit" |
| INV-MUT-1 | covered | `ledger.test` "a resubmitted envelope (lost response) returns the recorded outcome and position without re-executing", "concurrent submissions of one envelope run the body once and agree on outcome and position", "concurrent duplicates from an already-registered client commit one effect and one ledger row" |
| INV-MUT-2 | partial | `Transport.test` "a processed push returns core's result; a rejection keeps its class and fields" (closedness of the set is type-level only) |
| INV-MUT-3 | covered | `Mutation.test` "a domain failure combined with a defect dies with the defect, never a Rejected outcome", "StoreUnavailable alongside a domain failure stays transient, never a permanent rejection", "an unencodable domain failure next to a valid one dies, in either order, never a DomainError"; `ledger.test` "a rejection combined with a defect is not ledgered as an ordinary rejection" |
| INV-MUT-4 | covered | `ledger.test` "a rejected mutation records its outcome and leaves none of its writes behind" |
| INV-MUT-5 | covered | `ledger.test` "a unique violation from a stale snapshot retries to Applied instead of stopping the batch transient", "a conflict combined with a rejection is retried, and the retry's outcome is ledgered (transient beats rejection)", "… on every attempt stops the batch as transient with nothing ledgered", "a conflict combined with a programmer defect dies without retry (defect beats transient)" |
| INV-MUT-6 | covered | `ledger.test` "a persistent unique violation is ledgered with a position instead of blocking later mutations", "a check constraint is rejected immediately without retries or partial writes" |
| INV-MUT-7 | covered | `ledger.test` "seq < 1 is a malformed envelope: not executed, not recorded, not answered as clientExpired" |
| INV-MUT-8 | partial | `Mutation.test` "the seeded random is a function of (clientId, mutationId), so a rebase replays identically" (client `ctx.now = createdAt` untested) |
| INV-MUT-9 | partial | `Mutation.test` "rejects two mutations with the same name at construction" (queries untested) |
| INV-MUT-10 | covered | `ledger.test` "a client id registered to one principal is refused to another" |
| INV-POS-1 | covered | `ordering.test` "a reader pulling after a later writer commits never skips an earlier writer still in flight", "without the trigger's advisory lock that reader skips the earlier writer (the ordering check can fail)", "a reader advancing its cursor during many random out-of-order commits never misses a change (stress)" |
| INV-POS-2 | covered | `capture.test` "TRUNCATE on a synced table fails instead of silently bypassing the log", "writes from outside any mutator (scripts, cascades) are captured like any other" |
| INV-POS-3 | partial | `scopes.test` "a scope move deletes the row for old-scope readers and delivers it to new-scope readers" (payload-free log rows untested) |
| INV-POS-4 | covered | `rpc.test` "a lost shared LISTEN ends every open pokes stream with ServerUnavailable; after it is re-established a new stream gets its initial poke and notifications", "subscribe-then-poke: a commit before the stream opened is covered by the initial poke" |
| INV-POS-5 | untested | — |
| INV-PULL-1 | covered | `rpc.test` "push, pull and pokes asserting another principal than the session's run no statement or policy and change no row, even for a bound client", "push, pull and pokes from an old client without the field still bind an unknown client on first sight" |
| INV-PULL-2 | covered | `Simulation.test` "random schedules keep every client = replay(pending, server@cursor) and lose/duplicate no intent"; `scopes.test` "under random writes, moves, grants and revokes a reader never receives an unreadable payload and converges" |
| INV-PULL-3 | covered | `gc.test` "a client whose cursor GC passed must resnapshot and then converges; a client at head is unaffected", "a cursor the server never completed a page set at is refused rather than trusted", "a continuation replayed by another client, principal, or cursor, or garbled, is refused", "a resumed page set whose originating cursor the server never completed is refused" |
| INV-PULL-4 | covered | `scopes.test` "a revoked scope is purged from the replica and nothing from it is delivered afterwards", "a granted scope arrives in full, page by page, with no gap against concurrent writes" |
| INV-PULL-5 | covered | `gc.test` "a well-formed forged continuation cannot record a held set no snapshot produced"; `scopes.test` "under random writes, moves, grants and revokes a reader never receives an unreadable payload and converges" |
| INV-PULL-6 | covered | `gc.test` "a well-formed forged continuation cannot record a held set no snapshot produced" |
| INV-PULL-7 | partial | `scopes.test` "a granted scope arrives in full, page by page, with no gap against concurrent writes" (walks-before-log ordering and keyset continuation untested directly) |
| INV-PULL-8 | untested | — |
| INV-PULL-9 | covered | `scopes.test` "under random writes, moves, grants and revokes a reader never receives an unreadable payload and converges", "pulled rows are the wire form: epoch-ms timestamps, and no column the SyncTable does not declare" |
| INV-PULL-10 | covered | `rpc.test` "a pushed mutation's result position and outcome reach the client and come back as its ack"; `ledger.test` "a mutation that writes nothing still gets an ack delivered by pull"; `gc.test` "an acked row older than the retention expires, and its resubmission answers Rejected{ClientExpired} at the recorded position instead of executing again" |
| INV-PULL-11 | covered | `gc.test` "re-pulling from the previous cursor after a lost PullDone yields the same converged state", "ledger rows written before migration 003 come through it unstamped and are never expired until a pull acks them" |
| INV-PULL-12 | covered | `gc.test` "expiring the newest ledger row never moves the head back: a cursor at its position does not regress and a page set resumed at it continues" |
| INV-PULL-13 | covered | `gc.test` "a client whose cursor GC passed must resnapshot and then converges; a client at head is unaffected"; `Simulation.test` "ResnapshotRequired after a suspended pull keeps seqs, pushedThrough and cursor written meanwhile" |
| INV-PUSH-1 | partial | `Transport.test` "the outbox batch is not a valid PushRequest (101 mutations)" (the cap); the 50 default and the `min(…, 100)` clamp have no test |
| INV-PUSH-2 | covered | `ledger.test` "a batch stops at a transient failure without recording it, and its resubmission continues in order"; `Transport.test` "a push batch the server stopped is reported as kind server" |
| INV-PUSH-3 | covered | `rpc.test` "a connection lost mid-batch is a short results list, never a defect the client would halt on", "an infrastructure defect in principalFor is a transient stop (empty results), not a push failure" |
| INV-PUSH-4 | covered | `ledger.test` "seq < 1 is a malformed envelope…"; `scenarios.sh` "sync: MalformedEnvelope halts push; pull and pokes reconnects continue; pending export unaffected"; `version-gate.e2e` "a client below the generated floor stops retrying, can mutate offline, and delivers the original intents after update" |
| INV-PUSH-5 | covered | `startup.test` "body-supplied cookie and origin headers cannot change a push" |
| INV-PUSH-6 | covered | `Leader.test` "a push whose response is lost keeps the entry pending until the pull installs its ack, and that cycle sets no lastSyncedAt"; `Simulation.test` "constraint rejections neither release before covering acks nor lose fields in retained notices; ClientExpired still releases on receipt" |
| INV-CLI-1 | covered | `Simulation.test` "random schedules keep every client = replay(pending, server@cursor) and lose/duplicate no intent" |
| INV-CLI-2 | covered | `Simulation.test` "a client that applies PullPages before PullDone is caught by an invariant (the check bites)"; `SqliteSyncStorage.test` "after a rebuild, installing the null-cursor set replays pending on top of it and recaptures undo" |
| INV-CLI-3 | covered | `Store.test` "undo images restore the exact prior state after a handler that fails midway"; `scope-create.e2e` "an optimistic createScope of an id the server already has is rejected ScopeForbidden, and the pull undoes the grant and the write that depended on it; a fresh id is applied and stays", "when only the creation is processed and rejected, a dependent mutation stays pending with no optimistic effect…" |
| INV-CLI-4 | covered | `Outbox.test` "an enqueue retried after its original was released returns the original seq and is not pending"; `Leader.test` "mutate retried across LeaderLost after the lost leader committed leaves exactly one outbox row", "an enqueue cancelled while its COMMIT runs still wakes the loop"; Simulation invariant "enqueue is idempotent on mutationId" |
| INV-CLI-5 | covered | `SqliteSyncStorage.test` "nextSeq starts at 1 and never reuses a seq, even after its released row is pruned"; `Outbox.test` "pruning released rows keeps pending ones and never lets a seq be reused"; `Simulation.test` "an allocator that reuses a released entry's seq is caught by the never-reused check" |
| INV-CLI-6 | covered | `Simulation.ts` invariant "pending set never holds an acked mutation", checked on every step of `Simulation.test` random schedules (no test shows the check biting) |
| INV-CLI-7 | untested | — |
| INV-CLI-8 | covered | `Outbox.test` "released rows inside the retention window are not pruned"; `expiry.e2e` "a tab's enqueue retried after its reply was lost, the released marker pruned (24 h) and the ledger row expired (7 d) gets a fresh seq and is answered ClientExpired at the original position…" |
| INV-CLI-9 | covered | `Simulation.test` "a known-table row missing declared keys is server-outdated: no ack, cursor or staging change"; `version-gate.e2e` "a newer client preserves valid staging on server-outdated and converges without rebuilding when the server catches up"; `SyncLoop.test` "a pull that fails with MalformedChange is retried with backoff and never halts push" |
| INV-CLI-10 | covered | `Simulation.test` "ResnapshotRequired after a suspended pull keeps seqs, pushedThrough and cursor written meanwhile"; `gc.test` "a page set interrupted mid row walk whose position GC passed must resnapshot instead of resuming" |
| INV-CLI-11 | covered | `SqliteRowStore.test` "a stored row that no longer decodes must die naming its table and row id, not with a bare SchemaError" |
| INV-CLI-12 | covered | `SqliteSyncStorage.test` "an index-only change keeps rows and cursor; a data-shape change rebuilds and resets them together", "a data-shape migration interrupted at any scheduler boundary leaves the old replica or the new one, never a mix" |
| INV-CLI-13 | covered | `version-gate.e2e` "unknown-table changes must not stall an old client or be lost after its registry update" |
| INV-LOOP-1 | covered | `SyncLoop.test` "a transient push failure is retried with backoff and clears once a cycle succeeds"; `Leader.test` "requestSync during backoff runs the next cycle at once and leaves a halted push halted" |
| INV-LOOP-2 | covered | `SyncLoop.test` "lastSyncedAt stays null after a cycle whose pull failed, and is set by the first cycle that installs"; `Leader.test` "a push whose response is lost keeps the entry pending until the pull installs its ack, and that cycle sets no lastSyncedAt" |
| INV-LOOP-3 | covered | `SyncLoop.test` "an integrity error halts push for good while pull keeps running"; `scenarios.sh` "sync: MalformedEnvelope halts push; pull and pokes reconnects continue; pending export unaffected", "sync: after handover the new leader pushes once, halts again, and still exports the entry" |
| INV-LOOP-4 | covered | `version-gate.e2e` "a client below the generated floor stops retrying, can mutate offline, and delivers the original intents after update"; `Transport.test` "a syncHeaders that dies is reported as kind credentials", "a refused connection is reported as kind network…", "a server Unauthorized is reported as kind unauthorized", "HTTP auth refusals before RPC decoding report unauthorized, retry without halting, and keep the pending mutation; other statuses stay network", "a server ServerUnavailable is reported as kind unavailable" |
| INV-LOOP-5 | covered | `SyncLoop.test` "a defect beside a transient failure halts push (whole-cause classification); a pull defect is transient"; `observability.e2e` "the whole final push cause remains authoritative, including typed cleanup failures" |
| INV-LOOP-6 | covered | `Transport.test` "no syncStatus string carries a secret from the failure: …" (five cases) |
| INV-LOOP-7 | covered | `SyncLoop.test` "the pokes stream is reconnected with backoff after a clean end, a failure and a defect; every poke wakes", "only stream elements wake: a stream with no initial poke wakes nothing until it delivers one", "a pokes stream silent past idleTimeout is ended and reconnected, and the new stream's initial poke wakes; heartbeats keep it open without waking"; `rpc.test` "an idle pokes stream carries a heartbeat at least every heartbeatInterval, and no heartbeat makes the client pull" |
| INV-LOOP-8 | covered | `Transport.test` "a token renewed after the transport was built is used by the next push, pull and pokes reconnect, and syncHeaders runs once per request", "a pokes reconnect whose syncHeaders takes longer than idleTimeout evaluates them once, then sends once; silence after the send still ends the stream" |
| INV-LOOP-9 | covered | `SyncLoop.test` "status and changes wait for the first real pending count; a failed first count emits nothing until a retry counts", "a delayed first pending count holds back status and changes until it arrives; no seed 0 leaks" |
| INV-GC-1 | covered | `gc.test` "entries younger than the retention window survive GC", "log GC never deletes ledger rows or held-scope records (would cause double apply after resnapshot)" |
| INV-GC-2 | covered | `gc.test` "a client whose cursor GC passed must resnapshot and then converges; a client at head is unaffected", "a page set interrupted mid row walk whose position GC passed must resnapshot instead of resuming", "an unfinished page set whose log position GC passed must resnapshot instead of resuming" |
| INV-GC-3 | covered | `gc.test` "a retention below 48 h fails the job typed with LedgerRetentionTooShort before touching the ledger; 48 h runs", "an acked row younger than the retention, and an unacked row of any age, survive; age is measured from the ack, not from receipt", "ledger rows written before migration 003 come through it unstamped and are never expired until a pull acks them" |
| INV-GC-4 | covered | `gc.test` "the floor after a job is the highest seq of an expired prefix, PullDone.expiredThrough reports it, and the recorded position is the highest expired row", "expired rows that would not be a seq prefix fail the job loudly and roll back: no row deleted, no floor raised" |
| INV-GC-5 | covered | `gc.test` "log GC never deletes ledger rows or held-scope records…", "a job run with no eligible rows changes nothing: young acks, unacked rows and earlier floors stay as they are" |
| INV-GC-6 | covered | `gc.test` "expiring the newest ledger row never moves the head back…" |
| INV-BRW-1 | covered | `platform.test` "identities with all-dot segments are rejected", "an appId or profile over 255 UTF-8 bytes is refused even when the whole path fits; 255 bytes is accepted", "the longest identity whose journal path fits the pool…"; `scenarios.sh` "the longest database path the pool holds (appId 233 + profile 255) opens and round-trips; one character more, or a 256-byte profile with a short path, is refused Unsupported before OPFS is touched" |
| INV-BRW-2 | covered | `scenarios.sh` "single owner across 4 tabs (one leader id, exactly one leader tab, every tab served by it)", "single owner after handover…", "OPFS startup failure: the failed candidate releases the leader lock, the next tab leads and serves both" |
| INV-BRW-3 | covered | `scenarios.sh` "router ignores a stale announcement from a dead leader, even while its epoch is held shared…", "a late Connected for a dead older epoch (from a dead router instance) is ignored…"; `platform.test` "a name held only in shared mode does not count as held" |
| INV-BRW-4 | covered | `LeaderRoute.test` "a request issued before any leader connects must wait for one, not fail or use a stale connection", "a displaced connection must fail its in-flight request and stream with its LeaderLost, not hang", "a transport failure must surface as LeaderLost naming the leader, never as a raw RpcClientError"; `Leader.test` "mutate retried across LeaderLost after the lost leader committed leaves exactly one outbox row", "a query subscription across handover keeps the last emission until the new leader…"; `scenarios.sh` "handover: an enqueue lost with the leader is retried and lands exactly once" |
| INV-BRW-5 | covered | `scenarios.sh` "router killed while tabs connected: a new tab is served by the same leader; connected tabs keep their connection and open subscription", "router killed, then the leader tab closes: both remaining tabs are served by one new leader", "router killed before a successor's first Connected: the newly elected leader tab reconnects and is served by its own leader" |
| INV-BRW-6 | covered | `Leader.test` "a query re-emits after a write to a table only its latest run read, and not after one it stopped reading", "every emission of a query is one snapshot: two tables written together never disagree", "a registered query that writes fails, commits nothing, and leaves the next mutation writable", "raw exec is refused, and writes nothing, on a leader started without allowRawExec", "raw subscribe is refused before any SQL runs on a leader started without allowRawExec" |
| INV-BRW-7 | covered | `LeaderRoute.test` "after reset every request fails Reset: waiting, in flight (even as interruption) and later, never LeaderLost"; `scenarios.sh` "a leader that ignores the reset signal yields ReplicaInUse after closeTimeout (1 s), deletes nothing…", "reset with one pending write fails PendingWrites{1} and deletes nothing", "reset with discardPending removes echo-sync/<appId>/a and leaves sibling profile b intact", "coordinated reset: an infinite or NaN closeTimeout fails ResetFailed before anything is signalled or touched" |
| INV-BRW-8 | covered | `scenarios.sh` "a reset worker terminated mid-removal after its tombstone: the next LeaderClient starts on a fresh, empty replica with a new clientId; no tombstone or old pool file remains", "a second resetReplica (without discardPending) after an interrupted one answers Done…" |
| INV-BRW-9 | covered | `scenarios.sh` "fallback: second tab without SharedWorker fails closed with SecondaryTabWithoutRouter", "fallback: after the owning tab closes, the waiting tab takes over the same replica", "identity '.' is refused with LeaderUnavailable{Unsupported}, nothing is opened" |
| INV-BRW-10 | covered | `worker-lifetime.test` "leader teardown closes fresh transfers discarded before Init, still queued, or delivered to a saved late callback", "router teardown and rejection close fresh connect and announcement peers, including saved late callbacks"; `LeaderClient.test` "closing a client cannot abandon a transferred port queued for acceptance or waiting for its epoch check" |
| INV-SCH-1 | covered | `Migrate.test` "the generator never emits DDL for a rename, drop, lossy type change or unique-set change", "safe changes become exactly the pinned DDL" (group), "a NOT NULL column without a default is added nullable with the SPEC §13 TODO, never SET NOT NULL" |
| INV-SCH-2 | covered | `Migrate.test` "DDL that depends on a TODO rename or kind change is a TODO too" (group), "an intent naming something the diff does not contain is refused, not ignored" |
| INV-SCH-3 | covered | `Migrate.test` "migrations are numbered after the highest existing one", "scope_grants is the engine's table and is never planned; no change plans nothing"; `version-gate.e2e` "a client below the generated floor…" (history) |
| INV-SCH-4 | covered | `startup.test` "a server whose database drifted from the registry never serves; restored, it serves", "a registry table the app migrations never created fails startup as a Drift naming it, not a bare SqlError" |
| INV-SCH-5 | covered | `capture.test` "attaching a second table with an already-synced name in another schema is refused", "attach with a schema name differing only in case from an attached one is refused, not installed twice", "concurrent attaches of one table name from two schemas cannot both succeed"; `startup.test` "a table captured in two schemas fails startup with one Drift naming both, with every other mismatch", "a search_path that resolves the table name to an uncaptured relation fails startup with a Drift" |
| INV-SCH-6 | covered | `startup.test` "prepare run concurrently by several instances succeeds for all of them", "concurrent first starts on an empty database all succeed and run the app migration exactly once" |
| INV-SCH-7 | covered | `capture.test` "attach inside a caller's REPEATABLE READ transaction is refused (its stale snapshot would admit a duplicate name)", "an attach that fails between its two triggers leaves the table neither captured nor half-guarded" |
| INV-SCH-8 | covered | `startup.test` "an old server must serve push and pull on an expanded database, while a new server must refuse the unmigrated schema" |
| INV-TEL-1 | covered | `startup.test` "startup's internal steps (verify, its per-table checks) must never hand the host a span other than echo.server.startup"; `observability.e2e` "sync propagates exported IDs and sampling through suppressed spans and preserves an explicit external parent" |
| INV-TEL-2 | covered | `observability.e2e` "SQLite sync never exports a typed failure, defect, result or dependency span containing secret inputs", "RPC request context cannot bypass sanitization for typed failures, defects, SQL spans or tracerLogger events", "direct server batches and maintenance cannot export returned identities or fail from a throwing tracer" |
| INV-TEL-3 | covered | `observability.e2e` "throwing host sinks cannot change saved intent, retry deadlines, halt or successful installs", "closing a pulled subscription in the host context retains its own outcome and protects cleanup and the host parent" |
| INV-TEL-4 | covered | `observability.e2e` "the whole final push cause remains authoritative, including typed cleanup failures", "batch telemetry agrees with the loop"; `SyncLoop.test` "a pokes connection the idle timeout ends must record idle-timeout, and one the server ends server-end" |
| INV-TEL-5 | partial | `observability.e2e` "RPC request context cannot bypass sanitization for … tracerLogger events" (the Debug downgrade of interrupted causes is untested) |
| INV-ERR-1 | partial | `Transport.test` "push failures are fatal exactly when SPEC §10 says so, classified by real tags over a real RPC server" (closedness is type-level) |
| INV-ERR-2 | covered | `SqliteRowStore.test` "a stored row that no longer decodes must die naming its table and row id, not with a bare SchemaError"; `scenarios.sh` "OPFS startup failure alone: the tab's LeaderFailed names the operation and the browser's reason, not the app or profile path" |
| INV-PERF-1 | covered | `ordering.test` "without the trigger's advisory lock that reader skips the earlier writer (the ordering check can fail)" (the lock is held; its throughput consequence is not measured) |
| INV-PERF-2 | untested | — |
| INV-PERF-3 | untested | — |
| INV-PERF-4 | untested | — (only `echo.install`/`echo.rebase` timers exist) |
| INV-PERF-5 | untested | — |
| INV-PERF-6 | partial | `rpc.test` "more open pokes streams than pool connections do not block push or pull, and the server holds one LISTEN" (fan-out and notify counts unmeasured) |

Totals: 110 invariants; 93 covered, 9 partial (INV-MUT-2, INV-MUT-8, INV-MUT-9, INV-POS-3,
INV-PULL-7, INV-PUSH-1, INV-TEL-5, INV-ERR-1, INV-PERF-6), 8 untested (INV-ID-3, INV-POS-5,
INV-PULL-8, INV-CLI-7, INV-PERF-2, INV-PERF-3, INV-PERF-4, INV-PERF-5).

## 18. Limits and defaults

| name | value | where | tunable |
|---|---|---|---|
| `wireVersion` | 1 | `Protocol.ts` | no |
| push batch size | `min(pushBatchSize ?? 50, 100)` | `Engine.ts` | `pushBatchSize` |
| `PushRequest.mutations` | 1 … 100 | `Protocol.ts` | no |
| `pageLimit` (upserts + tombstones per page) | 500; `≥ 1` is an unvalidated precondition (INV-PULL-8) | `rpc.ts` | `pageLimit` |
| server `heartbeatInterval` (pokes and presence) | 15 s | `rpc.ts` | yes; must stay below clients' `idleTimeout` |
| client `idleTimeout` (pokes stream, presence stream, presence send) | 45 s | `SyncLoop.ts`, `Presence.ts` | yes; must exceed `heartbeatInterval` |
| sync loop backoff | 1 s doubling to 30 s, reset on a clean cycle | `SyncLoop.ts` | `retryDelay`, `maxRetryDelay` |
| pokes reconnect backoff | 1 s doubling to 30 s, reset once a poke arrived | `SyncLoop.ts` | `retryDelay`, `maxRetryDelay` |
| presence rejoin backoff (leader) | 1 s doubling to 30 s | `Presence.ts` | `retryDelay`, `maxRetryDelay` |
| presence send deadline (tab → leader) | 45 s | `LeaderClient.ts` | `presenceSendTimeout` |
| presence send retry (tab → leader) | 1 s doubling to 30 s | `LeaderClient.ts` | `presenceRetryDelay` |
| conflict retry (server mutation) | `Schedule.exponential("10 millis")` up to 4 retries (5 attempts) | `ledger.ts` | `conflictRetry` |
| `LISTEN` reconnect backoff | `min(50 ms · 2^n, 5 s)`, reset once confirmed | `rpc.ts` | no |
| presence revocation watch restart | 1 s | `rpc.ts` | no |
| pending-count stream restart (leader) | 1 s after any failure, forever | `SyncLoop.ts` | no |
| ledger expiry retention | default 7 d, minimum 48 h | `gc.ts` | `retention` |
| log GC retention | caller-supplied (no default) | `gc.ts` | argument |
| released-row marker retention | 24 h | `Outbox.ts` | `pruneReleased(retentionMillis)` |
| reset `closeTimeout` | 5 s, must be finite | `ResetReplica.ts` | `closeTimeout` |
| `awaitOpfsPoolFree` | 100 retries × 20 ms | `platform.ts` | no |
| `LeaderLost` retry spacing (`mutate`, `requestSync`, `dismissNotices`) | 50 ms, unbounded | `LeaderClient.ts` | no |
| resubscribe delay after `LeaderLost` | 50 ms | `LeaderClient.ts` | no |
| table name | `/^[a-z][a-z0-9_]*$/`, ≤ 48 | `SyncTable.ts` | no |
| column name | snake case, ≤ 63 | `SyncTable.ts` | no |
| index name | `${table}__${index}` ≤ 63 | `SyncTable.ts` | no |
| `appId`, `profile` | `^(?!\.+$)[A-Za-z0-9._-]+$`, ≤ 255 UTF-8 bytes each | `platform.ts` | no |
| database name (`echo-sync/<appId>/<profile>.db`) | ≤ 502 | `platform.ts` | no |
| advisory locks | position `7305797848413072995`, attach `…996`, schema bootstrap `…997` | migrations, `schema.ts` | no |
| `echo.install` / `echo.rebase` timer boundaries | 0.5, 1, 5, 10, 50, 100, 500, 1000 ms | `Telemetry.ts` | no |

## Appendix A. Glossary

- **Position**: the server's global, committed-order sequence (§6). **Cursor**: the position of
  the last `PullDone` a client installed. **Head**: the highest committed position.
- **Scope**: an access scope id; rows carry `access_scope_id`; grants give a principal a role in a
  scope. **Held set**: the scopes a client's replica contains at a cursor (`client_scopes`).
- **Page set**: the `PullPage*, PullDone` sequence between two cursors. **Continuation**: the
  server's unsigned resume token between pages of one set.
- **Outbox**: the client's durable queue of mutations with undo images. **Pending**: unreleased
  outbox entries. **Released**: an entry the server's verdict settled; kept as a marker for 24 h.
- **Rebase**: undo-all, install, settle, replay-all (§9).
- **Leader**: the one worker per identity that owns the SQLite database. **Router**: the
  `SharedWorker` that connects tabs to the leader. **Epoch**: a leader's instance lock.

## Appendix B. Open Questions index

| ID | section | question |
|---|---|---|
| OQ-ACL-1 | §4 | `*` is not refused as an authenticated principal |
| OQ-MUT-1 | §5 | no bound on a persistently transient mutation blocking a client's queue |
| OQ-MUT-2 | §5 | `createdAt` not on the wire; `ctx.now` differs between client and server |
| OQ-PULL-1 | §7 (Q2) | no explicit `cursor ≤ head` fence; recovery after restore relies on the missing `client_scopes` row |
| OQ-PULL-3 | §7 | `pageLimit` unvalidated; `0` loops forever |
| OQ-CLI-1 | §9 (Q3) | every install is a full rebase; no incremental path |
| OQ-LOOP-1 | §10 | the push halt is in memory per leader; a new leader retries the batch and halts again |
| OQ-BRW-1 | §12 | `awaitOpfsPoolFree` bound (2 s) then `LeaderFailed` with no retry |
| OQ-TEL-1 | §14 | closed telemetry vocabularies not asserted against a committed list |
| OQ-ERR-1 | §15 | `Unauthorized` has no `reason`; four conditions share one tag |
| OQ-PERF-1 | §16 | no statement counters; only install/rebase timers |

## Appendix C. Browser mechanisms and verification notes (non-normative)

These notes explain §9, §10 and §12; they add no invariants. Commands for the harness live in
`AGENTS.md`. Historical observations below are evidence from earlier runs, not guarantees for
other browsers or a claim that those runs were repeated during documentation consolidation.

### C.1 Ports, locks and OPFS

The router creates a fresh `MessageChannel` per tab, transferring one end with `Connect` to
the leader and the other with `Connected` to the tab. Tab↔leader traffic uses Effect's worker
RPC protocol and `BrowserWorkerRunner.layerMessagePort`. Workers are spawned by tabs, not by
the SharedWorker, so no nested workers are required. Epoch and tab death watchers acquire
locks in shared mode; acceptance checks count only exclusive holders, not those watchers.
Connections are objects: a displaced connection completes its lost signal before closing its
RPC scope, and a late handshake or watcher acts only on the still-active connection.

On `RouterStarted`, connected-epoch loss, or its own worker's election, a tab constructs the
SharedWorker again and sends `Hello`; the elected tab re-announces over a fresh port pair.
`Hello.connectedTo` prevents redundant links on a live router. Across router instances the
tab serializes acceptance and checks the epoch, ignoring and closing duplicate/dead ports.
Old router ports remain open to receive already-transferred connections. The accepted v1
bound per tab is 2 + router relaunches + observed leader epoch ends; the router also keeps a
queued tab-lock watcher per `Hello`, released on tab death. A constant bound would need a
`Hello` acknowledgement before closing old ports.

The stock `OpfsWorker.run` uses a fixed VFS name `opfs`, hence one origin-wide directory of
exclusive sync access handles. Independent identities then contend for that pool.
`OpfsSqlite.ts` instead implements the same internal port protocol with a per-identity pool.
Re-verify that protocol on every `@effect/sql-sqlite-wasm` upgrade. Wasm load, pool creation and
`open_v2` finish before waiting on the ready protocol: the SQL client's ready wait is
uninterruptible, so an open failure must be raised before it. The pool is topped up to six
files even when nonempty; the VFS otherwise grows only an empty pool, and interrupted creation
can leave too little capacity for the next write. Access handles can outlive the dead
worker's locks briefly (OQ-BRW-1).

Lock waits abort queued requests on interruption and release a grant racing with the abort.
The cancellation argument currently relies on Effect's default scheduler entering the
uninterruptible hold region synchronously after grant. A custom scheduler yielding there
(or before a shared watcher releases its grant) could retain a granted lock until agent death;
scheduler-independent resolver cleanup remains deferred.

The storage permit serializes transactions but is not fair: a releasing fiber can barge ahead
of a waiter. Await each enqueue to order one tab's calls; concurrent calls have no promised
relative order. `exec` and notices use `admit` (permit, no extra transaction); query emissions
and engine operations use storage transactions. Raw writes bypass outbox and undo capture.

Closing the tab client's scope terminates its candidate immediately, without worker
finalizers or a drain/flush. A committed enqueue remains in OPFS; an abandoned HTTP push may
or may not commit on the server. The successor reconciles the persisted outbox and ledger,
not the old request's lifetime. Complete-value streams re-subscribe rather than resume a
result version and may repeat their last value; raw subscriptions leave re-subscription to
the caller. Slice S5 accepts either server crash outcome and reports handler starts; it
requires one ledger row and eventual release, not proof that the abandoned request rolled back.

Reset reuses the app's `spawnLeader` and leader OPFS host with a `Reset` message rather than
`Init`, without migrating. `discardPending` never opens the database, allowing corrupt or
partially removed replicas to be removed. Otherwise open/count failure is closed (`ResetFailed`).
The close timeout includes asynchronous lock grant, so even a free lock can exceed a zero
budget. A late grant is immediately released. The reset signal is an event, not a durable
marker: a client built later queues behind the reset and can open afresh. A refused pending
decision leaves existing clients reset but data intact; notices go with the replica, and only
the outbox guards deletion. The tombstone uses Web Crypto SHA-256 and an identity-excluded
`~` prefix. Sibling directories and tombstones are independent.

### C.2 Browser evidence and limits

The API support figures below are documentation-derived unless a run is explicitly named.
Runtime capability checks, not browser detection, select the mode (§12).

| browser | documented API availability / evidence |
|---|---|
| Chrome/Edge desktop | Web Locks 69+, OPFS sync handles 102+, SharedWorker; headless Chromium scenarios exercised here historically |
| Chrome Android | Web Locks and OPFS; historically no SharedWorker, hence single-tab fallback; not device-tested |
| Firefox desktop | Web Locks 96+, OPFS sync handles 111+, module SharedWorker 114+; not run here |
| Safari macOS/iOS | Web Locks 15.4+, SharedWorker re-added in 16, sync handles 15.2+ with later fully synchronous methods; not device-tested |
| Playwright 1.64.1 / WebKit 27.2, Linux GTK headless | persistent-context OPFS, Web Locks and SharedWorker exercised; not Safari's process/storage model |

The effective language baseline is Chrome/Edge 110, Firefox 115, Safari 16, because core uses
ES2023 `toSorted`; storage APIs alone do not set the minimum. Older Safari 15.x handles had
async `getSize`/`flush`/`close`, unsupported by AccessHandlePoolVFS. Module workers need
Safari 15+. The leader checks `createSyncAccessHandle` itself and reports an open failure
rather than waiting forever.

The Linux WebKit run exercised election, OPFS durability and ordering, port transfers,
reactivity, streaming sync/presence, offline reload, conflicts, reset and fallback without
cross-origin isolation or SharedArrayBuffer. The pinned wa-sqlite 0.1.2 / SQLite 3.47.0 build
has JSON functions (`json_extract` works), but no FTS5 (`no such module: fts5`, no
`ENABLE_FTS5` compile option). The browser SharedWorker lifetime probe still fails on WebKit
after router recovery was fixed: it tests the browser, not our recovery.

Historical WebKit results after router recovery were 30/32 substrate checks (two failures)
and 12/12 slice checks, with Chromium-only CDP checks skipped. A closed tab's leader executing
the handover scenario's 10⁹-row synchronous wasm query retained leader/epoch locks for over
50 s; without that query a successor served after 0.76 s. The single-owner assertion then
failed and the following stale-announcement assertion lacked a new leader id. Neither was
an extra owner: long synchronous wasm caused unavailability. The cause is unidentified;
[WebKit 289686](https://bugs.webkit.org/show_bug.cgi?id=289686) is only a possible relation,
not an established explanation. The worker cannot observe a death signal while wasm blocks.
Real Safari needs a separate check. Router-kill CDP tests are skipped under WebKit; normal
handover there naturally exercises router death.

WebKit's 255-byte OPFS segment limit was reproduced (256 bytes fail `UnknownError`, 127
two-byte characters succeed, 128 fail), unlike Chromium's acceptance of 480-byte names.
Validation now rejects oversized segments before OPFS. Safari sharing that limit is expected,
not verified. Desktop GTK WebKit is not evidence for iOS eviction, background suspension,
process-per-tab hosting or private browsing. Documentation says iOS best-effort origin
eviction/ITP can delete both replica and pending outbox, `persist()` is heuristic (including
Home Screen use), and the full Storage API is available from 17; see
[WebKit storage policy](https://webkit.org/blog/14403/updates-to-storage-policy/).
Private/ephemeral contexts lack OPFS; background suspension can retain locks until process
death. Recheck these and SharedWorker hosting on real iPhone/macOS Safari before relying on them.

### C.3 What the harness proves

Scenarios use fresh app ids and await asserted conditions (`echo.until`); concurrent phases
rendezvous on BroadcastChannel barriers, not wall-clock timers subject to background throttling.
Vite pre-scans worker entries and the scripts warm up a page to avoid cold-start reloads.
`window.echo` is the scripting surface; failures return `{ ok: false, error }` for tag/reason
assertions. Faults include commit-before-reply worker death, OPFS-open failure, muted reset
signals, reset death before tombstone or mid-removal, forged announcements and held router
messages. The fake RPC endpoint always rejects push with `MalformedEnvelope`, answers empty
pull and ends pokes after one poke; it is separate from the real-server checks.

The emission sanity check (two tables kept equal by a trigger) still passes if its read
transaction is removed, because its statements already run back-to-back; it is not proof of
snapshot isolation. `Leader.test` covers that contract (INV-BRW-6). Browser hard-kill checks
cover enqueues that resolved; the commit-before-reply fault plus simultaneous same-id retries
covers the lost response. Slice offline checks wait for an actual `network` failure, not merely
a halted push. Conflict/rejection checks compare Postgres, both replicas and existing live
queries, and scan emission history so a transient leak cannot pass on final state alone.
S4 has no resync timer: silently dead proxy responses recover only through idle detection plus
reconnect backoff (§10). Scenario descriptions and assertions live beside the scripts; §17
is the contract-to-test registry.

## Appendix D. Postgres mechanisms and verification notes (non-normative)

These notes explain §3–§8, §10, §11 and §13 without changing their contracts.

### D.1 Composition and storage

`schema.ts` owns attach/catalog discovery and `resolveTables`; `ledger.ts` owns mutations and
client binding; `pull.ts` owns pages/continuations; `rowStore.ts` owns row codecs; `gc.ts` owns
maintenance. `rpc.ts` composes transport-neutral `principalFor`, `executeBatch`, `pull`,
`pushFailures` and `serverFailures`. The HTTP layer merges `SyncRpcs` and `PresenceRpcs` on one
path with NDJSON; it must accept the client HTTP protocol's trailing slash. RPC handlers need
the actual HTTP request context and defect if it is missing; another transport composes the
neutral steps using its own trusted boundary headers. `executeBatch` and `pull` require
`SqlClient`; failure combinators preserve their input requirements.

The HTTP protocol decodes the body before `principalFor`: the callback is not a pre-decoder
body-size gate. A stream authenticates at open, not continuously, so a live heartbeat does
not bound how long an expired credential remains honoured. A reconnect obtains new headers.
The pinned RPC implementation preserves the real HTTP request context when forking handlers,
for buffered and streaming responses alike. `examples/server` rejects cross-site and
same-site fetch metadata before reading its dev principal; sibling origins are not same-origin.
Origin/fetch metadata policy does not replace credentials: non-browser callers can omit it.

Push applies `pushFailures` across authentication and execution; SQL infrastructure failure
therefore yields an empty/short processed prefix, not a fatal push defect. `executeBatch`
already applies that mapping idempotently. `serverFailures` maps SQL anywhere in a pull cause
to `ServerUnavailable`, except when a non-SQL defect must remain a defect. Pokes uses stream
operators with first-typed-failure reduction, unlike the effect combinator's composite-cause
preservation. `ClientForbidden` maps to `Unauthorized` and interrupts pass through.

Wire-row writes encode each column; JSON uses `PgTypes.jsonb`. Reads decode only declared
columns; a captured table outside the registry has no payload and its log entries become
tombstones. Scope grant DDL adds the access-scope FK, role CHECK and principal index beyond
the core SyncTable manifest. The verifier does not inspect engine-internal CHECK/FK constraints;
migration history, not a startup catalog assertion, guarantees them. The ledger takes locks
in client-row → engine-position order. `ConstraintError` exposes no constraint name in the
pinned driver, unlike `UniqueViolation`. A SQL defect from failed savepoint/rollback cleanup
is transient and not a conflict retry. Whole-cause retry flags are computed before
`Effect.retry`, whose own filter considers only the first typed failure.

### D.2 Notification and presence internals

`layerNotifications` supplies one `Notifications` service to sync and presence. Per-stream
`LISTEN` would pin one pool connection each and eventually block push/pull. Each listener
attempt has its own scope, released before backoff. A capacity-one sliding notification
PubSub coalesces commits; a separate heartbeat PubSub prevents a heartbeat sliding out a poke.
Heartbeats have fixed cadence even on busy streams, not an inactivity-reset timer.

Presence uses a `Ref` of rooms and capacity-one change PubSubs; snapshots are freshly read
after change ticks. Each entry has a Deferred identifying its lifetime: replacement completes
the old one's signal and its release cannot remove the new one. Heartbeats merge beside the
change subscription only after the first snapshot. Track update/announcement is uninterruptible.
Revocation checks `(room, principal)` pairs with one indexed query per coalesced commit and
skips it when no rooms exist. An installation gate covers member-read through removal, but
never stream I/O or admission. Join checks grants before and after installation; an end signal
already completed takes precedence over admission's result. A snapshot read before removal
may still arrive, but one read afterwards is never emitted to that entry. Re-grant itself
does not recreate an entry; reconnect performs admission again.

### D.3 Evidence and cost caveats

Ledger lost-response tests discard a completed response; they do not inject a crash after
commit before reply. Backend termination during the body or before cleanup is real, and
verifies rollback/uncertain outcome handling. Concurrent scope creators exercise the 40001
retry (one applied, one forbidden, three body attempts). Ordering retains a no-lock variant
that demonstrably misses a change. Other deliberate mutations caught during development
removed walks, current-scope payload filtering, savepoints, client locking, continuation
bounds, attach locking/atomicity, or whole-cause classification; used READ COMMITTED for
mutations; returned undeclared columns; or changed ordering/JSON binding. The conformance and
integration suites caught these rather than merely accepting the correct implementation.

Historical ledger simulations (40 seeds × 400 steps, counting each seed's first violation):

| model | floors | late retries | double apply | never released | pass |
|---|---|---|---|---|---|
| identities deleted | no | off | 14 | 23 | 3 |
| identities deleted | no | on | 23 | 14 | 3 |
| identities deleted | yes | off | 0 | 0 | 40 |
| identities deleted | yes | on | 13 | 0 | 27 |
| identities kept, no receipt release | yes | on | 0 | 3 | 37 |
| identities kept, ClientExpired released on receipt | yes | on | 0 | 0 | 40 |

The kept-identity model also passed 120 seeds. The real Postgres expiry E2E covers the
time-dependent late retry after 24 h marker pruning and 7 d outcome expiry (§11).
Each mutation retains roughly 50–80 bytes of identity/index entries permanently; only outcome
payloads are reclaimed. PostgreSQL 15+ is needed by the verifier's
`pg_index.indnullsnotdistinct`; the engine schema alone needs PG14's replaceable triggers.
Integration suites create and drop fresh databases; `bun run test:pg` supplies the local URL.

### D.4 Client policy example

An app can derive a table-shape floor from generated history (§0.2, §13):

```ts
import { Effect } from "effect"
import { Migrate, Protocol } from "echo-sync"
import { Postgres } from "echo-sync/server"

const manifest = Migrate.decodeSnapshot(await Bun.file("migrations/manifest.json").text())
const floor = 3 // accept every recorded manifest since 003, not only the latest hash
const accepted = new Set((manifest.history ?? [])
  .filter((entry) => Number(entry.migration.split("_")[0]) >= floor)
  .map((entry) => entry.dataHash))
const clientPolicy: Postgres.ClientPolicy = (version) =>
  version !== undefined && version.wireVersion === Protocol.wireVersion && accepted.has(version.dataHash)
    ? Effect.void
    : Effect.fail(new Protocol.ClientOutdated({ message: "update required" }))
// Pass clientPolicy alongside registry, mutations and principalFor to Startup.layer / layerHttp.
```

This example refuses an absent version; the default serves it. Pre-gate clients cannot decode
the refusal or acquire new stop/status behavior retroactively. Version numbers decode as
arbitrary nonnegative integers so policy can choose its range. Raise a floor before a
non-additive column change, including writing nulls after relaxing NOT NULL; otherwise an old
client can misdiagnose the change as server-outdated. This floor covers table shapes, not
mutation names/args: keep names while queued clients exist and evolve args with unions/upcasts.
Removing a name or making args incompatible produces retained UnknownMutation/ArgsDecodeFailed
not an outdated stop. A separate mutation-registry floor remains unimplemented.

## Appendix E. Telemetry mechanisms and host integration (non-normative)

These notes explain §14; its vocabularies and privacy invariants remain authoritative.

### E.1 Runtime integration

No exporter, SDK, queue, sampling or flush policy ships with the engine. An unconfigured
native tracer still allocates spans and Effect keeps its in-memory metric registry. Hosts
can run `leaderWorker(self, options)` or `router(self)` with layers providing `Tracer.Tracer`,
`Logger.CurrentLoggers`, `References.MinimumLogLevel` and `Metric.CurrentMetricAttributes`.
The default `runLeaderWorker` / `runRouter` fork those same lazy scoped programs. Install host
telemetry separately in every realm (leader, router, tab) and in server startup, maintenance
and actual request execution; a tab's runtime does not cross a worker boundary. Interrupting
the program finalizes engine ports, fibers, locks and replica; the host owns termination and
browser handle release may finish asynchronously.

Routine loggers must discard diagnostics even with Debug enabled; a separate opt-in logger
can retain them under a sensitive-data policy. `Logger.Options` has a fiber, not an annotations
snapshot:

```ts
const routineLogger = Logger.make((options) => {
  if (options.fiber.getRef(References.CurrentLogAnnotations)["echo.diagnostics"] === true) return
  // Export options.message and host-approved metadata, never the diagnostic cause.
})
```

Compose the host runtime around `Startup.layer` and on request fibers; serving is not part
of the startup span. `Postgres.layerHttp` places a route-scoped boundary before automatic
RPC spans/context merge, leaving unrelated routes alone. Direct batch, ledger, pull,
maintenance, schema/migration and row-store entry points also suppress dependency telemetry
without inventing new span names. Host-authored SQL outside engine operations is host-owned.
The engine does not parse traceparent, baggage or forwarded headers; the host chooses trusted
parents. Host metric attributes, annotations and parent metadata must not contain secrets.

### E.2 Context, parents and sink isolation

Leader OPFS, sync, poke and per-tab RPC fibers are scoped to the leader; pending-count is a
child of the loop; presence and timeout/pull fibers use subscription scopes. Router callback
runners capture guarded references with `runForkWith` and attach with `Fiber.runIn`; epoch
watchers use the long-lived router scope, not the short announcement fiber. Tab connection,
epoch, deferred close and serialized acceptance runners similarly belong to the client's
scope; status publication uses `runSyncWith` on captured context. Check closure before
`runForkWith`, which evaluates synchronously, not only before attaching the fiber.

The boundary replaces tracer/loggers only; minimum log level, metric registry/attributes and
other host references inherit normally. Suppressed spans propagate the nearest exported
ancestor's IDs and sampling. Explicit external parents terminate traversal, as do effective
roots on suppressed ancestors; an explicit parent beats `root: true`. Captured boundary-parent
fallback applies only at the boundary. Streams capture context for both execution and owned
scope closure: channel service provision alone cannot protect finalizers closed by a host.
Outcome finalizers also capture their actual operation span.

Span proxies forward `Exit.void` at end. Merely capturing `Effect.exit` inside `withSpan`
would not protect an interruption outside that capture: pinned `useSpan`'s finalizer receives
the original exit. `observe` classifies in `onExit` inside `withSpan`; no additional
interruptibility mask is introduced. Each sink creation, attribute, end, logger and metric
update is guarded independently. Failed creation uses a non-exporting local span, other
deliveries are dropped, and failures are not recursively logged. Host callbacks must be
bounded and nonblocking: synchronous blocked JavaScript cannot be isolated. The promise is
unchanged commit/retry/halt decisions, not wall-clock immunity.

### E.3 Signal interpretation

| span | operation-specific interpretation beyond the vocabulary in §14 |
|---|---|
| `echo.enqueue` | transaction through commit; duplicate is not saved; interruption does not imply a masked commit rolled back |
| `echo.push` | one nonempty attempted batch including credentials and response bookkeeping; empty/halted cycles emit none; each retry is new |
| `echo.pull` | one set attempt through install, including resumed staging and internal resnapshots; success counts the installed set plus PullDone, failure the last observed set including staging |
| `echo.install` | transaction after PullDone, even empty; push halt does not suppress it |
| `echo.rebase` | inside install or push-covered release; completion alone is not proof of commit; empty replay counts zero |
| `echo.pokes` | credential preparation through first chunk or establishment failure; later termination uses the reconnect counter |
| `echo.leader.election` | lock wait ends elected before database open; interrupted candidates are not elected |
| `echo.leader.handover` | surviving observer's loss/recovery through replacement; no dead-worker tenure span |
| `echo.router.handover` | accepted live announcement; stale/duplicate announcements emit none |
| `echo.server.startup` | prepare through verification; Drift is server, idempotent startup ok |
| `echo.server.push` | authenticated batch including internal retries; empty batch ok with zero counts |
| `echo.server.pull` | authenticated page, result page/done; empty done still done |
| `echo.server.pokes` | subscription, clean end server-end and teardown interrupted |

There are no presence-operation or per-mutation server spans and no independent install/rebase
retry policy. Classification describes the operation, not loop policy: the whole final push
cause can include an outer cleanup failure that halts a loop after a transient batch span.
`echo.result` is the successful operation result; `echo.reason` distinguishes idle timeout
from clean end. The leader-loss counter is per surviving observer, not globally exactly once.
Without a host runtime attribute the pending gauge is last-writer across workers, not a sum.
Batch histograms include credential failures before send; page histograms count only installed
sets. Timers include failure/interruption and Effect adds `time_unit=milliseconds`.

Routine log levels: unknown-table skips, push/pull retry, presence retry and pokes reconnect
are Debug; integrity halt, candidate failure and router/tab watcher/acceptance failures are
Error; pending-count restart, revocation-watch restart and interrupted-reset completion are
Warn. Interrupt-related lines are always Debug. Only the shared routine/diagnostics helpers
call `Effect.log*` directly.

`test/observability.e2e.test.ts` covers real SQLite/Postgres HTTP with typed/defect canaries,
`tracerLogger`, metrics/hierarchy, 999/1000 ms retry boundaries, diagnostics, direct maintenance
and unrelated routes. Subscription closure from host context checks protected cleanup and
interrupted/defect outcomes; removing either context capture makes that regression fail.
The browser observability script separately exercises OPFS, worker RPC, inherited attributes,
late callbacks and lock release; it does not replace the substrate/slice gates.
