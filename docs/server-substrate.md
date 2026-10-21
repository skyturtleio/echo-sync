# Server substrate (Postgres)

`src/server/postgres` implements DESIGN.md §4, §6, §7, §8 on plain Postgres, against core's types
(`import { … } from "echo-sync"`). Synced tables are `SyncTable`s: the app creates each relation
from `table.postgres.createTable` and calls `attach(table)`. A mutation body is an
`Effect<void, MutationRejection | StoreUnavailable | SqlError, R>`; the RPC adapter builds it from
`Mutation.run` over `Store.make(RowStore)`. The ledger envelope is
`{ principalId, clientId, mutationId: string, seq: bigint }` (DESIGN §6): `mutationId` is the
unordered idempotency key, `seq` the client's outbox sequence.

| module | what it owns |
|---|---|
| `migrations/001_engine.ts`, `002_grant_id_check.ts`, `schema.ts` | engine schema (run via `PgMigrator`, history in `echo_sync.migrations`), `attach(SyncTable)`, catalog discovery, `resolveTables(registry)` |
| `ledger.ts` | `runMutation`, `runBatch`, client↔principal binding |
| `pull.ts` | `pull(registry, request)` (one page per call), the continuation |
| `rowStore.ts` | core's `RowStore` on Postgres; declared-column projection and row codec |
| `rpc.ts` | `handlers` / `presenceHandlers` / `layerHttp`: the RPC groups over HTTP POST + NDJSON, composed from transport-neutral steps: `principalFor` (authorize), `executeBatch` (push for an actor), `pull` (pull.ts), `serverFailures` (pull/pokes failure mapping) |
| `gc.ts` | `collectGarbage(retention)`, `expireLedger({ retention })` |

For how a consumer app wires the server (registry, mutations, app migrations, `Startup.layer`,
principal, Drift), see [getting-started.md](getting-started.md).

## Wire rows: declared columns through the table's codec

Every row that leaves the server, from a walk or from the log, is `SELECT <declared columns>`
decoded through `table.postgres.row` (DESIGN §11). Its decoded side is the wire row: snake_case
column keys, timestamps as epoch ms, int8 as a number, jsonb as is, every declared key present.
A column the app added to the relation but not to the SyncTable never leaves the database, and
there is one definition of the encoding (core's). Writes go the other way: each wire value is
encoded through `column.postgres`, jsonb bound with `PgTypes.jsonb`. A stored row that fails to
decode is a defect. Tested: a table with a timestamp and an undeclared column (walk and log
paths), and core's `Conformance.rowStore` suite (`conformance.test.ts`): seeded random range
plans against `Store.planMatches`/`planOrder`, exact read/write round-trips, and all-or-nothing
transactions under failure, defect and interruption at every boundary, on attached tables.

Only registry tables that are attached are synced (`resolveTables`). A captured relation outside
the registry is skipped; its log entries arrive as tombstones, never as payloads.

## RPC adapter

`layerHttp({ registry, mutations, principalFor, path = "/sync" })` mounts `Protocol.SyncRpcs`
and `Protocol.PresenceRpcs` (one merged group, one path, each group's handlers provided with
`Layer.provide`) with `RpcServer.layerHttp({ protocol: "http" })` and `RpcSerialization.layerNdjson`, the
transport the browser speaks. `principalFor(headers)` is the app's authentication; the principal
is never read from the payload. It runs on every push and pull, and once when a stream opens
(`pokes`, presence `room`; `track` is a plain call and runs it too). A credential that expires
while a stream is open is therefore not detected until that stream reconnects; the client's
`idleTimeout` and the server's heartbeat bound how long a dead connection stays open, not how long
an expired token is honoured on a live one. The client evaluates `syncHeaders` per request (DESIGN
§9.1 Credentials), so a reconnect carries the renewed credential. `ClientForbidden` becomes `Unauthorized`.

- push: one SERIALIZABLE ledger transaction per mutation (read-modify-write handlers would lose
  updates under READ COMMITTED; `40001` retries the whole attempt), `roleFor` read from `scope_grants` (own grant or
  `*`) inside it, `now` from the Clock, `random` = `seededRandom(clientId, mutationId)`. Every
  infrastructure failure (connection, pool, `40001` after the bounded retry, the catalog read
  before the batch) is a short results list, never a defect: the client halts push on any
  defect (§6). Tested with a real connection kill mid-batch over HTTP.
- pull and pokes classify a failure over its whole `Cause` (§6): any non-SQL defect keeps the
  whole cause as defects; otherwise a `SqlError` anywhere, as a failure or as the defect the
  pinned client's `orDie` ROLLBACK raises on a dead connection, becomes
  `Protocol.ServerUnavailable`; otherwise typed failures and interrupts pass through. A defect
  from the server therefore always means a bug. Push applies the same classification to the
  catalog read before the batch (a SQL-only cause is an empty result list). Tested over HTTP: an
  injected SqlError, a backend killed before ROLLBACK of a failed pull, and Fail(SqlError) plus a
  programmer defect (must stay a defect).
- pokes: `LISTEN echo_sync`. `next_position()` runs `pg_notify('echo_sync', '')`, which Postgres
  delivers at commit and coalesces per transaction. The stream carries no data.
  **One `LISTEN` per server process** (DESIGN §6): `PgClient.listen` pins a pool connection for
  its scope, so a `LISTEN` per stream would exhaust the pool at `maxConnections` open streams and
  block every push and pull. `layerNotifications` owns the single `LISTEN` (`sharedPokes` in
  rpc.ts, provided as the `Notifications` service to both the sync and the presence handlers)
  and fans notifications out through a sliding `PubSub` of capacity 1. A stream subscribes first,
  then emits its initial poke. If no `LISTEN` is confirmed, a new stream fails `ServerUnavailable`.
  A lost `LISTEN` connection ends every open stream with `ServerUnavailable` (tested by
  terminating the listener's backend), and the listener re-establishes it with backoff
  (exponential from 50 ms, capped at 5 s, reset once a `LISTEN` is confirmed, not configurable).
  Each attempt runs in its own scope, so an attempt that fails after reserving its connection
  releases it before the backoff. Clients reconnect on any end.
  **Liveness** (DESIGN §6): the element is `poke | heartbeat`, the first one a poke. While a
  `LISTEN` is confirmed, its session ticks every `heartbeatInterval` (`SyncServerOptions`, default
  15 s) into a second sliding `PubSub`, and every subscribed stream emits a `heartbeat` per tick, so
  an idle stream is never silent longer than one interval. It is a separate `PubSub` because a tick
  sliding over a not-yet-taken notification in one capacity-1 queue would lose the poke. Heartbeats
  are a fixed cadence, not "after an interval without a poke": a busy stream also gets them, which
  costs one small frame per interval. The client wakes only on `poke` and ends a stream that
  delivers nothing for `idleTimeout` (`LeaderOptions`, default 45 s).
- presence (DESIGN §12, `presence.ts` plus `presenceHandlers` in rpc.ts): process-local, no
  tables. A `Ref` of rooms, each a map of entries plus a sliding `PubSub<void>` of capacity 1;
  a room exists while it has an entry. `room` checks `roleFor` (any role, the principal's or
  `*`, as for reads) before joining, so an unauthorized caller fails `Forbidden { room }` and
  never enters. The entry is added with `acquireRelease` in the stream's scope and removed when the
  stream ends for any reason; on HTTP that includes the client aborting its request. A stream
  subscribes before announcing its join, and each tick is turned into a fresh `RoomSnapshot`
  (entries ordered by principal, client, tab), so snapshots coalesce under load and never miss
  the latest state. As DESIGN §12 states: the payload carries `clientId` (the entry key needs it);
  the key also includes the principal, so naming another principal's client id cannot replace
  or end its entry; a rejoin with the same key replaces the entry and ends the older stream
  (a per-member `Deferred` the stream interrupts on, completed after the new member is
  installed), so every open stream holds an entry and keeps its room's `PubSub` current; the
  older stream's release does not remove the newer member (the same `Deferred` is its identity).
  `track`'s state update and its announcement are one uninterruptible step.
  Room streams carry the §6 heartbeat (DESIGN §16 item 13): the element is `snapshot |
  heartbeat`, the first one the join snapshot, and every stream emits a `heartbeat` every
  `heartbeatInterval` (the same `SyncServerOptions` field as pokes, default 15 s) from its own
  timer merged beside its change subscription, started only after the join snapshot has been
  emitted (so that snapshot is always the first element). A heartbeat therefore never slides out a pending
  change tick, is never a snapshot, and reads or changes no room state. `track` needs no grant lookup: it only
  changes an entry the same principal holds.
  **Revocation** (DESIGN §16 item 13): a watcher in `presenceHandlers` consumes its own
  `Notifications` subscription. On every poke (one per committed writing transaction, so every
  grant change, plus one when the subscription starts) it asks one query which `(room, principal)`
  pairs with an open stream still hold the scope (a `scope_grants` row for the principal or for
  `*`, the join predicate). For each pair that does not, `Presence.reevaluate` removes every
  entry of that principal in that room in one `Ref` update, fails each of their streams
  `Forbidden { room }` (the per-member `Deferred` now carries that failure; a replacement still
  succeeds it), and announces once, so the remaining members get one snapshot without them.
  A check holds an installation gate from reading the members until the removal, and `enter`
  takes it only around the install (interruptibly, never across `admit` or stream I/O), so no
  entry is installed mid-check: a principal's entries all leave in the same step, and an entry
  installed after the check (a rejoin after a re-grant the check's read predates) is judged by its
  own `admit`, not by that check. A snapshot read after an entry's removal is never emitted to its
  stream; one already read before it may still be delivered. The check runs on every commit rather
  than only on `scope_grants` entries: the pokes carry no data, and one indexed query per
  committed transaction (coalesced while one runs, skipped while no room is open) costs less
  than reading the log tail to filter. Join race: `room` checks the grant before installing the
  entry and again after (`join`'s `admit`, before announcing). A revoke committed before the
  second check's snapshot fails the join; one committed after it notifies a check that starts
  after the entry is installed. `admit` runs inside the stream's end boundary, so a revocation
  while it is still pending ends the stream `Forbidden` at once, and an end signal already
  completed when `admit` starts or returns takes precedence over its outcome (`Forbidden` for a
  revocation, empty for a replacement). A lost `LISTEN` or failed check restarts the watch after 1 s,
  and its first poke re-checks everything. A re-grant does nothing automatically; across a
  server restart presence state is gone anyway and a rejoin re-checks the grant.

## Engine state

- `position_seq`: one position space shared by log entries and ledger rows.
- `next_position()`: takes `pg_advisory_xact_lock(<"echosync" as int8>)`, then calls `nextval`.
  Every allocation goes through it: the row trigger, the ledger insert, and the trigger on
  `scope_grants`. The lock is held until commit, so the next allocation can only happen after
  this transaction is visible. Positions are therefore commit-ordered (§7). The cost is that
  writes to synced tables are serialized from their first captured row until commit, so keep
  transactions short.
- `log`: identity only, `(position, table_name, row_id, access_scope_id, op, txid, committed_at)`.
  `table_name` is the unqualified name, so `attach` refuses a second synced table with the same
  name in another schema (`TableNameConflict`).
- `attach` is atomic and serialized. It owns its transaction at READ COMMITTED (calling it
  inside a caller's transaction is a defect, DESIGN §4) and runs the name check and both trigger
  DDLs in it, behind a second advisory lock ("echosync" + 1, so it doesn't stall writers).
  READ COMMITTED matters: each statement after the lock gets a fresh snapshot, whereas an
  ambient REPEATABLE READ snapshot taken earlier would miss a concurrent attach that already
  committed (the reviewer's reproduction, now a test). Two concurrent attaches of one name can't both pass the check.
  A failure between the two DDLs leaves neither trigger, never a captured table that can still
  be truncated. Both cases are tested, with timing and failure injected through event triggers.
- `echo_sync_capture_changes()`: row trigger. A delete, a scope move, or a primary-key change
  writes a `delete` entry for the old identity and scope. An insert or update writes an `upsert`
  entry for the new one. A statement trigger makes `TRUNCATE` raise.
- `scope_grants` is core's system SyncTable. Migration 001 creates it from
  `ScopeGrants.postgres.createTable` / `createIndexes` (so `UNIQUE (access_scope_id,
  principal_id)` has one definition) inside `echo_sync`, then adds what the SyncTable cannot
  express: the FK to `access_scopes ON DELETE CASCADE`, a role CHECK, and a principal index. A
  later change to ScopeGrants' storage shape needs its own migration, like any SyncTable. It has
  the same trigger as any synced table, so a grant row is logged inside the scope it grants.
- `engine_state.gc_horizon`: the only engine fact that cannot be derived from other tables.
- `clients.expired_through` / `expired_position` (default 0): the §16.5 expiry floor and the
  position of the highest expired ledger row. Raised only by `expireLedger`; never deleted.
- `mutations.outcome` is `NULL` once expired (migration 003): the row's identity stays forever.
- `mutations.acked_at` (migration 003, `NULL` until stamped): when the client proved it installed
  the row's ack. See "Ledger expiry" below.
- `client_scopes(client_id, position, scope_ids)`: the set of scopes a client holds as of a
  completed pull. The reason this table exists is under "Pull" below.

### Scope creation

`ctx.store.createScope(scopeId)` (core Store, DESIGN §4) calls `StoreOptions.claimScope`. The
server's claim is `claimScope` in rpc.ts:
`INSERT INTO echo_sync.access_scopes … ON CONFLICT DO NOTHING RETURNING id`, run in the
mutation's transaction and savepoint. The `access_scopes` row therefore commits or rolls back
together with the owner grant. A rejected mutation leaves the id fresh, and an orphaned scope,
whose grants are all gone, keeps its row and is never fresh again.

Mutations run at SERIALIZABLE. A concurrent second creator blocks on the first creator's
uncommitted row. When the first commits, the second fails with 40001 and the ledger retries it.
On the retry the claim returns no row, so the outcome is `ScopeForbidden { required: "owner" }`.
The concurrent-creator test (scopeCreate.test.ts) asserts one `Applied`, one `ScopeForbidden`,
and three body attempts, which shows the retry path was taken.



Migration 002 adds `scope_grants_id_derived`: `id = char_length(access_scope_id) || ':' ||
access_scope_id || ':' || principal_id` (DESIGN §4, 5242acf). core's `grantId` prefixes the
code-point count (`[...scopeId].length`), which equals `char_length` in a UTF8 database, so core
and the CHECK derive the same id for every scope id. Under SQL_ASCII `char_length` counts bytes,
so 002 first refuses any server encoding other than UTF8. The constraint is validated, so a
database holding a non-derived grant fails 002 (23514) and startup stops until the row is
repaired.

`Migrate.verify` does not check that the constraint exists. It compares the catalog with the
registry's manifests, which carry no CHECKs, and `scope_grants`' other engine constraints (the
`access_scopes` FK, the role CHECK) are not verified either: engine-internal DDL is guaranteed by
the engine migration history (002 and its constraint commit in one transaction). Its only
undo is a hand-written `DROP CONSTRAINT`.

## Mutation ledger

`runMutation(envelope, body) → { outcome, position }` owns its transaction. Calling it inside an
ambient `withTransaction` would make it a savepoint whose outcome isn't committed when it
returns, so that is a defect (tested). In one transaction:

1. Upsert the `clients` row, which binds `client_id` to the principal on first sight (any other
   principal gets `ClientForbidden`) and row-locks the client until commit. Concurrent
   submissions from one client serialize here, so a duplicate waits for the first submission
   and then sees its ledger row. The body of a duplicate never runs, even in an aborted
   attempt, and an app-level unique violation in a duplicate's body can't mask the winner's
   outcome.
2. A known `(client_id, mutation_id)` ⇒ the recorded outcome and its original position, or, if
   its outcome expired, `Rejected{ClientExpired}` at that same position, whatever `seq` the
   attempt carries (checked before the floor, so two expired ids never share the floor's
   position).
3. An unknown id with `seq ≤ expired_through` ⇒ `Rejected{ClientExpired}` at `expired_position`.
   The body is not run, and nothing is recorded or allocated.
4. Otherwise: run the body inside a savepoint, so a rejection rolls back its writes;
   then insert the ledger row with `next_position()` `RETURNING position`; then commit. The
   ledger position is allocated after the body's log entries and under the same lock, so a
   mutation that writes nothing still gets an ack position.

Lock order is client row, then engine lock, everywhere, so the two can't deadlock.

`seq < 1` is a malformed envelope (core's `Protocol.MalformedEnvelope`), checked before anything else: nothing
runs and nothing is recorded. `runBatch` checks every item first, so one bad item rejects the
whole push. Without this guard, `seq ≤ 0` would fall under the default floor 0 and be answered
`ClientExpired` although it was never processed. Core's `Seq` already refuses it on the wire.

Failures are classified over the whole `Cause`, never by its first reason, with precedence
defect > transient > rejection (DESIGN §6):
- any defect or interruption anywhere ⇒ die with the full cause (typed failures turned into
  defects): never retried, never ledgered, never reported transient;
- otherwise any SQL failure or `StoreUnavailable` anywhere ⇒ transient. The transaction could not have committed, so
  a rejection in the same cause is not trusted. A conflict (`40001`, deadlock) is retried; when
  the retries run out, or the failure is a connection error, the batch stops as
  `MutationTransient` with nothing ledgered;
- a rejection is ledgered only when the body's cause consists solely of `MutationRejection`s.

One interpretation to note: a defect carrying a `SqlError` is the pinned `SqlClient`'s way of
reporting a failed savepoint or rollback cleanup on a dead connection. It counts as transient,
not as a defect, and it is not retried.

`Effect.retry` looks only at the first typed failure, so every failed attempt is wrapped with a
`retryable` flag computed over the whole cause before retry sees it. Tests: a real `40001` plus a
rejection retries and ledgers `Applied`; the same compound cause on every attempt stops the
batch with nothing ledgered; a `40001` plus a programmer defect dies after one attempt; both
orderings of SQL defect and programmer defect die.

`40001` and deadlocks are retried with a `Schedule` (default: 4 retries, exponential from
10 ms). Anything else becomes `MutationTransient`, which means *outcome unknown*, not "not
recorded". That covers any other `SqlError`, a conflict still failing after the retries, and
SqlError defects. The pinned `SqlClient` raises savepoint and rollback cleanup failures on a
dead connection as defects, and `runMutation` maps those to transient too (tested by killing
the backend from another connection after the body succeeds). A COMMIT whose reply is lost may
have committed; the client's resubmission then finds the ledger row. Exactly-once is therefore
a statement about committed effects: an aborted attempt may have run the body, but only a
committed one has visible effects, and there is at most one per identity.

`runBatch` runs mutations in order. Rejections are recorded and the batch continues. The first
transient failure stops the batch; the result reports the processed prefix (each with its
position) and the mutation it stopped at.

What the tests cover, and what they only simulate:
- **Lost response:** the same envelope is resubmitted after a completed call. This is a
  simulation; the response object is simply discarded.
- **Crash before commit:** real. The backend is killed during the body
  (`pg_terminate_backend(pg_backend_pid())`), or from another connection after the body. No
  writes or ledger row survive, and the retry applies once.
- **Crash after commit, before the reply:** not injected. It is equivalent to a lost response
  at the database level, because the ledger row is committed and the retry finds it.

## Pull

```
pull({ principalId, clientId, cursor: Position | null, continuation: Option<PullContinuation>, limit })
  → PullPage { changes, acks, continuation } | PullDone { changes, acks, nextCursor, expiredThrough }
  ! ResnapshotRequired { reason: gcHorizon | unknownCursor | staleContinuation }
```

Each call is one `REPEATABLE READ, READ ONLY` snapshot.

### Membership: a state diff, not a replay

The wire cursor is a bare `Position`. To decide what to purge and what to walk, the server needs
to know which scopes the client already holds. It looks that up in `client_scopes` at the cursor
position. At each `PullDone` it records the set it just completed under `nextCursor`. Each page
then:

1. purges (`scopePurged`) every held scope that is no longer readable;
2. starts a row walk for every readable scope that is not held. Initial sync is the same thing
   starting from an empty set, with the log position equal to `head` at the first page;
3. continues outstanding walks, keyset-paged by `(table name, id::text COLLATE "C")`;
4. once no walk is outstanding, reads log entries `> position` in readable scopes. Payloads
   come from the same snapshot. An entry whose row is gone, or whose row's current scope is not
   readable, becomes a `tombstone`, so no payload ever leaves a readable scope.

Why not replay grant log entries? A replay has to reconstruct readability at the cursor from
history, and history is lossy: the log stores identity only, GC deletes it, and a revoke plus a
re-grant between two pulls can cancel out. The first implementation tried a replay and diverged
under the random grant/revoke test. A state diff is correct by construction: "held" is an
explicit fact about what this client received, and "readable" is read from the current snapshot.

### No gap on grant

Suppose scope S becomes readable between the client's cursor `c` and a page. The page walks S's
current rows at its snapshot T. Every change to S that committed after T gets a position greater
than everything visible at T, because positions are commit-ordered. That includes positions
greater than or equal to `c`, and the log read after the walk starts at `c`. Changes before T
are already in the walked payloads; their log entries, if any are `> c`, re-deliver current
payloads, which is idempotent. If S is revoked mid-walk, the next page purges it. The tests for
this run a walk three rows per page while writers update, delete, insert, and move rows out of
the scope.

### The continuation

On the wire the continuation is core's opaque branded string. On the server it is
`Schema.fromJsonString(Continuation)` over a private struct, with positions as decimal strings:

```
{ clientId, principalId, cursor: Position | null, position: Position,
  scopes: string[], walking: [{ scopeId, after: { table, rowId } | null }] }
```

A plain JSON string survives structured clone and SQLite text storage unchanged. The client
resends it verbatim with the same cursor. It is untrusted input (DESIGN §8): every value is
checked against the request and the page's snapshot, not just decoded. These fail with
`ResnapshotRequired { reason: "staleContinuation" }`:

- a decode failure;
- a `clientId`, `principalId` or `cursor` that doesn't match the request;
- a `position` that is negative, below the originating cursor, or above this snapshot's head.
  A future position is never clamped or carried forward.

A `position` below the GC horizon fails with `gcHorizon`. The originating cursor must have a
`client_scopes` record even when resuming; otherwise the failure is `unknownCursor`.

The continuation is not signed. Payload filtering by the principal's current grants is what
prevents a leak; the checks above are what keep "one `nextCursor`, one held set" true for every
input. Without them, a forged future position would become `nextCursor`, and
`ON CONFLICT DO NOTHING` would keep whatever set reached that position first (the reviewer's
reproduction, now a test).

### Page size

`limit` is a server choice (an argument of `pull`, set by the RPC handler), not a wire field.
It bounds upserts and tombstones per page; purges are not counted.

### One `nextCursor`, one held set

`client_scopes` is written with `ON CONFLICT DO NOTHING`, so two `PullDone`s with the same
`nextCursor` must carry identical `scope_ids`. They do:

- At `PullDone` no walk is outstanding, so every readable scope has been either kept or walked
  and every unreadable one purged. The server records the readable set of that snapshot
  directly, never the set the request carried.
- At `PullDone`, `nextCursor` is that snapshot's `head` (`max` over log, ledger and GC horizon).
  Resumed positions are checked to be ≤ head, so no client-supplied value reaches `nextCursor`.
- The readable set is a function of the `scope_grants` rows. Every change to them, including
  cascades from `access_scopes`, goes through the capture trigger and so allocates a position.
  Positions are commit-ordered (§7). A snapshot whose head is `H` therefore sees every commit
  with a position ≤ `H` and none above it.
- So two snapshots with the same `head` see the same grant state, and the same readable set.

### Cursor `null` replaces the replica

After `ResnapshotRequired`, the log entries for revokes and deletes the client missed may be
gone. A set that started from cursor `null` is therefore installed with `RowStore.clearAll`
first (§8). The test replica does the same, and the GC test checks that a row whose tombstone
was collected disappears after the resnapshot.

### `expiredThrough`

`PullDone.expiredThrough` is the client's `clients.expired_through`, read in the page's snapshot.
It stays 0 unless the app runs `expireLedger`.

### Acks

Acks are ledger rows of this client in `(previous position, position]` for each page's log
advance. Over the whole set that is `(cursor, nextCursor]`. An initial sync acks everything up
to `head`.

### Lost responses and crash points

`client_scopes` is written after the read snapshot with `ON CONFLICT DO NOTHING`. Rows below
the cursor the client presents are pruned on that pull. Whatever cursor the client still holds
keeps its record until the client presents a newer one, so a lost `PullDone` or a crash between
those statements converges on retry. A cursor with no record is refused (`unknownCursor`),
never trusted. For the bigints: `@effect/sql-pg` 4.0.5 already decodes `int8` to `bigint`
(verified), so no extra decode is needed on the server side.

## GC

`collectGarbage(retention)` deletes log entries whose `committed_at` is older than
`Clock.now - retention` and raises `gc_horizon` to the highest deleted position, in one
transaction. It never touches the ledger, `clients`, or `client_scopes` (tested). §16.5: a
resnapshot acks only the ledger rows that survive; ledger rows have their own policy below.
A cursor or continuation below the horizon gets `ResnapshotRequired { reason: "gcHorizon" }`.

### Ledger expiry (DESIGN §16 item 5)

Expiry reclaims outcome payloads, not rows. A ledger row's identity `(client_id, mutation_id,
seq, position)` is kept forever; only its `outcome` (and `acked_at`) is cleared.

- **Ack stamp.** A pull that presents a cursor (accepted: the read snapshot found its
  `client_scopes` record) sets `acked_at = now()` on this client's rows with `position ≤ cursor`,
  an outcome, and no stamp yet, after the snapshot, beside the other idempotent post-pull
  statements. A presented cursor proves the client installed every ack at or below it (§8: acks
  for `(cursor, nextCursor]` arrive in the set that produced the cursor). A row never stamped is
  never expired, whatever its age; an expired row is never stamped again.
- **`expireLedger({ retention })`** (default 7 days; a retention below 48 h fails the Effect with
  a typed `LedgerRetentionTooShort` before any SQL, so a scheduled job fails on its first run and
  can `catchTag` it) is one run, by the Effect `Clock` like
  `collectGarbage`; the app schedules it (`examples/server` runs it hourly). One READ COMMITTED
  transaction sets `outcome = NULL, acked_at = NULL` on every row with an outcome and
  `acked_at ≤ now − retention`, and per client raises `expired_through` to the highest cleared
  `seq` and `expired_position` to the highest cleared position, both with `GREATEST`, so they only
  move forward. It then checks that no row of a raised client at `seq ≤` its floor still has an
  outcome and dies if one does, rolling back the clear and the floor (tested with a forged
  non-prefix). It returns `{ clients, expired }`. A ledger transaction holds the client row
  `FOR UPDATE`, so it sees the floor before or after a job, never half of one.
- **Push.** A known `(client_id, mutation_id)` whose outcome was cleared is answered
  `Rejected{ClientExpired}` at that row's own `position` and is not executed, whatever `seq` the
  attempt carries; this is checked before the floor, which answers (at `expired_position`) only
  an id it has no row for at `seq ≤ expired_through`. A tab's `enqueue` retried after its reply was lost can arrive after any delay
  (`LeaderLost` is retried indefinitely, and a tab can be suspended for days), after its
  released-row marker was pruned, so the outbox gives it a fresh `seq` above the floor; only the
  kept identity recognises it. No retention value would bound that delay (DESIGN §16 item 5).
- **Pull.** Acks carry only rows that still have an outcome: an expired row is never acked again.
  A device that lost an ack to expiry (its storage went back in time: a restored backup, a copied
  profile) is told by `expiredThrough ≥` that seq and releases the mutation as `OutcomeUnknown`.
- **Client release.** A `Rejected{ClientExpired}` push result is released on receipt whatever its
  position, with an `OutcomeUnknown` notice (DESIGN §8, second release corollary; `Engine.ts`):
  an expired identity is never acked, and after a replica rebuild (null cursor) the first
  corollary (`position ≤ cursor`) could never release it.
- **Reconcile simulation** (40 seeds × 400 steps, naive ledger GC = `gcLedger` by position with
  the log, ignoring acks; each seed counted by its first violated invariant). Rows before the
  identity ruling were measured at 832c948; the last row is this branch, now the committed test.

  | ledger GC model | floor + `expiredThrough` | late retries of pruned ids | double apply | never released | pass |
  | --- | --- | --- | --- | --- | --- |
  | rows deleted | no | off | 14 | 23 | 3 |
  | rows deleted | no | on | 23 | 14 | 3 |
  | rows deleted | yes | off | 0 | 0 | 40 |
  | rows deleted | yes | on | 13 | 0 | 27 |
  | identities kept, no client release rule | yes | on | 0 | 3 | 37 |
  | identities kept + ClientExpired released on receipt | yes | on (committed) | 0 | 0 | 40 |

  `SIM_SEEDS=120` also passes 120/120. The time-dependent shape (a retry days after the ack) is
  the Postgres E2E above.
- **Head.** Pull's head is the greatest of the GC horizon, the log's and the ledger's highest
  positions, and `max(clients.expired_position)`, so expiring the newest ledger row never moves
  a cursor or a resumed page set back (tested).
- **Cost.** Every mutation leaves its identity row (about 50–80 bytes with its index entries)
  forever; the outcome `jsonb` is what is reclaimed. Floors and identities are never deleted;
  forgetting idle clients is out of scope (DESIGN §16 item 5).
- **Tested** (gc.test.ts, test/expiry.e2e.test.ts): resubmission at the same or a fresh `seq`
  after expiry answers `ClientExpired` at the original position with one execution; young acked
  and unacked rows keep their outcomes; floor, `expired_position` and `expiredThrough`; the
  prefix assertion; a run with nothing eligible changes nothing; pre-003 rows are never expired
  until acked; and end to end with the real Engine, both the restored-backup case and the
  reviewer's ten-day lost-reply sequence (enqueue committed, reply lost, pushed and acked, marker
  pruned after 24 h, row expired after 7 days, retry under `seq` 2 ⇒ executed once, an
  `OutcomeUnknown` notice, outbox drained).

## Running the Postgres tests

```
docker run -d --name echo-pg -p 5432:5432 -e POSTGRES_PASSWORD=echo postgres:17
bun run test:pg          # ECHO_PG_URL defaults to postgres://postgres:echo@localhost:5432/postgres
```

The server requires PostgreSQL 15+ (DESIGN §11: the startup verifier reads
`pg_index.indnullsnotdistinct`). The engine schema alone needs only PG14 (`CREATE OR REPLACE
TRIGGER`).

Each test creates a fresh database and drops it afterwards. Without `ECHO_PG_URL` the suites are
skipped, so `bun run check` works with no database.

The ordering test keeps a variant whose `next_position()` has no lock. That variant asserts the
reader *does* miss changes, so the invariant test is shown to be able to fail. Mutation checks
done by hand while building this:

| deliberate bug | caught by |
|---|---|
| no walk for newly granted scopes | grant test, random scopes test |
| no current-scope check on log payloads | random scopes test (leak) |
| no savepoint around the mutation body | rejected-mutation test |
| client registered outside the mutation transaction (no per-client lock) | concurrent-duplicates test (body ran more than once) |
| no SqlError-defect → transient mapping | savepoint-cleanup-failure test |
| no future-position check on continuations | forged-continuation test |
| no advisory lock in `attach` | concurrent-attach test |
| `attach` DDLs not in one transaction | incomplete-attach test |
| `attach` accepts an ambient transaction / no READ COMMITTED | REPEATABLE READ attach test |
| outer `catchTag` + `catchDefect` instead of whole-cause classification | compound-cause test |
| `catchTag` on the body's rejection | rejection-plus-defect test |
| no `seq ≥ 1` guard | malformed-envelope test |
| retry only when every reason is a conflict | conflict-plus-rejection retry and exhaustion tests |
| retry despite a defect in the cause | conflict-plus-defect test |
| `SELECT t.*` / undecoded payloads | wire-form test, every scope test |
| `ASC NULLS LAST`, or jsonb not bound as jsonb | Conformance.rowStore (range, read/write) |
| a transient stop turned into a die | connection-lost-mid-batch RPC test |
| mutation transaction at READ COMMITTED | two-client increment and same-id insert tests |
| RowStore defaulting an unmapped table to `public` | unattached-table test |
| unquoted regclass lookup in `attach` | `"Public"` schema attach test |
| tag-only `SqlError` mapping on pull | rollback-connection-loss and SQL-plus-defect RPC tests |
| SqlError cleanup defects treated as defects | connection-lost-mid-batch RPC test |
