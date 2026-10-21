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
| `migrations/001_engine.ts`, `schema.ts` | engine schema (run via `PgMigrator`, history in `echo_sync.migrations`), `attach(SyncTable)`, catalog discovery, `resolveTables(registry)` |
| `ledger.ts` | `runMutation`, `runBatch`, client↔principal binding |
| `pull.ts` | `pull(registry, request)` (one page per call), the continuation |
| `rowStore.ts` | core's `RowStore` on Postgres; declared-column projection and row codec |
| `rpc.ts` | `handlers` / `layerHttp`: `Protocol.SyncRpcs` over HTTP POST + NDJSON |
| `gc.ts` | `collectGarbage(retention)` |

## Wire rows: declared columns through the table's codec

Every row that leaves the server, from a walk or from the log, is `SELECT <declared columns>`
decoded through `table.postgres.row` (DESIGN §11). Its decoded side is the wire row: snake_case
column keys, timestamps as epoch ms, int8 as a number, jsonb as is, every declared key present.
A column the app added to the relation but not to the SyncTable never leaves the database, and
there is one definition of the encoding (core's). Writes go the other way: each wire value is
encoded through `column.postgres`, jsonb bound with `PgTypes.jsonb`. A stored row that fails to
decode is a defect. Tested: a table with a timestamp and an undeclared column (walk and log
paths), and `RowStore.range` against core's reference predicate and order (code-point text,
NULLS FIRST ascending).

Only registry tables that are attached are synced (`resolveTables`). A captured relation outside
the registry is skipped; its log entries arrive as tombstones, never as payloads.

## RPC adapter

`layerHttp({ registry, mutations, principalFor, path = "/sync" })` mounts `Protocol.SyncRpcs`
with `RpcServer.layerHttp({ protocol: "http" })` and `RpcSerialization.layerNdjson`, the
transport the browser speaks. `principalFor(headers)` is the app's authentication; the principal
is never read from the payload. `ClientForbidden` becomes `Unauthorized`.

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
  delivers at commit and coalesces per transaction. The stream carries no data. A lost LISTEN
  connection ends it with `ServerUnavailable` (tested by terminating the listener's backend); the
  client reconnects on any end.

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
  position of the highest expired ledger row. No v1 job raises them.
- `client_scopes(client_id, position, scope_ids)`: the set of scopes a client holds as of a
  completed pull. The reason this table exists is under "Pull" below.

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
2. `seq ≤ expired_through` ⇒ `Rejected{ClientExpired}` at `expired_position`. The body is not
   run, and nothing is recorded or allocated.
3. A known `(client_id, mutation_id)` ⇒ the recorded outcome and its original position.
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
It is always 0 in v1.

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
resnapshot acks only the ledger rows that survive. Expiring ledger rows while mutations are
still pending would therefore re-execute them. Any future expiry must raise
`clients.expired_through` / `expired_position` in the same transaction that deletes ledger rows.
A cursor or continuation below the horizon gets `ResnapshotRequired { reason: "gcHorizon" }`.

## Running the Postgres tests

```
docker run -d --name echo-pg -p 5432:5432 -e POSTGRES_PASSWORD=echo postgres:17
bun run test:pg          # ECHO_PG_URL defaults to postgres://postgres:echo@localhost:5432/postgres
```

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
| `ASC NULLS LAST`, or jsonb not bound as jsonb | RowStore range test |
| a transient stop turned into a die | connection-lost-mid-batch RPC test |
| mutation transaction at READ COMMITTED | two-client increment and same-id insert tests |
| RowStore defaulting an unmapped table to `public` | unattached-table test |
| unquoted regclass lookup in `attach` | `"Public"` schema attach test |
| tag-only `SqlError` mapping on pull | rollback-connection-loss and SQL-plus-defect RPC tests |
| SqlError cleanup defects treated as defects | connection-lost-mid-batch RPC test |
