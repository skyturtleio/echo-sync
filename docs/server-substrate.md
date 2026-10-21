# Server substrate (Postgres)

`src/server/postgres` implements DESIGN.md §4, §6, §7, §8 on plain Postgres. It does not depend
on the core types yet. It works on table names and rows shaped
`id text primary key, access_scope_id text not null, …`. A mutation body is an
`Effect<void, MutationRejected | SqlError, R>`.

| module | what it owns |
|---|---|
| `migrations/001_engine.ts`, `schema.ts` | engine schema (run via `PgMigrator`, history in `echo_sync.migrations`), `attach(table)`, catalog discovery of synced tables |
| `ledger.ts` | `runMutation`, `runBatch`, client↔principal binding |
| `pull.ts` | `pull` (one page per call), `PullContinuation` |
| `gc.ts` | `collectGarbage(retention)` |

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
- `echo_sync_capture_changes()`: row trigger. A delete, a scope move, or a primary-key change
  writes a `delete` entry for the old identity and scope. An insert or update writes an `upsert`
  entry for the new one. A statement trigger makes `TRUNCATE` raise.
- `scope_grants` is a synced table (DESIGN v0.2). Its id is derived by core from
  `(access_scope_id, principal_id)`, which is also unique here. It has the same trigger as any
  synced table, so a grant row is logged inside the scope it grants.
- `engine_state.gc_horizon`: the only engine fact that cannot be derived from other tables.
- `client_scopes(client_id, position, scope_ids)`: the set of scopes a client holds as of a
  completed pull. The reason this table exists is under "Pull" below.

## Mutation ledger

`runMutation(envelope, body)`:

1. Bind `client_id` to the principal on first sight. Any other principal later gets
   `ClientForbidden`, so one principal can never read another's acks or replay its mutation ids.
2. If `(client_id, mutation_id)` is already in the ledger, return the recorded outcome without
   running the body.
3. Otherwise, in one transaction: run the body inside a savepoint, so a `MutationRejected` rolls
   back its writes; then insert the ledger row with `next_position()`; then commit. The ledger
   position is allocated after the body's log entries and under the same lock, so a mutation that
   writes nothing still gets an ack position.
4. `40001` and deadlocks are retried with a `Schedule` (default: 4 retries, exponential backoff
   from 10 ms). Two concurrent submissions of the same envelope race on the ledger primary key;
   the loser returns the winner's recorded outcome. Any other `SqlError`, or a conflict still
   failing after the retries, becomes `MutationTransient` and nothing is recorded.

`runBatch` runs mutations in order. Rejections are recorded and the batch continues. The first
transient failure stops the batch; the result reports the processed prefix and the mutation it
stopped at.

Crash points: if the process dies before commit, the transaction rolls back, so there are no
writes and no ledger row, and the retry executes it. If it dies after commit, the retry finds
the ledger row. Both cases are tested; a killed backend stands in for the crash.

## Pull

```
pull({ principalId, clientId, cursor: Position | null, continuation: Option<PullContinuation>, limit })
  → PullPage { changes, acks, continuation } | PullDone { changes, acks, nextCursor }
  ! ResnapshotRequired { reason: gcHorizon | unknownCursor | staleContinuation }
```

Each call is one `REPEATABLE READ, READ ONLY` snapshot.

### Membership: a state diff, not a replay

The wire cursor is a bare `Position`. To decide what to purge and what to walk, the server needs
to know which scopes the client already holds. It looks that up in `client_scopes` at the cursor
position. At each `PullDone` it records the set it just completed under `nextCursor`. Each page
then:

1. purges (`ScopePurged`) every held scope that is no longer readable;
2. starts a row walk for every readable scope that is not held. Initial sync is the same thing
   starting from an empty set, with the log position equal to `head` at the first page;
3. continues outstanding walks, keyset-paged by `(table name, id::text COLLATE "C")`;
4. once no walk is outstanding, reads log entries `> position` in readable scopes. Payloads
   come from the same snapshot. An entry whose row is gone, or whose row's current scope is not
   readable, becomes a `Delete`, so no payload ever leaves a readable scope.

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

`PullContinuation` is a Schema'd value, encoded with positions as decimal strings:

```
{ clientId, principalId, cursor: Position | null, position: Position,
  scopes: string[], walking: [{ scopeId, after: { table, rowId } | null }] }
```

The client treats it as opaque and resends it. The server checks `clientId`, `principalId` and
`cursor` against the request; a mismatch is `staleContinuation`. A `position` below the GC
horizon is `gcHorizon`. The continuation is not signed, and does not need to be: every payload
is filtered by the request principal's grants as of this page, so a forged continuation can
only make the forger skip or re-receive its own data.

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
transaction. The ledger and `client_scopes` are not touched; their retention is §16.5. A cursor
or continuation below the horizon gets `ResnapshotRequired { reason: "gcHorizon" }`.

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
