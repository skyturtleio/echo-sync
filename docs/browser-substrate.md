# Browser substrate (`src/client/worker`, `src/client/Outbox.ts`)

Implements DESIGN §9's single-owner topology and the durable outbox primitive. Verified in headless
Chromium with the scripted scenarios at the end of this document.

## Topology

```
tab A ──spawns──▶ leader-candidate worker A ──holds──▶ Web Lock echo:<app>:<profile>:leader
  │                       │ owns OPFS pool echo-sync/<app>/<profile>/  (SQLite db echo-sync/<app>/<profile>.db)
  │ Hello / Leader        │ serves LeaderRpcs on one MessagePort per tab
  ▼                       │
router SharedWorker ──Connect{port}──▶ leader;   ──Connected{port}──▶ every tab
  ▲
tab B ──spawns──▶ leader-candidate worker B (queued on the leader lock; never opens the db)
```

- Every tab spawns one **leader-candidate** dedicated worker. Exactly one holds the exclusive Web Lock
  `echo:<appId>:<profile>:leader`; only the holder opens the database. Candidates that lose queue on
  the lock, so the next leader is chosen by the browser the moment the current holder dies.
- The leader also holds a per-incarnation **epoch lock** `…:leader:<leaderId>` and every tab holds a
  **tab lock** `…:tab:<tabId>` for its lifetime. Death is detected by acquiring these in shared mode,
  never by timeouts or by waiting on a port (a dead `MessagePort` is silent).
- The **router** SharedWorker never touches storage. It remembers the current leader's control port and
  creates a fresh `MessageChannel` per tab: one end to the leader (`Connect`), one to the tab
  (`Connected`). When the leader changes, it relinks every live tab.
- Tab↔leader traffic is Effect RPC (`RpcClient.layerProtocolWorker` / `RpcServer.layerProtocolWorkerRunner`
  over `BrowserWorker` / `BrowserWorkerRunner.layerMessagePort`) for the Schema-defined `LeaderRpcs`:
  `exec` (only with `allowRawExec`), `enqueue`, `subscribe` (registered query, stream), `subscribeRaw`
  (raw live query under `query_only`, stream), `notices` (stream), `dismissNotices`, `leaderInfo`.
- Workers are spawned by the **tab**, never by the SharedWorker: no nested workers are needed (Safari
  shipped workers-in-SharedWorker late), and a candidate's lifetime is its tab's.

### Why not `OpfsWorker.run` (deviation from DESIGN §9 wording)

`@effect/sql-sqlite-wasm`'s `OpfsWorker.run` registers AccessHandlePoolVFS under the fixed name `"opfs"`.
That VFS uses its name as its OPFS directory and opens an **exclusive** sync access handle on every file
in it. So all databases on the origin share one pool, and a live leader for `app=a` made the leader for
`app=b` fail with `NoModificationAllowedError: Access Handles cannot be created if there is another open
Access Handle` (reproduced). `OpfsSqlite.ts` is a ~100-line host that speaks the same port protocol
(so the stock `SqliteClient.layer({ worker, installReactivityHooks: true })` drives it) but uses a
per-identity pool directory `echo-sync/<appId>/<profile>`. The protocol is internal to that package:
re-verify `OpfsSqlite.ts` on every `@effect/sql-sqlite-wasm` bump.

Before opening, a new leader waits until every file in its pool accepts a sync access handle: Chromium
can release a dead worker's handles slightly after its locks. After opening the pool it tops it up to
6 files: the VFS only adds capacity to an *empty* directory, so a candidate that died while creating
its pool left the next leader with one file and `SQLITE_CANTOPEN` on its first write (found by the
OPFS-startup-failure scenario).

`OpfsSqlite.open` performs every fallible step (wasm load, pool, `open_v2`) before anything waits on the
port protocol. `SqliteClient` waits for the host's "ready" uninterruptibly; with `OpfsWorker.run` a host
that failed to open never sent it, so a failed leader hung forever holding its locks (review P1,
reproduced with `?app=.`).

## Liveness: a candidate that cannot serve (DESIGN §9)

- Any startup failure (identity rejected, capability missing, pool or database cannot open, migration
  fails) closes the candidate's scope, which releases the leader and epoch locks, reports `Failed` to its
  tab, and then ends the worker (`self.close()`), so nothing a half-finished open acquired outside the
  scope survives. The next queued candidate is elected.
- Every lock wait is interruptible: `holdLock` aborts its queued Web Locks request on interruption, and a
  grant that races with the abort is released immediately, so a cancelled waiter never ends up holding
  the lock. Web Locks rejections surface as a typed `LockError` instead of being swallowed.
- Known limitation (review, nonblocking): lock-cancellation safety relies on Effect's default
  scheduler, which resumes a granted `request` and enters `holdUntilScopeCloses`' uninterruptible
  region synchronously. A custom scheduler that yields between the grant's `resume` and that region
  (or before `awaitLockReleased` releases its shared grant) could, on interruption, leave a granted lock
  held until the agent dies. Making it scheduler-independent means keeping the grant's resolver for the
  interruption cleanup and returning immediately from the shared-watcher callback; deferred.
- Identity segments that are all dots (`.`, `..`) are rejected in the tab before anything is spawned.

## Stale announcements

The router accepts a `Leader` announcement only if that leader's epoch lock is held **in exclusive
mode** when the announcement is processed (`isLockHeld`; shared holders are `awaitLockReleased`
watchers, which briefly hold a dead leader's epoch right after it dies, and must not count), and processes announcements one at a time. A dead leader's announcement that
arrives after its successor's (the relaying tab can be slow) is dropped instead of replacing the live
route. On the tab side every connection is an object; its handshake and epoch watcher only act while it
is still the active connection, and a displaced connection completes its `lost` signal (typed
`LeaderLost` to every in-flight call) before its RPC scope is closed.

## Handover semantics (DESIGN §16 item 1)

| What | Behavior | Why |
|---|---|---|
| Request in flight when the leader dies | Fails with `LeaderUnavailable{reason:"LeaderLost"}` | The tab cannot know whether it committed. |
| `mutate` after `LeaderLost` | Retried automatically with the id fixed before the first attempt, so exactly one outbox row | `LeaderClient.mutate` (DESIGN §9.1); tested in `Leader.test.ts` with the loss after the commit. |
| `enqueue` retry after `LeaderLost` | Safe: idempotent on `mutationId`, pending or released (returns the existing `seq`, allocates none) | Verified across a real handover and at the commit-before-lost-response crash point, with two tabs retrying the same id at once. |
| `exec` retry | Only if the statement is idempotent | Raw SQL, harness/diagnostics only. |
| Request issued while no leader is connected | Waits for the next leader | Leadership gaps are short (lock queue). |
| Open `subscribe` (registered query), `notices` or `syncStatus` stream | **Resumes** on the next leader; nothing is emitted in between, so the consumer keeps the last emission until the fresh first one | DESIGN §9.1; see below. |
| `requestSync` after `LeaderLost` | Retried like `mutate`; a wake is idempotent (coalesced into the sliding wake queue) | DESIGN §9.1. |
| Open `subscribeRaw` stream | **Terminates** with `LeaderUnavailable{LeaderLost}`; the caller re-subscribes | See below. |
| Replica / outbox | Persist (same OPFS file; the new leader reopens it) | Verified. |

The server side of a stream (its `Reactivity` registration) dies with the old leader, and the new leader
cannot know which result version the subscriber last saw. A resumed stream therefore never tries to
continue: the tab re-subscribes and the new leader sends a fresh, complete first result, which may repeat
the last value (never skip a change). `subscribe`, `notices` and `syncStatus` do this inside `LeaderClient`
(their results are whole values, so a repeat is harmless); the raw `subscribeRaw` stream still ends with
`LeaderLost` and leaves it to the caller (the harness scenarios assert that end).

### Closing the tab client's scope (DESIGN §9.1)

`LeaderClient.make`'s only finalizer for the leader is `worker.terminate()` on this tab's own leader
candidate (the harness closes the scope on `pagehide`). `terminate()` stops the worker's event loop at
once: no Effect finalizer in the worker runs, so nothing drains, flushes or freezes. What that means for a
push in flight at that moment:

- The outbox entry was committed before `enqueue` resolved, so it is still pending in OPFS. The browser
  releases the dead worker's Web Locks and access handles, the next queued candidate becomes leader and
  reopens the same file.
- The worker's HTTP push request is abandoned with it. The server may have committed it (ledger row, no
  ack delivered) or not (its transaction rolls back with the dropped connection, or never started).
- The next leader resumes reconciliation from the persisted state: it pushes the unreleased entries
  above `pushedThrough` (which only advances after a push response was received, so an entry whose
  response was lost is pushed again). If the first push committed, the ledger answers the repeat with
  the recorded outcome and position (§9 idempotency); otherwise it applies now. Execution on the server
  is exactly once; ack delivery is not (the same outcome may arrive in a push response and in a pull).
  The entry stays pending, and counted in `pendingMutations`, until the pull that installs its ack
  releases it (§8 backstop).

Evidence: slice scenario S5 (`slice.sh`, "leader closed while its push is held on the server") closes the
owning tab while its push is parked at a server gate inside the ledger transaction. The successor sends
its own push while the entry is still pending, and after the gate opens Postgres holds exactly one ledger
row and the entry is released. Closing the tab ends the worker the same way the scope finalizer does (a
dedicated worker does not outlive its document), so no separate scope-close scenario was added. A worker
that ends itself after a commit is covered too: "crash after commit" and "worker-only crash" in
`scenarios.sh`.

### Resetting a replica (`resetReplica`, DESIGN §9.1)

`ResetReplica.resetReplica({ appId, profile, spawnLeader }, { discardPending? })` deletes one identity's
replica, for example on sign-out. It is a static function (exported from `echo-sync/client`), so it runs
while no tab client for that identity is alive. Closing the tab clients first is the app's job, through its
own cross-tab sign-out signal.

- It spawns the app's leader worker (`spawnLeader`, the same factory `LeaderClient.make` takes) and sends it
  `Reset` instead of `Init`. That worker never becomes leader: it opens the pool through the leader's own
  OPFS host (`openConnection` in `Leader.ts`), so there is no second OPFS host path. It does not migrate.
- It takes the identity's leader lock with `ifAvailable`. If the lock is held, it fails `ReplicaInUse` and
  touches nothing. It never queues, so it cannot be granted ahead of or behind a waiting candidate.
- Without `discardPending`, under the lock it opens the database and counts unreleased `echo_outbox` rows.
  A count above 0 fails `PendingWrites { count }`. A database that does not open or an outbox that cannot be
  read fails closed with `ResetFailed`. Either way nothing is deleted.
- With `discardPending` it never opens the database (DESIGN §9.1, 5fcccb7): it only waits, as a new leader
  does, for a predecessor's access handles to be released. So a corrupt or partially removed replica can
  still be reset.
- Then it closes whatever it opened (database and pool, releasing the access handles), removes the directory
  `echo-sync/<appId>/<profile>` recursively, and releases the lock last. A candidate queued meanwhile is
  elected only after the directory is gone, so it creates a fresh replica with a new `clientId`.
- Sibling identities (`echo-sync/<appId>/<other profile>`) are untouched. A replica that never existed
  resets successfully.
- Notices belong to the replica and go with it. The outbox is the only guarded content.
- Any other failure (unsupported browser, invalid identity, an OPFS error, a `spawnLeader` or
  `postMessage` that throws) is `ResetFailed { message }`; interruption stays interruption.
  Interrupting the reset terminates its worker, and the browser then releases the lock and handles. A
  removal that had already begun may be partial. What a leader makes of such a directory is not
  established; a reset with `discardPending` never opens it, so retry that.

Evidence: `scenarios.sh` "reset …" checks (live leader → `ReplicaInUse` with the directory listing
unchanged; one pending write → `PendingWrites{1}` unchanged; `discardPending` removes `a` and keeps `b`; the
next leader for `a` has an empty outbox and a new `clientId`; `b` keeps its rows and `clientId`; a replica
whose database header is overwritten is refused with `ResetFailed` and left intact without
`discardPending`, and removed with it; throwing `spawnLeader` and `postMessage` are `ResetFailed`).

### Presence (`presence` / `track`, DESIGN §9.1, §12, §16 item 13)

`presence(room, state)` on the tab client is a `Stream<RoomSnapshot>`. `track(room, state)` replaces this
tab's state in a room it has joined; for a room it has not joined it does nothing.

- **Leader-owned.** The leader opens one upstream `PresenceRpcs.room` stream per tab subscription
  (`src/client/sync/Presence.ts`), keyed on the server by `(principal, clientId, tabId)`. The tab id is the
  tab client's own. Presence uses no permit and no SQL, and never touches the replica or the outbox. A
  local-only leader (no `syncUrl`) fails `SyncDisabled`.
- **Entry lifetime.** The entry exists exactly while the tab's stream runs. Closing it interrupts the RPC
  stream, which ends the leader's upstream stream, and the server drops the entry. A second `presence` for
  a room this tab already has open fails `PresenceAlreadyOpen`.
- **Liveness.** Heartbeats stay on the leader and only snapshots reach the tab. No element at all for
  `idleTimeout` (the `LeaderOptions` value that `followPokes` uses, 45 s by default) ends the upstream
  stream, and so does any failure or clean end. The leader then rejoins after a backoff, and the tab
  stream does not end. Only `Forbidden { room }` (no grant in the room's scope) ends it; `Unauthorized` is a
  credential failure and is rejoined like any other (DESIGN §9.1 Credentials).
- **Track and delivery (DESIGN §9.1, 07a8f48).** `track` completes once this tab has recorded the desired
  state, in one synchronous step before any message to the leader. Interrupting the caller afterwards does
  not retract it, and a `track` for a closed stream is dropped. Delivery is two hops of the same shape, tab
  to leader and leader to server, each a supervised sender owned by the open stream:
  - every join carries the latest state, and after the join's first snapshot the sender sends any newer
    state, one send at a time, latest wins (an undelivered older state may be skipped);
  - each attempt has a deadline (`idleTimeout` on the leader, 45 s by default in the tab). An attempt that
    fails (`LeaderLost` included: the retry goes to the current leader), dies or times out is retried with
    backoff and the latest state;
  - a sender never ends or restarts its stream, and never waits for the stream to fail: a heartbeating room
    stream cannot strand a state. Snapshots never wait for a send, and the join's end (idle timeout,
    reconnect, handover, close) interrupts one in flight.
- **Registration.** Each layer registers a subscription with `acquireRelease`, as one synchronous
  check-and-insert step. Its release removes only itself, so a stream cancelled at any point leaves nothing
  behind, and two simultaneous opens of one room in a tab produce exactly one upstream open; the other
  fails `PresenceAlreadyOpen`.
- **Handover.** Like `subscribe`, the stream re-joins on the next leader and emits nothing in between. A
  same-key rejoin ends the older server stream (DESIGN §12), which is expected. Other tabs see this tab's
  entry disappear for at most the reconnect gap.

Evidence:
- `Presence.test.ts`: heartbeats are filtered; a silent connection is rejoined with the latest state while
  the stream stays open; a track that raced the join is delivered after its first snapshot; a hung send
  (room stream heartbeating, so no rejoin) and a send that dies are both retried with the latest state; a
  subscription cancelled at any point (swept under a small `MaxOpsBeforeYield`) sends no later track;
  `Unauthorized` rejoins and only `Forbidden` ends the subscription.
- `Leader.test.ts`: the tab re-joins with the latest state across handover; closing the stream drops the
  room; simultaneous opens give one upstream open; cancellation at any point frees the room; a hung tab
  send and a tab send lost with `LeaderLost` are retried with the latest state on the same join.
- `scenarios.sh` (real server): an entry is listed only while its stream is open, and after the leader tab
  closes, the new leader re-joins F with its last tracked state.

## Fallback without SharedWorker

Capability-tested in the tab (`typeof SharedWorker === "function"`). Without it, mode is `single-tab`:

- The tab whose candidate wins the lock is served directly (its own worker, no router).
- Every other tab fails **closed**: requests fail with `LeaderUnavailable{SecondaryTabWithoutRouter}`.
  Its candidate stays queued on the leader lock and never opens the database.
- When the owning tab closes, the queued candidate is elected, opens the same replica, and that tab
  becomes the served tab. Verified.

There is no path to a second owner: opening the database is reachable only after acquiring the leader
lock, in every mode. Missing Web Locks / dedicated workers / OPFS ⇒ `unsupported`: every request fails
with `LeaderUnavailable{Unsupported}` and nothing is opened. A transport (BroadcastChannel-based
routing) that lets secondary tabs reach the owner without SharedWorker is the obvious v2; it was not
needed to keep the invariant.

## Support matrix

| Browser | Web Locks | SharedWorker | OPFS sync access handles (dedicated worker) | Mode | Source |
|---|---|---|---|---|---|
| Chromium (desktop, headless) | yes | yes | yes | `shared-worker` | **Verified here** (scenarios below) |
| Chrome / Edge desktop | 69+ | yes | 102+ | `shared-worker` | Documentation (MDN / caniuse); not run here |
| Chrome Android | yes | historically **no** | yes | `single-tab` | Documentation; the runtime check decides |
| Firefox desktop | 96+ | yes | 111+ | `shared-worker` | Documentation; not run here |
| Safari macOS / iOS | 15.4+ | 16+ (re-added) | 15.2+ (`createSyncAccessHandle`), full sync API later | `shared-worker` expected | Documentation; not run here |
| Language runtime (ES2023: `Array.prototype.toSorted`, used by core) | — | — | — | — | Chrome/Edge 110+, Firefox 115+, Safari 16+ |

**Effective minimum browser** is the maximum over all rows: **Chrome/Edge 110, Firefox 115, Safari 16**.
The binding row is the ES2023 language runtime (core's tsconfig target, `toSorted`), not any storage API.

Caveats, all from documentation and untested here:

- **Safari**: SharedWorker was removed for years and re-added in 16; OPFS in private browsing and
  storage eviction on iOS are stricter; older 15.x sync access handles had async `getSize/flush/close`
  which AccessHandlePoolVFS does not support. Module workers (`type: "module"`) need Safari 15+.
- **Firefox**: module SharedWorkers need Firefox 114+ (or bundle the workers as classic scripts).
- **Nested workers** are not required by this design (see Topology), so their uneven support does not
  matter here.
- The leader also checks `createSyncAccessHandle` itself; a browser lacking it reports `LeaderFailed`
  with the reason instead of hanging.

## SQLite build capabilities (`@effect/wa-sqlite` 0.1.2, SQLite 3.47.0)

- `CREATE VIRTUAL TABLE fts_probe USING fts5(body)` **fails**: `no such module: fts5`. `pragma_compile_options`
  has no `ENABLE_FTS5`, and no shipped `.wasm` contains the string `fts5`. (DESIGN §10 / open item 6.)
- `SELECT json_extract('{"a":{"b":42}}', '$.a.b')` **works** (returns `42`).

Reproduce: in the harness page, `await echo.exec("CREATE VIRTUAL TABLE t USING fts5(b)")` and
`await echo.exec("SELECT json_extract('{\"a\":1}', '$.a') AS v")`.

## Outbox (`echo_outbox`)

`seq INTEGER PRIMARY KEY AUTOINCREMENT`, `mutation_id UNIQUE`, args as JSON text, `released_at`.
Operations: `append` (joins the caller's transaction; idempotent on `mutationId`), `pending` (unreleased
rows in `seq` order, with `seq`), `markReleased(seqs)`, `pruneReleased(retention = 24 h)` (run at leader
startup). `enqueue` returns only after the append's transaction has committed. All writes go through the
one leader connection, so `seq` is total and gap-free across tabs; AUTOINCREMENT keeps its high-water
mark in `sqlite_sequence`, so neither release nor pruning lets a seq be reused.

Release marks rather than deletes (DESIGN §9): an `enqueue` retried after `LeaderLost` can arrive after
the original was pushed, acked and released; the row is still there, so the retry returns the original
`seq` and inserts nothing. Retries arriving after the retention window would be appended as new intents.

Crash points: a kill before COMMIT leaves no row and no seq bump (journal rollback; vitest); a kill after
COMMIT but before the response leaves the row, and the caller's retry returns it (browser scenario with
injected crash). Rows that fail to decode are treated as defects, not caller errors.

The pull staging table (`echo_staged`), notices (`echo_notices`) and the SQLite `ReplicaRowStore` live in
`src/client/sync/SqliteSyncStorage.ts` and `src/client/SqliteRowStore.ts`; engine tables migrate forward by
`echo_meta.engine_schema_version`.

## Leader wiring: engine, sync loop, pokes

The app's leader worker entry calls `runLeaderWorker(self, { registry, mutations, syncUrl? })`; each tab
passes its `principal` to `LeaderClient.make` (one principal per `profile`; the leader acts as the
principal its own tab sent). At startup the leader runs `SqliteSyncStorage.migrate(registry)` (engine
tables and replica schema, one transaction) and builds `Engine.make` over the SQLite storage.

- `enqueue` is `Engine.mutate`: the handler runs optimistically and the outbox row commits in the same
  transaction; a known `mutationId` returns its original seq without running anything; a local rejection
  fails with `LocallyRejected` and saves nothing. Each successful enqueue wakes the sync loop.
- With `syncUrl`, the leader runs `SyncLoop` (push then pull per cycle, one backoff 1 s → 30 s; any defect
  or `MalformedEnvelope` in a push halts push for this leader's lifetime, pull keeps running, nothing is
  released) and `SyncLoop.followPokes`, which wakes the loop on every poke and reconnects the pokes stream
  after any end (failure, defect, clean completion) with the same backoff. Reconnecting does not wake
  the loop; the server's initial poke (sent right after LISTEN, DESIGN §6 aec6cd4) does.
  A new leader starts a fresh loop, so a halt re-trips on its first push (DESIGN §6).
- Without `syncUrl` the replica is local-only: enqueue still applies and persists; `syncStatus` fails
  with `SyncDisabled`.
- `syncStatus` is `{ push, lastError, pendingMutations, lastSyncedAt, phase, nextRetryAt }` (DESIGN §9.1).
  `lastError` is `null | { kind, message }`: `kind` is `credentials` (`syncHeaders` raised),
  `unauthorized`, `unavailable` (`ServerUnavailable`), `network` (no usable RPC result) or `server`
  (the fallback, local `StoreUnavailable` included); `message` is a library constant per kind, never
  raw error text, and the `Halted` reason follows the same rule. The full cause goes to the leader log.
  `pendingMutations` is a recount of unreleased `echo_outbox` rows taken under the storage permit after
  every write to that table (`SqliteSyncStorage.pendingCount`, reactive on the connection's update hook),
  so it follows each enqueue and release, not only cycle ends, and cannot drift. A leader publishes no
  status until its first count has landed (a status before it could only claim 0 pending writes), so
  across a handover the tab keeps the previous leader's last emission until the new leader has counted. `lastSyncedAt` is set by a
  cycle whose push finished cleanly and whose pull installed a `PullDone`; it is in memory, so a new leader
  starts at `null`. `phase` is set by the loop (`pushing`, `pulling`, `backoff` with `nextRetryAt`, `idle`).
  `requestSync` offers to the loop's sliding wake queue: during backoff it cuts the wait short, and it
  never touches a `Halted` push.
- `syncStatus` (stream, resumes on handover like `subscribe`), `requestSync`, `notices` (stream) and
  `exportPending` (pending outbox in seq order; reads storage only, so it is available while push is
  halted) are RPCs on `LeaderRpcs`.
- `RpcClient.layerProtocolHttp({ url })` posts to `<url>/` (trailing slash): the server route must accept
  it. The harness's fake server normalizes it.
- Ordering (DESIGN §9): the storage permit serializes transactions and makes a queued one cancellable,
  but it is not fair: a releasing fiber may barge ahead of a waiter. Order among one tab's mutations
  comes from the caller: await each `enqueue` before issuing the next. Enqueues issued concurrently,
  from one tab or several, each get a distinct gap-free seq, but in no promised order. A mutate queued
  behind a long pull install can be interrupted while it waits; its handler never runs, and the interrupt
  completes no later than the end of the holder's transaction.
- Every tab RPC that touches SQLite takes the same permit: `enqueue`/`exportPending` through the
  engine's storage transaction, each `subscribe` emission's read transaction through it too, and `exec`
  and `notices` through `admit` (permit, no transaction of its own). None waits on the connection lease uncancellably.
  `exec` is for the harness and diagnostics: its writes bypass the engine (no outbox row, no undo image).

## Tests

`bun run check` runs the vitest invariants (Node; wa-sqlite's memory VFS and Node's own Web Locks):

- `src/client/Outbox.test.ts`: an enqueue retried after release returns the original seq and is not
  pending; a rolled-back append leaves no row and no seq gap; pruning keeps pending rows and never
  reuses a seq; rows inside the retention window survive pruning.
- `src/client/worker/platform.test.ts`: an interrupted lock wait never acquires the lock afterwards (the
  previous `acquireRelease` implementation hangs on this test); Web Locks rejections surface as
  `LockError`; all-dot identities are rejected.

## Running the browser scenarios

```sh
bun install
bun run spike                          # Vite harness on http://127.0.0.1:5199 (or: amp orb service start spike --command 'bun run spike')
examples/browser-spike/scenarios.sh    # drives Chromium with agent-browser; one PASS/FAIL per invariant, non-zero exit on failure
```

Each scenario uses a fresh `appId`, so runs are independent. No assertion depends on a sleep: every
check awaits the condition it asserts (`echo.until`), and concurrent phases start from a BroadcastChannel
barrier (`echo.barrier` / `echo.release`). A wall-clock start time was not a rendezvous: background tabs'
timers are throttled, so one tab could finish all its enqueues before the other started. Vite scans the
worker entries up front (`optimizeDeps.entries`) so a cold server does not reload pages mid-run, and the
script opens a warm-up page first.

Harness-only fault injection (in `examples/browser-spike/leader.worker.ts`, enabled per tab by URL flag):
`crashAfterCommit` ends that tab's leader worker instead of sending a committed `enqueue` response whose
mutation id starts with `crash-after-commit`; `failOpen` makes OPFS startup fail when another tab posts on
`echo-test:fail-open`. `echo.forgeLeaderAnnouncement(id)` replays a leader announcement to the router.
`?syncUrl=<url>` gives that tab's leader a sync loop; `examples/browser-spike/fakeSync.ts` mounts a fake
`SyncRpcs` server at `/fake-sync/rpc` (Vite middleware) whose push always fails with `MalformedEnvelope`,
whose pull answers an empty `PullDone` and whose pokes stream ends after one poke, with per-client
counters at `/fake-sync/stats?clientId=…`. The two `sync:` scenarios use it.

Invariants checked:

1. Single DB owner across 4 tabs (one leader id, exactly one leader tab, every tab served by it).
2. Single owner after closing the leader tab (the remaining 3 agree on one *new* leader).
3. Handover: in-flight request → `LeaderLost`; open subscription → `LeaderLost`; an enqueue lost with the
   leader is retried (≥ 2 attempts) and lands exactly once; replica and outbox persist.
4. The router ignores a replayed announcement from the dead leader: every tab keeps its live route for 2 s.
5. Crash after commit: the leader dies after committing an enqueue but before responding, while two tabs
   enqueue that same `mutationId` simultaneously; both retries return the committed seq, there is one row,
   and the next new intent gets seq + 1.
5b. Worker-only crash: a tab outlives its own leader worker (the same fault, on that tab's own enqueue).
   Its continuous `status` stream flips `isLeaderTab` to false when that worker's epoch lock is released,
   and follows the new leader elected in another tab. Ownership is tied to the epoch, not the tab.
6. OPFS startup failure: the first tab's candidate holds the leader lock while its open is blocked, a
   second tab queues, the open fails; the second tab becomes leader and serves both.
7. Saved means durable: an enqueued entry is visible after reload; after hard-killing the only tab
   mid-burst, every `enqueue` that had resolved is present, with contiguous seqs.
8. Gap-free ordering: 2 tabs × 150 concurrent enqueues produce one contiguous seq range, keep each tab's
   order, actually interleave (> 10 tab switches), and re-enqueueing a `mutationId` returns its original seq.
9. Reactivity: a live query in one tab re-emits after writes from another tab and converges to the
   final value.
10. Fallback: without SharedWorker the second tab fails closed; after the owner closes, it takes over.
11. Identity `.` is refused with `Unsupported` before anything is opened.

Emission consistency: the invariant is "no write interleaves between the statements of one emission".
It holds by construction: `subscribe` runs the statement batch in `withTransaction`, which leases the
leader's single SQLite connection (its semaphore) for the whole batch, so no other request's statement
can run in between. This is not proven by a browser test. The harness also checks two tables kept equal
by a trigger, but that check still passes with the transaction removed (the reader's statements
already run back to back), so it is a sanity check only.

`window.echo` in the harness page (`status`, `ready`, `info`, `exec`, `enqueue`, `enqueueRetrying`,
`subscribe`, `until`, `barrier`, `release`, `forgeLeaderAnnouncement`, `watchStatus`/`statusLog`) is the scripting surface; failures come back as values (`{ ok: false, error }`) so scripts
can assert on `_tag` / `reason`.
