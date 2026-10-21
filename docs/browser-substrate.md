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
  `exec`, `enqueue`, `subscribe` (stream), `leaderInfo`.
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
| `enqueue` retry after `LeaderLost` | Safe: idempotent on `mutationId`, pending or released (returns the existing `seq`, allocates none) | Verified across a real handover and at the commit-before-lost-response crash point, with two tabs retrying the same id at once. |
| `exec` retry | Only if the statement is idempotent | Raw SQL, harness/diagnostics only. |
| Request issued while no leader is connected | Waits for the next leader | Leadership gaps are short (lock queue). |
| Open `subscribe` stream | **Terminates** with `LeaderUnavailable{LeaderLost}`; the caller re-subscribes | See below. |
| Replica / outbox | Persist (same OPFS file; the new leader reopens it) | Verified. |

Streams terminate rather than resume transparently because the server side of the stream (its
`Reactivity` registration) died with the old leader, and the new leader cannot know which result
version the subscriber last saw. A silent resume would either duplicate or skip an emission. Re-subscribing
gives a fresh, complete first emission from the new leader, which is what a UI binding wants anyway. A
higher-level query layer can hide this with `Stream.retry` on `LeaderLost` (the harness does exactly that).

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

Not yet here: the pull staging table (§8) and the SQLite `ReplicaRowStore` (`purgeScope`, `clearAll`).
Both depend on the `RowStore`/`ReplicaRowStore` interfaces and the `SyncTable` registry from `src/core`,
which are not on main yet.

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

Invariants checked:

1. Single DB owner across 4 tabs (one leader id, exactly one leader tab, every tab served by it).
2. Single owner after closing the leader tab (the remaining 3 agree on one *new* leader).
3. Handover: in-flight request → `LeaderLost`; open subscription → `LeaderLost`; an enqueue lost with the
   leader is retried (≥ 2 attempts) and lands exactly once; replica and outbox persist.
4. The router ignores a replayed announcement from the dead leader: every tab keeps its live route for 2 s.
5. Crash after commit: the leader dies after committing an enqueue but before responding, while two tabs
   enqueue that same `mutationId` simultaneously; both retries return the committed seq, there is one row,
   and the next new intent gets seq + 1.
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
`subscribe`, `until`, `barrier`, `release`, `forgeLeaderAnnouncement`) is the scripting surface; failures come back as values (`{ ok: false, error }`) so scripts
can assert on `_tag` / `reason`.
