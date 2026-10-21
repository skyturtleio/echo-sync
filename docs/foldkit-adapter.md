# FoldKit adapter: what it needs from `src/client`

Input for a DESIGN section; nothing here is implemented. `src/foldkit/index.ts` is a placeholder.
FoldKit is not a dependency of this repo, so this note does not assume its API. It lists what any
UI adapter needs from `src/client`, as message streams and commands. The reference consumer is the
vertical-slice harness: `examples/browser-spike/main.ts` (tab side) and `app.ts` (registry and
mutations), against `examples/server/main.ts`.

| Adapter capability | `src/client` today | Missing |
| --- | --- | --- |
| Start a replica for (appId, profile, principal) | `LeaderClient.make` (`src/client/worker/LeaderClient.ts`), scoped; the app supplies `spawnLeader`/`spawnRouter` worker constructors | A documented way to bundle the worker entry files; today each app writes them (`examples/browser-spike/leader.worker.ts`, `router.worker.ts`) |
| Typed live query | `client.subscribe(LiveQuery)` streams `QueryResult` (raw SQL statements + table names, untyped rows), one read transaction per emission (`LeaderRpcs.ts` `subscribe`) | DESIGN §10's `Query.make` (typed args, mandatory `success` Schema, index plans) does not exist. Needed: `Query` in core, compiled on the leader to statements over `SqliteRowStore` with table dependencies, and a result decode on the tab |
| Re-subscribe on handover | Streams end with `LeaderUnavailable{LeaderLost}` (documented in docs/browser-substrate.md); callers retry, as `main.ts` `liveCount` does | A helper that resubscribes on `LeaderLost` and keeps the last emission, so the UI does not flash empty |
| Mutate | `client.enqueue({ mutationId, name, args })` returns after commit; idempotent on `mutationId`; fails `LocallyRejected` or `SqlFailure`/`LeaderUnavailable` | A typed wrapper over `Mutation.make` (encode `args` with the mutation's schema, generate `mutationId`); a retry-on-`LeaderLost` helper (`main.ts` `enqueueRetrying` is the pattern); the caller must await each enqueue to keep per-tab order (DESIGN §9) |
| Server rejections / outcome unknown | `client.notices` returns every retained notice (`SqliteSyncStorage.notices`) | No stream and no acknowledge: the UI must poll and dedupe by `id`. Needed: a notices stream (reactive on `echo_notices`) and a decision on whether notices are acknowledged or cleared (DESIGN §9 says "retained for inspection" and stops there) |
| Leader / connection status | `client.status` (mode, `isLeaderTab`, `leaderId`) as a one-shot `Effect`; `leaderInfo` RPC | A status stream (the harness polls every 250 ms); `mode: "unsupported"` and `SecondaryTabWithoutRouter` need a UI state |
| Sync status | `client.syncStatus` streams push state (`Active`/`Halted`) and `lastError`; `SyncDisabled` when local-only | Nothing structural; "online/offline" is only `lastError !== null` today |
| Recovery export | `client.exportPending` (always available, also while push is halted) | A UI affordance only |
| Credentials | `LeaderOptions.syncHeaders(principal)`, evaluated once per leader | Refresh without a leader restart (open, DESIGN §4/§6: `Unauthorized` is retried, so a renewed credential needs a new leader today) |

## Open questions for the DESIGN section

1. Where queries are defined and compiled: the leader needs the query registry (as it has the
   mutation registry) so tabs send `(name, args)`, not SQL. That makes `subscribe` take a query
   name, and changes `LeaderRpcs`.
2. Raw SQL `exec`/`subscribe` stay for the harness, or move behind a debug flag.
3. Whether the adapter owns `mutationId` generation (needed for idempotent retry across handover).
4. Notice lifecycle (ack, clear, cap).
5. Pokes liveness (DESIGN §16 item 9) affects what "connected" means in the status the adapter shows.
