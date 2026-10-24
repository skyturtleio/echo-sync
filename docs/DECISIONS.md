# Echo Sync — decision record

One short paragraph per ruling, oldest first. Each entry names the `docs/SPEC.md` section it
affects; the SPEC states the resulting behavior and is the only normative text. An entry here
records *why* and *when*, not *what*. Dates are the day the ruling first appeared in the design
record (git history of the former `docs/DESIGN.md` §16); the original text carried no date for
most of them. Thread names and reviewed heads are dropped. Numbering is the original §16
numbering; current citations use `DECISIONS N`, and historical `DESIGN §16.N` citations in
commit messages map to the same entry.

A new ruling is appended here, in the same branch as the SPEC change it motivates, as
`N. <title> (YYYY-MM-DD, RESOLVED | DEFERRED | UNSCHEDULED)`.

---

1. **Router relaunch and OPFS name bound** (2026-10-21, RESOLVED) → SPEC §12 (INV-BRW-1,
   INV-BRW-5). WebKit terminates a `SharedWorker` when the tab that created it closes, even with
   other tabs connected; the relaunched router had no routes, so no tab was served. Ruling: the
   router is rebuildable from its clients (a router announces itself on first connect and clients
   re-register their routes). Separately, WebKit caps an OPFS name at 255 UTF-8 bytes and
   identity validation only bounded the total, so a 256–488 byte `profile` passed validation and
   failed as `LeaderFailed{UnknownError}`. Ruling: `appId` and `profile` are each bounded at
   255 bytes with `Unsupported`. iPhone Safari caveats (best-effort origin eviction, no OPFS in
   private browsing) are documentation only.

2. **Grant-added snapshot delivery** (2026-10-21, RESOLVED) → SPEC §7 (INV-PULL-4). A principal
   who gains a scope mid-stream needs that scope's current rows, not only its future changes.
   Ruling: pull diffs the client's stored scope membership against the current grants and walks
   newly held scopes before continuing the log.

3. **Global catalog snapshot format** (2026-10-21, DEFERRED) → SPEC §1 non-goals. A packaged
   catalog snapshot (leaning NDJSON+gzip) is deferred past the v1 vertical slice; pull walks
   scopes row by row instead.

4. **Protocol conformance suite for a non-TypeScript client** (2026-10-22, DEFERRED
   indefinitely by the owner) → SPEC §1 non-goals. TypeScript is the only client language for
   the foreseeable future; revisit only if a non-TS client is planned.

5. **Ledger expiry, separate from log GC** (2026-10-21, RESOLVED) → SPEC §5, §9 (INV-CLI-4,
   INV-CLI-6), §11 (INV-GC-3..6). The ledger could not grow forever, but a
   forgotten mutation id turns a lost-ack resubmission into a double apply. Ruling: identities
   are kept forever and only outcomes expire; a per-client floor answers `seq ≤ floor` with the
   terminal `Rejected{ClientExpired}` carrying the highest expired position; `PullDone` carries
   `expiredThrough` so a pending mutation at or below it settles as `OutcomeUnknown`; rows are
   stamped `acked_at` in the pull install transaction and an unstamped row never expires;
   `expireLedger` refuses a retention under 48 h (twice the released-row window) and defaults to
   7 days; the deleted set is asserted to be a `seq` prefix; pull head includes the maximum
   expired position so a cursor never regresses; floors are permanent. Review of the first
   implementation found that `mutate` retries `LeaderLost` indefinitely with the same id, so a
   tab suspended past the released-row window could re-enqueue a duplicate the server no longer
   recognised; keeping identities forever closed that hole.

6. **Local full-text search** (2026-10-21, DEFERRED) → SPEC §1 non-goals. The pinned wa-sqlite
   build lacks FTS5. Owner (2026-10-22): not needed by NilBurger, required for the SkyTime
   flight-logbook port; decide custom build vs. non-FTS search before that port starts.

7. **Two-direction wire version gate** (2026-10-22, RESOLVED) → SPEC §0.2, §10 (INV-LOOP-4),
   §13. A PWA user can dismiss a reload prompt and run an old client against a newer server
   indefinitely, and a newer client could corrupt its replica against an older server. Ruling:
   every `push` and `pull` carries `clientVersion {dataHash, wireVersion}`; the server decides
   through an app-supplied `clientPolicy` whose default is *serve*; a refused client gets
   `ClientOutdated`, which is terminal for that leader (no retry, outbox and replica kept,
   `lastError.kind: "outdated"`, the app owns the update prompt); a row of a known table missing a
   declared key is `ServerOutdated` (`lastError.kind: "server-outdated"`, backoff, never a
   rebuild); a `PullPage` is decoded before staging so an incompatible page is never persisted;
   the generator keeps a history of committed `dataHash`es in `migrations/manifest.json` as the
   table-shape floor. Production note: NilBurger deployed this server-first under the default
   policy on 2026-10-22; a refusing policy or a non-additive change waits until every deployed
   client is on a gate-aware pin.

8. **Query `limit` pushdown through the scope filter** (2026-10-21, RESOLVED) → SPEC §4
   (INV-ACL-7). A `limit` applied after the readability filter in memory read unbounded rows.
   Ruling: `IndexRangePlan` carries `readableBy` and `limit`; every `RowStore` applies
   readability as a `scope_grants` semi-join and stops after `limit` readable rows;
   `Store.planResult` is the shared reference and the row-store conformance run covers it.

9. **Pokes liveness** (2026-10-21, RESOLVED) → SPEC §10 (INV-LOOP-7), §18. A silently dead pokes
   stream behind a proxy left a client waiting forever. Ruling: the stream element is
   `Poke | Heartbeat`; the server emits a heartbeat every `heartbeatInterval` (15 s); the client
   ends and reconnects a stream with no element for `idleTimeout` (45 s); heartbeats never wake
   the loop.

10. **Credential refresh** (2026-10-21, RESOLVED) → SPEC §10 (INV-LOOP-8). Ruling: `syncHeaders`
    returns an Effect evaluated per outgoing request (push, pull, every stream (re)connect); the
    app owns refresh; `Unauthorized` stays retried with backoff and is never fatal.

11. **Replica reset on sign-out** (2026-10-21, RESOLVED) → SPEC §12 (INV-BRW-7). Ruling:
    `resetReplica` is engine-owned, guarded by the leader lock (`ReplicaInUse`) and by the outbox
    (`PendingWrites{count}` unless `discardPending`), and deletes only the identity's OPFS
    directory. Superseded in part by item 24 (coordinated close).

12. **Presence** (2026-10-21, RESOLVED) → SPEC §10, §1 non-goals. Implemented as designed:
    `room` stream joins and observes, `track` updates, entries keyed by `(clientId, tabId)`,
    authorized by scope grant, state process-local and in memory; the tab client's `presence` /
    `track` are leader-owned and survive handover. Multi-instance presence is a non-goal.

13. **Presence liveness and revocation** (2026-10-21, RESOLVED) → SPEC §10. The `room` stream
    element is `RoomSnapshot | Heartbeat` (the same `Heartbeat` as pokes) and the server emits a
    heartbeat every `heartbeatInterval` on every open room stream, so the tab client can apply
    the same idle timeout; a revoked grant ends the room stream. Decided before the tab client
    existed so the wire never changed.

14. **Presence tab-side tuning** (2026-10-21, RESOLVED) → SPEC §18. `LeaderClientConfig`
    extends `TabApiOptions` (`presenceSendTimeout`, `presenceRetryDelay`); omission keeps the
    45 s / 1 s-doubling-to-30 s defaults.

15. **Constraint violations in a mutation body** (2026-10-22, RESOLVED) → SPEC §5 (INV-MUT-5,
    INV-MUT-6), §15. Every `SqlError` was classified transient, so a mutation that could never
    succeed (unique or check violation) was retried forever and, because the outbox pushes in
    `seq` order, blocked everything behind it. Ruling: handlers reach the database only through
    `Store`, which maps a constraint failure to `ConstraintViolated{kind: unique | other,
    constraint, message}`; a unique violation is retried a bounded number of times (it may be a
    race), then ledgered as a permanent rejection; `other` is ledgered at once; whole-cause
    precedence is preserved and the rejection round-trips to a retained client notice.

16. **`principalFor` receives body-forgeable headers** (2026-10-22, RESOLVED) → SPEC §8
    (INV-PUSH-5). The pinned `effect/rpc` HTTP protocol builds handler headers as
    `requestHeaders.concat(message.headers)`, so any NDJSON request message could override an
    auth header. Ruling: every RPC passes only the real `HttpServerRequest.headers` to
    `principalFor`; a raw-NDJSON spoofing regression covers it. Addendum: request policy must not
    be gated by a raw pathname predicate.

17. **FoldKit adapter as a library export** (2026-10-22, RESOLVED) → SPEC §1. `echo-sync/foldkit`
    turns `subscribe`, `syncStatus`, `notices`, `status` into streams and `mutate`,
    `requestSync`, `dismissNotices` into effects over a client source, extracted from NilBurger's
    production adapter; FoldKit stays a devDependency and the adapter adds no sync semantics.

18. **HTTP 401/403 classified as `network`** (2026-10-22, RESOLVED) → SPEC §10 (INV-LOOP-4). The
    pinned `RpcClient.makeProtocolHttp` never inspects the HTTP status, so a refusal emitted
    before RPC decoding (middleware, a proxy) with an empty body became a protocol defect.
    Ruling: HTTP 401/403 before RPC decoding report `unauthorized` with the constant message,
    retry with backoff without halting, and keep the outbox; other statuses keep their
    classification.

19. **`SyncStatus` exported as a type only** (2026-10-22, RESOLVED) → SPEC §10 (INV-LOOP-9).
    `LeaderClient` re-exports `SyncError`, `SyncErrorKind` and `SyncStatus` as runtime schema
    values so an adapter declares no copy of the union.

20. **Observability** (2026-10-22, RESOLVED) → SPEC §14 (INV-TEL-1..5). The engine emitted
    nothing a host could export, and its `Effect.log*` calls attached raw causes that can carry
    header values, URLs and bodies. Ruling: backend-neutral, host-owned telemetry through the
    pinned `effect` primitives only; worker entries expose the program as an Effect the host
    runs with its own tracer, loggers and metric attributes; fixed `echo.`-prefixed span names;
    attribute values from closed sets or counts/durations only, never ids, cursors, mutation
    names, row data, headers or free text; a sanitizing `Span` proxy installed through an
    engine-owned wrapping `Tracer` so every host-visible span ends `Exit.void` with approved
    attributes only; raw-cause log lines replaced by fixed-text lines. Exporters, OTLP and
    sampling are out of scope.

21. **First-registration account binding** (2026-10-22, RESOLVED) → SPEC §5 (INV-MUT-10), §7 (INV-PULL-1).
    A replica registered under one principal could be driven by a request authenticated as
    another. Ruling: the server asserts the replica's principal on every sync and presence
    request and refuses a mismatch; the binding is permanent per `clientId`; wire compatibility
    with the previous protocol kept.

22. **Cloudflare/edge server composition** (2026-10-22, UNSCHEDULED) → SPEC §1 non-goals, §16
    (INV-PERF-6). The engine is runtime-agnostic at the client and the wire, but the server
    composition assumes one long-lived process: pokes are a process-scoped shared `LISTEN` plus
    an in-memory `PubSub`, and presence is process-local. Recorded so the seam is not forgotten.

23. **Tab-side resubscription drops defects** (2026-10-22, RESOLVED) → SPEC §12 (INV-BRW-4).
    `Stream.catchIf` re-fails a non-matching error without its original cause, so a defect inside
    a `LeaderLost` cause was lost. Ruling: `resubscribing` recovers only a defect-free
    all-`LeaderLost` cause via `Stream.catchCauseIf`; the same loss in `streamWithLeader`'s
    `RpcClientError` mapping is fixed with `Cause.map`.

24. **Engine-owned coordinated reset** (2026-10-22, RESOLVED; supersedes the item 11 sentence
    "the app closes its tab clients first") → SPEC §12 (INV-BRW-7). `resetReplica` failed
    `ReplicaInUse` whenever any leader was alive and nothing closed the other tabs' clients.
    Ruling: `resetReplica` broadcasts `ResetSignal` on `echo:<appId>:<profile>:reset` after
    spawning its worker, which waits for the leader lock in one request bounded by
    `closeTimeout` (default 5 s, must be finite); every client of the identity closes on the
    signal and fails new work `LeaderUnavailable{Reset}`.

25. **Sign-out that keeps the replica** (2026-10-22, RESOLVED: no engine change) → SPEC §12.
    Item 24 coordinates deletion; a "keep my data" sign-out leaves other tabs' clients alive on
    readable data until each tab's app closes its own scope. Ruling: no engine-level
    `closeAll`; the app owns cross-tab close without deletion.

26. **Reset durability across a crash** (2026-10-22, RESOLVED) → SPEC §12 (INV-BRW-8). A crash
    during the recursive `removeEntry` left a partial directory a leader could open. Ruling: the
    reset worker writes the tombstone `echo-sync/<appId>/~reset-<sha256(profile) hex>` under the
    leader lock, then removes the directory, then the tombstone; a leader that finds the
    tombstone after taking its locks completes the removal before opening the database. The `~`
    is outside the identity charset so it can never collide with a profile.

27. **Self-revocation of a grant** (2026-10-23, RESOLVED) → SPEC §4 (INV-ACL-4). A member
    could not leave a scope without being its owner. Ruling: `Store.delete` skips the owner
    check only for the `scope_grants` row whose `principal_id` is the actor and the actor is not
    `*`; a denied self-reinsert after leaving is covered.

28. **Consumer strictness flags** (2026-10-23, RESOLVED) → SPEC §0. A consumer compiling with
    `noImplicitReturns` or `noPropertyAccessFromIndexSignature` failed inside echo-sync source.
    Ruling: the package compiles clean under those flags so a consumer's tsconfig does not have
    to relax them for a git dependency.

29. **Presence departure lag behind proxies** (2026-10-23, RESOLVED: no engine change) → SPEC
    §1 admissions, §10. A presence entry lives exactly while its `room` HTTP stream is open;
    directly against the server an abort clears it in milliseconds, but a development reverse
    proxy never forwarded the client abort, so the partner saw the departure only after the
    heartbeat/idle bound (about 45 s). Ruling: accepted as an admission; no explicit leave RPC.

30. **Sync failures are classified once, by type** (2026-10-23, RESOLVED) → SPEC §10
    (INV-LOOP-4, INV-LOOP-5), §15. `Engine` was generic over its transport error, so the loop,
    `isFatalPush` and telemetry each walked `reason` strings. Ruling: one typed `SyncFailure`
    union classified once; status `kind`, halt reason and telemetry outcome derive from it.

31. **Errors keep their cause and say where** (2026-10-23, RESOLVED) → SPEC §15 (INV-ERR-2).
    Engine-stored data that failed to decode died with a bare `SchemaError` naming neither table
    nor row, and browser storage calls failed with `new Error(String(cause))`. Ruling: typed
    errors (`CorruptStorage`, `MalformedChange`, …) carry the original cause and the location.

32. **Telemetry vocabularies are closed types** (2026-10-24, RESOLVED) → SPEC §14 (INV-TEL-2,
    INV-TEL-5). Span names, outcomes, counters and log lines were runtime allow-lists beside a
    `string` parameter, so a misspelled outcome became `server` and a missing log line was
    dropped silently. Ruling: `Telemetry.Name`, outcomes and `LogMessage` are closed literal
    types checked by the compiler.
