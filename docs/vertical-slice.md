# Vertical slice: the browser leader against the real server

The harness (examples/browser-spike) runs the real leader worker (`src/client`) against the landed
Postgres server (`src/server/postgres`) through a small Bun entry, examples/server/main.ts. Nothing in
`src/server` or `src/core` is specific to the slice.

```
┌──────────────┐  Effect RPC   ┌──────────────┐  HTTP POST + NDJSON  ┌──────────────┐   ┌──────────┐
│ tabs (main.ts│──────────────▶│ leader worker│─────────────────────▶│ Vite :5199   │──▶│ Bun :5200│──▶ PG17
│ window.echo) │  MessagePort  │ OPFS SQLite  │  /sync               │ proxy /sync, │   │ /sync,   │
└──────────────┘               └──────────────┘                      │ /admin       │   │ /admin   │
                                                                     └──────────────┘   └──────────┘
```

## The app

examples/browser-spike/app.ts is shared by both sides: one synced table, `todos`
(`id`, `accessScopeId`, `title`), and two mutations.

- `t` writes nothing. The substrate scenarios use it.
- `todos.add` inserts a todo. The server's copy adds checks the client cannot evaluate. A title containing
  `server-rejects` fails with the declared domain error `{ reason: "blocked-title" }`, so the
  rejection reaches the client only as a ledger outcome. A title containing `[gate:NAME]` parks the server
  handler inside the push transaction, before any write, until `/admin/open?gate=NAME`. S2 and S5 use it to
  hold the server's answer for exactly as long as they assert.

## The server entry (dev only)

examples/server/main.ts:

1. Creates the database `ECHO_SLICE_DB` (default `echo_slice`) if it is missing.
2. Runs `Postgres.migrate`.
3. Creates each app table from its SyncTable DDL (`table.postgres.createTable`, `createIndexes`), as the
   app's own migration would, and attaches it with `Postgres.attach`.
4. Serves `Postgres.layerHttp` (core `SyncRpcs`, HTTP POST with NDJSON) at `/sync`.

**`principalFor` is app-owned (DESIGN §4). This entry's implementation is a dev shim:** it trusts the
`x-echo-principal` request header, which the harness leader sets to its tab's `?principal=`. There is no
authentication. Only examples/server reads the header; nothing in `src/server` does. A real app
authenticates the request (session cookie, bearer token) in its own `principalFor`.

The leader sends credentials with `LeaderOptions.syncHeaders(principal)`. It is a `src/client` API:
`Transport.layerProtocolHttp(url, headers)` puts the headers on every push, pull and pokes request,
and a test in `Transport.test.ts` checks all three.

`/admin/*` exists only so the scenarios can drive and observe the server:

| Route | Does |
| --- | --- |
| `grant?scope=&principal=&role=` | creates the scope if needed and inserts the grant (default role `writer`) |
| `revoke?scope=&principal=` | deletes the grant |
| `ledger?mutationId=` | every `echo_sync.mutations` row for that id, across clients |
| `todos?scope=` | the server's `todos` rows in a scope |
| `open?gate=` | opens a scenario gate (see the app) |
| `stats?clientId=&todo=` | RPC requests this process received per tag for a client; how often the server's `todos.add` handler started for a todo id |

The Vite harness proxies `/sync` and `/admin` to the server (`ECHO_SLICE_SERVER`, default
`http://127.0.0.1:5200`). One harness fix was needed. When the server dies mid-response, Vite's proxy kept
the browser's pokes response open indefinitely, so an idle client never saw the stream end and never
reconnected. Scenario 5 caught this: after a restart an idle client sent no request for 121 s. The proxy
now destroys the browser response when the upstream response ends incomplete, which is what a direct
connection does.

## Running it

```sh
# Postgres 17 (in an orb: start dockerd first, e.g. amp orb service start dockerd --command 'sudo dockerd')
docker run -d --name echo-pg -p 5432:5432 -e POSTGRES_PASSWORD=echo postgres:17

amp orb service start echo-server --command 'bun examples/server/main.ts'   # or run it in a terminal
amp orb service start spike --command 'bun run spike'                       # Vite harness on :5199

examples/browser-spike/scenarios.sh   # the 18 substrate scenarios (real-server mode by default)
examples/browser-spike/slice.sh       # the 6 vertical-slice scenarios (always real server)
```

`ECHO_PG_URL` (default `postgres://postgres:echo@localhost:5432/postgres`), `ECHO_SLICE_DB` and
`ECHO_SLICE_PORT` (default 5200) configure the server. slice.sh restarts the server in scenario 5 with
`$ECHO_SERVER_STOP` and `$ECHO_SERVER_START`, which default to stopping and starting the `echo-server`
orb service. Each run uses fresh app ids, scopes and mutation ids, so runs never interfere and the
database can be reused.

## Which server a scenario runs against

`open_tab` in examples/browser-spike/lib.sh decides, for every tab both scripts open:

- **Real server (default):** the tab's URL gets `syncUrl=<base>/sync`, so its leader syncs with the
  slice server.
- **Fake server:** a scenario whose URL already names a `syncUrl` keeps it. The two `sync:` scenarios
  point at `/fake-sync/rpc` (examples/browser-spike/fakeSync.ts), a fault injector whose push always
  answers `MalformedEnvelope`.
- **Local only:** with `ECHO_SYNC=off`, scenarios.sh adds no `syncUrl` (nothing syncs) and does not
  require the server. slice.sh always uses the real server.

## Scenarios

One PASS/FAIL line each. slice.sh also prints what each passing check observed.

| # | Scenario | Script | Mode |
| --- | --- | --- | --- |
| 1 | single owner across 4 tabs | scenarios.sh | real |
| 2 | single owner after handover | scenarios.sh | real |
| 3 | handover: in-flight request fails with `LeaderLost` | scenarios.sh | real |
| 4 | handover: open subscription terminates with `LeaderLost` | scenarios.sh | real |
| 5 | handover: a lost enqueue is retried and lands exactly once | scenarios.sh | real |
| 6 | handover: replica and outbox persist | scenarios.sh | real |
| 7 | router ignores a stale leader announcement | scenarios.sh | real |
| 8 | crash after commit: retries return the committed seq | scenarios.sh | real |
| 9 | OPFS startup failure releases the leader lock | scenarios.sh | real |
| 10 | saved means durable: entry visible after reload | scenarios.sh | real |
| 11 | saved means durable: every resolved enqueue survives a hard kill | scenarios.sh | real |
| 12 | gap-free ordering under 2 concurrent tabs | scenarios.sh | real |
| 13 | reactivity: cross-tab write re-emits a live query | scenarios.sh | real |
| 14 | fallback: second tab without SharedWorker fails closed | scenarios.sh | real |
| 15 | fallback: waiting tab takes over the replica | scenarios.sh | real |
| 16 | sync: `MalformedEnvelope` halts push; pull, pokes and export continue | scenarios.sh | **fake** |
| 17 | sync: after handover the new leader re-trips the halt once | scenarios.sh | **fake** |
| 18 | identity `.` is refused | scenarios.sh | real |
| S1 | two clients on one scope: A's `todos.add` reaches B's live query through push, poke and pull with no refresh; the ledger has exactly one row, for A's client, `Applied`; A's entry is released | slice.sh | real |
| S2 | a real mutator rejection: while the server holds the push at a gate, A shows the row optimistically and has no notice; after the gate opens A gets the typed `Rejected{DomainError{blocked-title}}` notice and the row is undone; neither B nor the server ever holds it (B is checked after it shows a later marker) | slice.sh | real |
| S3a | revoke: both replicas drop the scope's rows and grant rows, and B's live query re-emits empty | slice.sh | real |
| S3b | re-grant: the scope is walked again; both replicas hold exactly the server's rows, and B's live query is awaited until it emits them | slice.sh | real |
| S4 | server restart mid-session: 3 enqueues made while the server is down; clients reconnect through the pokes backoff, B catches up from the initial poke, each offline mutation has exactly one ledger row, A's push is `Active` again | slice.sh | real |
| S5 | the leader tab closes while its push is held at a gate on the server: the next leader of the same replica sends its own push request while the entry is still pending, and after the gate opens the ledger has exactly one row for that client and the entry is released | slice.sh | real |

Rows 1–15 and 18 exercise the substrate. With the real server they also run a live sync loop, and the
Postgres ledger has exactly one row for each exactly-once mutation they enqueue (`before-handover`,
`retried-across-handover`, `crash-after-commit-1`, `survives-reload`).

### What the slice does not prove

- S4's catch-up time is bounded by the pokes reconnect backoff (1 s doubling to 30 s), not by a resync
  timer: v1 has none (DESIGN §6). A client behind an intermediary that holds a dead response open, as
  Vite's proxy did, stays deaf until something else wakes it. The HTTP RPC transport has no heartbeat to
  detect that.
- S5 holds the first push open across the leader's closure, but when the gate opens the server may still
  commit that abandoned request, or roll it back and commit the successor's. Both outcomes must give one
  ledger row, so the check accepts either; the evidence line reports how many times the server's handler
  started (the deduplicated second push may skip the handler).
