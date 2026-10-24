# Echo Sync — agent guidance

Read `docs/SPEC.md` first. It is the contract: the normative specification, derived from the
code, with every invariant under a stable `INV-<AREA>-<n>` ID (§17 maps each to its test) and
every limit in §18. `docs/DECISIONS.md` is the dated record of the rulings behind it. If the SPEC
is wrong, say so in your report and fix the SPEC; do not silently diverge in either direction.
The complete `docs/` set is `SPEC.md` (contract, with explicitly non-normative mechanism and
verification appendices), `DECISIONS.md` (rulings), and `getting-started.md` (the human how-to).

## Repo shape

- The repository root is the package (`echo-sync`), consumed as a git dependency; it ships
  TypeScript source (`exports` → `src/**/*.ts`). No build step, no npm publish.
- `src/core` (shared: SyncTable, AccessScope, Mutation, Store interface, RPC contracts),
  `src/server` (Postgres + RPC handlers), `src/client` (browser leader worker, replica, outbox,
  queries), `src/foldkit` (the FoldKit adapter, entry point `echo-sync/foldkit`, DECISIONS 17).
  The engine is UI-framework agnostic: `src/core`, `src/client` and `src/server` carry no
  framework knowledge, and every UI adapter is a separate entry point that is a thin translation
  of the `src/client` tab-side API (SPEC §9, §12), adds no sync semantics, and imports nothing from
  the framework (lint-enforced; FoldKit is a devDependency for `examples/foldkit` only). FoldKit
  is the owner's standard UI stack, so its adapter ships here as the standard way FoldKit apps
  consume the engine. Browser code must never import `src/server`.
- Runtime: Bun. Tooling: TypeScript strict (`exactOptionalPropertyTypes`,
  `noUncheckedIndexedAccess`), `oxlint`, `oxfmt`, `vitest` + `@effect/vitest`.
- Runtime dependencies are Effect packages only, exact-pinned to one version.

## Effect v4

- Verify every API against the pinned source in `node_modules/effect/src/**` (and the
  `@effect/*` packages) before using it. Do not trust memory of v3 APIs.
- v4 import paths: `effect/sql`, `effect/rpc`, `effect/http`, `effect/workers`,
  `effect/testing`, `effect/Schema` (stable). `effect/schema` (lowercase) is a different,
  unstable barrel.
- No `any`, no `!`, no `as` outside platform edges (and then with a comment).
- Functions returning an Effect: `Effect.fn("Module.name")` for coarse steps (once per request, startup,
  election, reset or connection), for its call-site stack frames in diagnostics; `Effect.fnUntraced` on
  hot paths (per row, mutation, change or stream element), where it costs about 15× per call. Only
  non-exported functions that always run inside `Telemetry.boundary` may use `Effect.fn`, with a name
  that is not a `Telemetry.Name`: it opens its span in the caller's tracer, outside its own pipeables, so
  on an exported entry point the span and its failure cause would reach the host's exporter. Exported
  entry points (`leaderWorker`, `router`, `verify`, …) stay plain functions or `fnUntraced`. Spans the
  host may see are created only by `Telemetry.observe`/`observeStream`, with names from `Telemetry.Name`.

## Workflow

- Work on a `ws/<name>` branch. Commit in logical steps.
- Before merging, get an independent review of the diff (a reviewing subagent or a second model),
  address its findings, and say in your report which SPEC invariants (`INV-` IDs) you tested.
- To land: `git fetch`, rebase linearly onto `origin/main`, run `bun run check` and
  `bun run test:pg` (plus `examples/browser-spike/scenarios.sh` and `slice.sh` when `src/client`
  changed), then fast-forward push `main`. Never force-push `main`. No pull requests.
- A change to engine behavior is a design change: in the same branch, update `docs/SPEC.md`
  (the affected section, its invariant digest and the §17/§18 tables; a new invariant takes the
  next unused ID, a withdrawn one keeps its ID and is marked withdrawn) and append a dated
  paragraph to `docs/DECISIONS.md` with the problem, the ruling and the SPEC section it changed.
  Consumers pin a commit SHA, so an app picks up a change only by bumping its pin.

## Tests

Test invariants (`docs/SPEC.md` §17 lists them with their covering tests). Each test's name
states the failure it catches; when a test covers an invariant, cite its `INV-` ID in §17.
Do not add per-function unit tests, snapshot tests of SQL strings, or tests whose expected
values were derived from the implementation.

## Local environment

- `.agents/setup` installs locked dependencies and prepares a disposable PostgreSQL 18
  (production's major) on `127.0.0.1:5432` with trust auth; `.agents/resume` makes sure it is
  running. They are plain shell scripts and the only environment-specific code in the repo; any
  machine with Bun and a PostgreSQL reachable at `ECHO_PG_URL` works the same way.
- `bun run check` is the default gate (types, lint, format, unit tests). `bun run test:pg` runs the
  Postgres integration tests and defaults `ECHO_PG_URL` to the local database. That database is
  throwaway (crash durability off): never expose it or put production data in it.
- Browser scenarios: `bun run spike` (Vite harness on :5199), then
  `examples/browser-spike/scenarios.sh` and `slice.sh` drive Chromium through `agent-browser`;
  run them as described below. SPEC §17 maps their contract assertions to invariant IDs.

### Browser scenarios

The Vite harness proxies `/sync` and `/admin` to `examples/server/main.ts` on :5200. Start
Postgres, the example server and Vite before `scenarios.sh`; `slice.sh` starts/stops its own
server and restarts it in S4 (the server-restart check). In an Amp orb, use supervised services,
not detached shell jobs:

```sh
amp orb service start echo-server --command 'bun examples/server/main.ts'
amp orb service start echo-spike --command 'bun run spike' --portal
examples/browser-spike/scenarios.sh
amp orb service stop echo-server
examples/browser-spike/slice.sh
bash examples/browser-spike/observability.sh
```

On another machine, run `bun examples/server/main.ts` and `bun run spike` in separate terminals.
Install dependencies with `bun install`; `.agents/setup` prepares the local Postgres. The
example server creates `ECHO_SLICE_DB` (default `echo_slice`) if missing and verifies startup
before listening. Its `x-echo-principal` is a dev shim, not authentication. Old harness databases
that predate `echo_sync.app_migrations` can fail migration 001 with "relation already exists";
recreate only that disposable database once. Never use production data here.

Server configuration: `ECHO_PG_URL` (default local Postgres URL above), `ECHO_SLICE_DB`,
`ECHO_SLICE_PORT` (5200). Vite's `ECHO_SLICE_SERVER` defaults to `http://127.0.0.1:5200`.
`ECHO_SERVER_LOG` captures slice server output. To let slice manage a supervised server instead,
set `ECHO_SERVER_START` / `ECHO_SERVER_STOP` to its start/stop commands and start it before
the run. Fresh app ids, scopes and mutation ids make runs independent; reuse the database.

`scenarios.sh` defaults to real-server mode. Explicit `syncUrl` in a scenario is preserved:
the two `sync:` fault scenarios use `/fake-sync/rpc`. `ECHO_SYNC=off` runs substrate scenarios
local-only without a server; slice always uses the real server. Both print PASS/FAIL and
exit nonzero on failure; slice prints observed evidence too.

Slice sections are independent: `SLICE_ORDER="base offline conflict deleteedit"` is the default
(S1–S5, S6, S7, S8). Reorder them to check isolation or select one section while running a
`SLICE_CONTROL` deliberate fault. Each fault must fail its named check, not the others:

| `SLICE_CONTROL` | deliberate fault | fails |
|---|---|---|
| `skip-offline` | edit pushes immediately instead of going offline | S6 |
| `drop-outbox` | remove queued entry before reload | S6 |
| `stay-offline` | never reconnect A | S6 |
| `b-offline-after-push` | B cannot pull A after its own ack | S7a/b/c |
| `a-first` | reverse reconnect order | S7a/b |
| `no-reject` | disable server title rejection | S7c |
| `transient-rejected` | inject a rejected title into one query emission | S7c history scan |
| `a-stays-offline` | never reconnect A | S8a/b |
| `skip-b-delete` | leave the row A edits alive | S8b |

The network gate `echo.setOffline(true)` rejects only that identity's sync requests, aborts
streaming responses and persists in localStorage across reload, without blocking wasm loads.
A check waits for `lastError.kind === "network"` before treating the gate as active.

Harness app: `todos` has id, scope, title and nullable note. `t` writes nothing;
`todos.add/update` have a server-only `server-rejects` title check, and add's `[gate:NAME]`
parks a push before its write. Missing update/delete targets reject `RowNotFound`.
Dev-only `/admin` routes support `grant?scope=&principal=&role=` (writer by default),
`revoke?scope=&principal=`, `ledger?mutationId=`, `todos?scope=`, `open?gate=`,
`rejections?enabled=0|1`, and `stats?clientId=&todo=` (request and handler-start counts).
These are scenario controls, not engine API. SPEC Appendix C records evidence limitations.

### WebKit smoke

Playwright is deliberately outside repo dependencies. Install it in an external cache:

```sh
mkdir -p ~/.cache/echo-webkit
bun add --cwd ~/.cache/echo-webkit playwright@1.64.1
bunx --cwd ~/.cache/echo-webkit playwright install-deps webkit
bunx --cwd ~/.cache/echo-webkit playwright install webkit
WEBKIT=1 examples/browser-spike/scenarios.sh
WEBKIT=1 examples/browser-spike/slice.sh
WEBKIT=1 examples/browser-spike/webkit/sharedworker-lifetime.sh
```

`ECHO_PLAYWRIGHT_DIR` overrides `~/.cache/echo-webkit`; `ECHO_WEBKIT_COMMAND_TIMEOUT_MS`
defaults to 180 s. `ECHO_WEBKIT_LOG=1` and `ECHO_WEBKIT_DAEMON_LOG=<file>` capture commands and
console output. The driver runs one daemon on its own port per run, stopped on exit, with a
fresh persistent profile (ephemeral contexts lack OPFS). If the Debian 12 unzip hangs, the
1.64.1 build can be extracted manually from
`https://cdn.playwright.dev/dbazure/download/playwright/builds/webkit/2370/webkit-debian-12.zip`
into `~/.cache/ms-playwright/webkit-2370`, then marked with `INSTALLATION_COMPLETE` there.
Older Playwright 1.56.1 / WebKit 26.0 disabled StorageAPI/FileSystem/AccessHandle on Linux
([upstream issue](https://github.com/microsoft/playwright/issues/18235)); use the newer build.
The driver JSON-encodes eval results inside the page, matching CDP rather than Playwright's
Error serializer. This tests desktop Linux GTK WebKit, not macOS/iPhone Safari. The lifetime
probe expects browser SharedWorker survival and still fails on that WebKit even though the
engine now recovers; the Chromium run uses agent-browser/CDP and is unchanged.
