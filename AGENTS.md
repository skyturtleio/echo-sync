# Echo Sync — agent guidance

Read `docs/DESIGN.md` first. It is the contract. If the design is wrong, say so in your
report; do not silently diverge.

## Repo shape

- The repository root is the package (`echo-sync`), consumed as a git dependency; it ships
  TypeScript source (`exports` → `src/**/*.ts`). No build step, no npm publish.
- `src/core` (shared: SyncTable, AccessScope, Mutation, Store interface, RPC contracts),
  `src/server` (Postgres + RPC handlers), `src/client` (browser leader worker, replica, outbox,
  queries), `src/foldkit` (the FoldKit adapter, entry point `echo-sync/foldkit`, DESIGN §16.17).
  The engine is UI-framework agnostic: `src/core`, `src/client` and `src/server` carry no
  framework knowledge, and every UI adapter is a separate entry point that is a thin translation
  of the `src/client` tab-side API (DESIGN §9.1), adds no sync semantics, and imports nothing from
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
  address its findings, and say in your report which DESIGN §14 invariants you tested.
- To land: `git fetch`, rebase linearly onto `origin/main`, run `bun run check` and
  `bun run test:pg` (plus `examples/browser-spike/scenarios.sh` and `slice.sh` when `src/client`
  changed), then fast-forward push `main`. Never force-push `main`. No pull requests.
- A change to engine behavior is a design change: record it in `docs/DESIGN.md` (a §16 item with
  the problem and the ruling) in the same branch. Consumers pin a commit SHA, so an app picks up
  a change only by bumping its pin.

## Tests

Test invariants (see `docs/DESIGN.md` §14). Each test's name states the failure it catches.
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
  see `docs/vertical-slice.md`.
