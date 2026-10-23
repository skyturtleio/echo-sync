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

## Workflow

- Work on a `ws/<name>` branch. Commit in logical steps. Push the branch when you want review.
- When your deliverable is ready, message the reviewer thread with the branch name and a short
  summary of what to look at and which invariants you tested. Address its findings, re-request
  review. On approval: `git fetch`, rebase onto `origin/main`, run the full checks, and
  fast-forward push to `main`. Never force-push `main`.
- Message the coordinator thread at milestones, blockers, or when you believe the design needs
  to change.

## Tests

Test invariants (see `docs/DESIGN.md` §14). Each test's name states the failure it catches.
Do not add per-function unit tests, snapshot tests of SQL strings, or tests whose expected
values were derived from the implementation.

## Orbs

- `.agents/setup` installs frozen Bun dependencies and prepares a disposable PostgreSQL 18
  database (matching production's major, from PGDG). Bun, Node and `agent-browser` come from the orb base image.
- `.agents/resume` ensures the local-only Postgres service in `.amp/services.yaml` is running.
  Use `bun run check` for the default checks and `bun run test:pg` for Postgres integration
  tests; the latter supplies the local connection URL. No project secrets are required.
- The database trusts loopback connections and disables crash durability for test speed.
  Never expose it through a portal or use it for production data. Data lives outside the
  checkout at `~/.local/share/echo-sync/postgres-18` and is preserved across wake/setup reruns.
  Older PostgreSQL 15 data is left untouched; it cannot be opened directly by PostgreSQL 18.
- Start the optional browser harness with `amp orb service start spike --command 'bun run spike'`
  when needed; setup does not run tests or start application servers.
