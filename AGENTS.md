# Echo Sync — agent guidance

Read `docs/DESIGN.md` first. It is the contract. If the design is wrong, say so in your
report; do not silently diverge.

## Repo shape

- The repository root is the package (`echo-sync`), consumed as a git dependency; it ships
  TypeScript source (`exports` → `src/**/*.ts`). No build step, no npm publish.
- `src/core` (shared: SyncTable, AccessScope, Mutation, Store interface, RPC contracts),
  `src/server` (Postgres + RPC handlers), `src/client` (browser leader worker, replica, outbox,
  queries). The library is UI-framework agnostic: no adapter for any UI library lives here;
  apps build on the `src/client` tab-side API (DESIGN §9.1). Browser code must never import
  `src/server`.
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
