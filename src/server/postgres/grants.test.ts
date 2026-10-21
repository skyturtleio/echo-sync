import { assert, describe, it } from "@effect/vitest"
import * as PgMigrator from "@effect/sql-pg/PgMigrator"
import { Effect, Exit } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, ScopeGrants } from "echo-sync"
import engine from "./migrations/001_engine.ts"
import { migrate, migrationsTable } from "./schema.ts"
import { EmptyDatabase, pgAvailable, TestDatabase } from "./test-support.ts"

/** An out-of-band grant insert (raw SQL, around the store); its exit and the error text. */
const insertGrant = (id: string, scope: string, principal: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* sql`INSERT INTO echo_sync.access_scopes (id) VALUES (${scope}) ON CONFLICT DO NOTHING`
    const exit = yield* Effect.exit(
      sql`INSERT INTO echo_sync.scope_grants (id, access_scope_id, principal_id, role, granted_at)
          VALUES (${id}, ${scope}, ${principal}, 'reader', now())`
    )
    return Exit.isSuccess(exit) ? "accepted" : String(exit.cause)
  })

const derived = (scope: string, principal: string) =>
  ScopeGrants.grantId(AccessScope.AccessScopeId.make(scope), AccessScope.PrincipalId.make(principal))

describe.skipIf(!pgAvailable)("scope_grants id derivation (§4)", () => {
  it.live("a grant written around the store with an id that is not grantId(scope, principal) is rejected", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      // Accepted: exactly the ids core derives, including a BMP non-ASCII scope id, where the
      // JS length (2) differs from the UTF-8 byte length (3) but not from char_length.
      for (const [scope, principal] of [
        ["s1", "p"],
        ["é1", "p"],
        ["a:b", "c:d"]
      ] as const) {
        assert.strictEqual(yield* insertGrant(derived(scope, principal), scope, principal), "accepted", scope)
      }
      // Rejected: arbitrary id, a wrong length prefix, the id of another pair, and the
      // separator moved between the parts (same concatenation, different prefix).
      const rejected: Array<readonly [string, string, string]> = [
        ["x", "s2", "p"],
        ["3:s2:p", "s2", "p"],
        [derived("s2", "q"), "s2", "p"],
        ["1:a:b:c", "a:b", "c"]
      ]
      // A scope id with a character above U+FFFF: rejected both with core's id (JS length 3)
      // and with the id char_length would give (2), so no id is ever mis-derived.
      rejected.push([derived("s\u{1F600}", "p"), "s\u{1F600}", "p"], ["2:s\u{1F600}:p", "s\u{1F600}", "p"])
      for (const [id, scope, principal] of rejected) {
        const result = yield* insertGrant(id, scope, principal)
        assert.include(result, "scope_grants_id_derived", `${id} must be rejected by the CHECK`)
      }
      const ids = yield* sql<{ id: string }>`SELECT id FROM echo_sync.scope_grants ORDER BY id`
      assert.deepStrictEqual(
        ids.map((r) => r.id),
        [derived("a:b", "c:d"), derived("s1", "p"), derived("é1", "p")].toSorted()
      )
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("an existing database with a non-derived grant fails migration 002 loudly instead of skipping it", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      // A database at engine 001 only, as deployed before 002 existed, with one bad grant.
      yield* sql`CREATE SCHEMA echo_sync`
      yield* PgMigrator.make({})({ loader: PgMigrator.fromRecord({ "001_engine": engine }), table: migrationsTable })
      assert.strictEqual(yield* insertGrant("legacy", "s1", "p"), "accepted")

      const failed = yield* Effect.exit(migrate)
      assert.isTrue(Exit.isFailure(failed))
      assert.include(String(Exit.isFailure(failed) ? failed.cause : ""), "scope_grants_id_derived")
      const applied = yield* sql<{ name: string }>`SELECT name FROM echo_sync.migrations ORDER BY migration_id`
      assert.deepStrictEqual(
        applied.map((r) => r.name),
        ["engine"]
      )

      // Repaired, it applies.
      yield* sql`UPDATE echo_sync.scope_grants SET id = ${derived("s1", "p")} WHERE id = 'legacy'`
      yield* migrate
      const after = yield* sql<{ name: string }>`SELECT name FROM echo_sync.migrations ORDER BY migration_id`
      assert.deepStrictEqual(
        after.map((r) => r.name),
        ["engine", "grant_id_check"]
      )
    }).pipe(Effect.provide(EmptyDatabase))
  )
})
