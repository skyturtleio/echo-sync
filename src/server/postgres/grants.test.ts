import { assert, describe, it } from "@effect/vitest"
import * as PgMigrator from "@effect/sql-pg/PgMigrator"
import { DateTime, Effect, Exit, Option } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, ScopeGrants, Store } from "echo-sync"
import engine from "./migrations/001_engine.ts"
import { runMutation } from "./ledger.ts"
import * as RowStore from "./rowStore.ts"
import { migrate, migrationsTable, resolveTables } from "./schema.ts"
import { emptyDatabase, EmptyDatabase, pgAvailable, registry, TestDatabase } from "./test-support.ts"

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
      // Accepted: exactly the ids core derives. é1: code points 2, UTF-8 bytes 3 (catches an
      // octet_length CHECK). s😀: code points 2, UTF-16 units 3 (core's prefix is code points).
      const accepted = [
        ["s1", "p"],
        ["é1", "p"],
        ["a:b", "c:d"],
        ["s\u{1F600}", "p"]
      ] as const
      for (const [scope, principal] of accepted) {
        assert.strictEqual(yield* insertGrant(derived(scope, principal), scope, principal), "accepted", scope)
      }
      assert.strictEqual(derived("s\u{1F600}", "p"), "2:s\u{1F600}:p")
      // Rejected: arbitrary id, a wrong length prefix (including the UTF-16 count for s😀), the id of
      // another pair, and the separator moved between the parts (same concatenation, other prefix).
      const rejected = [
        ["x", "s2", "p"],
        ["3:s2:p", "s2", "p"],
        ["3:s\u{1F600}:q", "s\u{1F600}", "q"],
        [derived("s2", "q"), "s2", "p"],
        ["1:a:b:c", "a:b", "c"]
      ] as const
      for (const [id, scope, principal] of rejected) {
        const result = yield* insertGrant(id, scope, principal)
        assert.include(result, "scope_grants_id_derived", `${id} must be rejected by the CHECK`)
      }
      const ids = yield* sql<{ id: string }>`SELECT id FROM echo_sync.scope_grants ORDER BY id`
      assert.deepStrictEqual(
        ids.map((r) => r.id),
        accepted.map(([scope, principal]) => derived(scope, principal)).toSorted()
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

  it.live(
    "a grant for a scope id outside the BMP, written through the Store in a mutation, commits instead of retrying forever",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        const scope = AccessScope.AccessScopeId.make("team-\u{1F600}")
        const principal = AccessScope.PrincipalId.make("q")
        yield* sql`INSERT INTO echo_sync.access_scopes (id) VALUES (${scope})`
        const body = Effect.gen(function* () {
          const tables = yield* resolveTables(registry)
          const rows = yield* RowStore.make(new Map(tables.map((t) => [t.table.name, t.schema])))
          const { store } = Store.make(rows, {
            actor: AccessScope.PrincipalId.make("p"),
            roleFor: () => Effect.succeed(Option.some<AccessScope.Role>("owner"))
          })
          yield* store.insert(ScopeGrants.ScopeGrants, {
            id: ScopeGrants.grantId(scope, principal),
            accessScopeId: scope,
            principalId: principal,
            role: "reader",
            grantedAt: yield* DateTime.now
          })
        })
        const result = yield* runMutation({ principalId: "p", clientId: "c", mutationId: "m1", seq: 1n }, body)
        assert.strictEqual(result.outcome._tag, "Applied")
        const rows = yield* sql<{ id: string }>`SELECT id FROM echo_sync.scope_grants WHERE access_scope_id = ${scope}`
        assert.deepStrictEqual(rows, [{ id: "6:team-\u{1F600}:q" }])
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "an engine migration on a non-UTF8 database fails loudly instead of installing a CHECK that counts bytes",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        const exit = yield* Effect.exit(migrate)
        assert.isTrue(Exit.isFailure(exit))
        assert.include(String(Exit.isFailure(exit) ? exit.cause : ""), "echo_sync requires a UTF8 database")
        const constraints = yield* sql<{ n: number }>`
        SELECT count(*)::int AS n FROM pg_constraint WHERE conname = 'scope_grants_id_derived'`
        assert.deepStrictEqual(constraints, [{ n: 0 }])
      }).pipe(Effect.provide(emptyDatabase("ENCODING 'SQL_ASCII' TEMPLATE template0 LC_COLLATE 'C' LC_CTYPE 'C'")))
  )
})
