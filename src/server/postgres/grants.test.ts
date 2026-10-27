import { assert, describe, it } from "@effect/vitest"
import * as PgMigrator from "@effect/sql-pg/PgMigrator"
import { Array as Arr, DateTime, Effect, Exit, Option, Order } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, ScopeGrants, Store } from "echo-sync"
import engine from "./migrations/001_engine.ts"
import { type BodyError, runMutation } from "./ledger.ts"
import { claimScope, roleFor } from "./rpc.ts"
import * as RowStore from "./rowStore.ts"
import { migrate, migrationsTable, resolveTables } from "./schema.ts"
import {
  emptyDatabase,
  EmptyDatabase,
  Items,
  pgAvailable,
  registry,
  Replica,
  RowId,
  serverView,
  TestDatabase
} from "./test-support.ts"

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

/** A mutation by `actor` as the RPC adapter runs it: the server's role lookup and claim; its outcome tag. */
const mutate = (
  actor: string,
  mutationId: string,
  seq: bigint,
  handler: (store: Store.EchoStore) => Effect.Effect<void, BodyError>
) =>
  Effect.gen(function* () {
    const body = Effect.gen(function* () {
      const sql = yield* SqlClient
      const rows = yield* RowStore.make(
        new Map(Arr.map(yield* resolveTables(registry), (t) => [t.table.name, t.schema]))
      )
      const { store } = Store.make(rows, {
        actor: AccessScope.PrincipalId.make(actor),
        roleFor: roleFor(sql, actor),
        claimScope: claimScope(sql),
        now: yield* DateTime.now
      })
      yield* handler(store)
    })
    const { outcome } = yield* runMutation({ principalId: actor, clientId: `${actor}-device`, mutationId, seq }, body)
    return outcome._tag === "Applied" ? "Applied" : `Rejected:${outcome.error._tag}`
  })

describe.skipIf(!pgAvailable)("scope_grants id derivation (INV-ACL-5)", () => {
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
        Arr.map(ids, (r) => r.id),
        Arr.sort(
          Arr.map(accepted, ([scope, principal]) => derived(scope, principal)),
          Order.String
        )
      )
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
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
        Arr.map(applied, (r) => r.name),
        ["engine"]
      )

      // Repaired, it applies.
      yield* sql`UPDATE echo_sync.scope_grants SET id = ${derived("s1", "p")} WHERE id = 'legacy'`
      yield* migrate
      const after = yield* sql<{ name: string }>`SELECT name FROM echo_sync.migrations ORDER BY migration_id`
      assert.deepStrictEqual(
        Arr.map(after, (r) => r.name),
        ["engine", "grant_id_check", "ledger_acked_at", "engine_epoch", "scope_walk_index", "catalogs"]
      )
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
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
          const rows = yield* RowStore.make(new Map(Arr.map(tables, (t) => [t.table.name, t.schema])))
          const { store } = Store.make(rows, {
            actor: AccessScope.PrincipalId.make("p"),
            roleFor: () => Effect.succeed(Option.some<AccessScope.Role>("owner")),
            claimScope: () => Effect.succeed(false),
            now: yield* DateTime.now
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
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
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
        // The engine migrations run in one migrator transaction: 001's DDL and history roll back too.
        const [state] = yield* sql<{ grants: boolean; history: number }>`
          SELECT to_regclass('echo_sync.scope_grants') IS NOT NULL AS grants,
                 (SELECT count(*)::int FROM echo_sync.migrations) AS history`
        assert.deepStrictEqual(state, { grants: false, history: 0 })
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns this encoding-specific disposable database until test completion.
      }).pipe(Effect.provide(emptyDatabase("ENCODING 'SQL_ASCII' TEMPLATE template0 LC_COLLATE 'C' LC_CTYPE 'C'")))
  )
})

const write = (store: Store.EchoStore) => store.patch(Items, RowId.make("shared"), { body: "v1" })

describe.skipIf(!pgAvailable)("self-revocation of a grant (SPEC §4)", () => {
  it.live(
    "a reader or writer deleting its own grant is Applied: its client is purged of the scope, the owner keeps the rows and sees the tombstone",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        const scope = AccessScope.AccessScopeId.make("couple")
        yield* sql`INSERT INTO echo_sync.access_scopes (id) VALUES (${scope})`
        for (const [principal, role] of [
          ["o", "owner"],
          ["w", "writer"],
          ["r", "reader"]
        ] as const) {
          yield* sql`INSERT INTO echo_sync.scope_grants (id, access_scope_id, principal_id, role, granted_at)
                     VALUES (${derived(scope, principal)}, ${scope}, ${principal}, ${role}, now())`
        }
        yield* sql`INSERT INTO items (id, access_scope_id, body) VALUES ('shared', ${scope}, 'v0')`
        const [owner, writer, reader] = Arr.map(["o", "w", "r"], (p) => new Replica(p, `${p}-device`))
        if (owner === undefined || writer === undefined || reader === undefined) return assert.fail("replicas")
        for (const replica of [owner, writer, reader]) yield* replica.sync()
        assert.isTrue(reader.rows.has("items/shared") && writer.rows.has("items/shared"))

        const leave = (who: string) => (store: Store.EchoStore) =>
          store.delete(ScopeGrants.ScopeGrants, ScopeGrants.grantId(scope, AccessScope.PrincipalId.make(who)))
        // Relinquishing first, then writing in the scope: the write has no grant, and the whole
        // mutation (the delete included) rolls back.
        assert.strictEqual(
          yield* mutate("w", "leave-then-write", 1n, (store) => Effect.andThen(leave("w")(store), write(store))),
          "Rejected:ScopeForbidden"
        )
        assert.strictEqual(yield* mutate("r", "leave", 1n, leave("r")), "Applied")
        assert.strictEqual(
          yield* mutate("w", "write-then-leave", 2n, (store) => Effect.andThen(write(store), leave("w")(store))),
          "Applied"
        )
        const grants = yield* sql<{ principal_id: string }>`
          SELECT principal_id FROM echo_sync.scope_grants WHERE access_scope_id = ${scope}`
        assert.deepStrictEqual(
          Arr.map(grants, (g) => g.principal_id),
          ["o"]
        )

        const before = new Map(Arr.map([owner, writer, reader], (replica) => [replica, replica.received.length]))
        for (const replica of [owner, writer, reader]) yield* replica.sync()
        const since = (replica: Replica) => replica.received.slice(before.get(replica))
        for (const leaver of [writer, reader]) {
          assert.deepStrictEqual(
            Arr.filter(since(leaver), (c) => c._tag === "scopePurged"),
            [{ _tag: "scopePurged", scopeId: scope }],
            leaver.principalId
          )
          assert.deepStrictEqual([...leaver.rows.keys()], [], leaver.principalId)
        }
        const tombstones = Arr.flatMap(since(owner), (c) => (c._tag === "tombstone" ? [c.id] : []))
        assert.deepStrictEqual(
          Arr.sort(tombstones, Order.String),
          Arr.sort([derived(scope, "r"), derived(scope, "w")], Order.String)
        )
        assert.strictEqual(owner.rows.get("items/shared")?.["body"], "v1")
        assert.deepStrictEqual(owner.view(), yield* serverView("o"))
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
      }).pipe(Effect.provide(TestDatabase))
  )
})
