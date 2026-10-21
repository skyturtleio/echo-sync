import { assert, describe, it } from "@effect/vitest"
import { DateTime, Deferred, Effect, Ref } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Errors, ScopeGrants, Store } from "echo-sync"
import { type BodyError, runMutation } from "./ledger.ts"
import { claimScope, roleFor } from "./rpc.ts"
import * as RowStore from "./rowStore.ts"
import { resolveTables } from "./schema.ts"
import { Items, pgAvailable, registry, Replica, RowId, TestDatabase } from "./test-support.ts"

// Scope creation on the server (DESIGN §4): the Store-level contract is in Conformance; these are
// the Postgres-only invariants (atomic claim, orphaned scopes, delivery).

const S = AccessScope.AccessScopeId.make("created")

/** A mutation body as the RPC adapter builds it: the server's role lookup and claim, the given actor. */
const asMutation = (actor: string, handler: (store: Store.EchoStore) => Effect.Effect<void, BodyError>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    const rows = yield* RowStore.make(new Map((yield* resolveTables(registry)).map((t) => [t.table.name, t.schema])))
    const { store } = Store.make(rows, {
      actor: AccessScope.PrincipalId.make(actor),
      roleFor: roleFor(sql, actor),
      claimScope: claimScope(sql),
      now: yield* DateTime.now
    })
    yield* handler(store)
  })

const submit = (actor: string, mutationId: string, body: Effect.Effect<void, BodyError, SqlClient>) =>
  runMutation({ principalId: actor, clientId: `${actor}-device`, mutationId, seq: 1n }, body)

const outcomeOf = (processed: { readonly outcome: { readonly _tag: string; readonly error?: { _tag: string } } }) =>
  processed.outcome._tag === "Applied" ? "Applied" : `Rejected:${processed.outcome.error?._tag}`

const grantsIn = (scope: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    return yield* sql<{ principal_id: string; role: string }>`
      SELECT principal_id, role FROM echo_sync.scope_grants WHERE access_scope_id = ${scope} ORDER BY principal_id`
  })
const accessRows = (scope: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    return yield* sql<{ n: number }>`SELECT count(*)::int AS n FROM echo_sync.access_scopes WHERE id = ${scope}`
  })

describe.skipIf(!pgAvailable)("scope creation on Postgres (§4)", () => {
  it.live(
    "two concurrent creators of one scope id get exactly one Applied and one ScopeForbidden{owner}, never two owners",
    () =>
      Effect.gen(function* () {
        // Both bodies take their snapshot (ledger reads) before either claims; then both claim.
        // Mutations run at SERIALIZABLE: the second claim blocks on the first one's uncommitted
        // access_scopes row and fails with 40001 when it commits; the ledger retries that
        // mutation, and the retry's ON CONFLICT DO NOTHING returns no row → ScopeForbidden.
        const arrived = yield* Ref.make(0)
        const open = yield* Deferred.make<void>()
        const attempts = yield* Ref.make(0)
        const creator = (actor: string) =>
          asMutation(actor, (store) =>
            Effect.gen(function* () {
              if ((yield* Ref.updateAndGet(attempts, (n) => n + 1)) <= 2) {
                if ((yield* Ref.updateAndGet(arrived, (n) => n + 1)) === 2) yield* Deferred.succeed(open, undefined)
                yield* Deferred.await(open)
              }
              yield* store.createScope(S)
            })
          )
        const [a, b] = yield* Effect.all([submit("a", "create", creator("a")), submit("b", "create", creator("b"))], {
          concurrency: "unbounded"
        })
        if (a === undefined || b === undefined) return assert.fail("missing results")
        const outcomes = [outcomeOf(a), outcomeOf(b)]
        assert.deepStrictEqual(outcomes.toSorted(), ["Applied", "Rejected:ScopeForbidden"])
        // The 40001 path: two first attempts and exactly one retry.
        assert.strictEqual(yield* Ref.get(attempts), 3)
        const winner = outcomes[0] === "Applied" ? "a" : "b"
        assert.deepStrictEqual(yield* grantsIn(S), [{ principal_id: winner, role: "owner" }])
        assert.deepStrictEqual(yield* accessRows(S), [{ n: 1 }])
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "a scope whose grants were all revoked is never fresh again; a rejected mutation's claim rolls back with it",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        const created = yield* submit(
          "a",
          "create",
          asMutation("a", (store) => store.createScope(S))
        )
        assert.strictEqual(outcomeOf(created), "Applied")
        yield* sql`DELETE FROM echo_sync.scope_grants WHERE access_scope_id = ${S}`
        // Orphaned: no grants (a replica would call it fresh), but the access_scopes row stays.
        const hijack = yield* submit(
          "b",
          "hijack",
          asMutation("b", (store) => store.createScope(S))
        )
        assert.strictEqual(outcomeOf(hijack), "Rejected:ScopeForbidden")
        assert.deepStrictEqual(yield* grantsIn(S), [])

        // Claim, then a rejection later in the same handler: the savepoint discards the
        // access_scopes row with the grant, so the id stays fresh.
        const other = AccessScope.AccessScopeId.make("rolled-back")
        const rejected = yield* submit(
          "c",
          "claim-then-reject",
          asMutation("c", (store) =>
            Effect.andThen(
              store.createScope(other),
              Effect.fail(new Errors.RowNotFound({ table: Items.name, id: "missing" }))
            )
          )
        )
        assert.strictEqual(outcomeOf(rejected), "Rejected:RowNotFound")
        assert.deepStrictEqual(yield* accessRows(other), [{ n: 0 }])
        assert.deepStrictEqual(yield* grantsIn(other), [])
        const retried = yield* submit(
          "d",
          "create-other",
          asMutation("d", (store) => store.createScope(other))
        )
        assert.strictEqual(outcomeOf(retried), "Applied")
      }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "the creator's next pull delivers the new scope's grant and the rows written in it; another principal receives none of it",
    () =>
      Effect.gen(function* () {
        const creator = new Replica("a", "a-device")
        const stranger = new Replica("b", "b-device")
        yield* creator.sync()
        yield* stranger.sync()
        const created = yield* submit(
          "a",
          "create-and-write",
          asMutation("a", (store) =>
            Effect.andThen(
              store.createScope(S),
              // The new owner grant authorizes a write in the same mutation.
              store.insert(Items, { id: RowId.make("first"), accessScopeId: S, body: "hello" })
            )
          )
        )
        assert.strictEqual(outcomeOf(created), "Applied")
        yield* creator.sync()
        yield* stranger.sync()
        const grantKey = `${ScopeGrants.ScopeGrants.name}/${ScopeGrants.grantId(S, AccessScope.PrincipalId.make("a"))}`
        const keys = [...creator.rows.keys()].toSorted()
        assert.deepStrictEqual(keys, [grantKey, "items/first"].toSorted())
        assert.strictEqual(creator.rows.get(grantKey)?.["role"], "owner")
        assert.deepStrictEqual([...stranger.rows.keys()], [])
        assert.isFalse(stranger.received.some((c) => JSON.stringify(c).includes(S)))
      }).pipe(Effect.provide(TestDatabase))
  )
})
