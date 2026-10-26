import { describe, expect, it } from "@effect/vitest"
import * as PgClient from "@effect/sql-pg/PgClient"
import { Effect, Option, Schema } from "effect"
import * as Reactivity from "effect/reactivity/Reactivity"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation } from "echo-sync"
import { executeBatch } from "../src/server/postgres/rpc.ts"
import {
  grant,
  Items,
  pgAvailable,
  registry,
  Replica,
  RowId,
  TestDatabase
} from "../src/server/postgres/test-support.ts"
import { countingClient } from "./sql-counter.ts"

interface WalkPlan {
  readonly "Node Type": string
  readonly "Actual Rows": number
  readonly "Index Cond"?: string
  readonly "Rows Removed by Filter"?: number
  readonly "Shared Hit Blocks": number
  readonly Plans?: ReadonlyArray<WalkPlan>
}

describe.skipIf(!pgAvailable)("Postgres statement budgets", () => {
  it.live(
    "scope walks seek past the cursor without scanning earlier rows or another scope, even with generic plans",
    () =>
      Effect.gen(function* () {
        const source = yield* SqlClient
        yield* grant("alice", "s")
        // Interleave an equally large unreadable scope: an id-only index is not sufficient.
        yield* source`ALTER TABLE items DISABLE TRIGGER echo_sync_capture`
        yield* source`INSERT INTO items (id, access_scope_id)
        SELECT lpad(i::text, 6, '0') || suffix, scope
        FROM generate_series(1, 100000) i,
          (VALUES ('-a', 's'), ('-b', 'other')) scopes(suffix, scope)
        ORDER BY scope DESC, i`
        yield* source`ALTER TABLE items ENABLE TRIGGER echo_sync_capture`
        yield* source`ANALYZE items`
        const counter = yield* countingClient(source, PgClient.makeCompiler())
        yield* counter.client`SET plan_cache_mode = force_generic_plan`
        const replica = new Replica("alice", "seek")
        yield* replica.sync(500).pipe(Effect.provideService(SqlClient, counter.client))
        expect(replica.rows.size).toBe(100001) // items plus the readable grant
        const walks = counter.statements.filter((s) => s.sql.includes('FROM "public"."items" t'))
        expect(walks).toHaveLength(201)
        // Explain the driver's actual cached statement, not EXPLAIN <parameterized SQL>, which
        // Postgres custom-plans even under force_generic_plan. Check first and late pages.
        for (const walk of [walks[0], walks.at(-2)]) {
          if (walk === undefined) return yield* Effect.die("missing walk query")
          const prepared = yield* counter.client<{ name: string; generic_plans: bigint }>`
          SELECT name, generic_plans FROM pg_prepared_statements WHERE statement = ${walk.sql}`
          const statement = prepared[0]
          expect(statement?.generic_plans).toBeGreaterThan(0n)
          if (statement === undefined) return yield* Effect.die("walk was not prepared")
          const args = walk.params
            .map((p) =>
              typeof p === "number" ? String(p) : p === null ? "NULL" : `'${String(p).replaceAll("'", "''")}'`
            )
            .join(", ")
          const explained = yield* counter.client.unsafe<{ "QUERY PLAN": ReadonlyArray<{ Plan: WalkPlan }> }>(
            `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE "${statement.name}" (${args})`
          )
          const root = explained[0]?.["QUERY PLAN"][0]?.Plan
          expect(root?.["Actual Rows"]).toBe(walk.rows)
          const scan = root?.Plans?.[0]
          expect(scan?.["Node Type"]).toBe("Index Scan")
          expect(scan?.["Index Cond"]).toContain("access_scope_id = $1")
          if (walk !== walks[0]) expect(scan?.["Index Cond"]).toMatch(/id > \(?\$2/)
          expect(scan?.["Rows Removed by Filter"] ?? 0).toBe(0)
          expect(root?.["Shared Hit Blocks"]).toBeLessThan(100)
        }
      }).pipe(Effect.scoped, Effect.provide(TestDatabase), Effect.provide(Reactivity.layer))
  )

  it.live("a pushed mutation adds only its N reads and M writes to the fixed ledger cost", () =>
    Effect.gen(function* () {
      const source = yield* SqlClient
      yield* grant("alice", "s")
      yield* source`UPDATE echo_sync.scope_grants SET role = 'writer'`
      yield* source`INSERT INTO items (id, access_scope_id) VALUES ('i', 's')`
      const counter = yield* countingClient(source, PgClient.makeCompiler())
      const mutations = Mutation.makeRegistry([
        Mutation.make("work", {
          args: Schema.Struct({ reads: Schema.Int, writes: Schema.Int }),
          handler: (ctx, { reads, writes }) =>
            Effect.gen(function* () {
              for (let i = 0; i < reads; i++) yield* ctx.store.get(Items, RowId.make("i"))
              for (let i = 0; i < writes; i++) yield* ctx.store.patch(Items, RowId.make("i"), { body: `write-${i}` })
            })
        })
      ])
      for (const { n, m } of [
        { n: 0, m: 0 },
        { n: 2, m: 3 }
      ]) {
        counter.reset()
        const result = yield* executeBatch({ registry, mutations }, AccessScope.PrincipalId.make("alice"), {
          clientId: Ids.ClientId.make(`c-${n}`),
          mutations: [
            {
              mutationId: Ids.MutationId.make("work"),
              seq: Ids.Seq.make(1),
              createdAt: 1_700_000_000_000,
              name: "work",
              args: { reads: n, writes: m }
            }
          ]
        }).pipe(Effect.provideService(SqlClient, counter.client))
        expect(result.results.map((r) => r.outcome._tag)).toEqual(["Applied"])
        // rpc.executeBatch: epoch 1 + catalog 1. ledger.runMutation: BEGIN, SET, INSERT client,
        // lock SELECT, dedup SELECT, SAVEPOINT, RELEASE, INSERT ledger, COMMIT = 9.
        // Writer-clock validation uses Effect's clock and adds no SQL statements.
        // Store.get: row + authorization = 2; patch: row + authorization + write = 3.
        // Trigger functions/log inserts are server-side work, NOT connection statements.
        expect(counter.statements).toHaveLength(11 + 2 * n + 3 * m)
        // Epoch 1 + catalog 3 + lock 1; each get/patch returns row + own grant.
        expect(counter.statements.reduce((sum, s) => sum + s.rows, 0)).toBe(5 + 2 * n + 2 * m)
      }
      expect((yield* source<{ body: string }>`SELECT body FROM items WHERE id = 'i'`)[0]?.body).toBe("write-2")
    }).pipe(Effect.scoped, Effect.provide(TestDatabase), Effect.provide(Reactivity.layer))
  )

  it.live("empty, log and scope-walk pages retain their derived statement and returned-row budgets", () =>
    Effect.gen(function* () {
      const source = yield* SqlClient
      yield* grant("alice", "s")
      const replica = new Replica("alice", "reader")
      yield* replica.sync()
      const counter = yield* countingClient(source, PgClient.makeCompiler())
      // Non-null cursor, completed set: epoch 1 + register 1 + BEGIN/SET/COMMIT 3 + state,
      // held, grants, catalog, log, acks 6 + cursor cleanup/ack stamp/held insert 4 = 15.
      // SELECT rows: epoch 1 + state 1 + held 1 + readable 1 + catalog 3 = 7.
      counter.reset()
      const empty = yield* replica.pullOnce(100).pipe(Effect.provideService(SqlClient, counter.client))
      expect(empty._tag).toBe("PullDone")
      expect(empty.changes).toHaveLength(0)
      expect(counter.statements).toHaveLength(15)
      expect(counter.statements.reduce((sum, s) => sum + s.rows, 0)).toBe(7)

      const k = 3
      for (let i = 0; i < k; i++) yield* source`INSERT INTO items (id, access_scope_id) VALUES (${`i-${i}`}, 's')`
      counter.reset()
      const log = yield* replica.pullOnce(100).pipe(Effect.provideService(SqlClient, counter.client))
      expect(log.changes).toHaveLength(k)
      // K log rows + K payload rows; payload fetch is batched by table, adds ONE statement.
      expect(counter.statements).toHaveLength(16)
      expect(counter.statements.reduce((sum, s) => sum + s.rows, 0)).toBe(7 + 2 * k)

      // Fresh null-cursor set: no held SELECT or three cursor writes (-4), walk all
      // three tables (+3). Under budget, so still reads log and records PullDone: 14.
      // Epoch 1 + state 1 + grants 1 + catalog 3 + walked items K + walked grant 1 = 7 + K.
      const fresh = new Replica("alice", "fresh")
      counter.reset()
      const walk = yield* fresh.pullOnce(100).pipe(Effect.provideService(SqlClient, counter.client))
      expect(walk._tag).toBe("PullDone")
      expect(walk.changes).toHaveLength(k + 1)
      expect(counter.statements).toHaveLength(14)
      expect(counter.statements.reduce((sum, s) => sum + s.rows, 0)).toBe(7 + k)

      // Walk lookahead: first table items fills limit 2, reads 3; no log or held
      // insert, only one walk table: 14 - 2 tables - log - held insert = 10.
      const paged = new Replica("alice", "paged")
      counter.reset()
      const page = yield* paged.pullOnce(2).pipe(Effect.provideService(SqlClient, counter.client))
      expect(page._tag).toBe("PullPage")
      expect(page.changes).toHaveLength(2)
      expect(counter.statements).toHaveLength(10)
      expect(counter.statements.reduce((sum, s) => sum + s.rows, 0)).toBe(6 + 3)
      expect(Option.isSome(paged.continuation)).toBe(true)
    }).pipe(Effect.scoped, Effect.provide(TestDatabase), Effect.provide(Reactivity.layer))
  )
})
