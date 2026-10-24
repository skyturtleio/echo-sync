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

describe.skipIf(!pgAvailable)("Postgres statement budgets", () => {
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
              name: "work",
              args: { reads: n, writes: m }
            }
          ]
        }).pipe(Effect.provideService(SqlClient, counter.client))
        expect(result.results.map((r) => r.outcome._tag)).toEqual(["Applied"])
        // rpc.executeBatch: catalog 1. ledger.runMutation: BEGIN, SET, INSERT client,
        // lock SELECT, dedup SELECT, SAVEPOINT, RELEASE, INSERT ledger, COMMIT = 9.
        // Store.get: row + authorization = 2; patch: row + authorization + write = 3.
        // Trigger functions/log inserts are server-side work, NOT connection statements.
        expect(counter.statements).toHaveLength(10 + 2 * n + 3 * m)
        // Catalog has 3 tables, lock returns 1; each get/patch returns row + own grant.
        expect(counter.statements.reduce((sum, s) => sum + s.rows, 0)).toBe(4 + 2 * n + 2 * m)
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
      // Non-null cursor, completed set: register 1 + BEGIN/SET/COMMIT 3 + state,
      // held, grants, catalog, log, acks 6 + cursor cleanup/ack stamp/held insert 4 = 14.
      // SELECT rows: state 1 + held 1 + readable 1 + catalog 3 = 6.
      counter.reset()
      const empty = yield* replica.pullOnce(100).pipe(Effect.provideService(SqlClient, counter.client))
      expect(empty._tag).toBe("PullDone")
      expect(empty.changes).toHaveLength(0)
      expect(counter.statements).toHaveLength(14)
      expect(counter.statements.reduce((sum, s) => sum + s.rows, 0)).toBe(6)

      const k = 3
      for (let i = 0; i < k; i++) yield* source`INSERT INTO items (id, access_scope_id) VALUES (${`i-${i}`}, 's')`
      counter.reset()
      const log = yield* replica.pullOnce(100).pipe(Effect.provideService(SqlClient, counter.client))
      expect(log.changes).toHaveLength(k)
      // K log rows + K payload rows; payload fetch is batched by table, adds ONE statement.
      expect(counter.statements).toHaveLength(15)
      expect(counter.statements.reduce((sum, s) => sum + s.rows, 0)).toBe(6 + 2 * k)

      // Fresh null-cursor set: no held SELECT or three cursor writes (-4), walk all
      // three tables (+3). Under budget, so still reads log and records PullDone: 13.
      // State 1 + grants 1 + catalog 3 + walked items K + walked grant 1 = 6 + K.
      const fresh = new Replica("alice", "fresh")
      counter.reset()
      const walk = yield* fresh.pullOnce(100).pipe(Effect.provideService(SqlClient, counter.client))
      expect(walk._tag).toBe("PullDone")
      expect(walk.changes).toHaveLength(k + 1)
      expect(counter.statements).toHaveLength(13)
      expect(counter.statements.reduce((sum, s) => sum + s.rows, 0)).toBe(6 + k)

      // Walk lookahead: first table items fills limit 2, reads 3; no log or held
      // insert, only one walk table: 13 - 2 tables - log - held insert = 9.
      const paged = new Replica("alice", "paged")
      counter.reset()
      const page = yield* paged.pullOnce(2).pipe(Effect.provideService(SqlClient, counter.client))
      expect(page._tag).toBe("PullPage")
      expect(page.changes).toHaveLength(2)
      expect(counter.statements).toHaveLength(9)
      expect(counter.statements.reduce((sum, s) => sum + s.rows, 0)).toBe(5 + 3)
      expect(Option.isSome(paged.continuation)).toBe(true)
    }).pipe(Effect.scoped, Effect.provide(TestDatabase), Effect.provide(Reactivity.layer))
  )
})
