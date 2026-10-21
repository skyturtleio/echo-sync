// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Layer, type Scope } from "effect"
import * as SqlClient from "effect/sql/SqlClient"
import { Registry } from "echo-sync"
import * as Conformance from "../../core/testing/Conformance.ts"
import * as Outbox from "../Outbox.ts"
import * as SqliteRowStore from "../SqliteRowStore.ts"
import * as SqliteSyncStorage from "./SqliteSyncStorage.ts"

// The engine's store assumptions, checked on real SQLite (wa-sqlite, memory VFS): the same suite the
// in-memory reference passes. Each subject is a fresh database whose schema `migrate` created.
const registry = Registry.make(Conformance.tables)

const freshDatabase = Effect.gen(function* () {
  const context = yield* Layer.build(SqliteClient.layerMemory({}))
  const sql = Context.get(context, SqlClient.SqlClient)
  yield* SqliteSyncStorage.migrate(registry).pipe(Effect.provideService(SqlClient.SqlClient, sql))
  return sql
})

const register = (checks: ReadonlyArray<Conformance.Check<Scope.Scope>>) => {
  for (const check of checks) {
    it.effect(check.name, () => Effect.map(check.run, (cases) => assert.isAbove(cases, 0)), 60_000)
  }
}

describe("SqliteRowStore conforms to ReplicaRowStore", () => {
  register(
    Conformance.replicaRowStore(
      Effect.map(freshDatabase, (sql) => ({
        rows: SqliteRowStore.make(sql, registry),
        transaction: SqliteSyncStorage.make(sql, registry).transaction
      }))
    )
  )
})

describe("SqliteSyncStorage conforms to SyncStorage", () => {
  register(
    Conformance.syncStorage(
      Effect.map(freshDatabase, (sql) => ({
        storage: SqliteSyncStorage.make(sql, registry),
        // A reload: a new storage (new permit, nothing cached) over the same database.
        reopen: Effect.sync(() => SqliteSyncStorage.make(sql, registry)),
        // The retention window has passed for every released row.
        pruneReleased: Outbox.pruneReleased(-1).pipe(Effect.provideService(SqlClient.SqlClient, sql))
      }))
    )
  )
})
