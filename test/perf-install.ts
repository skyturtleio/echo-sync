// oxlint-disable-next-line import/no-unassigned-import -- wa-sqlite file fetch under Node/Bun
import "../src/client/testing/nodeWasm.ts"
import { strict as assert } from "node:assert"
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The benchmark checks Git synchronously before entering its measured Effect runtime.
import { execFileSync } from "node:child_process"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { Array as Arr, Console, Effect, Option, Stream, String as Str } from "effect"
import * as Reactivity from "effect/reactivity/Reactivity"
import { AccessScope, Ids, Protocol, ScopeGrants } from "echo-sync"
import { clientRegistry, Counters, grantRow, mutations, principal } from "../src/core/testing/SimApp.ts"
import * as Engine from "../src/client/sync/Engine.ts"
import * as SqliteSyncStorage from "../src/client/sync/SqliteSyncStorage.ts"
import { SqlClient } from "effect/sql/SqlClient"
import { makeCompilerSqlite } from "effect/sql/Statement"
import { countingClient } from "./sql-counter.ts"

const row = (id: string, value: number) => ({ id, access_scope_id: "s", value, updated_at: 0 })

/** Reproducible Bun fallback: same engine + wa-sqlite as browser, memory VFS, not OPFS.
 * One pre-image per pending increment, no acks, non-null cursor, overlapping server rows.
 * Pages are staged before timing; timing includes reading/decoding staged JSON and COMMIT. */
export const installFixture = Effect.fnUntraced(function* (p: number, c: number, snapshot = false) {
  const source = yield* SqliteClient.makeMemory({})
  yield* SqliteSyncStorage.migrate(clientRegistry).pipe(Effect.provideService(SqlClient, source))
  const counter = yield* countingClient(source, makeCompilerSqlite())
  const storage = yield* SqliteSyncStorage.make(counter.client, clientRegistry)
  const actor = principal("alice")
  const context = { storage, registry: clientRegistry, mutations, actor }
  const engine = yield* Engine.make({
    ...context,
    clientId: Ids.ClientId.make("device"),
    transport: { push: () => Effect.succeed({ results: [] }), pull: () => Stream.die("unused") }
  })
  yield* storage.transaction(
    Effect.gen(function* () {
      yield* storage.setMeta({ cursor: snapshot ? null : Ids.Position.make(1n), pushedThrough: 0, resnapshot: false })
      yield* storage.rows.write(
        ScopeGrants.ScopeGrants,
        ScopeGrants.grantId(AccessScope.AccessScopeId.make("s"), actor),
        Option.some(grantRow("s", "alice", "writer"))
      )
      for (let i = 0; i < p; i++) yield* storage.rows.write(Counters, `i-${i}`, Option.some(row(`i-${i}`, 100)))
    })
  )
  for (let i = 0; i < p; i++) yield* engine.mutate(Ids.MutationId.make(`m-${i}`), "increment", { id: `i-${i}`, by: 3 })
  const changes: Array<Protocol.Change> = Array.from({ length: c }, (_, i) => ({
    _tag: "upsert",
    table: "counters",
    row: row(`i-${i}`, 200)
  }))
  // A snapshot must include the grant or the replay correctly loses authorization.
  if (snapshot) changes.push({ _tag: "upsert", table: "scope_grants", row: grantRow("s", "alice", "writer") })
  let pages = 0
  for (let i = 0; i < changes.length; i += 500) {
    yield* storage.transaction(
      storage.stage({
        _tag: "PullPage",
        changes: changes.slice(i, i + 500),
        acks: [],
        continuation: Protocol.PullContinuation.make(`page-${pages++}`)
      })
    )
  }
  counter.reset()
  const start = performance.now()
  yield* storage.transaction(
    Effect.gen(function* () {
      const staged = yield* storage.staged
      yield* Engine.applyPageSet(context, staged, {
        _tag: "PullDone",
        changes: [],
        acks: [],
        nextCursor: Ids.Position.make(2n),
        expiredThrough: Ids.SeqFloor.make(0)
      })
    })
  )
  const millis = performance.now() - start
  const statements = [...counter.statements]
  const final = yield* storage.transaction(
    Effect.all({ pending: storage.pending, rows: storage.rows.scan(Counters), meta: storage.meta })
  )
  return { millis, statements, final, pages }
})

if (import.meta.main) {
  assert.equal(
    Str.trim(execFileSync("git", ["status", "--porcelain"], { encoding: "utf8" })),
    "",
    "measure only a clean tree"
  )
  const commit = Str.trim(execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }))
  for (const { p, c } of [
    { p: 5, c: 50 },
    { p: 500, c: 5000 }
  ]) {
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Benchmark runner owns one Reactivity instance for each scoped fixture execution.
    const result = await Effect.runPromise(installFixture(p, c).pipe(Effect.scoped, Effect.provide(Reactivity.layer)))
    // Validate outside the timed region; never quote timings for a broken install.
    assert.equal(result.statements.length, 8 + 9 * p + Math.ceil(c / 500))
    assert.equal(result.final.pending.length, p)
    assert.equal(result.final.rows.length, c)
    assert.equal(result.final.meta.cursor, 2n)
    for (const stored of result.final.rows) {
      const index = Number(Str.slice(2)(String(stored["id"])))
      assert.equal(stored["value"], index < p ? 203 : 200)
    }
    Effect.runSync(
      Console.log(
        JSON.stringify({
          commit,
          pending: p,
          changes: c,
          pages: result.pages,
          millis: result.millis,
          statements: result.statements.length,
          rowsReturned: Arr.reduce(result.statements, 0, (sum, s) => sum + s.rows),
          finalRows: result.final.rows.length,
          finalPending: result.final.pending.length
        })
      )
    )
  }
}
