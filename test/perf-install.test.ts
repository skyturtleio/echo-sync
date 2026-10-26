import { expect, it } from "@effect/vitest"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { Effect, Option } from "effect"
import * as Reactivity from "effect/reactivity/Reactivity"
import { SqlClient } from "effect/sql/SqlClient"
import { makeCompilerSqlite } from "effect/sql/Statement"
import { AccessScope, Ids, Protocol } from "echo-sync"
import { clientRegistry, Counters, mutations, principal } from "../src/core/testing/SimApp.ts"
import * as Engine from "../src/client/sync/Engine.ts"
import * as SqliteSyncStorage from "../src/client/sync/SqliteSyncStorage.ts"
import { countingClient } from "./sql-counter.ts"
import { installFixture } from "./perf-install.ts"
import { paginate } from "../src/client/sync/testing/Pull.ts"

const upsert = (id: string, value: number): Protocol.Change => ({
  _tag: "upsert",
  table: "counters",
  row: { id, access_scope_id: "s", value, updated_at: 0 }
})

it.live("batched replica upserts keep the last duplicate and split before SQLite's parameter limit", () =>
  Effect.gen(function* () {
    const source = yield* SqliteClient.makeMemory({})
    yield* SqliteSyncStorage.migrate(clientRegistry).pipe(Effect.provideService(SqlClient, source))
    const counter = yield* countingClient(source, makeCompilerSqlite())
    const storage = yield* SqliteSyncStorage.make(counter.client, clientRegistry)
    // Four columns: 8192 rows cross the pinned SQLite's 32766-bind limit.
    const rows = Array.from({ length: 8192 }, (_, i) => ({
      id: `i-${i}`,
      access_scope_id: "s",
      value: i,
      updated_at: 0
    }))
    rows.splice(1, 0, { id: "i-0", access_scope_id: "s", value: -7, updated_at: 0 })
    rows.push({ id: "i-8191", access_scope_id: "s", value: -9, updated_at: 0 })
    rows.push({ id: "i-1", access_scope_id: "s", value: -11, updated_at: 0 })
    const revision = yield* storage.transaction(storage.revision)
    // The first chunk really writes; a bad integer in chunk two must roll it back,
    // including the new provenance wrapper's revision publication.
    expect(
      (yield* Effect.exit(
        storage.transaction(
          storage.rows.writeMany(Counters, [...rows, { id: "bad", access_scope_id: "s", value: 0.5, updated_at: 0 }])
        )
      ))._tag
    ).toBe("Failure")
    expect(yield* storage.transaction(storage.rows.scan(Counters))).toEqual([])
    expect(yield* storage.transaction(storage.revision)).toEqual(revision)
    counter.reset()
    yield* storage.transaction(storage.rows.writeMany(Counters, rows))
    const writes = counter.statements.filter((s) => s.sql.startsWith('INSERT INTO "counters"'))
    expect(writes).toHaveLength(2)
    expect(writes.every((s) => s.params.length <= 32766)).toBe(true)
    expect(yield* storage.transaction(storage.rows.read(Counters, "i-0"))).toEqual(Option.some(rows[1]))
    expect(yield* storage.transaction(storage.rows.read(Counters, "i-8191"))).toEqual(
      Option.some({ id: "i-8191", access_scope_id: "s", value: -9, updated_at: 0 })
    )
    expect(yield* storage.transaction(storage.rows.read(Counters, "i-1"))).toEqual(Option.some(rows.at(-1)))
    expect(yield* storage.transaction(storage.rows.scan(Counters))).toHaveLength(8192)
    expect(yield* storage.transaction(storage.revision)).toEqual({ ...revision, offset: revision.offset + 1 })
  }).pipe(Effect.scoped, Effect.provide(Reactivity.layer))
)

it.live("install batches never move upserts across a tombstone, scope purge, or page boundary", () =>
  Effect.gen(function* () {
    const source = yield* SqliteClient.makeMemory({})
    yield* SqliteSyncStorage.migrate(clientRegistry).pipe(Effect.provideService(SqlClient, source))
    const storage = yield* SqliteSyncStorage.make(source, clientRegistry)
    yield* storage.transaction(
      Engine.applyPageSet(
        { storage, registry: clientRegistry, mutations, actor: principal("alice") },
        [
          {
            _tag: "PullPage",
            acks: [],
            continuation: Protocol.PullContinuation.make("a"),
            changes: [
              upsert("x", 1),
              upsert("y", 2),
              upsert("x", 3),
              { _tag: "tombstone", table: "counters", id: "x" },
              upsert("x", 4),
              { _tag: "scopePurged", scopeId: AccessScope.AccessScopeId.make("s") },
              upsert("z", 5)
            ]
          },
          { _tag: "PullPage", acks: [], continuation: Protocol.PullContinuation.make("b"), changes: [upsert("x", 6)] }
        ],
        {
          _tag: "PullDone",
          acks: [],
          nextCursor: Ids.Position.make(1n),
          expiredThrough: Ids.SeqFloor.make(0),
          changes: [upsert("x", 7), { _tag: "tombstone", table: "counters", id: "z" }]
        }
      )
    )
    expect(yield* storage.transaction(storage.rows.scan(Counters))).toEqual([
      { id: "x", access_scope_id: "s", value: 7, updated_at: 0 }
    ])
  }).pipe(Effect.scoped, Effect.provide(Reactivity.layer))
)

it.live("a resumed K-page pull reads only linear staged rows and installs the whole set once", () =>
  Effect.gen(function* () {
    const source = yield* SqliteClient.makeMemory({})
    yield* SqliteSyncStorage.migrate(clientRegistry).pipe(Effect.provideService(SqlClient, source))
    const counter = yield* countingClient(source, makeCompilerSqlite())
    for (const k of [8, 24]) {
      let requests = 0
      const open = () =>
        Effect.gen(function* () {
          const storage = yield* SqliteSyncStorage.make(counter.client, clientRegistry)
          return yield* Effect.map(
            Engine.make({
              storage,
              registry: clientRegistry,
              mutations,
              actor: principal("alice"),
              clientId: Ids.ClientId.make("linear"),
              transport: {
                push: () => Effect.succeed({ results: [] }),
                pull: paginate((request) =>
                  Effect.sync(() => {
                    const index = Option.isNone(request.continuation) ? 0 : Number(request.continuation.value)
                    if (requests++ === k / 2) throw new Error("reload")
                    expect(index).toBe(requests > k / 2 + 1 ? requests - 2 : requests - 1)
                    return index === k
                      ? {
                          _tag: "PullDone",
                          changes: [],
                          acks: [],
                          nextCursor: Ids.Position.make(BigInt(k)),
                          expiredThrough: Ids.SeqFloor.make(0)
                        }
                      : {
                          _tag: "PullPage",
                          changes: [
                            {
                              _tag: "upsert",
                              table: "counters",
                              row: { id: `page-${index}`, access_scope_id: "s", value: index, updated_at: 0 }
                            }
                          ],
                          acks: [],
                          continuation: Protocol.PullContinuation.make(String(index + 1))
                        }
                  })
                )
              }
            }),
            (engine) => ({ engine, storage })
          )
        })
      counter.reset()
      const first = yield* open()
      expect((yield* Effect.exit(first.engine.pull))._tag).toBe("Failure")
      expect(yield* first.storage.transaction(first.storage.rows.scan(Counters))).toHaveLength(k === 8 ? 0 : 8)
      const resumed = yield* open()
      expect(yield* resumed.storage.transaction(resumed.storage.stagedProgress)).toEqual({
        continuation: Option.some(String(k / 2)),
        count: k / 2
      })
      yield* resumed.engine.pull
      const stagedReads = counter.statements.filter((s) => /^\s*SELECT\b/i.test(s.sql) && s.sql.includes("echo_staged"))
      // One resumed tail read + our progress check + K rows at install. The initial empty
      // tail read returns no row; streaming no longer re-reads progress before each page.
      // A second full-set read would exceed this budget, even though it is still linear.
      expect(stagedReads.reduce((n, s) => n + s.rows, 0)).toBeLessThanOrEqual(k + 2)
      expect(
        (yield* resumed.storage.transaction(resumed.storage.rows.scan(Counters)))
          .map((r) => r["value"])
          .toSorted((a, b) => Number(a) - Number(b))
      ).toEqual(Array.from({ length: k }, (_, i) => i))
      expect(yield* resumed.storage.transaction(resumed.storage.staged)).toEqual([])
      expect(yield* resumed.storage.transaction(resumed.storage.stagedProgress)).toEqual({
        continuation: Option.none(),
        count: 0
      })
    }
  }).pipe(Effect.scoped, Effect.provide(Reactivity.layer))
)

it.live("install undoes P entries, writes C changes and replays P handlers without hidden statements", () =>
  Effect.gen(function* () {
    for (const snapshot of [false, true]) {
      const p = 3
      const c = 7
      const result = yield* installFixture(p, c, snapshot)
      // Engine install: BEGIN + staged SELECT + meta SELECT + pending (undo SELECT,
      // Outbox.pending SELECT) + setMeta + clearStaged + COMMIT = 8.
      // Each pending increment: undo write 1; replay get (row + 2 grants) 3,
      // patch (row + 2 grants + write) 4, setUndo 1 => 9P. Install = one upsert per 500-row page.
      // Null cursor adds clearAll for 2 tables + the snapshot's grant write.
      expect(result.statements).toHaveLength(8 + 9 * p + Math.ceil(c / 500) + (snapshot ? 3 : 0))
      // Each pending is returned twice; replay reads 2 counter rows + own grant twice
      // (Everyone absent). Meta 1 and one staged JSON row: 6P + 2 returned rows.
      expect(result.statements.reduce((sum, s) => sum + s.rows, 0)).toBe(6 * p + 2)
      const writes = result.statements.filter((s) => s.sql.startsWith('INSERT INTO "counters"'))
      expect(writes).toHaveLength(p + Math.ceil(c / 500) + p)
      // Identities/order also catch undoing one entry P times: authoritative writes
      // would hide that mistake in the final replica. Restore is reverse seq order.
      const idColumn = Counters.columns.findIndex((column) => column.name === "id")
      const valueColumn = Counters.columns.findIndex((column) => column.name === "value")
      expect(
        writes.flatMap((s) =>
          Array.from({ length: s.params.length / Counters.columns.length }, (_, i) => [
            s.params[i * Counters.columns.length + idColumn],
            s.params[i * Counters.columns.length + valueColumn]
          ])
        )
      ).toEqual([
        ...Array.from({ length: p }, (_, i) => [`i-${p - 1 - i}`, 100]),
        ...Array.from({ length: c }, (_, i) => [`i-${i}`, 200]),
        ...Array.from({ length: p }, (_, i) => [`i-${i}`, 203])
      ])
      expect(result.statements.filter((s) => s.sql.startsWith("UPDATE echo_outbox SET undo"))).toHaveLength(p)
      expect(result.final.rows.map((r) => [r["id"], r["value"]])).toEqual(
        Array.from({ length: c }, (_, i) => [`i-${i}`, i < p ? 203 : 200])
      )
      expect(result.final.pending).toHaveLength(p)
      expect(
        result.final.pending.map((entry) => ({
          mutationId: entry.envelope.mutationId,
          seq: entry.envelope.seq,
          undo: entry.undo.map((image) => ({
            table: image.table.name,
            id: image.id,
            before: Option.map(image.before, (r) => [r["id"], r["value"]])
          }))
        }))
      ).toEqual(
        Array.from({ length: p }, (_, i) => ({
          mutationId: `m-${i}`,
          seq: i + 1,
          undo: [{ table: "counters", id: `i-${i}`, before: Option.some([`i-${i}`, 200]) }]
        }))
      )
      expect(result.final.meta.cursor).toBe(2n)
    }
  }).pipe(Effect.scoped, Effect.provide(Reactivity.layer))
)
