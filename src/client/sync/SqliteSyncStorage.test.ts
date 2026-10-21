// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { expect, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Layer, Option } from "effect"
import * as SqlClient from "effect/sql/SqlClient"
import * as Outbox from "../Outbox.ts"
import * as SqliteSyncStorage from "./SqliteSyncStorage.ts"
import type { RowWriter } from "./Wire.ts"

// Stand-in for the SQLite ReplicaRowStore: one table, statements on the same connection.
const Db = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    yield* Outbox.migrate
    yield* SqliteSyncStorage.migrate
    yield* sql`CREATE TABLE replica (id TEXT PRIMARY KEY, row TEXT NOT NULL)`
  })
).pipe(Layer.provideMerge(SqliteClient.layerMemory({})))

const makeStorage = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  const rows: RowWriter<never> & { readonly ids: Effect.Effect<ReadonlyArray<string>> } = {
    write: (_table, id, row) =>
      Effect.orDie(
        Option.isSome(row)
          ? sql`INSERT OR REPLACE INTO replica (id, row) VALUES (${id}, ${JSON.stringify(row.value)})`
          : sql`DELETE FROM replica WHERE id = ${id}`
      ).pipe(Effect.asVoid),
    purgeScope: () => Effect.void,
    clearAll: Effect.orDie(Effect.asVoid(sql`DELETE FROM replica`)),
    ids: Effect.orDie(sql<{ id: string }>`SELECT id FROM replica ORDER BY id`).pipe(
      Effect.map((r) => r.map((x) => x.id))
    )
  }
  return { storage: SqliteSyncStorage.make(sql, rows), rows }
})

const entry = (seq: number, mutationId: string) => ({
  envelope: { clientId: "c", mutationId, seq, name: "t", args: { mutationId } },
  createdAt: 0,
  undo: [{ table: "t", id: mutationId, before: Option.none() }]
})

it.live("a transaction failing on one fiber rolls back only its own writes while another fiber commits", () =>
  Effect.gen(function* () {
    const { storage, rows } = yield* makeStorage
    const wrote = yield* Deferred.make<void>()
    const failing = yield* Effect.forkChild(
      storage
        .transaction(
          Effect.gen(function* () {
            yield* storage.rows.write("t", "lost", Option.some({ id: "lost", access_scope_id: "s" }))
            yield* storage.appendPending(entry(yield* storage.nextSeq, "lost"))
            yield* storage.setMeta({ cursor: 9n, pushedThrough: 9, resnapshot: true })
            yield* Deferred.succeed(wrote, undefined)
            yield* Effect.sleep("20 millis")
            return yield* Effect.fail("crash")
          })
        )
        .pipe(Effect.flip)
    )
    yield* Deferred.await(wrote)
    // Starts while the failing transaction is open; must not run inside it.
    yield* storage.transaction(
      Effect.gen(function* () {
        yield* storage.rows.write("t", "kept", Option.some({ id: "kept", access_scope_id: "s" }))
        yield* storage.appendPending(entry(yield* storage.nextSeq, "kept"))
      })
    )
    expect(yield* Fiber.join(failing)).toBe("crash")
    expect(yield* rows.ids).toEqual(["kept"])
    const pending = yield* storage.transaction(storage.pending)
    expect(pending.map((e) => [e.envelope.mutationId, e.envelope.seq])).toEqual([["kept", 1]])
    expect(pending[0]?.undo).toEqual([{ table: "t", id: "kept", before: Option.none() }])
    expect(yield* storage.meta).toEqual({ cursor: null, pushedThrough: 0, resnapshot: false })
  }).pipe(Effect.provide(Db))
)

it.effect("nextSeq starts at 1 and never reuses a seq, even after its released row is pruned", () =>
  Effect.gen(function* () {
    const { storage } = yield* makeStorage
    const append = (id: string) =>
      storage.transaction(
        Effect.gen(function* () {
          const seq = yield* storage.nextSeq
          yield* storage.appendPending(entry(seq, id))
          return seq
        })
      )
    expect(yield* append("a")).toBe(1)
    expect(yield* append("b")).toBe(2)
    yield* storage.transaction(storage.release([2]))
    expect((yield* storage.lookup("b")).pipe(Option.map((e) => e.seq))).toEqual(Option.some(2))
    expect((yield* storage.pending).map((e) => e.envelope.seq)).toEqual([1])
    yield* Effect.provideService(Outbox.pruneReleased(-1), SqlClient.SqlClient, yield* SqlClient.SqlClient)
    expect(yield* storage.lookup("b")).toEqual(Option.none())
    expect(yield* append("c")).toBe(3)
  }).pipe(Effect.provide(Db))
)

it.effect("staged pages, meta and notices round-trip with bigint positions", () =>
  Effect.gen(function* () {
    const { storage } = yield* makeStorage
    const page = {
      _tag: "PullPage" as const,
      changes: [
        { _tag: "upsert" as const, table: "t", row: { id: "r", access_scope_id: "s", n: 1 } },
        { _tag: "scopePurged" as const, scopeId: "s2" }
      ],
      acks: [{ mutationId: "m", position: 2n ** 60n, outcome: { _tag: "Applied" as const } }],
      continuation: "k"
    }
    yield* storage.transaction(
      Effect.gen(function* () {
        yield* storage.stage(page)
        yield* storage.setMeta({ cursor: 2n ** 60n + 1n, pushedThrough: 3, resnapshot: false })
      })
    )
    expect(yield* storage.staged).toEqual([page])
    expect((yield* storage.meta).cursor).toBe(2n ** 60n + 1n)
    yield* storage.clearStaged
    expect(yield* storage.staged).toEqual([])
  }).pipe(Effect.provide(Db))
)

it.live("an interrupted transaction rolls back rows, outbox, seq and meta together", () =>
  Effect.gen(function* () {
    const { storage, rows } = yield* makeStorage
    const wrote = yield* Deferred.make<void>()
    const fiber = yield* Effect.forkChild(
      storage.transaction(
        Effect.gen(function* () {
          yield* storage.rows.write("t", "x", Option.some({ id: "x", access_scope_id: "s" }))
          yield* storage.appendPending(entry(yield* storage.nextSeq, "x"))
          yield* storage.setMeta({ cursor: 5n, pushedThrough: 1, resnapshot: false })
          yield* Deferred.succeed(wrote, undefined)
          return yield* Effect.never
        })
      )
    )
    yield* Deferred.await(wrote)
    yield* Fiber.interrupt(fiber)
    expect(yield* rows.ids).toEqual([])
    expect(yield* storage.pending).toEqual([])
    expect(yield* storage.meta).toEqual({ cursor: null, pushedThrough: 0, resnapshot: false })
    // The rolled-back seq is given back: the next allocation is still 1.
    expect(yield* storage.transaction(storage.nextSeq)).toBe(1)
  }).pipe(Effect.provide(Db))
)
