// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "./testing/nodeWasm.ts"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { expect, it } from "@effect/vitest"
import * as Arr from "effect/Array"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as SqlClient from "effect/sql/SqlClient"
import { TestClock } from "effect/testing"
import * as Outbox from "./Outbox.ts"

const Db = Layer.effectDiscard(Outbox.migrate).pipe(Layer.provideMerge(SqliteClient.layerMemory({})))
const append = (mutationId: string) => Outbox.append({ clientId: "c", mutationId, name: "t", args: { mutationId } })
const rowCount = Effect.flatMap(SqlClient.SqlClient, (sql) => sql<{ n: number }>`SELECT count(*) AS n FROM echo_outbox`)

it.effect("an enqueue retried after its original was released returns the original seq and is not pending", () =>
  Effect.gen(function* () {
    const first = yield* append("m")
    expect(first.seq).toBe(1)
    yield* Outbox.markReleased([first.seq])
    const retried = yield* append("m")
    expect(retried.seq).toBe(first.seq)
    expect(yield* Outbox.pending).toEqual([])
    expect((yield* rowCount)[0]?.n).toBe(1)
    // and the retry consumed no seq: the next new intent is contiguous
    expect((yield* append("n")).seq).toBe(first.seq + 1)
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test composition root owns an isolated in-memory database for this test only.
  }).pipe(Effect.provide(Db))
)

it.effect("an append whose transaction rolls back leaves no row and no gap in seq", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const a = yield* append("a")
    const crashed = yield* sql.withTransaction(Effect.andThen(append("lost"), Effect.fail("crash"))).pipe(Effect.flip)
    expect(crashed).toBe("crash")
    const b = yield* append("b")
    expect(b.seq).toBe(a.seq + 1)
    expect(Arr.map(yield* Outbox.pending, (e) => e.mutationId)).toEqual(["a", "b"])
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test composition root owns an isolated in-memory database for this test only.
  }).pipe(Effect.provide(Db))
)

it.effect("pruning released rows keeps pending ones and never lets a seq be reused", () =>
  Effect.gen(function* () {
    const a = yield* append("a")
    const b = yield* append("b")
    yield* Outbox.markReleased([b.seq])
    yield* TestClock.adjust(Outbox.defaultReleasedRetentionMillis + 1)
    yield* Outbox.pruneReleased()
    expect(Arr.map(yield* Outbox.pending, (e) => e.seq)).toEqual([a.seq])
    expect((yield* rowCount)[0]?.n).toBe(1)
    expect((yield* append("c")).seq).toBe(b.seq + 1)
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test composition root owns an isolated in-memory database for this test only.
  }).pipe(Effect.provide(Db))
)

it.effect("released rows inside the retention window are not pruned", () =>
  Effect.gen(function* () {
    const a = yield* append("a")
    yield* Outbox.markReleased([a.seq])
    yield* TestClock.adjust(Outbox.defaultReleasedRetentionMillis - 1)
    yield* Outbox.pruneReleased()
    expect((yield* append("a")).seq).toBe(a.seq)
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test composition root owns an isolated in-memory database for this test only.
  }).pipe(Effect.provide(Db))
)
