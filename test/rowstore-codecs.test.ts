// oxlint-disable-next-line import/no-unassigned-import -- side-effect: Node fetch shim for wa-sqlite
import "../src/client/testing/nodeWasm.ts"
import { assert, describe, expect, it } from "@effect/vitest"
import * as PgClient from "@effect/sql-pg/PgClient"
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { Array as Arr, Cause, Effect, Exit, Layer, Option, Schema } from "effect"
import * as Reactivity from "effect/reactivity/Reactivity"
import { SqlClient } from "effect/sql/SqlClient"
import { makeCompilerSqlite } from "effect/sql/Statement"
import { AccessScope, Registry, type Store, SyncTable } from "echo-sync"
import * as SqliteRowStore from "../src/client/SqliteRowStore.ts"
import * as PostgresRowStore from "../src/server/postgres/rowStore.ts"
import { EmptyDatabase, pgAvailable } from "../src/server/postgres/test-support.ts"
import { countingClient } from "./sql-counter.ts"

const Items = SyncTable.make(
  "codec_items",
  {
    id: Schema.String.pipe(Schema.brand("CodecItemId")),
    accessScopeId: AccessScope.AccessScopeId,
    flag: Schema.Boolean,
    rank: Schema.NullOr(Schema.Int),
    title: Schema.String,
    at: Schema.DateTimeUtc,
    doc: Schema.Struct({ nested: Schema.Array(Schema.Json) })
  },
  { indexes: { by_flag_rank: ["flag", "rank"] } }
)
const registry = Registry.make([Items])
const plan: Store.IndexRangePlan = {
  index: Items.indexes.by_flag_rank,
  eq: [],
  lower: Option.none(),
  upper: Option.none(),
  order: "asc",
  readableBy: Option.none(),
  limit: Option.none()
}
const row = (id: string, rank: number | null): SyncTable.EncodedRow => ({
  id,
  access_scope_id: "scope",
  flag: true,
  rank,
  title: "keep",
  at: -1234,
  doc: { nested: [true, null, { value: "json" }] }
})

const defect = <A, E>(exit: Exit.Exit<A, E>): Error => {
  assert(Exit.isFailure(exit))
  assert.equal(exit.cause.reasons.length, 1)
  const reason = exit.cause.reasons[0]
  assert(reason !== undefined && Cause.isDieReason(reason))
  assert(reason.defect instanceof Error)
  return reason.defect
}

for (const dialect of ["sqlite", "postgres"] as const) {
  const fixture = Effect.gen(function* () {
    const source = yield* SqlClient
    yield* source.unsafe(Items[dialect].createTable)
    const counter = yield* countingClient(source, dialect === "sqlite" ? makeCompilerSqlite() : PgClient.makeCompiler())
    const store =
      dialect === "sqlite"
        ? SqliteRowStore.make(counter.client, registry)
        : yield* PostgresRowStore.make(new Map([[Items.name, "public"]])).pipe(
            Effect.provideService(SqlClient, counter.client)
          )
    return { store, counter }
  })
  const database = (dialect === "sqlite" ? SqliteClient.layerMemory({}) : EmptyDatabase).pipe(
    Layer.provideMerge(Reactivity.layer)
  )

  describe.skipIf(dialect === "postgres" && !pgAvailable)(`${dialect} trusted row codecs`, () => {
    it.live(
      "range codecs run only on execution, validate limits first, and retain contextual defects without SQL",
      () =>
        Effect.gen(function* () {
          const { store, counter } = yield* fixture
          let reads = 0
          const invalid = {
            ...plan,
            get after() {
              reads++
              return ["not a boolean", null, "a"]
            }
          }
          const query = store.range(Items, invalid)
          const limited = store.range(Items, {
            ...plan,
            limit: Option.some(-1),
            get after() {
              return invalid.after
            }
          })
          expect(reads).toBe(0)
          expect(defect(yield* Effect.exit(limited))).toBeInstanceOf(RangeError)
          expect(reads).toBe(0)
          for (let run = 0; run < 2; run++) {
            const before = reads
            const error = defect(yield* Effect.exit(query))
            expect(reads).toBeGreaterThan(before)
            expect(error.message).toContain("range")
            expect(error.message).toContain("codec_items")
            expect(error.message).toContain("flag")
            expect(Schema.isSchemaError(error.cause)).toBe(true)
          }
          for (const bounds of [
            { eq: ["not a boolean"] },
            { lower: Option.some({ value: "not a boolean", inclusive: false }) },
            { upper: Option.some({ value: "not a boolean", inclusive: true }) },
            { before: ["not a boolean", null, "z"] }
          ]) {
            const error = defect(yield* Effect.exit(store.range(Items, { ...plan, ...bounds })))
            expect(Schema.isSchemaError(error.cause)).toBe(true)
            expect(error.message).toContain("flag")
          }
          expect(counter.statements).toEqual([])
          // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test composition root owns its disposable database and reserved connection.
        }).pipe(Effect.scoped, Effect.provide(database))
    )

    it.live("reexecuted ranges bind nullable cursors afresh and row writes round-trip JSON and scalar values", () =>
      Effect.gen(function* () {
        const { store, counter } = yield* fixture
        const rows = [row("a", null), row("c", null), row("b", 2), row("d", 2), row("e", 5)]
        for (const value of rows) {
          const id = value["id"]
          assert(typeof id === "string")
          yield* counter.client.withTransaction(store.write(Items, id, Option.some(value)))
          expect(yield* store.read(Items, id)).toEqual(Option.some(value))
        }
        const cursor: Array<Store.KeyValue> = [true, null, "a"]
        const title = Arr.findFirst(Items.columns, (c) => c.name === "title")
        assert(Option.isSome(title))
        const query = store.range(Items, {
          ...plan,
          eq: [true],
          after: cursor,
          before: [true, 5, "e"],
          text: { column: title.value, needle: "keep" },
          limit: Option.some(2)
        })
        expect(Arr.map(yield* query, (r) => r["id"])).toEqual(["c", "b"])
        cursor[2] = "c"
        expect(Arr.map(yield* query, (r) => r["id"])).toEqual(["b", "d"])
        const invalidWrite = store.write(Items, "bad", Option.some({ ...row("bad", 0), flag: "bad" }))
        counter.reset()
        const error = defect(yield* Effect.exit(invalidWrite))
        expect(error.message).toContain("write")
        expect(error.message).toContain("codec_items")
        expect(error.message).toContain("bad")
        expect(Schema.isSchemaError(error.cause)).toBe(true)
        expect(counter.statements).toEqual([])
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test composition root owns its disposable database and reserved connection.
      }).pipe(Effect.scoped, Effect.provide(database))
    )

    it.live("writes encode current rows on each execution and reject missing nullable fields before SQL", () =>
      Effect.gen(function* () {
        const { store, counter } = yield* fixture
        const input = { ...row("lazy", 1) }
        const write = store.write(Items, "lazy", Option.some(input))
        expect(counter.statements).toEqual([])
        for (const rank of [8, 9]) {
          input["rank"] = rank
          yield* counter.client.withTransaction(write)
          expect(yield* store.read(Items, "lazy")).toEqual(Option.some(input))
        }
        if (dialect === "sqlite") {
          counter.reset()
          const replica = SqliteRowStore.make(counter.client, registry)
          const batch = replica.writeMany(Items, [input])
          expect(counter.statements).toEqual([])
          for (const rank of [10, 11]) {
            input["rank"] = rank
            yield* counter.client.withTransaction(batch)
            expect(yield* store.read(Items, "lazy")).toEqual(Option.some(input))
          }
        }
        delete input["rank"]
        counter.reset()
        const error = defect(yield* Effect.exit(write))
        expect(error.message).toContain('row "lazy"')
        expect(Schema.isSchemaError(error.cause)).toBe(true)
        expect(String(error.cause)).toContain('["rank"]')
        expect(counter.statements).toEqual([])
        if (dialect === "postgres") {
          const stored = { ...(yield* Schema.encodeEffect(Items.postgres.row)(row("decode", 1))) }
          const decode = PostgresRowStore.decodeRow(Items, stored)
          for (const flag of [false, true]) {
            stored["flag"] = flag
            expect((yield* decode)["flag"]).toBe(flag)
          }
        }
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Per-test composition root owns its disposable database and reserved connection.
      }).pipe(Effect.scoped, Effect.provide(database))
    )
  })
}
