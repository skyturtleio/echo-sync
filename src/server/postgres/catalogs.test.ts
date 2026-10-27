/* oxlint-disable effecttsgo/strict-effect-provide -- Tests are entry points owning disposable Postgres databases. */
import { describe, expect, it } from "@effect/vitest"
import { Array as Arr, Effect, Layer, Schema, String as Str } from "effect"
import type { PgClient } from "@effect/sql-pg/PgClient"
import { HttpRouter } from "effect/http"
import { SqlClient } from "effect/sql/SqlClient"
import { AccessScope, Mutation, Protocol, Registry, SyncTable } from "echo-sync"
import { gunzipSync } from "node:zlib"
import { createSynced, pgAvailable, TestDatabase } from "./test-support.ts"
import * as Catalog from "./catalogs.ts"
import { layerHttp } from "./rpc.ts"
import { resolveTables } from "./schema.ts"
import { verify } from "../migrate/verify.ts"

const Airport = SyncTable.catalog(
  "airports",
  {
    id: Schema.String.pipe(Schema.brand("AirportId")),
    name: Schema.String
  },
  { indexes: { by_name: ["name"] } }
)
const registry = Registry.make([Airport])

describe.skipIf(!pgAvailable)("global catalogs", () => {
  it.effect(
    "identical catalog reimports keep their version; committed imports change discovery without capture or redeploy",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        yield* createSynced(Airport)
        yield* sql`INSERT INTO airports VALUES ('z', 'Zulu'), ('a', 'Alpha')`
        const catalog = yield* Catalog.Catalogs
        const first = yield* catalog.get(Airport)
        expect(new TextDecoder().decode(gunzipSync(first.bytes))).toBe(
          '[{"id":"a","name":"Alpha"},{"id":"z","name":"Zulu"}]'
        )
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`TRUNCATE airports`
            yield* sql`INSERT INTO airports VALUES ('a', 'Alpha'), ('z', 'Zulu')`
          })
        )
        expect((yield* catalog.get(Airport)).version).toBe(first.version)
        yield* sql`UPDATE airports SET name = 'Changed' WHERE id = 'a'`
        expect((yield* catalog.versions)["airports"]).not.toBe(first.version)
        yield* sql`DELETE FROM airports WHERE id = 'z'`
        const deleted = yield* catalog.get(Airport)
        expect(new TextDecoder().decode(gunzipSync(deleted.bytes))).toBe('[{"id":"a","name":"Changed"}]')
        expect(yield* sql`SELECT * FROM echo_sync.log WHERE table_name = 'airports'`).toEqual([])
        expect(Arr.map(yield* resolveTables(registry), (t) => t.table.name)).toEqual(["scope_grants"])
        yield* verify(registry)
        yield* sql`DROP TRIGGER echo_sync_catalog ON airports`
        const drift = yield* Effect.flip(verify(registry))
        expect(drift._tag).toBe("Drift")
        if (drift._tag === "Drift")
          expect(Arr.some(drift.mismatches, (m) => m.table === "airports" && Str.includes("trigger")(m.problem))).toBe(
            true
          )
      }).pipe(Effect.provide(Catalog.layer(registry).pipe(Layer.provideMerge(TestDatabase))))
  )

  it.effect(
    "catalog HTTP refuses anonymous and wildcard actors, serves immutable authenticated bytes and rejects stale versions",
    () =>
      Effect.gen(function* () {
        yield* createSynced(Airport)
        const sql = yield* SqlClient
        yield* sql`INSERT INTO airports VALUES ('a', 'Alpha')`
        const db = yield* Effect.context<SqlClient | PgClient>()
        const cache = yield* Layer.build(Catalog.layer(registry))
        const catalog = yield* Catalog.Catalogs.pipe(Effect.provide(cache))
        const version = (yield* catalog.get(Airport)).version
        const app = layerHttp({
          registry,
          mutations: Mutation.makeRegistry([]),
          principalFor: (headers) =>
            headers["x-principal"] === undefined
              ? Effect.fail(new Protocol.Unauthorized({ reason: "authenticationRefused", message: "sign in" }))
              : Effect.succeed(AccessScope.PrincipalId.make(headers["x-principal"]))
        }).pipe(Layer.provide(Layer.succeedContext(db)))
        const web = HttpRouter.toWebHandler(app, { disableLogger: true })
        yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
        const url = `http://localhost/sync/catalog/airports/${version}`
        const request = (principal?: string) =>
          Effect.promise(() =>
            web.handler(new Request(url, principal === undefined ? {} : { headers: { "x-principal": principal } }))
          )
        expect((yield* request()).status).toBe(401)
        expect((yield* request("*")).status).toBe(401)
        const response = yield* request("p")
        expect(response.status).toBe(200)
        expect(response.headers.get("cache-control")).toContain("private")
        expect(response.headers.get("etag")).toBe(`"${version}"`)
        const unchanged = yield* Effect.promise(() =>
          web.handler(
            new Request(url, {
              headers: { "x-principal": "p", "if-none-match": `"${version}"` }
            })
          )
        )
        expect(unchanged.status).toBe(304)
        expect(
          new TextDecoder().decode(gunzipSync(new Uint8Array(yield* Effect.promise(() => response.arrayBuffer()))))
        ).toContain("Alpha")
        yield* sql`UPDATE airports SET name = 'New'`
        expect((yield* request("p")).status).toBe(404)
      }).pipe(Effect.scoped, Effect.provide(TestDatabase))
  )
})
