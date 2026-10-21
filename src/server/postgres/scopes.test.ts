import { assert, describe, it } from "@effect/vitest"
import { Effect, Fiber, Random, Ref } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { grant, pgAvailable, Replica, revoke, serverView, TestDatabase } from "./test-support.ts"

const insert = (table: string, id: string, scope: string, body = "") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* sql`INSERT INTO ${sql(table)} (id, access_scope_id, body) VALUES (${id}, ${scope}, ${body})`
  })

const readableScopes = (principalId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    const rows = yield* sql<{ scope_id: string }>`
      SELECT access_scope_id AS scope_id FROM echo_sync.scope_grants WHERE principal_id IN (${principalId}, '*')`
    return new Set(rows.map((r) => r.scope_id))
  })

describe.skipIf(!pgAvailable)("scope delivery (§4, §8)", () => {
  it.live("a scope move deletes the row for old-scope readers and delivers it to new-scope readers", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* grant("old", "s1")
      yield* grant("new", "s2")
      yield* insert("items", "a", "s1", "v1")
      const oldReader = new Replica("old", "old-device")
      const newReader = new Replica("new", "new-device")
      yield* oldReader.sync()
      yield* newReader.sync()
      assert.isTrue(oldReader.rows.has("items/a"))
      assert.isFalse(newReader.rows.has("items/a"))

      yield* sql`UPDATE items SET access_scope_id = 's2', body = 'v2' WHERE id = 'a'`
      yield* oldReader.sync()
      yield* newReader.sync()
      assert.isFalse(oldReader.rows.has("items/a"))
      assert.deepStrictEqual(newReader.view(), yield* serverView("new"))
      assert.strictEqual(newReader.rows.get("items/a")?.scopeId, "s2")
      // The old-scope reader received the move as a tombstone, never the new payload.
      assert.isFalse(oldReader.received.some((c) => c._tag === "Upsert" && c.scopeId === "s2"))
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a revoked scope is purged from the replica and nothing from it is delivered afterwards", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* grant("p", "s1", "s2")
      yield* insert("items", "a", "s1")
      yield* insert("notes", "b", "s2")
      const replica = new Replica("p", "d")
      yield* replica.sync()
      assert.isTrue(replica.rows.has("items/a") && replica.rows.has("notes/b"))

      yield* revoke("p", "s1")
      yield* sql`UPDATE items SET body = 'after revoke' WHERE id = 'a'`
      yield* insert("items", "c", "s1")
      const before = replica.received.length
      yield* replica.sync()
      const after = replica.received.slice(before)
      assert.deepStrictEqual(
        after.filter((c) => c._tag === "ScopePurged"),
        [{ _tag: "ScopePurged", scopeId: "s1" }]
      )
      assert.isFalse(after.some((c) => c._tag === "Upsert" && c.scopeId === "s1"))
      assert.deepStrictEqual(replica.view(), yield* serverView("p"))
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live(
    "a granted scope arrives in full, page by page, with no gap against concurrent writes",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        yield* grant("p", "mine")
        yield* grant("other", "shared")
        for (let i = 0; i < 30; i++) yield* insert(i % 2 === 0 ? "items" : "notes", `pre${i}`, "shared", "v0")
        const replica = new Replica("p", "d")
        yield* replica.sync()
        assert.isFalse([...replica.rows.values()].some((r) => r.scopeId === "shared"))

        yield* grant("p", "shared")
        // Writers keep changing the shared scope (updates, deletes, inserts, moves out) while the
        // replica receives the snapshot three rows at a time.
        const done = yield* Ref.make(false)
        const writer = yield* Effect.forkChild(
          Effect.gen(function* () {
            for (let i = 0; i < 60; i++) {
              const n = yield* Random.nextIntBetween(0, 29)
              const table = n % 2 === 0 ? "items" : "notes"
              const op = i % 4
              if (op === 0) yield* sql`UPDATE ${sql(table)} SET body = ${`v${i}`} WHERE id = ${`pre${n}`}`
              else if (op === 1) yield* sql`DELETE FROM ${sql(table)} WHERE id = ${`pre${n}`}`
              else if (op === 2) yield* insert(table, `new${i}`, "shared", `v${i}`)
              else yield* sql`UPDATE ${sql(table)} SET access_scope_id = 'mine' WHERE id = ${`pre${n}`}`
              yield* Effect.sleep("1 millis")
            }
            yield* Ref.set(done, true)
          })
        )
        while (!(yield* Ref.get(done))) yield* replica.pullOnce(3)
        yield* Fiber.join(writer)
        yield* replica.sync(3)
        assert.deepStrictEqual(replica.view(), yield* serverView("p"))
      }).pipe(Effect.provide(TestDatabase)),
    30_000
  )

  it.live(
    "under random writes, moves, grants and revokes a reader never receives an unreadable payload and converges",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient
        const scopes = ["s0", "s1", "s2", "s3"]
        const principals = ["p0", "p1", "p2"]
        for (const scope of scopes) {
          yield* sql`INSERT INTO echo_sync.access_scopes (id) VALUES (${scope})`
        }
        yield* grant("p0", "s0")
        yield* grant("p1", "s1")
        yield* grant("p2", "s2")
        const replicas = principals.map((p) => new Replica(p, `${p}-device`))

        for (let step = 0; step < 300; step++) {
          const roll = yield* Random.nextIntBetween(0, 99)
          const scope = yield* Random.choice(scopes)
          const table = (yield* Random.nextBoolean) ? "items" : "notes"
          const id = `r${yield* Random.nextIntBetween(0, 19)}`
          if (roll < 35) {
            yield* sql`
              INSERT INTO ${sql(table)} (id, access_scope_id, body) VALUES (${id}, ${scope}, ${`b${step}`})
              ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body`
          } else if (roll < 55) {
            yield* sql`UPDATE ${sql(table)} SET access_scope_id = ${scope} WHERE id = ${id}`
          } else if (roll < 65) {
            yield* sql`DELETE FROM ${sql(table)} WHERE id = ${id}`
          } else if (roll < 72) {
            const principal = yield* Random.choice([...principals, "*"])
            yield* grant(principal, scope)
          } else if (roll < 78) {
            yield* revoke(yield* Random.choice([...principals, "*"]), scope)
          } else {
            const replica = yield* Random.choice(replicas)
            const readable = yield* readableScopes(replica.principalId)
            const response = yield* replica.pullOnce(yield* Random.nextIntBetween(1, 5))
            for (const change of response.changes) {
              if (change._tag === "Upsert") assert.isTrue(readable.has(change.scopeId), `leaked ${change.scopeId}`)
            }
          }
        }
        for (const replica of replicas) {
          yield* replica.sync(4)
          assert.deepStrictEqual(replica.view(), yield* serverView(replica.principalId), replica.principalId)
        }
      }).pipe(Random.withSeed("echo-sync-scopes"), Effect.provide(TestDatabase)),
    60_000
  )
})
