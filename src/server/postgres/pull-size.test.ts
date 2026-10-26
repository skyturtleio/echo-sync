import { assert, describe, it } from "@effect/vitest"
import { Effect, Option, Schema } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { Protocol } from "echo-sync"
import { registerClient } from "./ledger.ts"
import { maxPullPageBytes, pull } from "./pull.ts"
import { grant, pgAvailable, registry, TestDatabase } from "./test-support.ts"

const encoded = Schema.encodeSync(Schema.toCodecJson(Protocol.PullResponse))
const bytes = (response: Protocol.PullResponse) => new TextEncoder().encode(JSON.stringify(encoded(response))).length
const request = { principalId: "p", clientId: "d", cursor: null, continuation: Option.none<string>(), limit: 500 }

const records = Effect.gen(function* () {
  const sql = yield* SqlClient
  return yield* sql`SELECT position, scope_ids FROM echo_sync.client_scopes WHERE client_id = 'd' ORDER BY position`
})

describe.skipIf(!pgAvailable)("pull byte safety", () => {
  it.live("UTF-8 oversized walks split without losing or duplicating rows or recording an unfinished cursor", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* grant("p", "s")
      const body = "水".repeat(180_000)
      // A character-count cap would incorrectly accept all three rows together.
      assert.isBelow(body.length * 3, maxPullPageBytes)
      assert.isAbove(new TextEncoder().encode(body).length * 3, maxPullPageBytes)
      yield* sql`INSERT INTO items (id, access_scope_id, body)
                SELECT 'row-' || n, 's', ${body} FROM generate_series(1, 3) n`
      let continuation = Option.none<string>()
      const ids: Array<string> = []
      let pages = 0
      while (true) {
        assert.isBelow(pages++, 10)
        const page = yield* pull(registry, { ...request, continuation })
        assert.isAtMost(bytes(page), maxPullPageBytes)
        for (const change of page.changes) {
          if (change._tag === "upsert" && change.table === "items") {
            ids.push(String(change.row["id"]))
            assert.strictEqual(change.row["body"], body)
          }
        }
        if (page._tag === "PullDone") {
          assert.strictEqual((yield* records).length, 1)
          break
        }
        assert.deepStrictEqual(yield* records, [])
        continuation = Option.some(page.continuation)
      }
      assert.isAbove(pages, 1)
      assert.deepStrictEqual(ids, ["row-1", "row-2", "row-3"])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("byte-split log pages deliver every change and intervening ack exactly once (INV-PULL-7/10/11)", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* grant("p", "s")
      const initial = yield* pull(registry, request)
      assert.strictEqual(initial._tag, "PullDone")
      if (initial._tag !== "PullDone") return
      for (let n = 1; n <= 3; n++) {
        yield* sql`INSERT INTO items (id, access_scope_id, body) VALUES (${`row-${n}`}, 's', ${"🙂".repeat(140_000)})`
        // Each row and outcome fits alone, but one of each exceeds the page cap.
        const outcome = { _tag: "Rejected", error: { _tag: "UnknownMutation", name: "x".repeat(600_000) } }
        yield* sql`INSERT INTO echo_sync.mutations (client_id, mutation_id, seq, position, outcome)
                  VALUES ('d', ${`ack-${n}`}, ${n}, echo_sync.next_position(), ${JSON.stringify(outcome)}::jsonb)`
      }
      let continuation = Option.none<string>()
      const ids: Array<string> = []
      const acks: Array<string> = []
      let pages = 0
      while (true) {
        assert.isBelow(pages++, 10)
        const page = yield* pull(registry, { ...request, cursor: initial.nextCursor, continuation })
        assert.isAtMost(bytes(page), maxPullPageBytes)
        ids.push(...page.changes.flatMap((c) => (c._tag === "upsert" ? [String(c.row["id"])] : [])))
        acks.push(...page.acks.map((ack) => ack.mutationId))
        if (page._tag === "PullDone") break
        assert.strictEqual((yield* records).length, 1)
        continuation = Option.some(page.continuation)
      }
      assert.isAbove(pages, 1)
      assert.deepStrictEqual(ids, ["row-1", "row-2", "row-3"])
      assert.deepStrictEqual(acks, ["ack-1", "ack-2", "ack-3"])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("oversized rows and mixed acks complete initial and incremental pulls without losing progress", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* grant("p", "s")
      yield* registerClient("p", "d")
      const initial = yield* pull(registry, request)
      if (initial._tag !== "PullDone") return assert.fail("expected initial snapshot")
      const body = "水".repeat(400_000)
      yield* sql`INSERT INTO items (id, access_scope_id, body) VALUES ('large', 's', ${body}), ('next', 's', 'small')`
      const outcome = { _tag: "Rejected", error: { _tag: "UnknownMutation", name: "x".repeat(maxPullPageBytes) } }
      yield* sql`INSERT INTO echo_sync.mutations (client_id, mutation_id, seq, position, outcome)
                VALUES ('d', 'ack', 1, echo_sync.next_position(), ${JSON.stringify(outcome)}::jsonb)`
      for (const cursor of [null, initial.nextCursor]) {
        let continuation = Option.none<string>()
        const ids: Array<string> = []
        const acks: Array<string> = []
        let pages = 0
        while (true) {
          assert.isBelow(pages++, 10)
          const page = yield* pull(registry, { ...request, cursor, continuation })
          for (const change of page.changes) {
            if (change._tag !== "upsert" || change.table !== "items") continue
            ids.push(String(change.row["id"]))
            if (change.row["id"] === "large") {
              assert.strictEqual(change.row["body"], body)
              assert.isAbove(bytes(page), maxPullPageBytes)
              assert.deepStrictEqual(page.acks, [])
            }
          }
          acks.push(...page.acks.map((ack) => ack.mutationId))
          if (page._tag === "PullDone") break
          assert.strictEqual((yield* records).length, cursor === null ? 1 : 2)
          continuation = Option.some(page.continuation)
        }
        assert.deepStrictEqual(ids, ["large", "next"])
        assert.deepStrictEqual(acks, ["ack"])
      }
      assert.deepStrictEqual(yield* sql`SELECT acked_at FROM echo_sync.mutations WHERE client_id = 'd'`, [
        { acked_at: null }
      ])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("ack-only backlogs split within the byte cap and resume without dropping or repeating outcomes", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* registerClient("p", "small")
      yield* sql`INSERT INTO echo_sync.mutations (client_id, mutation_id, seq, position, outcome)
                SELECT 'small', 'small-' || n, n, echo_sync.next_position(), '{"_tag":"Applied"}'::jsonb
                FROM generate_series(1, 501) n`
      const smallFirst = yield* pull(registry, { ...request, clientId: "small" })
      if (smallFirst._tag !== "PullPage") return assert.fail("expected count-limited acks")
      assert.strictEqual(smallFirst.acks.length, 500)
      const smallLast = yield* pull(registry, {
        ...request,
        clientId: "small",
        continuation: Option.some(smallFirst.continuation)
      })
      assert.strictEqual(smallLast._tag, "PullDone")
      assert.deepStrictEqual(
        [...smallFirst.acks, ...smallLast.acks].map((ack) => ack.mutationId),
        Array.from({ length: 501 }, (_, i) => `small-${i + 1}`)
      )
      yield* registerClient("p", "d")
      const initial = yield* pull(registry, request)
      if (initial._tag !== "PullDone") return assert.fail("expected empty initial snapshot")
      for (let n = 1; n <= 3; n++) {
        const outcome = { _tag: "Rejected", error: { _tag: "UnknownMutation", name: "x".repeat(600_000) } }
        yield* sql`INSERT INTO echo_sync.mutations (client_id, mutation_id, seq, position, outcome)
                  VALUES ('d', ${`ack-${n}`}, ${n}, echo_sync.next_position(), ${JSON.stringify(outcome)}::jsonb)`
      }
      for (const cursor of [null, initial.nextCursor]) {
        let continuation = Option.none<string>()
        const seen: Array<string> = []
        let pages = 0
        while (true) {
          assert.isBelow(pages++, 5)
          const page = yield* pull(registry, { ...request, cursor, continuation })
          assert.isAtMost(bytes(page), maxPullPageBytes)
          assert.deepStrictEqual(page.changes, [])
          seen.push(...page.acks.map((ack) => ack.mutationId))
          if (page._tag === "PullDone") break
          continuation = Option.some(page.continuation)
        }
        assert.strictEqual(pages, 3)
        assert.deepStrictEqual(seen, ["ack-1", "ack-2", "ack-3"])
      }
      const outcome = { _tag: "Rejected", error: { _tag: "UnknownMutation", name: "x".repeat(maxPullPageBytes) } }
      yield* sql`INSERT INTO echo_sync.mutations (client_id, mutation_id, seq, position, outcome)
                VALUES ('d', 'too-large', 4, echo_sync.next_position(), ${JSON.stringify(outcome)}::jsonb)`
      const first = yield* pull(registry, request)
      if (first._tag !== "PullPage") return assert.fail("expected ack pagination")
      let continuation = Option.some<string>(first.continuation)
      for (let n = 0; n < 2; n++) {
        const page = yield* pull(registry, { ...request, continuation })
        if (page._tag !== "PullPage") return assert.fail("must not complete before oversized ack")
        continuation = Option.some(page.continuation)
      }
      const final = yield* pull(registry, { ...request, continuation })
      assert.strictEqual(final._tag, "PullDone")
      assert.isAbove(bytes(final), maxPullPageBytes)
      assert.deepStrictEqual(
        final.acks.map((ack) => ack.mutationId),
        ["too-large"]
      )
      const delivered = final.acks[0]?.outcome
      if (delivered?._tag !== "Rejected" || delivered.error._tag !== "UnknownMutation") {
        return assert.fail("expected UnknownMutation outcome")
      }
      assert.strictEqual(delivered.error.name, outcome.error.name)
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("oversized continuation metadata still resumes without recording an unfinished cursor", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* grant("p", "s")
      const initial = yield* pull(registry, request)
      if (initial._tag !== "PullDone") return assert.fail("expected initial snapshot")
      yield* sql`INSERT INTO items (id, access_scope_id) VALUES ('a', 's'), ('b', 's')`
      // Continuations are untrusted: repeated readable scopes are not authorization, but still
      // consume bytes when re-encoded. Two log rows ensure this candidate needs a continuation.
      const continuation = JSON.stringify({
        clientId: "d",
        principalId: "p",
        cursor: String(initial.nextCursor),
        position: String(initial.nextCursor),
        scopes: Array.from({ length: 180_000 }, () => "s"),
        walking: []
      })
      const before = yield* records
      const page = yield* pull(registry, {
        ...request,
        cursor: initial.nextCursor,
        continuation: Option.some(continuation),
        limit: 1
      })
      if (page._tag !== "PullPage") return assert.fail("expected continuation")
      assert.isAbove(bytes(page), maxPullPageBytes)
      assert.deepStrictEqual(yield* records, before)
      const final = yield* pull(registry, {
        ...request,
        cursor: initial.nextCursor,
        continuation: Option.some(page.continuation),
        limit: 1
      })
      assert.strictEqual(final._tag, "PullDone")
      assert.deepStrictEqual(
        [...page.changes, ...final.changes].map((c) => (c._tag === "upsert" ? c.row["id"] : "")),
        ["a", "b"]
      )
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )

  it.live("a huge configured row budget still constructs at most 500 changes per candidate", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient
      yield* grant("p", "s")
      yield* sql`INSERT INTO items (id, access_scope_id) SELECT 'row-' || n, 's' FROM generate_series(1, 600) n`
      const page = yield* pull(registry, { ...request, limit: Number.MAX_SAFE_INTEGER })
      assert.strictEqual(page._tag, "PullPage")
      assert.strictEqual(page.changes.length, 500)
      assert.isAtMost(bytes(page), maxPullPageBytes)
      assert.deepStrictEqual(yield* records, [])
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Test root owns the disposable database until test completion.
    }).pipe(Effect.provide(TestDatabase))
  )
})
