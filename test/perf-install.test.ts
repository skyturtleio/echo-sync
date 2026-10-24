import { expect, it } from "@effect/vitest"
import { Effect, Option } from "effect"
import * as Reactivity from "effect/reactivity/Reactivity"
import { Counters } from "../src/core/testing/SimApp.ts"
import { installFixture } from "./perf-install.ts"

it.live("install undoes P entries, writes C changes and replays P handlers without hidden statements", () =>
  Effect.gen(function* () {
    for (const snapshot of [false, true]) {
      const p = 3
      const c = 7
      const result = yield* installFixture(p, c, snapshot)
      // Engine install: BEGIN + staged SELECT + meta SELECT + pending (undo SELECT,
      // Outbox.pending SELECT) + setMeta + clearStaged + COMMIT = 8.
      // Each pending increment: undo write 1; replay get (row + 2 grants) 3,
      // patch (row + 2 grants + write) 4, setUndo 1 => 9P. Install = C writes.
      // Null cursor adds clearAll for 2 tables + the snapshot's grant write.
      expect(result.statements).toHaveLength(8 + 9 * p + c + (snapshot ? 3 : 0))
      // Each pending is returned twice; replay reads 2 counter rows + own grant twice
      // (Everyone absent). Meta 1 and one staged JSON row: 6P + 2 returned rows.
      expect(result.statements.reduce((sum, s) => sum + s.rows, 0)).toBe(6 * p + 2)
      const writes = result.statements.filter((s) => s.sql.startsWith('INSERT INTO "counters"'))
      expect(writes).toHaveLength(p + c + p)
      // Distinct values expose skipped/duplicated phases even if the total stays constant:
      // undo restores 100, install writes 200, replay adds 3 to the authoritative 200.
      const valueColumn = Counters.columns.findIndex((column) => column.name === "value")
      expect(writes.map((s) => s.params[valueColumn])).toEqual([
        ...Array.from({ length: p }, () => 100),
        ...Array.from({ length: c }, () => 200),
        ...Array.from({ length: p }, () => 203)
      ])
      expect(result.statements.filter((s) => s.sql.startsWith("UPDATE echo_outbox SET undo"))).toHaveLength(p)
      expect(result.final.rows.map((r) => [r["id"], r["value"]])).toEqual(
        Array.from({ length: c }, (_, i) => [`i-${i}`, i < p ? 203 : 200])
      )
      expect(result.final.pending).toHaveLength(p)
      expect(
        result.final.pending.map((entry) => entry.undo.map((image) => Option.map(image.before, (r) => r["value"])))
      ).toEqual(Array.from({ length: p }, () => [Option.some(200)]))
      expect(result.final.meta.cursor).toBe(2n)
    }
  }).pipe(Effect.scoped, Effect.provide(Reactivity.layer))
)
