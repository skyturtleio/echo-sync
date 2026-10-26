import { expect, it } from "vitest"
import { String as Str } from "effect"
import { checkRegistry, testTitles } from "../scripts/registry.ts"

it("a nonexistent cited test, duplicate invariant or unindexed question cannot pass the registry gate", () => {
  const spec = `INV-X-1 OQ-X-1
## 17. Invariant registry
| INV-X-1 | covered | "real" |
## 18.
## Appendix B. Open Questions index
| OQ-X-1 | §0 | question |
## Appendix C.
`
  const titles = new Set(
    testTitles('it.effect("real", () => {}); // it("fake", () => {})\nconst s = "fake"', "example.test.ts")
  )
  expect(checkRegistry(spec, titles).errors).toEqual([])
  expect(checkRegistry(Str.replace('"real"', '"fake"')(spec), titles).errors).toHaveLength(1)
  expect(
    checkRegistry(Str.replace("## 18.", '| INV-X-1 | covered | "real" |\n## 18.')(spec), titles).errors
  ).toHaveLength(1)
  expect(checkRegistry(Str.replace("INV-X-1 OQ-X-1", "INV-X-2 OQ-X-2")(spec), titles).errors).toHaveLength(3)
  expect(checkRegistry(Str.replace('"real"', '"real", "nonexistent')(spec), titles).errors).not.toEqual([])
  expect(
    checkRegistry(Str.replace("## 18.", ' | INV-X-1 | covered | "nonexistent" |\n## 18.')(spec), titles).errors
  ).toHaveLength(2)
  expect(
    checkRegistry(
      Str.replace('| INV-X-1 | covered | "real" |', '<!--\n| INV-X-1 | covered | "real" |\n-->')(spec),
      titles
    ).errors
  ).not.toEqual([])
  expect(
    checkRegistry(Str.replace("| OQ-X-1 | §0 | question |", "<!--\n| OQ-X-1 | §0 | question |\n-->")(spec), titles)
      .errors
  ).not.toEqual([])
  expect(checkRegistry(Str.replace('"real"', '"real", ""')(spec), titles).errors).not.toEqual([])
})
