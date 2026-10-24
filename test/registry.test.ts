import { expect, it } from "vitest"
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
  expect(checkRegistry(spec.replace('"real"', '"fake"'), titles).errors).toHaveLength(1)
  expect(checkRegistry(spec.replace("## 18.", '| INV-X-1 | covered | "real" |\n## 18.'), titles).errors).toHaveLength(1)
  expect(checkRegistry(spec.replace("INV-X-1 OQ-X-1", "INV-X-2 OQ-X-2"), titles).errors).toHaveLength(3)
  expect(checkRegistry(spec.replace('"real"', '"real", "nonexistent'), titles).errors).not.toEqual([])
  expect(
    checkRegistry(spec.replace("## 18.", ' | INV-X-1 | covered | "nonexistent" |\n## 18.'), titles).errors
  ).toHaveLength(2)
  expect(
    checkRegistry(spec.replace('| INV-X-1 | covered | "real" |', '<!--\n| INV-X-1 | covered | "real" |\n-->'), titles)
      .errors
  ).not.toEqual([])
  expect(
    checkRegistry(spec.replace("| OQ-X-1 | §0 | question |", "<!--\n| OQ-X-1 | §0 | question |\n-->"), titles).errors
  ).not.toEqual([])
  expect(checkRegistry(spec.replace('"real"', '"real", ""'), titles).errors).not.toEqual([])
})
