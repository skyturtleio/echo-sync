import { parseSync, Visitor } from "oxc-parser"
import { readFileSync } from "node:fs"
import { globSync } from "node:fs"

/** Only executable test declarations count; comments and arbitrary strings never do. */
export function testTitles(source: string, file: string): string[] {
  const titles: string[] = []
  const tree = parseSync(file, source)
  if (tree.errors.length) throw new Error(`Cannot parse ${file}`)
  new Visitor({
    CallExpression(node) {
      const callee = source.slice(node.callee.start, node.callee.end)
      const title = node.arguments[0]
      if (
        /^(?:it|test)(?:\.effect|\.live|\.scoped|\.only)?$/.test(callee) &&
        title?.type === "Literal" &&
        typeof title.value === "string"
      )
        titles.push(title.value)
    }
  }).visit(tree.program)
  return titles
}

export function checkRegistry(spec: string, titles: ReadonlySet<string>): { errors: string[]; debt: string[] } {
  const registry = spec.split("## 17. Invariant registry\n")[1]?.split("## 18.")[0] ?? ""
  const appendix = spec.split("## Appendix B. Open Questions index\n")[1]?.split("## Appendix C.")[0] ?? ""
  const errors: string[] = []
  const debt: string[] = []
  const ids = new Set<string>()
  for (const match of registry.matchAll(/^\| (INV-[A-Z]+-\d+) \| (covered|partial|untested|withdrawn) \| (.*) \|$/gm)) {
    const [, id = "", status, citations = ""] = match
    if (ids.has(id)) errors.push(`Duplicate ${id}`)
    ids.add(id)
    if (status === "partial" || status === "untested") debt.push(id)
    const names = [...citations.matchAll(/"([^"\n]+)"/g)].map((m) => m[1] ?? "")
    if (names.length === 0 && status === "covered") errors.push(`${id}: no named test`)
    for (const name of names) if (!titles.has(name)) errors.push(`${id}: missing test ${JSON.stringify(name)}`)
  }
  for (const id of new Set(spec.match(/\bINV-[A-Z]+-\d+\b/g))) {
    if (!ids.has(id)) errors.push(`${id}: missing from §17`)
  }
  const indexed = [...appendix.matchAll(/^\| (OQ-[A-Z]+-\d+) \|/gm)].map((m) => m[1] ?? "")
  const text = spec.replace(appendix, "")
  const mentioned = new Set(text.match(/\bOQ-[A-Z]+-\d+\b/g))
  for (const id of indexed) if (!mentioned.has(id)) errors.push(`${id}: indexed but absent from text`)
  for (const id of mentioned) if (!indexed.includes(id)) errors.push(`${id}: absent from Appendix B`)
  if (ids.size === 0) errors.push("Missing invariant registry")
  return { errors, debt }
}

if (import.meta.main) {
  const titles = new Set(
    globSync(["src/**/*.test.ts", "test/**/*.test.ts"]).flatMap((file) => testTitles(readFileSync(file, "utf8"), file))
  )
  const result = checkRegistry(readFileSync("docs/SPEC.md", "utf8"), titles)
  console.log(`Registry debt (${result.debt.length}): ${result.debt.join(", ")}`)
  for (const error of result.errors) console.error(error)
  if (result.errors.length) process.exitCode = 1
}
