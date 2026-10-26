import { parseSync, Visitor } from "oxc-parser"
import { Array as Arr, Console, Effect, Option, String as Str } from "effect"
// oxlint-disable-next-line effecttsgo/node-builtin-import -- Synchronous source inspection is also used by ordinary Vitest callbacks.
import { readFileSync } from "node:fs"
// oxlint-disable-next-line effecttsgo/node-builtin-import -- Synchronous repository discovery at the CLI/test boundary.
import { globSync } from "node:fs"

/** Only executable test declarations count; comments and arbitrary strings never do. */
export function testTitles(source: string, file: string): string[] {
  const titles: string[] = []
  const tree = parseSync(file, source)
  if (tree.errors.length) throw new Error(`Cannot parse ${file}`)
  new Visitor({
    CallExpression(node) {
      const callee = Str.slice(node.callee.start, node.callee.end)(source)
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
  spec = Str.replace(/<!--[\s\S]*?(?:-->|$)/g, "")(spec)
  const registry = Str.split(Str.split(spec, "## 17. Invariant registry\n")[1] ?? "", "## 18.")[0] ?? ""
  const appendix =
    Str.split(Str.split(spec, "## Appendix B. Open Questions index\n")[1] ?? "", "## Appendix C.")[0] ?? ""
  const errors: string[] = []
  const debt: string[] = []
  const ids = new Set<string>()
  for (const row of Arr.filter(Arr.map(Str.split(registry, "\n"), Str.trim), (line) => /^\|\s*INV-/.test(line))) {
    const match = /^\| (INV-[A-Z]+-\d+) \| (covered|partial|untested|withdrawn) \| (.*) \|$/.exec(row)
    if (!match) {
      errors.push(`Malformed registry row: ${row}`)
      continue
    }
    const [, id = "", status, citations = ""] = match
    if (ids.has(id)) errors.push(`Duplicate ${id}`)
    ids.add(id)
    if (status === "partial" || status === "untested") debt.push(id)
    if ((Str.match(/"/g)(citations).pipe(Option.getOrUndefined)?.length ?? 0) % 2 !== 0)
      errors.push(`${id}: unterminated test citation`)
    const names = Arr.map([...Str.matchAll(/"([^"\n]*)"/g)(citations)], (m) => m[1] ?? "")
    if (names.length === 0 && status === "covered") errors.push(`${id}: no named test`)
    for (const name of names) if (!titles.has(name)) errors.push(`${id}: missing test ${JSON.stringify(name)}`)
  }
  for (const id of new Set(Str.match(/\bINV-[A-Z]+-\d+\b/g)(spec).pipe(Option.getOrElse(() => [])))) {
    if (!ids.has(id)) errors.push(`${id}: missing from §17`)
  }
  const indexed = Arr.map([...Str.matchAll(/^[ \t]*\| (OQ-[A-Z]+-\d+) \|/gm)(appendix)], (m) => m[1] ?? "")
  if (new Set(indexed).size !== indexed.length) errors.push("Duplicate open question in Appendix B")
  const text = Str.replace(appendix, "")(spec)
  const mentioned = new Set(Str.match(/\bOQ-[A-Z]+-\d+\b/g)(text).pipe(Option.getOrElse(() => [])))
  for (const id of indexed) if (!mentioned.has(id)) errors.push(`${id}: indexed but absent from text`)
  for (const id of mentioned) if (!Arr.contains(indexed, id)) errors.push(`${id}: absent from Appendix B`)
  if (ids.size === 0) errors.push("Missing invariant registry")
  return { errors, debt }
}

if (import.meta.main) {
  const titles = new Set(
    Arr.flatMap(globSync(["src/**/*.test.ts", "test/**/*.test.ts"]), (file) =>
      testTitles(readFileSync(file, "utf8"), file)
    )
  )
  const result = checkRegistry(readFileSync("docs/SPEC.md", "utf8"), titles)
  Effect.runSync(Console.log(`Registry debt (${result.debt.length}): ${Arr.join(result.debt, ", ")}`))
  for (const error of result.errors) Effect.runSync(Console.error(error))
  if (result.errors.length) process.exitCode = 1
}
