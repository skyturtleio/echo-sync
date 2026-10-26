// oxlint-disable-next-line effecttsgo/node-builtin-import -- Synchronous source scan runs in a plain Vitest callback, not an engine runtime.
import { readFileSync, readdirSync } from "node:fs"
import { Array as Arr, String as Str } from "effect"
import { expect, it } from "vitest"

const violationsIn = (source: string): string[] => {
  const violations: string[] = []
  const exports = Arr.flatMap([...Str.matchAll(/export\s*\{([^}]+)\}/g)(source)], (match) =>
    Arr.map(Str.split(match[1] ?? "", ","), (entry) => Str.split(Str.trim(entry), /\s+/)[0])
  )
  exports.push(
    ...Arr.flatMap([...Str.matchAll(/export\s+default\s+(\w+)/g)(source)], (match) =>
      match[1] === undefined ? [] : [match[1]]
    )
  )
  for (const match of Str.matchAll(
    /\b(export\s+)?(?:const|let|var)\s+(\w+)\s*(?::(?:[^=;]|=>)*?)?=\s*Effect\s*\.\s*fn\s*\(\s*["'`]/g
  )(source)) {
    if (match[1] || Arr.contains(exports, match[2] ?? "")) violations.push(match[2] ?? "")
  }
  if (/export\s+default\s+Effect\s*\.\s*fn\s*\(\s*["'`]/.test(source)) violations.push("default")
  return violations
}

it("exported symbols cannot open named Effect.fn spans outside their boundary", () => {
  const files = Arr.filter(
    readdirSync("src", { recursive: true, encoding: "utf8" }),
    (file) => Str.endsWith(".ts")(file) && !Str.endsWith(".test.ts")(file)
  )
  expect(
    Arr.flatMap(files, (file) =>
      Arr.map(violationsIn(readFileSync(`src/${file}`, "utf8")), (name) => `${file}: ${name}`)
    )
  ).toEqual([])
})

it.each([
  ['export const entry = Effect.fn("entry")(body)', ["entry"]],
  ['export const entry: () => Effect.Effect<void> = Effect.fn("entry")(body)', ["entry"]],
  ['const entry = Effect.fn("entry")(body); export { entry as alias }', ["entry"]],
  ['const entry = Effect.fn("entry")(body); export default entry', ["entry"]],
  ['export default Effect.fn("entry")(body)', ["default"]],
  ['const internal = Effect.fn("internal")(body); export const entry = Effect.fnUntraced(body)', []]
])("the export scan does not misclassify %s", (source, expected) => {
  expect(violationsIn(source)).toEqual(expected)
})
