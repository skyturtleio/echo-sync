import { readFileSync, readdirSync } from "node:fs"
import { expect, it } from "vitest"

const violationsIn = (source: string): string[] => {
  const violations: string[] = []
  const exports = [...source.matchAll(/export\s*\{([^}]+)\}/g)].flatMap((match) =>
    (match[1] ?? "").split(",").map((entry) => entry.trim().split(/\s+/)[0])
  )
  exports.push(...[...source.matchAll(/export\s+default\s+(\w+)/g)].map((match) => match[1]))
  for (const match of source.matchAll(
    /\b(export\s+)?(?:const|let|var)\s+(\w+)\s*(?::(?:[^=;]|=>)*?)?=\s*Effect\s*\.\s*fn\s*\(\s*["'`]/g
  )) {
    if (match[1] || exports.includes(match[2])) violations.push(match[2] ?? "")
  }
  if (/export\s+default\s+Effect\s*\.\s*fn\s*\(\s*["'`]/.test(source)) violations.push("default")
  return violations
}

it("exported symbols cannot open named Effect.fn spans outside their boundary", () => {
  const files = readdirSync("src", { recursive: true, encoding: "utf8" }).filter(
    (file) => file.endsWith(".ts") && !file.endsWith(".test.ts")
  )
  expect(
    files.flatMap((file) => violationsIn(readFileSync(`src/${file}`, "utf8")).map((name) => `${file}: ${name}`))
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
