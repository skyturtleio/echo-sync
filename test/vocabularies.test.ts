import { readFileSync } from "node:fs"
import { parseSync, Visitor } from "oxc-parser"
import { expect, it } from "vitest"
import vocabulary from "../docs/vocabularies.json"

// Inspect declarations rather than duplicating implementation values in a test. Telemetry's
// arrays are deliberately private; this does not add a runtime export or change its boundary.
const source = (file: string) => readFileSync(new URL(`../src/${file}.ts`, import.meta.url), "utf8")
const literals = (text: string): string[] => {
  const values: string[] = []
  new Visitor({
    Literal(node) {
      if (typeof node.value === "string") values.push(node.value)
    }
  }).visit(parseSync("v.ts", text).program)
  return values
}
const declaration = (text: string, name: string): string => {
  let result: string | undefined
  new Visitor({
    VariableDeclarator(node) {
      if (node.id.type === "Identifier" && node.id.name === name && node.init)
        result = text.slice(node.init.start, node.init.end)
    },
    TSTypeAliasDeclaration(node) {
      if (node.id.name === name && result === undefined)
        result = text.slice(node.typeAnnotation.start, node.typeAnnotation.end)
    },
    ClassDeclaration(node) {
      if (node.id?.name === name) result = text.slice(node.start, node.end)
    }
  }).visit(parseSync("v.ts", text).program)
  if (result === undefined) throw new Error(`Missing declaration ${name}`)
  return result
}
const errors = (text: string): string[] => {
  const tags: string[] = []
  new Visitor({
    ClassDeclaration(node) {
      if (
        node.superClass?.type === "CallExpression" &&
        /TaggedError/.test(text.slice(node.superClass.start, node.superClass.end))
      ) {
        const tag = node.superClass.arguments[0]
        if (tag?.type !== "Literal" || typeof tag.value !== "string") throw new Error("Nonliteral error tag")
        tags.push(tag.value)
      }
    }
  }).visit(parseSync("v.ts", text).program)
  return tags
}
const references = (text: string): string[] =>
  [...text.matchAll(/(?:^|\|)\s*(?:Protocol\.)?(\w+)(?:\.\w+)?/g)].map((m) => m[1] ?? "")
const same = (actual: readonly string[], expected: readonly string[]) =>
  expect([...new Set(actual)].toSorted()).toEqual([...expected].toSorted())

it("telemetry, wire errors and client failure or notice vocabularies cannot drift from SPEC §14–15", () => {
  const telemetry = source("internal/Telemetry")
  for (const key of ["spanNames", "outcomeValues", "countKeys", "logMessages"] as const)
    same(literals(`const x = ${declaration(telemetry, key)}`), vocabulary[key])
  same(literals(`type X = ${declaration(telemetry, "Attributes")}`), vocabulary.attributeKeys)
  // A span event is never admitted by the boundary (§14.1); event() is intentionally empty.
  const eventBodies: string[] = []
  new Visitor({
    MethodDefinition(node) {
      if (node.key.type === "Identifier" && node.key.name === "event")
        eventBodies.push(telemetry.slice(node.value.body?.start, node.value.body?.end))
    }
  }).visit(parseSync("v.ts", telemetry).program)
  expect(eventBodies).toEqual(["{}"])
  same([], vocabulary.eventNames)
  const protocol = source("core/Protocol")
  const failure = source("client/sync/SyncFailure")
  const mutation = source("core/Mutation")
  same(errors(protocol), vocabulary.ProtocolErrors)
  same(errors(failure), vocabulary.SyncFailureErrors)
  for (const [text, key] of [
    [protocol, "ResnapshotRequired"],
    [protocol, "UnauthorizedReason"],
    [failure, "PushIntegrityError"],
    [failure, "SyncErrorKind"]
  ] as const) {
    const values = literals(
      declaration(text, key).startsWith("export class") ? declaration(text, key) : `const x = ${declaration(text, key)}`
    )
    same(
      values.filter((value) => value !== key),
      vocabulary[key]
    )
  }
  same(literals(`const x = ${declaration(mutation, "Notice")}`), vocabulary.Notice)
  same(
    [...declaration(mutation, "MutationRejection").matchAll(/\b(\w+),?\s*$/gm)].map((m) => m[1] ?? ""),
    vocabulary.MutationRejection
  )
  same(references(declaration(failure, "PushReason")), vocabulary.PushReason)
  same(references(declaration(failure, "PullReason")), vocabulary.PullReason)
})
