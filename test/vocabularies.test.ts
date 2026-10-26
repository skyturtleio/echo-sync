// oxlint-disable-next-line effecttsgo/node-builtin-import -- This ordinary Vitest contract gate synchronously inspects committed source declarations.
import { globSync, readFileSync } from "node:fs"
import { Array as Arr, String as Str } from "effect"
import { parseSync, Visitor } from "oxc-parser"
import { expect, it } from "vitest"
import { RpcClientError } from "effect/rpc"
import { Protocol } from "echo-sync"
import { failureMeaning } from "../src/internal/ErrorCatalog.ts"
import type { PushReason } from "../src/client/sync/SyncFailure.ts"
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
        result = Str.slice(node.init.start, node.init.end)(text)
    },
    TSTypeAliasDeclaration(node) {
      if (node.id.name === name && result === undefined)
        result = Str.slice(node.typeAnnotation.start, node.typeAnnotation.end)(text)
    },
    ClassDeclaration(node) {
      if (node.id?.name === name) result = Str.slice(node.start, node.end)(text)
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
        /TaggedError/.test(Str.slice(node.superClass.start, node.superClass.end)(text))
      ) {
        const tag = node.superClass.arguments[0]
        if (tag?.type !== "Literal" || typeof tag.value !== "string") throw new Error("Nonliteral error tag")
        tags.push(tag.value)
      }
    }
  }).visit(parseSync("v.ts", text).program)
  return tags
}
const same = (actual: readonly string[], expected: readonly string[]) =>
  expect(Arr.sort(new Set(actual), Str.Order)).toEqual(Arr.sort(expected, Str.Order))

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
        eventBodies.push(Str.slice(node.value.body?.start, node.value.body?.end)(telemetry))
    }
  }).visit(parseSync("v.ts", telemetry).program)
  expect(eventBodies).toEqual(["{}"])
  same([], vocabulary.eventNames)
  const protocol = source("core/Protocol")
  const failure = source("client/sync/SyncFailure")
  const mutation = source("core/Mutation")
  for (const [text, key] of [
    [protocol, "ResnapshotRequired"],
    [protocol, "UnauthorizedReason"],
    [failure, "PushIntegrityError"],
    [failure, "SyncErrorKind"]
  ] as const) {
    const values = literals(
      Str.startsWith("export class")(declaration(text, key))
        ? declaration(text, key)
        : `const x = ${declaration(text, key)}`
    )
    same(
      Arr.filter(values, (value) => value !== key),
      vocabulary[key]
    )
  }
  same(literals(`const x = ${declaration(mutation, "Notice")}`), vocabulary.Notice)
  const codes: string[] = []
  for (const file of globSync("src/**/*.ts")) {
    if (/\/(?:testing|test)\/|\.test\.ts$/.test(file)) continue
    const text = readFileSync(file, "utf8")
    for (const statement of parseSync(file, text).program.body) {
      if (statement.type !== "ExportNamedDeclaration" || statement.declaration?.type !== "ClassDeclaration") continue
      const node = statement.declaration
      const body = Str.slice(node.start, node.end)(text)
      codes.push(...errors(body))
      if (node.superClass?.type === "Identifier" && node.superClass.name === "Error") {
        for (const field of node.body.body) {
          if (
            field.type === "PropertyDefinition" &&
            field.key.type === "Identifier" &&
            field.key.name === "name" &&
            field.value?.type === "Literal" &&
            typeof field.value.value === "string"
          )
            codes.push(field.value.value)
        }
      }
    }
  }
  codes.push(
    new RpcClientError.RpcClientError({
      reason: new RpcClientError.RpcClientDefect({ message: "transport", cause: null })
    })._tag
  )
  const spec = readFileSync(new URL("../docs/SPEC.md", import.meta.url), "utf8")
  const catalog = Str.split(Str.split(spec, "#### 15.1 Observable error catalog\n")[1] ?? "", "#### 15.2")[0] ?? ""
  const documented = Arr.map([...Str.matchAll(/^\| `([^`]+)`/gm)(catalog)], (match) => match[1] ?? "")
  expect(documented.length).toBeGreaterThan(0)
  expect(new Set(documented).size).toBe(documented.length)
  same(codes, documented)
})

it("a wire error without a catalog mapping cannot compile", () => {
  // Closed input check; the catalog's Record constraint separately checks every RPC-derived tag.
  // @ts-expect-error An additional wire tag requires a catalog entry before it can be classified.
  const missingMapping: (error: PushReason | { readonly _tag: "FutureWireError" }) => unknown = failureMeaning
  expect(missingMapping(new Protocol.Unauthorized({ message: "refused" }))).toEqual({
    kind: "unauthorized",
    outcome: "unauthorized"
  })
})
