import { globSync, mkdirSync, readFileSync, writeFileSync } from "node:fs"
import { Schema, SchemaAST } from "effect"
import { RpcSchema } from "effect/rpc"
import * as Protocol from "../src/core/Protocol.ts"

/** Enumerate each union branch and optional-key state, varying one field at a time.
 * This is branch coverage, not the Cartesian product of unrelated fields. Unsupported
 * schema nodes fail closed so new wire shapes require a deliberate generator decision. */
export function samples(ast: SchemaAST.AST): unknown[] {
  switch (ast._tag) {
    case "Never":
      return []
    case "Void":
      return [undefined]
    case "Null":
      return [null]
    case "Literal":
      return [ast.literal]
    case "String":
      return ["9007199254740993"]
    case "Number":
      return [1]
    case "Boolean":
      return [false, true]
    case "Union":
      return ast.types.flatMap(samples)
    case "Declaration":
      if (ast.annotations?.["expected"] === "JSON value")
        return [null, false, 7, "text", [1, null], { title: "vector", nested: [true] }]
      throw new Error("Unsupported wire declaration")
    case "Arrays": {
      const base = ast.elements.map((element) => samples(element)[0])
      const values: unknown[] = [base]
      for (const [index, element] of ast.elements.entries())
        for (const value of samples(element)) values.push(base.map((old, i) => (i === index ? value : old)))
      const rest = ast.rest[0]
      if (rest) for (const value of samples(rest)) values.push([...base, value])
      return values
    }
    case "Objects": {
      const base: Record<string, unknown> = {}
      for (const field of ast.propertySignatures) {
        if (typeof field.name !== "string") throw new Error("Non-string wire key")
        if (!SchemaAST.isOptional(field.type)) base[field.name] = samples(field.type)[0]
      }
      const values: unknown[] = [base]
      for (const field of ast.propertySignatures) {
        if (typeof field.name !== "string") throw new Error("Non-string wire key")
        for (const value of samples(field.type)) values.push({ ...base, [field.name]: value })
      }
      for (const index of ast.indexSignatures)
        for (const value of samples(index.type)) values.push({ ...base, example: value })
      return values
    }
    default:
      throw new Error(`Unsupported wire AST: ${ast._tag}`)
  }
}

export const wireSchemas = new Map<string, Schema.Top>()
for (const rpc of [...Protocol.SyncRpcs.requests.values(), ...Protocol.PresenceRpcs.requests.values()]) {
  wireSchemas.set(`${rpc._tag}.request`, rpc.payloadSchema)
  const success = rpc.successSchema
  wireSchemas.set(`${rpc._tag}.response`, RpcSchema.isStreamSchema(success) ? success.success : success)
  wireSchemas.set(`${rpc._tag}.error`, RpcSchema.isStreamSchema(success) ? success.error : rpc.errorSchema)
}
// Individual named stream variants and errors are also useful as standalone conformance inputs.
for (const [name, schema] of Object.entries(Protocol)) {
  if (!Schema.isSchema(schema)) continue
  const ast = Schema.toEncoded(schema).ast
  if (
    ast._tag === "Objects" &&
    ast.propertySignatures.some((field) => field.name === "_tag" && field.type._tag === "Literal")
  )
    wireSchemas.set(name, schema)
}

export const Vector = Schema.Struct({ json: Schema.String, value: Schema.Json })
export const VectorFile = Schema.Array(Vector)

export function codecs(schema: Schema.Top) {
  // RPC wire schemas are service-free. Rebuilding from their AST erases only the
  // existential service types exposed by RpcGroup; no schema transformation is removed.
  const codec = Schema.toCodecJson(Schema.make<Schema.Codec<unknown, unknown>>(schema.ast))
  return {
    schema: codec,
    decode: Schema.decodeUnknownSync(codec),
    encode: Schema.encodeUnknownSync(codec),
    value: Schema.encodeUnknownSync(Schema.toCodecJson(Schema.toType(codec)))
  }
}

export function generate(schema: Schema.Top): typeof VectorFile.Type {
  const codec = codecs(schema)
  const vectors = new Map<string, typeof Vector.Type>()
  for (const sample of samples(Schema.toEncoded(codec.schema).ast)) {
    // Never silently drop a branch if a new refinement makes our sample invalid.
    const decoded = codec.decode(sample)
    const json = JSON.stringify(codec.encode(decoded))
    if (json === undefined) throw new Error("RPC JSON codec produced undefined")
    vectors.set(json, { json, value: codec.value(decoded) })
  }
  return [...vectors.values()]
}

export function vectorFiles(): Map<string, string> {
  return new Map([...wireSchemas].map(([name, schema]) => [name, `${JSON.stringify(generate(schema), null, 2)}\n`]))
}

if (import.meta.main) {
  const check = process.argv.includes("--check")
  const files = vectorFiles()
  mkdirSync("spec/vectors", { recursive: true })
  let count = 0
  for (const [name, body] of files) {
    const path = `spec/vectors/${name}.json`
    count += Schema.decodeUnknownSync(VectorFile)(JSON.parse(body)).length
    if (check) {
      let existing = ""
      try {
        existing = readFileSync(path, "utf8")
      } catch {
        /* missing is drift */
      }
      if (existing !== body) {
        console.error(`Wire vector drift: ${path}; run bun run vectors`)
        process.exitCode = 1
      }
    } else writeFileSync(path, body)
  }
  for (const file of globSync("spec/vectors/*.json"))
    if (!files.has(file.slice("spec/vectors/".length, -5))) {
      console.error(`Obsolete vector file: ${file}`)
      process.exitCode = 1
    }
  console.log(`${count} wire vectors in ${files.size} files${check ? " checked" : " generated"}`)
}
