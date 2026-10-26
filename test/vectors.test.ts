// oxlint-disable-next-line effecttsgo/node-builtin-import -- The synchronous contract test compares committed files, not engine I/O.
import { globSync, readFileSync } from "node:fs"
import { Array as Arr, Effect, Schema, String as Str } from "effect"
import { expect, it } from "vitest"
import { codecs, VectorFile, vectorFiles, wireSchemas } from "../scripts/vectors.ts"

it("wire messages cannot change their committed decoded values, JSON bytes or variant coverage", () => {
  const expected = vectorFiles()
  expect(Arr.sort(globSync("spec/vectors/*.json"), Str.Order)).toEqual(
    Arr.sort(
      Arr.map([...expected.keys()], (name) => `spec/vectors/${name}.json`),
      Str.Order
    )
  )
  for (const [name, schema] of wireSchemas) {
    const body = readFileSync(`spec/vectors/${name}.json`, "utf8")
    expect(body, `${name} coverage; run bun run vectors for a conscious wire change`).toBe(expected.get(name))
    const vectors = Effect.runSync(Schema.decodeUnknownEffect(Schema.fromJsonString(VectorFile))(body))
    expect(vectors.length, `${name} has vectors`).toBeGreaterThan(0)
    const codec = codecs(schema)
    for (const vector of vectors) {
      const value = codec.decode(JSON.parse(vector.json))
      expect(codec.value(value), `${name} decoded value`).toEqual(vector.value)
      expect(JSON.stringify(codec.encode(value)), `${name} encoded bytes`).toBe(vector.json)
    }
  }
})
