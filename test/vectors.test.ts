import { globSync, readFileSync } from "node:fs"
import { Schema } from "effect"
import { expect, it } from "vitest"
import { codecs, VectorFile, vectorFiles, wireSchemas } from "../scripts/vectors.ts"

it("wire messages cannot change their committed decoded values, JSON bytes or variant coverage", () => {
  const expected = vectorFiles()
  expect(globSync("spec/vectors/*.json").toSorted()).toEqual(
    [...expected.keys()].map((name) => `spec/vectors/${name}.json`).toSorted()
  )
  for (const [name, schema] of wireSchemas) {
    const body = readFileSync(`spec/vectors/${name}.json`, "utf8")
    expect(body, `${name} coverage; run bun run vectors for a conscious wire change`).toBe(expected.get(name))
    const vectors = Schema.decodeUnknownSync(VectorFile)(JSON.parse(body))
    expect(vectors.length, `${name} has vectors`).toBeGreaterThan(0)
    const codec = codecs(schema)
    for (const vector of vectors) {
      const value = codec.decode(JSON.parse(vector.json))
      expect(codec.value(value), `${name} decoded value`).toEqual(vector.value)
      expect(JSON.stringify(codec.encode(value)), `${name} encoded bytes`).toBe(vector.json)
    }
  }
})
