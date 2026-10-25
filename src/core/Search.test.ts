import { expect, it } from "vitest"
import { Schema } from "effect"
import { AccessScope, Search, SyncTable } from "./index.ts"

it("search text normalizes case and whitespace without removing accents or interpreting punctuation", () => {
  const normalized = Search.normalize("  E\u0301COLE\tİ ß  %_\\'😀 \n")
  expect(normalized).toBe("école i\u0307 ß %_\\'😀")
  expect(Search.normalize(normalized)).toBe(normalized)
  const Table = SyncTable.make("searchable", {
    id: Schema.String.pipe(Schema.brand("Id")),
    accessScopeId: AccessScope.AccessScopeId,
    text: Search.Text
  })
  const row = { id: "id", access_scope_id: "scope", text: normalized }
  expect(Schema.encodeSync(Table.wire)(Schema.decodeUnknownSync(Table.wire)(row))).toEqual(row)
  for (const invalid of ["UPPER", "a  b", "e\u0301", "\0", "\ud800", "\udfff"]) {
    expect(Schema.is(Search.Text)(invalid)).toBe(false)
  }
  expect(Schema.is(Search.Text)("")).toBe(true)
})
