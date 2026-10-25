import { Schema } from "effect"

/** NFC, locale-independent lowercase, collapsed whitespace. Accents stay; portability is not validated. */
export const normalize = (text: string): string =>
  text.normalize("NFC").toLowerCase().normalize("NFC").replace(/\s+/gu, " ").trim()

/** PostgreSQL text and SQLite UTF-8 must represent exactly the same Unicode scalars. */
export const isPortable = (text: string): boolean => !text.includes("\0") && !/[\uD800-\uDFFF]/u.test(text)

/** Validation only: applications compute this field alongside the source fields on every write. */
export const Text = Schema.String.check(Schema.makeFilter((text) => isPortable(text) && normalize(text) === text)).pipe(
  Schema.brand("echo-sync/Search.Text")
)

/** Exclusive code-point upper bound for a prefix; undefined when no successor exists. */
export const prefixEnd = (prefix: string): string | undefined => {
  const points = Array.from(prefix)
  while (points.length > 0) {
    const last = points.pop()?.codePointAt(0)
    if (last !== undefined && last < 0x10ffff) {
      return points.join("") + String.fromCodePoint(last === 0xd7ff ? 0xe000 : last + 1)
    }
  }
  return undefined
}
