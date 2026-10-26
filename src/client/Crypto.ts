import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto"
import { Crypto, Effect } from "effect"

/** Lazy Web Crypto provision keeps standalone storage and tab APIs environment-free. */
export const randomUUIDv4: Effect.Effect<string> = Effect.flatMap(Crypto.Crypto, (crypto) =>
  crypto.randomUUIDv4()
).pipe(
  Effect.orDie,
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Standalone callers stay environment-free; acquire Web Crypto lazily for each execution, not at module load.
  Effect.provide(BrowserCrypto.layer)
)
