import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto"
import { Crypto, Effect } from "effect"

/** Lazy Web Crypto provision keeps standalone storage and tab APIs environment-free. */
export const randomUUIDv4: Effect.Effect<string> = Effect.flatMap(Crypto.Crypto, (crypto) =>
  crypto.randomUUIDv4()
).pipe(Effect.orDie, Effect.provide(BrowserCrypto.layer))
