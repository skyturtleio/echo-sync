import { Schema } from "effect"

/**
 * Log / ledger position (SPEC §6, §7). A branded non-negative `bigint` in memory (native
 * comparison, no precision loss) and a decimal string wherever it is encoded.
 */
export const Position = Schema.BigIntFromString.check(Schema.isGreaterThanOrEqualToBigInt(0n)).pipe(
  Schema.brand("echo-sync/Position")
)
export type Position = typeof Position.Type

/** A pull cursor: `null` means "never synced" (initial sync / resnapshot). */
export const Cursor = Schema.NullOr(Position)
export type Cursor = typeof Cursor.Type

export const ClientId = Schema.String.check(Schema.isNonEmpty()).pipe(Schema.brand("echo-sync/ClientId"))
export type ClientId = typeof ClientId.Type

/** Caller-generated idempotency key (INV-ID-4). Unordered: never compare two of these. */
export const MutationId = Schema.String.check(Schema.isNonEmpty()).pipe(Schema.brand("echo-sync/MutationId"))
export type MutationId = typeof MutationId.Type

/** Outbox sequence (INV-ID-2, INV-CLI-5): per-client, starts at 1, monotone, gap-free, never reused. */
export const Seq = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)).pipe(Schema.brand("echo-sync/Seq"))
export type Seq = typeof Seq.Type

/** Ledger-expiry floor (DECISIONS 5): highest expired `Seq`, `0` when nothing expired. */
export const SeqFloor = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(Schema.brand("echo-sync/SeqFloor"))
export type SeqFloor = typeof SeqFloor.Type
