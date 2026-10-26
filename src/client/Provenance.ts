import * as Arr from "effect/Array"
import * as Schema from "effect/Schema"

/** A committed snapshot in one storage lifetime. Never compare offsets across epochs. */
export const Revision = Schema.Struct({ epoch: Schema.String, offset: Schema.Int })
export type Revision = typeof Revision.Type

/** One newly committed optimistic enqueue, not its later reconciliation or a duplicate receipt. */
export const LocalChange = Schema.Struct({ before: Revision, after: Revision })
export type LocalChange = typeof LocalChange.Type

export interface Snapshot<A> {
  readonly value: A
  readonly revision: Revision
}

/**
 * Classify relative to a snapshot the app actually incorporated. Pass only receipts whose optimistic
 * effects it has also incorporated. This certifies provenance, not equivalence of app and query logic.
 * Missing receipts, reconciliation, initial state and changed storage lifetimes require refresh.
 */
export const classify = (
  incorporated: Revision | undefined,
  next: Revision,
  applied: ReadonlyArray<{ readonly change?: LocalChange | null }>
): "unchanged" | "local-only" | "refresh-required" => {
  if (incorporated === undefined || incorporated.epoch !== next.epoch || incorporated.offset > next.offset)
    return "refresh-required"
  if (incorporated.offset === next.offset) return "unchanged"
  const covered = new Set(
    // Public receipts may be sparse; native flatMap skipped holes, whereas Effect flatMap visits them.
    Arr.flatMap(
      Arr.filter(applied, (_, index) => index in applied),
      ({ change }: (typeof applied)[number]) =>
        change != null &&
        change.before.epoch === next.epoch &&
        change.after.epoch === next.epoch &&
        change.after.offset === change.before.offset + 1 &&
        change.before.offset >= incorporated.offset &&
        change.after.offset <= next.offset
          ? [change.after.offset]
          : []
    )
  )
  return covered.size === next.offset - incorporated.offset ? "local-only" : "refresh-required"
}
