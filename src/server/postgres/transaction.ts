import { Array as Arr, Cause, Data, Effect } from "effect"
import type { SqlClient } from "effect/sql/SqlClient"
import { isSqlError, type SqlError } from "effect/sql/SqlError"

/** A SQL defect introduced by transaction cleanup, never by its body. Internal, not a wire error. */
class CleanupFailure extends Data.TaggedError("SqlCleanupFailure")<{ readonly cause: SqlError }> {}

export const isCleanupFailure = (error: unknown): error is CleanupFailure => error instanceof CleanupFailure

/**
 * Shield the body's whole cause before handing it to SqlClient. Its rollback/release paths use
 * orDie; only new SQL defects outside the shield are infrastructure cleanup failures. Restoring
 * the body cause also keeps a domain failure + cleanup defect from becoming a committed rejection.
 */
export const withTransaction = <A, E, R>(
  sql: SqlClient,
  body: Effect.Effect<A, E, R>
): Effect.Effect<A, E | SqlError, R> => {
  class BodyFailure {
    readonly cause: Cause.Cause<E>
    constructor(cause: Cause.Cause<E>) {
      this.cause = cause
    }
  }
  return sql.withTransaction(Effect.catchCause(body, (cause) => Effect.fail(new BodyFailure(cause)))).pipe(
    Effect.catchCause((cause) =>
      Effect.failCause(
        Cause.fromReasons<E | SqlError>(
          Arr.flatMap(cause.reasons, (reason): ReadonlyArray<Cause.Reason<E | SqlError>> => {
            if (Cause.isFailReason(reason)) {
              return reason.error instanceof BodyFailure
                ? reason.error.cause.reasons
                : [Cause.makeFailReason(reason.error)]
            }
            if (Cause.isDieReason(reason) && reason.defect instanceof BodyFailure) return reason.defect.cause.reasons
            if (Cause.isDieReason(reason) && isSqlError(reason.defect)) {
              return [Cause.makeDieReason(new CleanupFailure({ cause: reason.defect }))]
            }
            return [reason]
          })
        )
      )
    )
  )
}
