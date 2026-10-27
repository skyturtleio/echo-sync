import { Data, Schema } from "effect"
import { AccessScopeId, Role } from "./AccessScope.ts"

/** Transient storage failure (connection, busy, serialization). Never recorded; stops the batch. */
export class StoreUnavailable extends Schema.TaggedError<StoreUnavailable, {}>()("StoreUnavailable", {
  message: Schema.String,
  /** Absent on legacy/custom adapters means unavailable, never a conflict inferred from text. */
  reason: Schema.optional(Schema.Literals(["SerializationConflict", "Deadlock", "Unavailable"]))
}) {}

export class RowNotFound extends Schema.TaggedError<RowNotFound, {}>()("RowNotFound", {
  table: Schema.String,
  id: Schema.String
}) {}

export class RowAlreadyExists extends Schema.TaggedError<RowAlreadyExists, {}>()("RowAlreadyExists", {
  table: Schema.String,
  id: Schema.String
}) {}

/** The actor lacks `required` on `scopeId` (`held` is its effective role, if any). */
export class ScopeForbidden extends Schema.TaggedError<ScopeForbidden, {}>()("ScopeForbidden", {
  scopeId: AccessScopeId,
  required: Role,
  held: Schema.NullOr(Role)
}) {}

/** A row (or patch, or index bound) a handler supplied does not satisfy the table's schema. */
export class InvalidRow extends Schema.TaggedError<InvalidRow, {}>()("InvalidRow", {
  table: Schema.String,
  message: Schema.String
}) {}

/** An authoritative database write violated an app constraint (SPEC §5). */
export class ConstraintViolated extends Schema.TaggedError<ConstraintViolated, {}>()("ConstraintViolated", {
  kind: Schema.Literals(["unique", "other"]),
  constraint: Schema.NullOr(Schema.String),
  message: Schema.String
}) {}

/**
 * A defect, never a typed failure: data the engine stored through a schema no longer decodes with it. `table`
 * is the table (or engine table) it was read from and `id` its row or mutation id when known; `cause` is the
 * schema's error, whose message names the offending field.
 */
export class CorruptStorage extends Data.TaggedError("CorruptStorage")<{
  readonly table: string
  readonly id: string | undefined
  readonly cause: unknown
}> {
  override get message(): string {
    const where = this.id === undefined ? `"${this.table}"` : `"${this.table}" row "${this.id}"`
    const detail = this.cause instanceof Error ? this.cause.message : String(this.cause)
    return (
      `stored data in ${where} does not decode with its schema (${detail}). Something other than this ` +
      `schema wrote it: a manual edit, a migration, or a schema change shipped without one.`
    )
  }
}

/** Permanent store errors a handler can produce; they become typed rejections. */
export type StoreRejection = RowNotFound | RowAlreadyExists | ScopeForbidden | InvalidRow | ConstraintViolated
export type StoreError = StoreRejection | StoreUnavailable
