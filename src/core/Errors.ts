import { Schema } from "effect"
import { AccessScopeId, Role } from "./AccessScope.ts"

/** Transient storage failure (connection, busy, serialization). Never recorded; stops the batch. */
export class StoreUnavailable extends Schema.TaggedError<StoreUnavailable>()("StoreUnavailable", {
  message: Schema.String
}) {}

export class RowNotFound extends Schema.TaggedError<RowNotFound>()("RowNotFound", {
  table: Schema.String,
  id: Schema.String
}) {}

export class RowAlreadyExists extends Schema.TaggedError<RowAlreadyExists>()("RowAlreadyExists", {
  table: Schema.String,
  id: Schema.String
}) {}

/** The actor lacks `required` on `scopeId` (`held` is its effective role, if any). */
export class ScopeForbidden extends Schema.TaggedError<ScopeForbidden>()("ScopeForbidden", {
  scopeId: AccessScopeId,
  required: Role,
  held: Schema.NullOr(Role)
}) {}

/** A row (or patch, or index bound) a handler supplied does not satisfy the table's schema. */
export class InvalidRow extends Schema.TaggedError<InvalidRow>()("InvalidRow", {
  table: Schema.String,
  message: Schema.String
}) {}

/** An authoritative database write violated an app constraint (DESIGN §16.15). */
export class ConstraintViolated extends Schema.TaggedError<ConstraintViolated>()("ConstraintViolated", {
  kind: Schema.Literals(["unique", "other"]),
  constraint: Schema.NullOr(Schema.String),
  message: Schema.String
}) {}

/** Permanent store errors a handler can produce; they become typed rejections. */
export type StoreRejection = RowNotFound | RowAlreadyExists | ScopeForbidden | InvalidRow | ConstraintViolated
export type StoreError = StoreRejection | StoreUnavailable
