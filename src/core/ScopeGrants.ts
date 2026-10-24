import { Schema } from "effect"
import { AccessScopeId, PrincipalId, Role } from "./AccessScope.ts"
import * as SyncTable from "./SyncTable.ts"

export const ScopeGrantId = Schema.String.check(Schema.isNonEmpty()).pipe(Schema.brand("echo-sync/ScopeGrantId"))
export type ScopeGrantId = typeof ScopeGrantId.Type

/**
 * Deterministic, injective grant id (INV-ACL-5): `(accessScopeId, principalId)` is unique by
 * construction because the id is derived from the pair. Length-prefixed so no separator can
 * be forged by either part. The prefix is the code-point count, not `.length` (UTF-16 code units),
 * so it equals Postgres `char_length` (UTF8) and the server CHECK derives the same id.
 */
export const grantId = (scopeId: AccessScopeId, principalId: PrincipalId): ScopeGrantId =>
  ScopeGrantId.make(`${[...scopeId].length}:${scopeId}:${principalId}`)

/**
 * System table of scope grants (SPEC §4). Replicated like any row (a grant lives in the scope
 * it grants), so clients can run optimistic role checks. Writes require `owner` on that scope,
 * except deleting one's own grant (DECISIONS 27) and `createScope`.
 */
export const ScopeGrants = SyncTable.make(
  "scope_grants",
  {
    id: ScopeGrantId,
    accessScopeId: AccessScopeId,
    principalId: PrincipalId,
    role: Role,
    grantedAt: Schema.DateTimeUtc
  },
  { unique: [["accessScopeId", "principalId"]] }
)
