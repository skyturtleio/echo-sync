import { Schema } from "effect"

/** Opaque, app-generated identity of an access scope (SPEC §4). */
export const AccessScopeId = Schema.String.check(Schema.isNonEmpty()).pipe(Schema.brand("echo-sync/AccessScopeId"))
export type AccessScopeId = typeof AccessScopeId.Type

/** Opaque principal string supplied by the app's `principalFor(request)`. `*` means everyone. */
export const PrincipalId = Schema.String.check(Schema.isNonEmpty()).pipe(Schema.brand("echo-sync/PrincipalId"))
export type PrincipalId = typeof PrincipalId.Type

/** The principal that a grant to `*` applies to: every principal (global catalogs). */
export const Everyone: PrincipalId = PrincipalId.make("*")

export const Role = Schema.Literals(["reader", "writer", "owner"])
export type Role = typeof Role.Type

const rank: Record<Role, number> = { reader: 0, writer: 1, owner: 2 }

/** Roles are ordered reader < writer < owner; a stronger role satisfies a weaker requirement. */
export const satisfies = (held: Role, required: Role): boolean => rank[held] >= rank[required]

/** Effective role when a principal holds several grants on one scope (its own and `*`). */
export const strongest = (roles: Iterable<Role>): Role | undefined => {
  let best: Role | undefined
  for (const role of roles) if (best === undefined || rank[role] > rank[best]) best = role
  return best
}
