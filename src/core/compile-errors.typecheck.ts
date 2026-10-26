/**
 * Compile-time contract: each `@ts-expect-error` below must stay an error, so `tsc --noEmit`
 * (part of `bun run check`) fails if a wrong usage starts compiling. Nothing here runs.
 */
import { Context, Effect, Schema, SchemaGetter } from "effect"
import { AccessScopeId } from "./AccessScope.ts"
import type { EchoStore } from "./Store.ts"
import * as SyncTable from "./SyncTable.ts"

const TodoId = Schema.String.pipe(Schema.brand("TodoId"))
const Todos = SyncTable.make(
  "todos",
  {
    id: TodoId,
    accessScopeId: AccessScopeId,
    projectId: Schema.NullOr(Schema.String),
    rank: Schema.Int,
    at: Schema.Date
  },
  { indexes: { by_project: ["projectId", "rank", "at"] } }
)

export const queries = (db: EchoStore) => [
  db
    .query(Todos)
    .withIndex("by_project", (q) => q.eq("projectId", null))
    .after({ projectId: null, rank: 2, at: new Date(0), id: TodoId.make("a") }) // oxlint-disable-line effecttsgo/global-date -- Native Date is the decoded cursor type being checked.
    .order("desc")
    .take(10),
  db
    .query(Todos)
    .withIndex("by_project", (q) => q.eq("projectId", null))
    // @ts-expect-error cursor includes equality-prefix fields too
    .after({ rank: 2, at: new Date(0), id: TodoId.make("a") }), // oxlint-disable-line effecttsgo/global-date -- Keep the Date field valid while checking a missing prefix.
  db
    .query(Todos)
    .withIndex("by_project")
    // @ts-expect-error implicit id is mandatory
    .before({ projectId: null, rank: 2, at: new Date(0) }), // oxlint-disable-line effecttsgo/global-date -- Keep the Date field valid while checking a missing id.
  db
    .query(Todos)
    .withIndex("by_project")
    // @ts-expect-error cursor date is a decoded Date, not epoch milliseconds
    .after({ projectId: null, rank: 2, at: 0, id: TodoId.make("a") }),
  db
    .query(Todos)
    .withIndex("by_project")
    .order("desc")
    .contains("projectId", "p")
    // @ts-expect-error fluent operations preserve cursor brands
    .after({ projectId: null, rank: 2, at: new Date(0), id: "a" }), // oxlint-disable-line effecttsgo/global-date -- Keep the Date field valid while checking an unbranded id.
  db
    .query(Todos)
    .withIndex("by_project", (q) => q.prefix("projectId", "ab"))
    .contains("projectId", "%"),
  // @ts-expect-error numeric fields cannot be searched as text
  db.query(Todos).withIndex("by_project").contains("rank", "1"),
  // @ts-expect-error prefix must name the next text index field
  db.query(Todos).withIndex("by_project", (q) => q.prefix("rank", "1")),
  db.query(Todos).withIndex("by_project", (q) => q.eq("projectId", null).eq("rank", 1).gte("at", new Date(0))), // oxlint-disable-line effecttsgo/global-date -- A valid Date range bound must compile.
  db.query(Todos).withIndex("by_project", (q) => q.eq("projectId", "p").gt("rank", 1).lt("rank", 5)),
  // @ts-expect-error unknown index name
  db.query(Todos).withIndex("by_nothing"),
  // @ts-expect-error wrong value type for the field
  db.query(Todos).withIndex("by_project", (q) => q.eq("projectId", 1)),
  // @ts-expect-error fields must be consumed in index order
  db.query(Todos).withIndex("by_project", (q) => q.eq("rank", 1)),
  // @ts-expect-error a range bound is never null (it would never match)
  db.query(Todos).withIndex("by_project", (q) => q.gt("projectId", null)),
  // @ts-expect-error after a lower bound only an upper bound on the same field may follow
  db.query(Todos).withIndex("by_project", (q) => q.gt("projectId", "a").lt("rank", 2)),
  // @ts-expect-error id must be the table's branded id
  db.get(Todos, "plain string")
]

class Clock extends Context.Service<Clock, { readonly now: number }>()("test/Clock") {}
const NeedsService = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformEffect((s: string) => Effect.map(Effect.service(Clock), () => s)),
    encode: SchemaGetter.transform((s: string) => s)
  })
)

export const declarations = () => [
  // @ts-expect-error a table without accessScopeId
  SyncTable.make("a", { id: TodoId }),
  // @ts-expect-error id must be a branded string
  SyncTable.make("b", { id: Schema.String, accessScopeId: AccessScopeId }),
  // @ts-expect-error unsupported leaf schema (bigint)
  SyncTable.make("c", { id: TodoId, accessScopeId: AccessScopeId, n: Schema.BigInt }),
  // @ts-expect-error field schemas must not require services (row codecs run with no context)
  SyncTable.make("e", { id: TodoId, accessScopeId: AccessScopeId, x: NeedsService }),
  // @ts-expect-error accessScopeId must be AccessScopeId, not any string
  SyncTable.make("d", { id: TodoId, accessScopeId: Schema.String })
]
