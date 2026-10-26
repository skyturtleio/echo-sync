import { Data, Effect, Schema } from "effect"
import type { PureSchema } from "./Mutation.ts"
import type { EchoStore, ReadError, ReadStore } from "./Store.ts"

/**
 * Registered read queries (SPEC §12.3), the read-side twin of `Mutation.make`. A query is a
 * deterministic function of its args and the rows it reads through `ReadStore`; the leader runs it in
 * one read transaction and re-runs it when a table the latest run read changes.
 */

// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class UnknownQuery extends Schema.TaggedError<UnknownQuery>()("UnknownQuery", {
  // oxlint-disable-next-line effecttsgo/error-payload-shadows-name -- name is identity payload; _tag is the discriminant. Preserve wire/durable compatibility.
  name: Schema.String
}) {}

/** The args a tab sent do not decode with the query's `args` schema: a bug in the caller. */
// oxlint-disable-next-line effecttsgo/schema-class-missing-brand -- Public errors retain structural assignability for consumers.
export class QueryArgsDecodeFailed extends Schema.TaggedError<QueryArgsDecodeFailed>()("QueryArgsDecodeFailed", {
  // oxlint-disable-next-line effecttsgo/error-payload-shadows-name -- name is identity payload; _tag is the discriminant. Preserve wire/durable compatibility.
  name: Schema.String,
  message: Schema.String
}) {}

/** What a registry needs from a query, with its types erased. */
export interface AnyQuery {
  readonly name: string
  /**
   * Decode `args` (JSON), run the handler, encode its result (JSON). Transaction boundaries are the
   * caller's. A result that does not encode with `success` is a bug in the query: a defect.
   */
  readonly execute: (db: ReadStore, args: Schema.Json) => Effect.Effect<Schema.Json, QueryArgsDecodeFailed | ReadError>
}

export interface Query<Name extends string, A extends PureSchema, S extends PureSchema> extends AnyQuery {
  readonly name: Name
  readonly args: A
  /** Mandatory: results cross the worker boundary encoded and are decoded with it on the tab. */
  readonly success: S
  readonly handler: (db: ReadStore, args: A["Type"]) => Effect.Effect<S["Type"], ReadError>
}

export const make = <const Name extends string, A extends PureSchema, S extends PureSchema>(
  name: Name,
  options: {
    readonly args: A
    readonly success: S
    readonly handler: (db: ReadStore, args: A["Type"]) => Effect.Effect<S["Type"], ReadError>
  }
): Query<Name, A, S> => {
  const decodeArgs: (u: unknown) => Effect.Effect<A["Type"], Schema.SchemaError> = Schema.decodeUnknownEffect(
    Schema.toCodecJson(options.args)
  )
  const encodeSuccess: (u: unknown) => Effect.Effect<Schema.Json, Schema.SchemaError> = Schema.encodeUnknownEffect(
    Schema.toCodecJson(options.success)
  )
  return {
    name,
    args: options.args,
    success: options.success,
    handler: options.handler,
    execute: (db, args) =>
      decodeArgs(args).pipe(
        Effect.mapError((e) => new QueryArgsDecodeFailed({ name, message: e.message })),
        Effect.flatMap((decoded) => options.handler(db, decoded)),
        Effect.flatMap((result) => Effect.orDie(encodeSuccess(result)))
      )
  }
}

export class DuplicateQuery extends Data.TaggedError("DuplicateQuery")<{ readonly message: string }> {
  constructor(message: string) {
    super({ message })
  }
}

export interface QueryRegistry {
  readonly queries: ReadonlyMap<string, AnyQuery>
}

/** Build a registry; throws `DuplicateQuery` if two queries share a name. */
export const makeRegistry = (queries: ReadonlyArray<AnyQuery>): QueryRegistry => {
  const byName = new Map<string, AnyQuery>()
  for (const q of queries) {
    if (byName.has(q.name)) throw new DuplicateQuery(`QueryRegistry: duplicate query "${q.name}"`)
    byName.set(q.name, q)
  }
  return { queries: byName }
}

/**
 * Run one registered query by name over `db`. The handler is given a fresh object with only the read
 * methods, so no cast inside a handler reaches `insert`/`patch`/`delete` (INV-BRW-6).
 */
export const run = (
  registry: QueryRegistry,
  name: string,
  args: Schema.Json,
  db: EchoStore
): Effect.Effect<Schema.Json, UnknownQuery | QueryArgsDecodeFailed | ReadError> => {
  const query = registry.queries.get(name)
  const reads: ReadStore = { get: db.get, query: db.query, scan: db.scan }
  return query === undefined ? Effect.fail(new UnknownQuery({ name })) : query.execute(reads, args)
}
