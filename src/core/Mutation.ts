import { Cause, type DateTime, Effect, Random, Schema } from "effect"
import type { PrincipalId } from "./AccessScope.ts"
import {
  InvalidRow,
  RowAlreadyExists,
  RowNotFound,
  ScopeForbidden,
  StoreUnavailable,
  type StoreError
} from "./Errors.ts"
import { ClientId, MutationId, Seq } from "./Ids.ts"
import type { EchoStore } from "./Store.ts"

/** Inputs of one handler run (DESIGN §6). `now` and `random` are fixed by the caller. */
export interface MutationContext {
  readonly actor: PrincipalId
  readonly now: DateTime.Utc
  readonly random: Random.Random
  readonly store: EchoStore
}

/**
 * Deterministic `random` for a mutation, seeded from its identity, so re-running a pending
 * mutation on every rebase produces the same optimistic result.
 */
export const seededRandom = (clientId: ClientId, mutationId: MutationId): Random.Random =>
  Effect.runSync(Effect.service(Random.Random).pipe(Random.withSeed(JSON.stringify([clientId, mutationId]))))

// ---------------------------------------------------------------------------------------------
// Rejections and outcomes (Schema-encodable: they travel in push results and acks)
// ---------------------------------------------------------------------------------------------

export class UnknownMutation extends Schema.TaggedError<UnknownMutation>()("UnknownMutation", {
  name: Schema.String
}) {}

export class ArgsDecodeFailed extends Schema.TaggedError<ArgsDecodeFailed>()("ArgsDecodeFailed", {
  name: Schema.String,
  message: Schema.String
}) {}

/** A domain error declared by the mutation, JSON-encoded with that mutation's `error` schema. */
export class DomainError extends Schema.TaggedError<DomainError>()("DomainError", {
  mutation: Schema.String,
  error: Schema.Json
}) {}

/**
 * Terminal (DESIGN §16.5): the server answered a `seq ≤` its expiry floor for this client. The
 * mutation was processed at some point; its outcome is unknown, its effect (if any) is installed.
 */
export class ClientExpired extends Schema.TaggedError<ClientExpired>()("ClientExpired", {
  mutationId: MutationId
}) {}

export const MutationRejection = Schema.Union([
  UnknownMutation,
  ArgsDecodeFailed,
  RowNotFound,
  RowAlreadyExists,
  ScopeForbidden,
  InvalidRow,
  DomainError,
  ClientExpired
])
export type MutationRejection = typeof MutationRejection.Type

export const Applied = Schema.TaggedStruct("Applied", {})
export const Rejected = Schema.TaggedStruct("Rejected", { error: MutationRejection })
export const MutationOutcome = Schema.Union([Applied, Rejected])
export type MutationOutcome = typeof MutationOutcome.Type

/** Durable intent identity + payload (outbox and ledger form). `args` is the encoded args. */
export const MutationEnvelope = Schema.Struct({
  clientId: ClientId,
  mutationId: MutationId,
  seq: Seq,
  name: Schema.String,
  args: Schema.Json
})
export type MutationEnvelope = typeof MutationEnvelope.Type

// ---------------------------------------------------------------------------------------------
// Definition and registry
// ---------------------------------------------------------------------------------------------

/** Args/error schemas must be service-free so the same handler runs on client and server. */
export type PureSchema = Schema.Top & { readonly DecodingServices: never; readonly EncodingServices: never }

/** What a registry needs from a mutation, with its types erased. */
export interface AnyMutation {
  readonly name: string
  /** Decode `args` (JSON), run the handler, classify failures. Transaction boundaries are the caller's. */
  readonly execute: (
    ctx: MutationContext,
    args: Schema.Json
  ) => Effect.Effect<void, MutationRejection | StoreUnavailable>
}

export interface Mutation<Name extends string, A extends PureSchema, E extends PureSchema> extends AnyMutation {
  readonly name: Name
  /** Args schema. Evolve it with a Union of the current shape and upcast old shapes. */
  readonly args: A
  readonly error: E | Schema.Never
  readonly handler: (ctx: MutationContext, args: A["Type"]) => Effect.Effect<void, E["Type"] | StoreError>
}

const isStoreFailure = (
  e: unknown
): e is RowNotFound | RowAlreadyExists | ScopeForbidden | InvalidRow | StoreUnavailable | ArgsDecodeFailed =>
  e instanceof RowNotFound ||
  e instanceof RowAlreadyExists ||
  e instanceof ScopeForbidden ||
  e instanceof InvalidRow ||
  e instanceof StoreUnavailable ||
  e instanceof ArgsDecodeFailed

/** Turns every failure in `cause` into a defect, keeping dies and interrupts. */
const asDefects = (cause: Cause.Cause<unknown>): Cause.Cause<never> =>
  Cause.fromReasons(cause.reasons.map((r) => (Cause.isFailReason(r) ? Cause.makeDieReason(r.error) : r)))

export const make = <const Name extends string, A extends PureSchema, E extends PureSchema = Schema.Never>(
  name: Name,
  options: {
    readonly args: A
    readonly error?: E
    readonly handler: (ctx: MutationContext, args: A["Type"]) => Effect.Effect<void, E["Type"] | StoreError>
  }
): Mutation<Name, A, E> => {
  const error = options.error ?? Schema.Never
  const errorSchema: PureSchema = error
  const decodeArgs: (u: unknown) => Effect.Effect<A["Type"], Schema.SchemaError> = Schema.decodeUnknownEffect(
    Schema.toCodecJson(options.args)
  )
  const encodeError = Schema.encodeUnknownEffect(Schema.toCodecJson(errorSchema))
  /**
   * Whole-cause classification (DESIGN §6). A defect or interruption anywhere dies with the
   * full cause (failures turned into defects), never a Rejected outcome. A StoreUnavailable
   * anywhere is transient. Only a cause made purely of permanent failures is a rejection.
   */
  const classify = (cause: Cause.Cause<unknown>): Effect.Effect<never, MutationRejection | StoreUnavailable> => {
    const failures = cause.reasons.filter(Cause.isFailReason).map((r) => r.error)
    const [first] = failures
    if (first === undefined || failures.length !== cause.reasons.length) {
      return Effect.failCause(asDefects(cause))
    }
    const transient = failures.find((e) => e instanceof StoreUnavailable)
    if (transient !== undefined) return Effect.fail(transient)
    // Every domain failure must be declared and encodable, not only the one reported (DESIGN §6):
    // an undeclared or unencodable domain error anywhere is a bug in the mutation, so the
    // mutation dies with the full cause. Otherwise the first failure in cause order is reported.
    return Effect.gen(function* () {
      const encoded: Array<Schema.Json> = []
      for (const e of failures) {
        if (isStoreFailure(e)) continue
        const result = yield* Effect.exit(encodeError(e))
        if (result._tag === "Failure") return yield* Effect.failCause(asDefects(cause))
        encoded.push(result.value)
      }
      if (isStoreFailure(first)) return yield* Effect.fail(first)
      // `first` is a domain failure, so it was encoded first.
      return yield* new DomainError({ mutation: name, error: encoded[0] ?? null })
    })
  }
  return {
    name,
    args: options.args,
    error,
    handler: options.handler,
    execute: (ctx, args) =>
      decodeArgs(args).pipe(
        Effect.mapError((e) => new ArgsDecodeFailed({ name, message: e.message })),
        Effect.flatMap((decoded) => options.handler(ctx, decoded)),
        Effect.catchCause((cause) => classify(cause))
      )
  }
}

export class DuplicateMutation extends Error {
  override readonly name = "DuplicateMutation"
}

export interface MutationRegistry {
  readonly mutations: ReadonlyMap<string, AnyMutation>
}

/** Build a registry; throws `DuplicateMutation` if two mutations share a name. */
export const makeRegistry = (mutations: ReadonlyArray<AnyMutation>): MutationRegistry => {
  const byName = new Map<string, AnyMutation>()
  for (const m of mutations) {
    if (byName.has(m.name)) throw new DuplicateMutation(`MutationRegistry: duplicate mutation "${m.name}"`)
    byName.set(m.name, m)
  }
  return { mutations: byName }
}

/** Run one envelope's handler. Permanent failures are `MutationRejection`; transient stay `StoreUnavailable`. */
export const run = (
  registry: MutationRegistry,
  envelope: { readonly name: string; readonly args: Schema.Json },
  ctx: MutationContext
): Effect.Effect<void, MutationRejection | StoreUnavailable> => {
  const mutation = registry.mutations.get(envelope.name)
  return mutation === undefined
    ? Effect.fail(new UnknownMutation({ name: envelope.name }))
    : mutation.execute(ctx, envelope.args)
}

// ---------------------------------------------------------------------------------------------
// Typed notice decoding (DESIGN §9.1)
// ---------------------------------------------------------------------------------------------

/** `decodeNotice` was given a notice of another mutation, or one its schemas cannot decode. */
export class NoticeMismatch extends Schema.TaggedError<NoticeMismatch>()("NoticeMismatch", {
  /** The mutation decoded against. */
  mutation: Schema.String,
  /** The envelope's mutation name. */
  name: Schema.String,
  message: Schema.String
}) {}

/** A `DomainError` whose payload is decoded with its mutation's `error` schema. */
export interface TypedDomainError<Name extends string, E> {
  readonly _tag: "DomainError"
  readonly mutation: Name
  readonly error: E
}

/** A notice's envelope args and outcome, typed by its mutation. Other rejections pass through. */
export interface DecodedNotice<Name extends string, A, E> {
  readonly args: A
  readonly outcome:
    | typeof Applied.Type
    | {
        readonly _tag: "Rejected"
        readonly error: Exclude<MutationRejection, DomainError> | TypedDomainError<Name, E>
      }
}

/**
 * Decodes a retained notice against `mutation`: `args` with its `args` schema and, for a
 * `Rejected` outcome whose error is a `DomainError`, the payload with its `error` schema. Any
 * other rejection is returned as is. A notice of a different mutation, or one these schemas
 * cannot decode (a different args version, a corrupted payload), is `NoticeMismatch`, never a
 * defect. Pure: the schemas are service-free, so it runs on the tab and on the server.
 */
export const decodeNotice = <Name extends string, A extends PureSchema, E extends PureSchema>(
  mutation: Mutation<Name, A, E>,
  notice: {
    readonly envelope: { readonly name: string; readonly args: Schema.Json }
    readonly outcome: MutationOutcome
  }
): Effect.Effect<DecodedNotice<Name, A["Type"], E["Type"]>, NoticeMismatch> =>
  Effect.gen(function* () {
    const { envelope, outcome } = notice
    const mismatch = (message: string) => new NoticeMismatch({ mutation: mutation.name, name: envelope.name, message })
    if (envelope.name !== mutation.name) {
      return yield* mismatch(`a notice of "${envelope.name}" cannot be decoded as "${mutation.name}"`)
    }
    const args: A["Type"] = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(mutation.args))(envelope.args).pipe(
      Effect.mapError((e) => mismatch(`args: ${e.message}`))
    )
    if (outcome._tag === "Applied") return { args, outcome }
    const domain = outcome.error
    if (domain._tag !== "DomainError") return { args, outcome: { _tag: "Rejected", error: domain } }
    if (domain.mutation !== mutation.name) {
      return yield* mismatch(`a domain error of "${domain.mutation}" cannot be decoded as "${mutation.name}"`)
    }
    const error: E["Type"] = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(mutation.error))(domain.error).pipe(
      Effect.mapError((e) => mismatch(`error: ${e.message}`))
    )
    return { args, outcome: { _tag: "Rejected", error: { _tag: "DomainError", mutation: mutation.name, error } } }
  })
