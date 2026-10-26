import { Array as Arr, Cause, DateTime, Effect, Option, Schema, SchemaGetter } from "effect"
import { describe, expect, it } from "@effect/vitest"
import { AccessScopeId, PrincipalId } from "./AccessScope.ts"
import { RowNotFound, ScopeForbidden, StoreUnavailable } from "./Errors.ts"
import { ClientId, MutationId, Seq } from "./Ids.ts"
import * as Mutation from "./Mutation.ts"
import * as Protocol from "./Protocol.ts"
import * as Store from "./Store.ts"
import * as SyncTable from "./SyncTable.ts"

const TodoId = Schema.String.pipe(Schema.brand("TodoId"))
const Todos = SyncTable.make("todos", {
  id: TodoId,
  accessScopeId: AccessScopeId,
  completedAt: Schema.NullOr(Schema.DateTimeUtc)
})

// v1 queued intents said `{ id, completed: boolean }`; v2 says `{ id, completedAt: epoch ms | null }`.
const V2 = Schema.Struct({ id: TodoId, completedAt: Schema.NullOr(Schema.Int) })
const V1 = Schema.Struct({ id: TodoId, completed: Schema.Boolean })
const V1Upcast = V1.pipe(
  Schema.decodeTo(V2, {
    // An old intent carries no time; "completed" in v1 meant "as of the server's now", encoded as 0
    // here and resolved by the handler below. Encoding back to v1 is never needed.
    decode: SchemaGetter.transform((v1: typeof V1.Type): typeof V2.Encoded => ({
      id: v1.id,
      completedAt: v1.completed ? 0 : null
    })),
    encode: SchemaGetter.transform((v2: typeof V2.Encoded) => ({
      id: TodoId.make(v2.id),
      completed: v2.completedAt !== null
    }))
  })
)

const setCompleted = Mutation.make("todos.setCompleted", {
  args: Schema.Union([V2, V1Upcast]),
  handler: (ctx, { id, completedAt }) =>
    ctx.store.patch(Todos, id, {
      completedAt: completedAt === null ? null : completedAt === 0 ? ctx.now : DateTime.makeUnsafe(completedAt)
    })
})

const makeStore = () => {
  const data = new Map<string, SyncTable.EncodedRow>()
  const rows: Store.RowStore = {
    read: (_, id) => Effect.sync(() => Option.fromNullishOr(data.get(id))),
    write: (_, id, row) =>
      Effect.sync(() => {
        if (Option.isSome(row)) data.set(id, row.value)
        else data.delete(id)
      }),
    range: () => Effect.succeed([]),
    scan: () => Effect.sync(() => [...data.values()])
  }
  return Store.make(rows, {
    actor: PrincipalId.make("*"),
    roleFor: () => Effect.succeed(Option.some("writer" as const)),
    claimScope: () => Effect.succeed(false),
    now: DateTime.makeUnsafe(0)
  }).store
}

describe("mutation args evolve without breaking queued intents (SPEC §5)", () => {
  it.effect(
    "unknown nested args are rejected through the wire registry unless stripping is explicitly enabled",
    Effect.fnUntraced(function* () {
      const args = Schema.Struct({
        customTimes: Schema.Record(Schema.String, Schema.Struct({ minutes: Schema.Int }))
      })
      const seen: Array<typeof args.Type> = []
      const handler = (_: Mutation.MutationContext, value: typeof args.Type) =>
        Effect.sync(() => {
          seen.push(value)
        })
      const strict = Mutation.make("strict", { args, handler })
      const loose = Mutation.make("loose", { args, handler, onExcessProperty: "ignore" })
      const registry = Mutation.makeRegistry([strict, loose])
      for (const input of [
        { customTimes: { night: { minutes: 12, typo: 3 } } },
        { customTimes: { night: { minutes: 12 } }, typo: 3 }
      ]) {
        const envelope = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(Mutation.MutationEnvelope))({
          clientId: "c",
          mutationId: "m",
          seq: 1,
          name: "strict",
          args: input
        })
        const error = yield* Effect.flip(Mutation.run(registry, envelope, ctx()))
        expect(error).toBeInstanceOf(Mutation.ArgsDecodeFailed)
        if (error._tag !== "ArgsDecodeFailed") throw error
        expect(error.message).toContain("typo")
        const codec = Schema.toCodecJson(Mutation.MutationOutcome)
        const wire = yield* Schema.encodeEffect(Schema.fromJsonString(codec))({ _tag: "Rejected", error })
        expect(yield* Schema.decodeUnknownEffect(Schema.fromJsonString(codec))(wire)).toEqual({
          _tag: "Rejected",
          error
        })
        expect(seen).toEqual([])
      }
      yield* Mutation.run(
        registry,
        {
          name: "loose",
          args: { customTimes: { night: { minutes: 12, typo: 3 } }, typo: 1 }
        },
        ctx()
      )
      yield* Mutation.run(
        registry,
        {
          name: "strict",
          args: { customTimes: { night: { minutes: 7 } } }
        },
        ctx()
      )
      expect(seen).toEqual([{ customTimes: { night: { minutes: 12 } } }, { customTimes: { night: { minutes: 7 } } }])
    })
  )

  it.effect(
    "a v1 envelope still in an outbox decodes through the wire envelope and runs as v2",
    Effect.fnUntraced(function* () {
      // Exactly what a v1 client persisted and later pushes, as JSON text.
      const envelope = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(Schema.toCodecJson(Mutation.MutationEnvelope))
      )(`{"clientId":"c1","mutationId":"m-1","seq":1,"name":"todos.setCompleted","args":{"id":"t1","completed":true}}`)
      const store = makeStore()
      const now = DateTime.makeUnsafe(1_700_000_000_000)
      yield* store.insert(Todos, { id: TodoId.make("t1"), accessScopeId: AccessScopeId.make("s"), completedAt: null })
      const ctx = {
        actor: PrincipalId.make("u"),
        now,
        random: Mutation.seededRandom(envelope.clientId, envelope.mutationId),
        store
      }
      yield* Mutation.run(Mutation.makeRegistry([setCompleted]), envelope, ctx)
      const row = yield* store.get(Todos, TodoId.make("t1"))
      expect(row.completedAt).toEqual(now)
    })
  )

  it.effect(
    "v1 args that are neither shape are a typed ArgsDecodeFailed rejection, not a defect",
    Effect.fnUntraced(function* () {
      const ctx = {
        actor: PrincipalId.make("u"),
        now: DateTime.makeUnsafe(0),
        random: Mutation.seededRandom(ClientId.make("c"), MutationId.make("m")),
        store: makeStore()
      }
      const err = yield* Effect.flip(
        Mutation.run(
          Mutation.makeRegistry([setCompleted]),
          { name: "todos.setCompleted", args: { id: "t1", completed: "yes" } },
          ctx
        )
      )
      if (err._tag === "StoreUnavailable") throw err
      expect(err._tag).toBe("ArgsDecodeFailed")
      // Rejections travel in push results: they must survive the wire.
      const wire = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.toCodecJson(Mutation.MutationOutcome)))({
        _tag: "Rejected",
        error: err
      })
      expect(
        yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.toCodecJson(Mutation.MutationOutcome)))(wire)
      ).toEqual({ _tag: "Rejected", error: err })
    })
  )
})

const draw = (c: string, m: string) => {
  const r = Mutation.seededRandom(ClientId.make(c), MutationId.make(m))
  return [r.nextDoubleUnsafe(), r.nextDoubleUnsafe()]
}

const ctx = () => ({
  actor: PrincipalId.make("u"),
  now: DateTime.makeUnsafe(0),
  random: Mutation.seededRandom(ClientId.make("c"), MutationId.make("m")),
  store: makeStore()
})
const Nope = Schema.Struct({ _tag: Schema.Literal("Nope") })
const failing = (body: Effect.Effect<void, { readonly _tag: "Nope" } | StoreUnavailable>) =>
  Mutation.makeRegistry([Mutation.make("x", { args: Schema.Struct({}), error: Nope, handler: () => body })])
/** A cause holding a declared domain failure plus `other`, as a parallel handler can produce. */
const compound = (other: Cause.Reason<{ readonly _tag: "Nope" } | StoreUnavailable>) =>
  Effect.failCause(Cause.fromReasons([Cause.makeFailReason({ _tag: "Nope" as const }), other]))

describe("mutation outcome classification over the whole cause (SPEC §5)", () => {
  it.effect(
    "a domain failure combined with a defect dies with the defect, never a Rejected outcome",
    Effect.fnUntraced(function* () {
      const exit = yield* Effect.exit(
        Mutation.run(failing(compound(Cause.makeDieReason("programmer bug"))), { name: "x", args: {} }, ctx())
      )
      const cause = exit._tag === "Failure" ? exit.cause : Cause.empty
      expect(exit._tag).toBe("Failure")
      expect(Cause.hasFails(cause)).toBe(false)
      expect(Cause.hasDies(cause)).toBe(true)
      expect(Cause.squash(cause)).not.toMatchObject({ _tag: "DomainError" })
    })
  )

  it.effect(
    "StoreUnavailable alongside a domain failure stays transient, never a permanent rejection",
    Effect.fnUntraced(function* () {
      const err = yield* Effect.flip(
        Mutation.run(
          failing(compound(Cause.makeFailReason(new StoreUnavailable({ message: "down" })))),
          { name: "x", args: {} },
          ctx()
        )
      )
      expect(err._tag).toBe("StoreUnavailable")
    })
  )
})

const strict = (reasons: ReadonlyArray<Cause.Reason<string | RowNotFound>>) =>
  Mutation.makeRegistry([
    Mutation.make("strict", {
      args: Schema.Struct({}),
      error: Schema.NonEmptyString,
      handler: () => Effect.failCause(Cause.fromReasons(reasons))
    })
  ])
const outcome = (reasons: ReadonlyArray<Cause.Reason<string | RowNotFound>>) =>
  Effect.exit(Mutation.run(strict(reasons), { name: "strict", args: {} }, ctx()))
describe("every domain failure in a cause must encode before any is reported (SPEC §5)", () => {
  const valid = Cause.makeFailReason("valid")
  const unencodable = Cause.makeFailReason("") // type-valid string, rejected by NonEmptyString

  it.effect(
    "an unencodable domain failure next to a valid one dies, in either order, never a DomainError",
    Effect.fnUntraced(function* () {
      for (const reasons of [
        [valid, unencodable],
        [unencodable, valid],
        [Cause.makeFailReason(new RowNotFound({ table: "t", id: "1" })), unencodable]
      ]) {
        const exit = yield* outcome(reasons)
        const cause = exit._tag === "Failure" ? exit.cause : Cause.empty
        expect(exit._tag).toBe("Failure")
        expect(Cause.hasFails(cause)).toBe(false)
        expect(Cause.hasDies(cause)).toBe(true)
      }
    })
  )

  it.effect(
    "when every failure encodes, the first in cause order is the rejection",
    Effect.fnUntraced(function* () {
      const err = yield* Effect.flip(
        Mutation.run(strict([valid, Cause.makeFailReason("second")]), { name: "strict", args: {} }, ctx())
      )
      expect(err).toMatchObject({ _tag: "DomainError", mutation: "strict", error: "valid" })
    })
  )
})

describe("mutation registry and identity", () => {
  it("rejects two mutations with the same name at construction", () => {
    expect(() => Mutation.makeRegistry([setCompleted, setCompleted])).toThrow(/duplicate mutation "todos.setCompleted"/)
  })

  it("the seeded random is a function of (clientId, mutationId), so a rebase replays identically", () => {
    expect(draw("c1", "m1")).toEqual(draw("c1", "m1"))
    expect(draw("c1", "m1")).not.toEqual(draw("c1", "m2"))
    expect(draw("c1", "m1")).not.toEqual(draw("c2", "m1"))
  })

  it.effect(
    "the wire rejects seq < 1 and keeps Position exact beyond 2^53",
    Effect.fnUntraced(function* () {
      const error = yield* Effect.flip(Schema.decodeUnknownEffect(Seq)(0))
      expect(String(error)).toMatch(/greater than or equal to 1/)
      const done = { _tag: "PullDone", changes: [], acks: [], nextCursor: "9007199254740993", expiredThrough: 0 }
      const decoded = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(Protocol.PullResponse))(done)
      expect(decoded._tag === "PullDone" && decoded.nextCursor).toBe(9007199254740993n)
    })
  )
})

/** The NoticeMismatch the decode failed with; anything else (success, defect) fails the test. */
const mismatchOf = Effect.fnUntraced(function* <A>(effect: Effect.Effect<A, Mutation.NoticeMismatch>) {
  const exit = yield* Effect.exit(effect)
  if (exit._tag === "Success") {
    const value = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(exit.value)
    throw new Error(`decoded to ${value}`)
  }
  if (!Arr.every(exit.cause.reasons, Cause.isFailReason)) throw new Error(Cause.pretty(exit.cause))
  return yield* Effect.orDie(Effect.flip(effect))
})

describe("decodeNotice types a stored notice by its mutation (SPEC §9.5)", () => {
  const TooLate = Schema.TaggedStruct("TooLate", { deadline: Schema.Date })
  const Locked = Schema.TaggedStruct("Locked", { by: Schema.String })
  // oxlint-disable-next-line effecttsgo/global-date -- Native Date is the declared mutation payload type under test.
  const deadline = new Date(1_700_000_000_000)
  const archive = Mutation.make("todos.archive", {
    args: Schema.Struct({ id: TodoId, at: Schema.Date }),
    error: Schema.Union([TooLate, Locked]),
    handler: () => Effect.fail({ _tag: "TooLate" as const, deadline })
  })
  // Its args are a subset of archive's: archive's args decode under it, so only the name tells them apart.
  const touch = Mutation.make("todos.touch", {
    args: Schema.Struct({ id: TodoId }),
    error: Locked,
    handler: () => Effect.void
  })
  // What a client queued for archive, as JSON (Date encodes as an ISO string).
  const envelope: Mutation.MutationEnvelope = {
    clientId: ClientId.make("c1"),
    mutationId: MutationId.make("m-1"),
    seq: Seq.make(3),
    name: "todos.archive",
    args: { id: "t1", at: "2023-11-14T22:13:20.000Z" }
  }
  // The same envelope with args as archive's Type, written by hand.
  const typedEnvelope = {
    clientId: "c1",
    mutationId: "m-1",
    seq: 3,
    name: "todos.archive",
    // oxlint-disable-next-line effecttsgo/global-date -- Assert that the wire codec reconstructs the native Date payload.
    args: { id: "t1", at: new Date("2023-11-14T22:13:20.000Z") }
  }
  const forbidden = new ScopeForbidden({ scopeId: AccessScopeId.make("s"), required: "writer", held: null })
  const rejected = (error: Mutation.MutationRejection): Mutation.Notice => ({ _tag: "Rejected", envelope, error })

  it.effect(
    "a notice of one mutation decoded against another, or one its schemas cannot read, is NoticeMismatch, never a value or a defect",
    Effect.fnUntraced(function* () {
      const cases: ReadonlyArray<[string, Effect.Effect<unknown, Mutation.NoticeMismatch>]> = [
        [
          "archive's OutcomeUnknown as touch (its args would decode)",
          Mutation.decodeNotice(touch, { _tag: "OutcomeUnknown", envelope })
        ],
        ["archive's rejection as touch", Mutation.decodeNotice(touch, rejected(forbidden))],
        [
          "args of another shape under the right name",
          Mutation.decodeNotice(archive, {
            _tag: "OutcomeUnknown",
            envelope: { ...envelope, args: { id: "t1", at: 5 } }
          })
        ],
        [
          "a domain payload outside the error schema",
          Mutation.decodeNotice(
            archive,
            rejected(new Mutation.DomainError({ mutation: "todos.archive", error: { _tag: "Gone" } }))
          )
        ],
        [
          "a domain error recorded for another mutation",
          Mutation.decodeNotice(
            archive,
            rejected(new Mutation.DomainError({ mutation: "todos.touch", error: { _tag: "Locked", by: "u" } }))
          )
        ]
      ]
      for (const [label, effect] of cases) {
        const error = yield* mismatchOf(effect).pipe(
          Effect.catchCause((cause) => Effect.die(new Error(`${label}: ${Cause.pretty(cause)}`)))
        )
        expect(error).toBeInstanceOf(Mutation.NoticeMismatch)
      }
    })
  )

  it.effect(
    "a DomainError payload round-trips to the declared error type; other rejections pass through untouched",
    Effect.fnUntraced(function* () {
      // The rejection exactly as the server produces it: the handler's failure, encoded by archive.
      const context = {
        actor: PrincipalId.make("u"),
        now: DateTime.makeUnsafe(0),
        random: Mutation.seededRandom(ClientId.make("c1"), MutationId.make("m-1")),
        store: makeStore()
      }
      const rejection = yield* Effect.flip(archive.execute(context, envelope.args))
      if (!Schema.is(Mutation.DomainError)(rejection)) throw new Error(`not a DomainError: ${String(rejection)}`)
      expect(yield* Mutation.decodeNotice(archive, rejected(rejection))).toEqual({
        _tag: "Rejected",
        envelope: typedEnvelope,
        error: { _tag: "DomainError", mutation: "todos.archive", error: { _tag: "TooLate", deadline } }
      })
      const passed = yield* Mutation.decodeNotice(archive, rejected(forbidden))
      expect(passed).toEqual({ _tag: "Rejected", envelope: typedEnvelope, error: forbidden })
      expect(passed._tag === "Rejected" ? passed.error : undefined).toBe(forbidden)
    })
  )

  it.effect(
    "an OutcomeUnknown notice (the user's recovery record) gets typed args and nothing else",
    Effect.fnUntraced(function* () {
      const decoded = yield* Mutation.decodeNotice(archive, { _tag: "OutcomeUnknown", envelope })
      expect(decoded).toStrictEqual({ _tag: "OutcomeUnknown", envelope: typedEnvelope })
    })
  )
})
