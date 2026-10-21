import { DateTime, Effect, Option, Schema, SchemaGetter } from "effect"
import { describe, expect, it } from "vitest"
import { AccessScopeId, PrincipalId } from "./AccessScope.ts"
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
  return Store.make(rows, { roleFor: () => Effect.succeed(Option.some("writer" as const)) }).store
}

describe("mutation args evolve without breaking queued intents (DESIGN §6)", () => {
  it("a v1 envelope still in an outbox decodes through the wire envelope and runs as v2", async () => {
    // Exactly what a v1 client persisted and later pushes, as JSON text.
    const queued = JSON.parse(
      `{"clientId":"c1","mutationId":"m-1","seq":1,"name":"todos.setCompleted","args":{"id":"t1","completed":true}}`
    )
    const envelope = Schema.decodeUnknownSync(Schema.toCodecJson(Mutation.MutationEnvelope))(queued)
    const store = makeStore()
    const now = DateTime.makeUnsafe(1_700_000_000_000)
    await Effect.runPromise(
      store.insert(Todos, { id: TodoId.make("t1"), accessScopeId: AccessScopeId.make("s"), completedAt: null })
    )
    const ctx = {
      actor: PrincipalId.make("u"),
      now,
      random: Mutation.seededRandom(envelope.clientId, envelope.mutationId),
      store
    }
    await Effect.runPromise(Mutation.run(Mutation.makeRegistry([setCompleted]), envelope, ctx))
    const row = await Effect.runPromise(store.get(Todos, TodoId.make("t1")))
    expect(row.completedAt).toEqual(now)
  })

  it("v1 args that are neither shape are a typed ArgsDecodeFailed rejection, not a defect", async () => {
    const ctx = {
      actor: PrincipalId.make("u"),
      now: DateTime.makeUnsafe(0),
      random: Mutation.seededRandom(ClientId.make("c"), MutationId.make("m")),
      store: makeStore()
    }
    const err = await Effect.runPromise(
      Effect.flip(
        Mutation.run(
          Mutation.makeRegistry([setCompleted]),
          { name: "todos.setCompleted", args: { id: "t1", completed: "yes" } },
          ctx
        )
      )
    )
    if (err._tag === "StoreUnavailable") throw err
    expect(err._tag).toBe("ArgsDecodeFailed")
    // Rejections travel in push results: they must survive the wire.
    const wire = Schema.encodeSync(Schema.toCodecJson(Mutation.MutationOutcome))({ _tag: "Rejected", error: err })
    expect(
      Schema.decodeUnknownSync(Schema.toCodecJson(Mutation.MutationOutcome))(JSON.parse(JSON.stringify(wire)))
    ).toEqual({ _tag: "Rejected", error: err })
  })
})

const draw = (c: string, m: string) => {
  const r = Mutation.seededRandom(ClientId.make(c), MutationId.make(m))
  return [r.nextDoubleUnsafe(), r.nextDoubleUnsafe()]
}

describe("mutation registry and identity", () => {
  it("rejects two mutations with the same name at construction", () => {
    expect(() => Mutation.makeRegistry([setCompleted, setCompleted])).toThrow(/duplicate mutation "todos.setCompleted"/)
  })

  it("the seeded random is a function of (clientId, mutationId), so a rebase replays identically", () => {
    expect(draw("c1", "m1")).toEqual(draw("c1", "m1"))
    expect(draw("c1", "m1")).not.toEqual(draw("c1", "m2"))
    expect(draw("c1", "m1")).not.toEqual(draw("c2", "m1"))
  })

  it("the wire rejects seq < 1 and keeps Position exact beyond 2^53", () => {
    expect(() => Schema.decodeUnknownSync(Seq)(0)).toThrow(/greater than or equal to 1/)
    const done = { _tag: "PullDone", changes: [], acks: [], nextCursor: "9007199254740993", expiredThrough: 0 }
    const decoded = Schema.decodeUnknownSync(Schema.toCodecJson(Protocol.PullResponse))(done)
    expect(decoded._tag === "PullDone" && decoded.nextCursor).toBe(9007199254740993n)
  })
})
