import { Effect, Schedule, Schema } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"

/**
 * A permanent failure of a mutation body (DESIGN.md §6). Recorded in the ledger as the
 * mutation's outcome; the batch continues.
 */
export class MutationRejected extends Schema.TaggedError<MutationRejected>()("MutationRejected", {
  reason: Schema.Literals(["decode", "forbidden", "domain"]),
  message: Schema.String
}) {}

/** The recorded result of a processed mutation. Identical on every resubmission. */
export const MutationOutcome = Schema.Union([
  Schema.TaggedStruct("Applied", {}),
  Schema.TaggedStruct("Rejected", {
    reason: Schema.Literals(["decode", "forbidden", "domain"]),
    message: Schema.String
  })
])
export type MutationOutcome = typeof MutationOutcome.Type

/**
 * A failure that says nothing about the mutation itself (serialization conflict after retries,
 * lost connection, unclassified database error). Nothing is recorded; the client retries from
 * this mutation, so order is preserved.
 */
export class MutationTransient extends Schema.TaggedError<MutationTransient>()("MutationTransient", {
  clientId: Schema.String,
  mutationId: Schema.Number,
  message: Schema.String
}) {}

/** The client id is registered to a different principal. */
export class ClientForbidden extends Schema.TaggedError<ClientForbidden>()("ClientForbidden", {
  clientId: Schema.String
}) {}

export interface Envelope {
  readonly principalId: string
  readonly clientId: string
  readonly mutationId: number
}

export interface LedgerOptions {
  /** Retry policy for serialization failures and deadlocks. Default: 4 retries, exponential from 10ms. */
  readonly conflictRetry?: Schedule.Schedule<unknown, SqlError> | undefined
}

const defaultConflictRetry = Schedule.exponential("10 millis").pipe(Schedule.upTo({ times: 4 }))

const decodeOutcome = Schema.decodeUnknownEffect(MutationOutcome)
const encodeOutcome = Schema.encodeSync(MutationOutcome)

const transient = (envelope: Envelope, error: SqlError) =>
  new MutationTransient({
    clientId: envelope.clientId,
    mutationId: envelope.mutationId,
    message: `${error.reason._tag}: ${error.message}`
  })

const isConflict = (error: SqlError) =>
  error.reason._tag === "SerializationError" || error.reason._tag === "DeadlockError"

/**
 * Binds a client id to its principal on first sight and refuses it to any other principal, so
 * one principal can never read another's acks or replay another's mutation ids.
 */
export const registerClient = Effect.fnUntraced(function* (principalId: string, clientId: string) {
  const sql = yield* SqlClient
  const rows = yield* sql<{ principal_id: string }>`
    INSERT INTO echo_sync.clients (client_id, principal_id) VALUES (${clientId}, ${principalId})
    ON CONFLICT (client_id) DO UPDATE SET last_seen_at = now()
    RETURNING principal_id`
  if (rows[0]?.principal_id !== principalId) {
    return yield* new ClientForbidden({ clientId })
  }
})

const lookup = Effect.fnUntraced(function* (envelope: Envelope) {
  const sql = yield* SqlClient
  const rows = yield* sql<{ outcome: unknown }>`
    SELECT outcome FROM echo_sync.mutations
    WHERE client_id = ${envelope.clientId} AND mutation_id = ${envelope.mutationId}`
  const row = rows[0]
  return row === undefined ? undefined : yield* Effect.orDie(decodeOutcome(row.outcome))
})

/**
 * Executes one mutation exactly once (DESIGN.md §6).
 *
 * Known identity ⇒ the recorded outcome, without re-executing. Otherwise one transaction:
 * body (in a savepoint, so a permanent failure discards its writes) → ledger row whose position
 * is allocated under the engine lock, after the body's log entries → commit. A mutation that
 * writes nothing still gets an orderable position, so its ack is delivered by pull.
 */
export const runMutation = <R>(
  envelope: Envelope,
  body: Effect.Effect<void, MutationRejected | SqlError, R>,
  options?: LedgerOptions
): Effect.Effect<MutationOutcome, MutationTransient | ClientForbidden, R | SqlClient> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient

    const execute = sql.withTransaction(
      Effect.gen(function* () {
        const outcome: MutationOutcome = yield* sql.withTransaction(body).pipe(
          Effect.as<MutationOutcome>({ _tag: "Applied" }),
          Effect.catchTag("MutationRejected", (e) =>
            Effect.succeed<MutationOutcome>({ _tag: "Rejected", reason: e.reason, message: e.message })
          )
        )
        yield* sql`
          INSERT INTO echo_sync.mutations (client_id, mutation_id, position, outcome)
          VALUES (${envelope.clientId}, ${envelope.mutationId}, echo_sync.next_position(),
                  ${JSON.stringify(encodeOutcome(outcome))}::jsonb)`
        return outcome
      })
    )

    return yield* Effect.gen(function* () {
      yield* registerClient(envelope.principalId, envelope.clientId)
      const recorded = yield* lookup(envelope)
      if (recorded !== undefined) return recorded
      return yield* execute.pipe(
        Effect.retry({ schedule: options?.conflictRetry ?? defaultConflictRetry, while: isConflict }),
        Effect.catchTag("SqlError", (error) =>
          // A concurrent submission of the same envelope committed first: its outcome is the outcome.
          error.reason._tag === "UniqueViolation" && error.reason.constraint === "mutations_pkey"
            ? Effect.flatMap(lookup(envelope), (outcome) =>
                outcome === undefined ? Effect.fail(error) : Effect.succeed(outcome)
              )
            : Effect.fail(error)
        )
      )
    }).pipe(Effect.catchTag("SqlError", (error) => Effect.fail(transient(envelope, error))))
  })

export interface BatchItem<R> {
  readonly mutationId: number
  readonly body: Effect.Effect<void, MutationRejected | SqlError, R>
}

export interface BatchResult {
  /** Outcomes for the processed prefix, in client order. */
  readonly processed: ReadonlyArray<{ readonly mutationId: number; readonly outcome: MutationOutcome }>
  /** The mutation the batch stopped at, unrecorded; the client resubmits from here. */
  readonly stopped: MutationTransient | null
}

/**
 * Batch at the HTTP level, commit at the mutation level: each mutation is its own transaction,
 * permanent failures are recorded and processing continues, the first transient failure stops
 * the batch.
 */
export const runBatch = <R>(
  principalId: string,
  clientId: string,
  items: ReadonlyArray<BatchItem<R>>,
  options?: LedgerOptions
): Effect.Effect<BatchResult, ClientForbidden, R | SqlClient> =>
  Effect.gen(function* () {
    const processed: Array<{ mutationId: number; outcome: MutationOutcome }> = []
    for (const item of items) {
      const result = yield* runMutation(
        { principalId, clientId, mutationId: item.mutationId },
        item.body,
        options
      ).pipe(Effect.catchTag("MutationTransient", (error) => Effect.succeed(error)))
      if (result instanceof MutationTransient) return { processed, stopped: result }
      processed.push({ mutationId: item.mutationId, outcome: result })
    }
    return { processed, stopped: null }
  })
