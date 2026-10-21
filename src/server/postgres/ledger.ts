import { Effect, Option, Schedule, Schema } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { isSqlError, type SqlError } from "effect/sql/SqlError"

/**
 * A permanent failure of a mutation body (DESIGN.md §6). Recorded in the ledger as the
 * mutation's outcome; the batch continues.
 */
export class MutationRejected extends Schema.TaggedError<MutationRejected>()("MutationRejected", {
  reason: Schema.Literals(["decode", "forbidden", "domain"]),
  message: Schema.String
}) {}

/**
 * The result of a processed mutation. Identical on every resubmission. `clientExpired` is never
 * recorded: it is answered from the client's expiry floor (§16.5).
 */
export const MutationOutcome = Schema.Union([
  Schema.TaggedStruct("Applied", {}),
  Schema.TaggedStruct("Rejected", {
    reason: Schema.Literals(["decode", "forbidden", "domain", "clientExpired"]),
    message: Schema.String
  })
])
export type MutationOutcome = typeof MutationOutcome.Type

/** A push outcome and the ledger position it was recorded at (§6). */
export interface Processed {
  readonly outcome: MutationOutcome
  readonly position: bigint
}

/**
 * The mutation's fate is unknown to the server's caller: serialization conflict after retries,
 * lost connection, unclassified database error, or a COMMIT whose reply was lost (in which case
 * it may in fact be recorded). The client resubmits from this mutation; dedup makes that safe.
 */
export class MutationTransient extends Schema.TaggedError<MutationTransient>()("MutationTransient", {
  clientId: Schema.String,
  mutationId: Schema.String,
  message: Schema.String
}) {}

/** The client id is registered to a different principal. */
export class ClientForbidden extends Schema.TaggedError<ClientForbidden>()("ClientForbidden", {
  clientId: Schema.String
}) {}

export interface Envelope {
  readonly principalId: string
  readonly clientId: string
  /** Idempotency key, unordered (e.g. a UUID). */
  readonly mutationId: string
  /** Client outbox sequence: per-client monotone, gap-free, starting at 1. */
  readonly seq: bigint
}

export interface LedgerOptions {
  /** Retry policy for serialization failures and deadlocks. Default: 4 retries, exponential from 10ms. */
  readonly conflictRetry?: Schedule.Schedule<unknown> | undefined
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

const isConflict = (error: SqlError | ClientForbidden) =>
  error._tag === "SqlError" && (error.reason._tag === "SerializationError" || error.reason._tag === "DeadlockError")

/**
 * Binds a client id to its principal on first sight and refuses it to any other principal, so
 * one principal can never read another's acks or replay another's mutation ids. Inside a
 * transaction the upsert also row-locks the client until commit.
 */
export const registerClient = Effect.fnUntraced(function* (principalId: string, clientId: string) {
  const sql = yield* SqlClient
  const rows = yield* sql<{ principal_id: string; expired_through: bigint; expired_position: bigint }>`
    INSERT INTO echo_sync.clients (client_id, principal_id) VALUES (${clientId}, ${principalId})
    ON CONFLICT (client_id) DO UPDATE SET last_seen_at = now()
    RETURNING principal_id, expired_through, expired_position`
  const row = rows[0]
  if (row === undefined || row.principal_id !== principalId) {
    return yield* new ClientForbidden({ clientId })
  }
  return row
})

/**
 * Executes one mutation at most once with committed effects (DESIGN.md §6), in a transaction it
 * owns: calling it inside an ambient `withTransaction` would turn it into a savepoint whose
 * outcome is not committed when returned, so that is a defect.
 *
 * One transaction: lock the client row (concurrent submissions of one client serialize here,
 * so a duplicate waits and then sees the winner's row) → `seq ≤ expired_through` ⇒
 * `Rejected{clientExpired}` at `expired_position`, body untouched → known identity ⇒ recorded
 * outcome → else body in a savepoint (a permanent failure discards its writes) → ledger row
 * whose position is allocated under the engine lock after the body's log entries → commit.
 * A mutation that writes nothing still gets an orderable position, so pull delivers its ack.
 */
export const runMutation = <R>(
  envelope: Envelope,
  body: Effect.Effect<void, MutationRejected | SqlError, R>,
  options?: LedgerOptions
): Effect.Effect<Processed, MutationTransient | ClientForbidden, R | SqlClient> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService))) {
      return yield* Effect.die("runMutation must own its transaction; do not call it inside withTransaction")
    }

    const execute = sql.withTransaction(
      Effect.gen(function* () {
        const client = yield* registerClient(envelope.principalId, envelope.clientId)
        if (envelope.seq <= client.expired_through) {
          const expired: Processed = {
            outcome: { _tag: "Rejected", reason: "clientExpired", message: "ledger expired for this seq" },
            position: client.expired_position
          }
          return expired
        }
        const recorded = yield* sql<{ outcome: unknown; position: bigint }>`
          SELECT outcome, position FROM echo_sync.mutations
          WHERE client_id = ${envelope.clientId} AND mutation_id = ${envelope.mutationId}`
        const row = recorded[0]
        if (row !== undefined) {
          const replay: Processed = { outcome: yield* Effect.orDie(decodeOutcome(row.outcome)), position: row.position }
          return replay
        }

        const outcome: MutationOutcome = yield* sql.withTransaction(body).pipe(
          Effect.as<MutationOutcome>({ _tag: "Applied" }),
          Effect.catchTag("MutationRejected", (e) =>
            Effect.succeed<MutationOutcome>({ _tag: "Rejected", reason: e.reason, message: e.message })
          )
        )
        const inserted = yield* sql<{ position: bigint }>`
          INSERT INTO echo_sync.mutations (client_id, mutation_id, seq, position, outcome)
          VALUES (${envelope.clientId}, ${envelope.mutationId}, ${envelope.seq}, echo_sync.next_position(),
                  ${JSON.stringify(encodeOutcome(outcome))}::jsonb)
          RETURNING position`
        const position = inserted[0]?.position
        if (position === undefined) return yield* Effect.die("INSERT … RETURNING returned no row")
        const processed: Processed = { outcome, position }
        return processed
      })
    )

    return yield* execute.pipe(
      Effect.retry({ schedule: options?.conflictRetry ?? defaultConflictRetry, while: isConflict }),
      Effect.catchTag("SqlError", (error) => Effect.fail(transient(envelope, error))),
      // The pinned SqlClient turns savepoint/rollback cleanup failures (a dead connection) into
      // defects carrying the SqlError; they are as transient as the failure that caused them.
      Effect.catchDefect((defect) =>
        isSqlError(defect) ? Effect.fail(transient(envelope, defect)) : Effect.die(defect)
      )
    )
  })

export interface BatchItem<R> {
  readonly mutationId: string
  readonly seq: bigint
  readonly body: Effect.Effect<void, MutationRejected | SqlError, R>
}

export interface BatchResult {
  /** Outcomes for the processed prefix, in client order. */
  readonly processed: ReadonlyArray<Processed & { readonly mutationId: string }>
  /** The mutation the batch stopped at, outcome unknown; the client resubmits from here. */
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
    const processed: Array<Processed & { mutationId: string }> = []
    for (const item of items) {
      const result = yield* runMutation(
        { principalId, clientId, mutationId: item.mutationId, seq: item.seq },
        item.body,
        options
      ).pipe(Effect.catchTag("MutationTransient", (error) => Effect.succeed(error)))
      if (result instanceof MutationTransient) return { processed, stopped: result }
      processed.push({ mutationId: item.mutationId, ...result })
    }
    return { processed, stopped: null }
  })
