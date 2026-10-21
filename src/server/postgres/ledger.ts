import { Cause, Effect, Option, Schedule, Schema } from "effect"
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

/**
 * The envelope itself is invalid (`seq < 1`, DESIGN §6): a request-level rejection. Nothing is
 * executed or recorded. Without this, `seq ≤ 0` would fall under the default expiry floor 0 and
 * be answered `clientExpired` although it was never processed.
 */
export class MalformedEnvelope extends Schema.TaggedError<MalformedEnvelope>()("MalformedEnvelope", {
  clientId: Schema.String,
  mutationId: Schema.String,
  message: Schema.String
}) {}

const checkEnvelope = (clientId: string, mutationId: string, seq: bigint) =>
  seq < 1n
    ? Effect.fail(new MalformedEnvelope({ clientId, mutationId, message: `seq must be ≥ 1, got ${seq}` }))
    : Effect.void

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

type AttemptError = SqlError | ClientForbidden | MutationRejected

const isConflict = (error: unknown) =>
  isSqlError(error) && (error.reason._tag === "SerializationError" || error.reason._tag === "DeadlockError")

/**
 * A failed attempt that must not be retried, carrying its whole cause. `Effect.retry` looks only
 * at the first typed failure, so a compound cause (a conflict plus anything else) is wrapped
 * here before retry sees it, and classified afterwards as a whole.
 */
class Settled {
  readonly attempt: Cause.Cause<AttemptError>
  constructor(attempt: Cause.Cause<AttemptError>) {
    this.attempt = attempt
  }
}

/** Only a cause made solely of conflict failures stays retryable. */
const gateRetry = <A, R>(attempt: Effect.Effect<A, AttemptError, R>): Effect.Effect<A, AttemptError | Settled, R> =>
  Effect.catchCause(attempt, (cause): Effect.Effect<never, AttemptError | Settled> =>
    cause.reasons.length > 0 && cause.reasons.every((r) => Cause.isFailReason(r) && isConflict(r.error))
      ? Effect.failCause(cause)
      : Effect.fail(new Settled(cause))
  )

/** The body failed with only `MutationRejected`: a permanent, recordable outcome. */
const asRejection = (cause: Cause.Cause<MutationRejected | SqlError>): MutationOutcome | undefined => {
  const [first] = cause.reasons
  const onlyRejections = cause.reasons.every((r) => Cause.isFailReason(r) && r.error instanceof MutationRejected)
  return onlyRejections && first !== undefined && Cause.isFailReason(first) && first.error instanceof MutationRejected
    ? { _tag: "Rejected", reason: first.error.reason, message: first.error.message }
    : undefined
}

/**
 * Classifies the whole failure cause of a mutation attempt. Only a cause made entirely of SQL
 * failures (typed, or the pinned SqlClient's cleanup defects carrying a SqlError) and
 * `ClientForbidden` becomes a typed error. Anything else (a programmer defect, an interruption,
 * a stray typed error) is re-raised with every typed failure turned into a defect, so the whole
 * cause is preserved and no `catchTag` downstream can mistake it for a retryable transient.
 */
const classify = (
  envelope: Envelope,
  cause: Cause.Cause<SqlError | ClientForbidden | MutationRejected>
): Effect.Effect<never, MutationTransient | ClientForbidden> => {
  const expected = cause.reasons.every(
    (r) =>
      (Cause.isFailReason(r) && (isSqlError(r.error) || r.error instanceof ClientForbidden)) ||
      (Cause.isDieReason(r) && isSqlError(r.defect))
  )
  if (expected) {
    for (const r of cause.reasons) {
      if (Cause.isFailReason(r) && r.error instanceof ClientForbidden) return Effect.fail(r.error)
    }
    const sqlError = cause.reasons
      .map((r) => (Cause.isFailReason(r) ? r.error : Cause.isDieReason(r) ? r.defect : undefined))
      .find(isSqlError)
    if (sqlError !== undefined) return Effect.fail(transient(envelope, sqlError))
  }
  return Effect.failCause(
    Cause.fromReasons(cause.reasons.map((r) => (Cause.isFailReason(r) ? Cause.makeDieReason(r.error) : r)))
  )
}

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
): Effect.Effect<Processed, MutationTransient | ClientForbidden | MalformedEnvelope, R | SqlClient> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* checkEnvelope(envelope.clientId, envelope.mutationId, envelope.seq)
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
          // A rejection combined with anything else (e.g. a defect in a finalizer) is not a
          // rejection: it must not be ledgered as one.
          Effect.catchCause((cause) => {
            const rejection = asRejection(cause)
            return rejection === undefined ? Effect.failCause(cause) : Effect.succeed(rejection)
          })
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

    return yield* gateRetry(execute).pipe(
      Effect.retry({ schedule: options?.conflictRetry ?? defaultConflictRetry, while: (e) => !(e instanceof Settled) }),
      // Unwrap: a settled attempt keeps its whole cause; otherwise the conflicts ran out.
      Effect.catch((error): Effect.Effect<never, AttemptError> =>
        error instanceof Settled ? Effect.failCause(error.attempt) : Effect.fail(error)
      ),
      Effect.catchCause((cause) => classify(envelope, cause))
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
): Effect.Effect<BatchResult, ClientForbidden | MalformedEnvelope, R | SqlClient> =>
  Effect.gen(function* () {
    // Request-level: one malformed envelope rejects the push before anything executes.
    for (const item of items) yield* checkEnvelope(clientId, item.mutationId, item.seq)
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
