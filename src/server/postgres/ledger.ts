import * as PgTypes from "@effect/sql-pg/PgTypes"
import { Cause, Effect, Option, Schedule, Schema } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { isSqlError, type SqlError } from "effect/sql/SqlError"
import { Errors, Ids, Mutation, Protocol } from "echo-sync"
import { isConflictUnavailable } from "./rowStore.ts"

type MutationOutcome = Mutation.MutationOutcome
const MutationOutcomeJson = Schema.toCodecJson(Mutation.MutationOutcome)
const isRejection = Schema.is(Mutation.MutationRejection)

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
 * `seq < 1` (DESIGN §6) is refused at request level with core's `MalformedEnvelope`: nothing
 * executed or recorded. Without this, `seq ≤ 0` would fall under the default expiry floor 0 and
 * be answered `ClientExpired` although it was never processed. Core's `Seq` already refuses it
 * on the wire; this guard keeps the ledger's own input honest.
 */
const checkEnvelope = (clientId: string, mutationId: string, seq: bigint) =>
  seq < 1n
    ? Effect.fail(
        new Protocol.MalformedEnvelope({
          clientId: Ids.ClientId.make(clientId),
          mutationId: Ids.MutationId.make(mutationId),
          message: `seq must be ≥ 1, got ${seq}`
        })
      )
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
  /** Retry policy for conflicts and unique violations. Default: 4 retries, exponential from 10ms. */
  readonly conflictRetry?: Schedule.Schedule<unknown> | undefined
}

const defaultConflictRetry = Schedule.exponential("10 millis").pipe(Schedule.upTo({ times: 4 }))

const decodeOutcome = Schema.decodeUnknownEffect(MutationOutcomeJson)
const encodeOutcome = Schema.encodeSync(MutationOutcomeJson)

const transient = (envelope: Envelope, error: SqlError | Errors.StoreUnavailable) =>
  new MutationTransient({
    clientId: envelope.clientId,
    mutationId: envelope.mutationId,
    message: isSqlError(error) ? `${error.reason._tag}: ${error.message}` : error.message
  })

/** What a mutation body may fail with: core's rejections and transients, or raw SQL. */
export type BodyError = Mutation.MutationRejection | Errors.StoreUnavailable | SqlError

type AttemptError = BodyError | ClientForbidden

const isConflict = (error: unknown) =>
  (isSqlError(error) &&
    (error.reason._tag === "SerializationError" ||
      error.reason._tag === "DeadlockError" ||
      error.reason._tag === "UniqueViolation")) ||
  (error instanceof Errors.StoreUnavailable && isConflictUnavailable(error))

/** A reason that makes the attempt a defect (DESIGN §6 precedence: defect > transient > rejection). */
const isDefectReason = (r: Cause.Reason<unknown>) =>
  Cause.isInterruptReason(r) ||
  // The pinned SqlClient raises savepoint/rollback cleanup failures on a dead connection as
  // defects carrying a SqlError; those are connection failures, i.e. transient.
  (Cause.isDieReason(r) && !isSqlError(r.defect))

/**
 * A failed attempt with its whole cause. `Effect.retry` looks only at the first typed failure,
 * so every failed attempt is wrapped here and retry decides on `retryable`, computed over the
 * whole cause: no defect anywhere, and a conflict (`40001`, deadlock, unique violation) anywhere. A rejection in
 * the same cause does not block the retry: it was decided on a snapshot that could not commit.
 */
class FailedAttempt {
  readonly attempt: Cause.Cause<AttemptError>
  readonly retryable: boolean
  constructor(attempt: Cause.Cause<AttemptError>) {
    this.attempt = attempt
    this.retryable =
      !attempt.reasons.some(Cause.isDieReason) &&
      !attempt.reasons.some(isDefectReason) &&
      attempt.reasons.some((r) => Cause.isFailReason(r) && isConflict(r.error))
  }
}

const wrapAttempt = <A, R>(attempt: Effect.Effect<A, AttemptError, R>): Effect.Effect<A, FailedAttempt, R> =>
  Effect.catchCause(attempt, (cause) => Effect.fail(new FailedAttempt(cause)))

/** Only permanent failures may become an outcome; unique violations wait for retry exhaustion. */
const asRejection = (
  cause: Cause.Cause<AttemptError>,
  uniqueExhausted = false
): Mutation.MutationRejection | undefined => {
  const errors = cause.reasons.flatMap((r) => {
    if (!Cause.isFailReason(r)) return []
    const error = r.error
    if (
      isSqlError(error) &&
      !error.reason.isRetryable &&
      (error.reason._tag === "ConstraintError" || (uniqueExhausted && error.reason._tag === "UniqueViolation"))
    ) {
      return [
        new Mutation.ConstraintViolated({
          constraint: error.reason._tag === "UniqueViolation" ? error.reason.constraint : undefined,
          message: error.message
        })
      ]
    }
    return [error]
  })
  const [first] = errors
  return errors.length === cause.reasons.length &&
    first !== undefined &&
    isRejection(first) &&
    errors.every(isRejection)
    ? first
    : undefined
}

/**
 * Classifies the whole failure cause of a mutation attempt, with precedence defect > transient >
 * rejection (DESIGN §6):
 * - any defect or interruption ⇒ die with the full cause (typed failures turned into defects), so
 *   no `catchTag` downstream can mistake it for a retryable transient;
 * - otherwise any remaining SQL failure ⇒ `MutationTransient` (the transaction could not have committed, so
 *   a rejection in the same cause is not trustworthy and is not ledgered);
 * - otherwise `ClientForbidden`.
 * A cause made solely of rejections never gets here: the body turns it into a ledgered outcome.
 */
const classify = (
  envelope: Envelope,
  cause: Cause.Cause<AttemptError>
): Effect.Effect<never, MutationTransient | ClientForbidden> => {
  const die = Effect.failCause(
    Cause.fromReasons<never>(
      cause.reasons.map((r) =>
        Cause.isFailReason(r)
          ? Cause.makeDieReason(r.error)
          : Cause.isDieReason(r)
            ? r
            : Cause.makeInterruptReason(r.fiberId)
      )
    )
  )
  if (cause.reasons.some(isDefectReason)) return die
  const failure = cause.reasons
    .map((r) => (Cause.isFailReason(r) ? r.error : Cause.isDieReason(r) ? r.defect : undefined))
    .find((e) => isSqlError(e) || e instanceof Errors.StoreUnavailable)
  if (isSqlError(failure) || failure instanceof Errors.StoreUnavailable) {
    return Effect.fail(transient(envelope, failure))
  }
  for (const r of cause.reasons) {
    if (Cause.isFailReason(r) && r.error instanceof ClientForbidden) return Effect.fail(r.error)
  }
  return die
}

/**
 * Binds a client id to its principal on first sight and refuses it to any other principal, so
 * one principal can never read another's acks or replay another's mutation ids.
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
 * `registerClient` for the SERIALIZABLE ledger transaction: binds on first sight, then row-locks
 * the client until commit without modifying it. Concurrent submissions of one client serialize
 * on the lock, and because the row is only locked, the holder's commit does not by itself fail a
 * waiter with 40001. A duplicate may still be rejected by SSI (at the ledger's unique key or
 * earlier); its retry then replays the recorded outcome. Updating the row here (as
 * `last_seen_at` does) would add a write conflict on every replay and cascade retries through
 * the remaining waiters.
 */
const lockClient = Effect.fnUntraced(function* (principalId: string, clientId: string) {
  const sql = yield* SqlClient
  yield* sql`
    INSERT INTO echo_sync.clients (client_id, principal_id) VALUES (${clientId}, ${principalId})
    ON CONFLICT (client_id) DO NOTHING`
  const rows = yield* sql<{ principal_id: string; expired_through: bigint; expired_position: bigint }>`
    SELECT principal_id, expired_through, expired_position FROM echo_sync.clients
    WHERE client_id = ${clientId} FOR UPDATE`
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
 * so a duplicate waits and then sees the winner's row) → known identity ⇒ its recorded outcome,
 * or `Rejected{clientExpired}` at its own position if expired (whatever `seq` the attempt carries)
 * → unknown id with `seq ≤ expired_through` ⇒ `Rejected{clientExpired}` at `expired_position`,
 * body untouched → else body in a savepoint (a permanent failure discards its writes) → ledger row
 * whose position is allocated under the engine lock after the body's log entries → commit.
 * A mutation that writes nothing still gets an orderable position, so pull delivers its ack.
 */
export const runMutation = <R>(
  envelope: Envelope,
  body: Effect.Effect<void, BodyError, R>,
  options?: LedgerOptions
): Effect.Effect<Processed, MutationTransient | ClientForbidden | Protocol.MalformedEnvelope, R | SqlClient> =>
  Effect.gen(function* () {
    const sql = yield* SqlClient
    yield* checkEnvelope(envelope.clientId, envelope.mutationId, envelope.seq)
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService))) {
      return yield* Effect.die("runMutation must own its transaction; do not call it inside withTransaction")
    }

    let bodyUniqueRejection = false
    const execute = sql.withTransaction(
      Effect.gen(function* () {
        bodyUniqueRejection = false
        // SERIALIZABLE, first statement of every attempt: handlers read-modify-write through the
        // Store (patch, counters, query-then-insert), and under READ COMMITTED two clients'
        // concurrent mutations would both commit on one stale read (lost update). A conflict is
        // 40001, which retries the whole attempt (§6).
        yield* sql`SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`
        const client = yield* lockClient(envelope.principalId, envelope.clientId)
        const recorded = yield* sql<{ outcome: unknown; position: bigint }>`
          SELECT outcome, position FROM echo_sync.mutations
          WHERE client_id = ${envelope.clientId} AND mutation_id = ${envelope.mutationId}`
        const row = recorded[0]
        // An expired row (outcome cleared, identity kept): processed before, outcome reclaimed.
        // Answered at its own position whatever seq this attempt carries, so a retry enqueued
        // under a fresh seq after the client forgot the id is never executed again (DESIGN §16.5).
        if (row !== undefined && row.outcome === null) {
          const expired: Processed = {
            outcome: {
              _tag: "Rejected",
              error: new Mutation.ClientExpired({ mutationId: Ids.MutationId.make(envelope.mutationId) })
            },
            position: row.position
          }
          return expired
        }
        if (row !== undefined) {
          const replay: Processed = { outcome: yield* Effect.orDie(decodeOutcome(row.outcome)), position: row.position }
          return replay
        }
        // Every seq ≤ the floor was processed and kept its identity, so an unknown id here is a
        // different id under an already-processed seq: the floor answers it (DESIGN §16.5), never
        // the body. A known id was answered above at its own position, whatever its seq.
        if (envelope.seq <= client.expired_through) {
          const expired: Processed = {
            outcome: {
              _tag: "Rejected",
              error: new Mutation.ClientExpired({ mutationId: Ids.MutationId.make(envelope.mutationId) })
            },
            position: client.expired_position
          }
          return expired
        }

        const outcome: MutationOutcome = yield* sql.withTransaction(body).pipe(
          Effect.as<MutationOutcome>({ _tag: "Applied" }),
          // A rejection combined with anything else (e.g. a defect in a finalizer) is not a
          // rejection: it must not be ledgered as one.
          Effect.catchCause((cause) => {
            const rejection = asRejection(cause)
            bodyUniqueRejection = rejection === undefined && asRejection(cause, true) !== undefined
            return rejection === undefined
              ? Effect.failCause(cause)
              : Effect.succeed<MutationOutcome>({ _tag: "Rejected", error: rejection })
          })
        )
        const inserted = yield* sql<{ position: bigint }>`
          INSERT INTO echo_sync.mutations (client_id, mutation_id, seq, position, outcome)
          VALUES (${envelope.clientId}, ${envelope.mutationId}, ${envelope.seq}, echo_sync.next_position(),
                  ${PgTypes.jsonb(encodeOutcome(outcome))})
          RETURNING position`
        const position = inserted[0]?.position
        if (position === undefined) return yield* Effect.die("INSERT … RETURNING returned no row")
        const processed: Processed = { outcome, position }
        return processed
      })
    )

    return yield* wrapAttempt(execute).pipe(
      Effect.retry({ schedule: options?.conflictRetry ?? defaultConflictRetry, while: (e) => e.retryable }),
      Effect.catch(
        (e): Effect.Effect<Processed, MutationTransient | ClientForbidden | Protocol.MalformedEnvelope, SqlClient> => {
          const rejection = bodyUniqueRejection ? asRejection(e.attempt, true) : undefined
          // The unique-failing transaction rolled back. Record its permanent outcome in a fresh
          // transaction, with the normal identity check and conflict retry, but never rerun its body.
          return rejection === undefined
            ? classify(envelope, e.attempt)
            : runMutation(envelope, Effect.fail(rejection), options)
        }
      )
    )
  })

export interface BatchItem<R> {
  readonly mutationId: string
  readonly seq: bigint
  readonly body: Effect.Effect<void, BodyError, R>
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
): Effect.Effect<BatchResult, ClientForbidden | Protocol.MalformedEnvelope, R | SqlClient> =>
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
