/**
 * Deterministic simulation of N clients against the reference server (SPEC §17).
 *
 * A seeded PRNG drives a schedule of local mutations, pushes (lost requests, transient stops
 * at random positions, lost responses), paginated pulls (failing between pages), server
 * out-of-band writes, grant/revoke/move, client reload (memory lost, disk kept), going
 * offline, log GC (forces resnapshot), optional ledger GC, clock skew, enqueue retries after
 * release (answered by released-row markers, or ended by the INV-PUSH-6 position backstop once the
 * markers were pruned), crashes in the middle of a storage transaction followed by a reload,
 * and local mutations/pushes committing while a pull is suspended on the network. Time is the
 * ambient `Clock` (run under `TestClock`), skewed per client. Intents count as committed from
 * `mutate`'s return value, independently of the outbox.
 *
 * Invariants checked after every client step:
 * - replica = replay(pending, server state at the installed cursor restricted to the client's
 *   readable scopes). This one equation rules out flicker (an optimistic effect vanishing
 *   before its authoritative effect is installed), double optimistic application (replaying
 *   over a row that already contains the mutation), torn page sets, and stale rows.
 * - the outbox never holds a mutation whose ledger position is ≤ the installed cursor;
 * - no mutation is ever executed twice on the server.
 * After quiescence: every locally committed mutation executed exactly once or was exhausted
 * unexecuted with exactly one notice; outboxes empty, replicas equal the server restricted
 * to readable scopes, and each server rejection surfaced as exactly one notice.
 */
import { Array as Arr, Clock, Deferred, Effect, Fiber, Option, Order, Schema } from "effect"
import { TestClock } from "effect/testing"
import { RpcClientError } from "effect/rpc"
import * as Engine from "../Engine.ts"
import { type PullFailure, PullTransportError, type PushFailure, PushTransportError } from "../SyncFailure.ts"
import { Ids, ScopeGrants } from "../../../core/index.ts"
import type { AccessScope, Mutation, Protocol, SyncTable } from "../../../core/index.ts"
import * as MemoryStore from "../../../core/testing/MemoryStore.ts"
import * as MemorySyncStorage from "./MemorySyncStorage.ts"
import * as RefServer from "../../../core/testing/RefServer.ts"
import {
  ANYONE,
  Counters,
  Notes,
  clientRegistry,
  grantRow,
  mutations,
  principal as asPrincipal,
  scope
} from "../../../core/testing/SimApp.ts"

type EncodedRow = SyncTable.EncodedRow
type MutationEnvelope = Mutation.MutationEnvelope
type PullDone = typeof Protocol.PullDone.Type
type PullResponse = Protocol.PullResponse
type Role = AccessScope.Role

const lost = (during: string) =>
  new RpcClientError.RpcClientError({
    reason: new RpcClientError.RpcClientDefect({ message: `lost ${during}`, cause: null })
  })
/** A lost push request or response: what the transport reports when no usable RPC result arrived. */
export const pushLost = (during: string) => new PushTransportError({ reason: lost(during) })
/** A lost pull request or response. */
export const pullLost = (during: string) => new PullTransportError({ reason: lost(during) })

export class InvariantViolated extends Schema.TaggedError<InvariantViolated>()("InvariantViolated", {
  seed: Schema.Number,
  step: Schema.Number,
  invariant: Schema.String,
  detail: Schema.String,
  trace: Schema.Array(Schema.String)
}) {
  override get message() {
    return `seed=${this.seed} step=${this.step}: ${this.invariant}: ${this.detail}\n  last events:\n    ${this.trace.join("\n    ")}`
  }
}

export interface Options {
  readonly seed: number
  readonly steps: number
  readonly restores?: boolean
  readonly pageSize?: number
  /** Deliberately wrong client that applies `PullPage`s as they arrive (proof the check bites). */
  readonly pullMode?: "staged" | "incremental"
  /**
   * Deliberately wrong seq allocator (proof the duplicate-seq check bites): next seq from the
   * highest pending entry, so a seq is reused once its holder is released.
   */
  readonly seqFromPending?: boolean
  /** GC the ledger together with the log ("ledger GC'd while client offline"). */
  readonly ledgerGc?: boolean
  /** One mutation is always answered transient, including during quiescence. */
  readonly persistentTransient?: boolean
}

export interface Stats {
  readonly restores: number
  readonly mutations: number
  readonly resnapshots: number
  readonly reloadsMidSet: number
  readonly notices: number
  readonly transientStops: number
  readonly exhausted: number
  readonly lostResponses: number
  /** Duplicates (pruned-marker retries) released by a push response's position (INV-PUSH-6 corollary). */
  readonly backstopReleases: number
  /** Enqueue retries after release whose marker was pruned that enqueued a duplicate. */
  readonly lateRetries: number
  /** Enqueue retries after release answered by the released-row marker (no new intent). */
  readonly markerHits: number
  /** Server writes to a table absent from the clients' registry. */
  readonly unknownTableWrites: number
  /** Crashes that actually happened in the middle of a storage transaction. */
  readonly crashes: number
  /** Local mutations/pushes run while a pull was suspended on the network. */
  readonly interleaved: number
  /** Pending mutations released by an expired ledger row rather than an ack. */
  readonly expired: number
  /** Schema rebuilds (replica emptied, cursor nulled, pending undo emptied). */
  readonly rebuilds: number
  /** Null-cursor installs after a rebuild that had pending entries to replay. */
  readonly rebuildInstalls: number
}

/** mulberry32: small, fast, good enough for schedules. */
const prng = (seed: number) => {
  let a = seed >>> 0
  const next = () => {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
  return {
    chance: (p: number) => next() < p,
    int: (n: number) => Math.floor(next() * n),
    pick: <A>(xs: ReadonlyArray<A>): Option.Option<A> => Option.fromNullishOr(xs[Math.floor(next() * xs.length)])
  }
}

const canonical = (tables: MemoryStore.Tables): string => {
  const lines: Array<string> = []
  for (const [table, rows] of tables) {
    for (const row of rows.values()) {
      const sorted = Arr.sort(Object.keys(row), Order.String).map((k) => `${k}=${JSON.stringify(row[k])}`)
      lines.push(`${table}/${row["id"]}{${sorted.join(",")}}`)
    }
  }
  return Arr.sort(lines, Order.String).join("\n")
}

const diff = (actual: string, expected: string) => {
  const a = new Set(actual.split("\n"))
  const e = new Set(expected.split("\n"))
  const extra = [...a].filter((l) => l !== "" && !e.has(l))
  const missing = [...e].filter((l) => l !== "" && !a.has(l))
  return `replica has [${extra.join("; ")}] but should have [${missing.join("; ")}]`
}

/**
 * Rows of `tables` the client should hold: known tables only (SPEC §7 skips the rest), in
 * scopes `principal` can read, judged by the grant rows in `tables`.
 */
const view = (tables: MemoryStore.Tables, principal: AccessScope.PrincipalId) =>
  Effect.sync(() => {
    const readable = new Set<string>()
    for (const row of tables.get(ScopeGrants.ScopeGrants.name)?.values() ?? []) {
      const who = row["principal_id"]
      if (who === principal || who === ANYONE) readable.add(String(row["access_scope_id"]))
    }
    const out: MemoryStore.Tables = new Map()
    for (const [table, rows] of tables) {
      if (Option.isNone(clientRegistry.get(table))) continue
      const kept = new Map<string, EncodedRow>()
      for (const [id, row] of rows) if (readable.has(String(row["access_scope_id"]))) kept.set(id, row)
      if (kept.size > 0) out.set(table, kept)
    }
    return out
  })

/** An out-of-band grant (or revoke, `role` none) with core's derived grant id. */
const grantWrite = (scopeId: string, who: string, role: Option.Option<Role>): RefServer.DirectWrite => ({
  table: ScopeGrants.ScopeGrants,
  id: ScopeGrants.grantId(scope(scopeId), asPrincipal(who)),
  row: Option.map(role, (r) => grantRow(scopeId, who, r))
})

const skewed = (base: Clock.Clock, ms: number): Clock.Clock => ({
  currentTimeMillisUnsafe: () => base.currentTimeMillisUnsafe() + ms,
  currentTimeMillis: Effect.map(base.currentTimeMillis, (t) => t + ms),
  currentTimeNanosUnsafe: () => base.currentTimeNanosUnsafe() + BigInt(ms) * 1_000_000n,
  currentTimeNanos: Effect.map(base.currentTimeNanos, (t) => t + BigInt(ms) * 1_000_000n),
  monotonicTimeNanosUnsafe: () => base.monotonicTimeNanosUnsafe(),
  monotonicTimeNanos: base.monotonicTimeNanos,
  sleep: (duration) => base.sleep(duration)
})

const dupKey = (clientId: string, seq: number) => `${clientId}#${seq}`

const SCOPES = ["s0", "s1", "s2", "s3"]
const PRINCIPALS = ["alice", "bob"]
const CLIENTS: ReadonlyArray<readonly [Ids.ClientId, AccessScope.PrincipalId]> = [
  [Ids.ClientId.make("c1"), asPrincipal("alice")],
  [Ids.ClientId.make("c2"), asPrincipal("alice")],
  [Ids.ClientId.make("c3"), asPrincipal("bob")]
]
const ROLES: ReadonlyArray<Role> = ["reader", "writer", "owner"]

export const run = (options: Options) =>
  Effect.gen(function* () {
    const rng = prng(options.seed)
    const server = RefServer.make({ pageSize: options.pageSize ?? 3, epochs: options.restores === true })
    const trace: Array<string> = []
    let step = 0
    let faults = true
    let persistent: { readonly clientId: Ids.ClientId; readonly mutationId: Ids.MutationId } | undefined
    const stats = {
      restores: 0,
      mutations: 0,
      resnapshots: 0,
      reloadsMidSet: 0,
      notices: 0,
      transientStops: 0,
      exhausted: 0,
      lostResponses: 0,
      expired: 0,
      lateRetries: 0,
      markerHits: 0,
      backstopReleases: 0,
      unknownTableWrites: 0,
      crashes: 0,
      interleaved: 0,
      rebuilds: 0,
      rebuildInstalls: 0
    }
    const committed: Array<MutationEnvelope> = []
    const ids: Array<string> = []

    const note = (event: string) => {
      trace.push(`#${step} ${event}`)
      if (trace.length > 25) trace.shift()
    }
    const violated = (invariant: string, detail: string) =>
      new InvariantViolated({ seed: options.seed, step, invariant, detail, trace: [...trace] })

    // Initial grants: a global catalog scope, private scopes, and a shared one.
    yield* server.writeDirect([
      grantWrite("s0", ANYONE, Option.some("reader")),
      grantWrite("s0", "alice", Option.some("owner")),
      grantWrite("s1", "alice", Option.some("owner")),
      grantWrite("s2", "bob", Option.some("owner")),
      grantWrite("s3", "alice", Option.some("writer")),
      grantWrite("s3", "bob", Option.some("writer"))
    ])

    const clients = yield* Effect.forEach(CLIENTS, ([clientId, principal]) =>
      Effect.gen(function* () {
        server.register(clientId, principal)
        const disk = MemorySyncStorage.makeDisk()
        const reference = MemorySyncStorage.make(disk)
        const storage: Engine.SyncStorage =
          options.seqFromPending === true
            ? { ...reference, nextSeq: Effect.sync(() => Ids.Seq.make((disk.outbox.at(-1)?.envelope.seq ?? 0) + 1)) }
            : reference
        // Set by the interleaving step: the next pull request announces itself and waits.
        const ctl: { gate: Option.Option<{ arrived: Deferred.Deferred<void>; release: Deferred.Deferred<void> }> } = {
          gate: Option.none()
        }
        const transport: Engine.Transport = {
          push: (request) =>
            Effect.gen(function* () {
              if (faults && rng.chance(0.1)) return yield* pushLost("push request")
              if (options.persistentTransient === true && persistent === undefined) {
                persistent = { clientId, mutationId: request.mutations[0].mutationId }
              }
              const blockedAt = request.mutations.findIndex(
                (entry) => persistent?.clientId === clientId && persistent.mutationId === entry.mutationId
              )
              const randomStop =
                faults && rng.chance(0.15) ? rng.int(request.mutations.length) : request.mutations.length
              const stop = blockedAt < 0 ? randomStop : Math.min(blockedAt, randomStop)
              const stopAt = stop < request.mutations.length ? Option.some(stop) : Option.none()
              const response = yield* server.push(request, stopAt)
              if (response.results.length < request.mutations.length) stats.transientStops++
              if (faults && rng.chance(0.15)) {
                stats.lostResponses++
                return yield* pushLost("push response")
              }
              return response
            }),
          pull: (request) =>
            Effect.gen(function* () {
              if (Option.isSome(ctl.gate)) {
                const { arrived, release } = ctl.gate.value
                ctl.gate = Option.none()
                yield* Deferred.succeed(arrived, undefined)
                yield* Deferred.await(release)
              }
              if (faults && rng.chance(0.15)) return yield* pullLost("pull request")
              const response = yield* server.pull(request).pipe(
                Effect.tapError(() =>
                  Effect.sync(() => {
                    stats.resnapshots++
                  })
                )
              )
              if (faults && rng.chance(0.1)) return yield* pullLost("pull response")
              return response
            })
        }
        const client = {
          clientId,
          principal,
          disk,
          ctl,
          storage,
          transport,
          online: true,
          skew: 0,
          /**
           * Set by a schema rebuild to the last seq allocated before it: those entries' optimistic
           * effects are invisible until the null-cursor set installs (INV-CLI-2).
           */
          rebuiltThrough: Option.none<{ readonly seq: number; readonly pending: number }>(),
          engine: yield* Engine.make({
            clientId,
            actor: principal,
            storage,
            registry: clientRegistry,
            mutations,
            transport,
            pushBatchSize: 4
          })
        }
        return client
      })
    )
    type Client = (typeof clients)[number]
    let backup = server.checkpoint()
    const lostReleased = new Set<string>()

    /** The deliberately wrong client: applies each `PullPage` as it arrives. */
    const incrementalPull = (client: Client) =>
      Effect.gen(function* () {
        const { storage, transport } = client
        const context = { storage, mutations, actor: client.principal, registry: clientRegistry }
        while (true) {
          const meta = client.disk.meta
          const from = meta.resnapshot ? null : meta.cursor
          const continuation = Option.fromNullishOr(client.disk.staged.at(-1)?.continuation)
          const response = yield* transport.pull({ clientId: client.clientId, cursor: from, continuation }).pipe(
            Effect.map(Option.some<PullResponse>),
            Effect.catchTag("ResnapshotRequired", () =>
              storage
                .transaction(
                  Effect.andThen(storage.clearStaged, storage.setMeta({ ...client.disk.meta, resnapshot: true }))
                )
                .pipe(Effect.as(Option.none<PullResponse>()))
            )
          )
          if (Option.isNone(response)) continue
          const page = response.value
          if (page._tag === "PullDone" || from === null) {
            if (page._tag === "PullPage") {
              yield* storage.transaction(storage.stage(page))
              continue
            }
            const done: PullDone = page
            return yield* storage.transaction(Engine.applyPageSet(context, client.disk.staged, done))
          }
          const early: PullDone = {
            _tag: "PullDone",
            changes: page.changes,
            acks: page.acks,
            nextCursor: from,
            expiredThrough: Ids.SeqFloor.make(0)
          }
          yield* storage.transaction(
            Effect.gen(function* () {
              yield* Engine.applyPageSet(context, [], early)
              yield* storage.stage({ ...page, changes: [], acks: [] })
            })
          )
        }
      })

    const expectedReplica = (client: Client) =>
      Effect.gen(function* () {
        const cursor = client.disk.meta.cursor
        const snapshot = cursor === null ? new Map() : server.historyAt(client.disk.meta.epoch, cursor)
        if (snapshot === undefined) return yield* violated("cursor names a server commit", `${cursor}`)
        const store = MemoryStore.make(yield* view(snapshot, client.principal))
        const hiddenThrough = Option.match(client.rebuiltThrough, { onNone: () => 0, onSome: (r) => r.seq })
        for (const entry of client.disk.outbox) {
          if (entry.envelope.seq <= hiddenThrough) continue
          const result = yield* Effect.orDie(
            Engine.runHandler(store, mutations, client.principal, entry.envelope, entry.createdAt)
          )
          if (Option.isSome(result.rejection)) yield* Engine.restore(store, result.undo)
        }
        return canonical(store.tables())
      })

    const checkClient = (client: Client) =>
      Effect.gen(function* () {
        // The null-cursor set installed: from here every pending entry must be replayed again.
        if (Option.isSome(client.rebuiltThrough) && client.disk.meta.cursor !== null) {
          if (client.rebuiltThrough.value.pending > 0) stats.rebuildInstalls++
          client.rebuiltThrough = Option.none()
        }
        const expected = yield* expectedReplica(client)
        const actual = canonical(client.disk.replica.tables())
        if (actual !== expected) {
          return yield* violated(`${client.clientId} replica = replay(pending, server@cursor)`, diff(actual, expected))
        }
        const { cursor, pushedThrough } = client.disk.meta
        for (const entry of client.disk.outbox) {
          // Once the client has the outcome (seq ≤ pushedThrough), an outcome covered by the
          // installed cursor must already have released the entry: by the pull that acked it,
          // or by the push response itself when no pull can ever ack it (INV-PUSH-6 corollary).
          if (entry.envelope.seq > pushedThrough) continue
          const ledger = server.ledgerEntry(client.clientId, entry.envelope.mutationId)
          if (
            (options.restores !== true || client.disk.meta.epoch === server.epoch()) &&
            cursor !== null &&
            Option.isSome(ledger) &&
            ledger.value.position <= cursor
          ) {
            return yield* violated(
              "pending set never holds an acked mutation",
              `${client.clientId}#${entry.envelope.mutationId}`
            )
          }
        }
        const seen = new Set<number>()
        for (const notice of client.disk.notices) {
          if (seen.has(notice.envelope.seq)) {
            return yield* violated(
              "a rejection becomes a notice once",
              `${client.clientId}#${notice.envelope.mutationId}`
            )
          }
          seen.add(notice.envelope.seq)
        }
        for (const [key, count] of server.executions) {
          if (count > 1) return yield* violated("no intent applied twice on the server", `${key} executed ${count}×`)
        }
      })

    /** Network failures and local rejections are normal outcomes of a step. */
    const ignoreExpected = <A, E, R>(self: Effect.Effect<A, E, R>) =>
      self.pipe(
        Effect.asVoid,
        Effect.catch((error) => {
          if (error instanceof PushTransportError || error instanceof PullTransportError) {
            return Effect.sync(() => note(`  ↳ ${error._tag}`))
          }
          if (error instanceof Engine.LocallyRejected) return Effect.sync(() => note(`  ↳ ${error._tag}`))
          return Effect.fail(error)
        })
      )

    const pull = (client: Client) => (options.pullMode === "incremental" ? incrementalPull(client) : client.engine.pull)

    const withClock = <A, E, R>(client: Client, self: Effect.Effect<A, E, R>) =>
      Effect.clockWith((clock) => Effect.provideService(self, Clock.Clock, skewed(clock, client.skew)))

    const randomMutation = (client: Client) => {
      // Mostly rows this client can see (so most intents apply locally), sometimes any id.
      const visible = [...(client.disk.replica.tables().get(Counters.name)?.keys() ?? [])]
      const pool = visible.length > 0 && rng.chance(0.85) ? visible : ids
      const id = rng.pick(pool)
      const other = rng.pick(pool)
      const target = rng.pick(SCOPES)
      const kind = rng.int(10)
      if (kind < 2 || Option.isNone(id) || Option.isNone(other) || Option.isNone(target)) {
        const fresh = `${client.clientId}-${ids.length}`
        ids.push(fresh)
        return { name: "create", args: { id: fresh, scope: Option.getOrElse(target, () => "s1") } }
      }
      if (kind < 6) return { name: "increment", args: { id: id.value, by: 1 + rng.int(5) } }
      if (kind < 8) return { name: "transfer", args: { from: id.value, to: other.value, amount: 1 + rng.int(3) } }
      if (kind < 9) return { name: "move", args: { id: id.value, scope: target.value } }
      return { name: "remove", args: { id: id.value } }
    }

    const serverWrite = Effect.gen(function* () {
      const scopeId = Option.getOrElse(rng.pick(SCOPES), () => "s0")
      const principal = Option.getOrElse(rng.pick([...PRINCIPALS, ANYONE]), () => ANYONE)
      const kind = rng.int(5)
      if (kind === 4) {
        // A table added by a newer server deploy: clients must skip it and keep syncing.
        stats.unknownTableWrites++
        const id = `n${rng.int(8)}`
        note(`server: write ${Notes.name}/${id} in ${scopeId} (unknown to clients)`)
        return yield* server.writeDirect([
          {
            table: Notes,
            id,
            row: rng.chance(0.8) ? Option.some({ id, access_scope_id: scopeId, body: `b${step}` }) : Option.none()
          }
        ])
      }
      if (kind === 0) {
        const role = Option.getOrElse(rng.pick(ROLES), (): Role => "reader")
        note(`server: grant ${principal} ${role} on ${scopeId}`)
        return yield* server.writeDirect([grantWrite(scopeId, principal, Option.some(role))])
      }
      if (kind === 1) {
        note(`server: revoke ${principal} on ${scopeId}`)
        return yield* server.writeDirect([grantWrite(scopeId, principal, Option.none())])
      }
      const id = rng.pick(ids)
      if (Option.isNone(id)) return
      const row = yield* server.store.read(Counters, id.value)
      if (Option.isNone(row)) return
      const value = Number(row.value["value"])
      note(`server: out-of-band ${kind === 2 ? "bump" : "delete"} ${id.value}`)
      yield* server.writeDirect([
        {
          table: Counters,
          id: id.value,
          row: kind === 2 ? Option.some({ ...row.value, value: value + 100 }) : Option.none()
        }
      ])
    })

    /**
     * Records an intent the caller was told is saved, independently of the outbox. A seq already
     * recorded for a different mutationId is a reused seq (INV-CLI-5: never reused).
     */
    const record = (envelope: MutationEnvelope) =>
      Effect.gen(function* () {
        const same = committed.find((e) => e.clientId === envelope.clientId && e.seq === envelope.seq)
        if (same === undefined) {
          committed.push(envelope)
          return
        }
        if (same.mutationId !== envelope.mutationId) {
          return yield* violated(
            "a seq is never reused",
            `${envelope.clientId} seq ${envelope.seq}: ${same.mutationId} and ${envelope.mutationId}`
          )
        }
      })

    /** Duplicates enqueued after their original's marker was pruned, by clientId#seq. */
    const duplicates = new Set<string>()
    /** Runs a push and counts duplicates its response released (only the backstop can). */
    const countingBackstop = <A, E, R>(client: Client, self: Effect.Effect<A, E, R>) =>
      Effect.gen(function* () {
        const pendingBefore = client.disk.outbox
          .map((p) => dupKey(client.clientId, p.envelope.seq))
          .filter((key) => duplicates.has(key))
        const result = yield* self
        const pendingAfter = new Set(client.disk.outbox.map((p) => dupKey(client.clientId, p.envelope.seq)))
        stats.backstopReleases += pendingBefore.filter((key) => !pendingAfter.has(key)).length
        return result
      })

    const reload = (client: Client) =>
      Effect.gen(function* () {
        client.engine = yield* Engine.make({
          clientId: client.clientId,
          actor: client.principal,
          storage: client.storage,
          registry: clientRegistry,
          mutations,
          transport: client.transport,
          pushBatchSize: 4
        })
      })

    const mutateStep = (client: Client) =>
      Effect.gen(function* () {
        // Enqueue retried after its original was released (a handover retry landing late).
        const releasedHere = committed.filter(
          (e) => e.clientId === client.clientId && !client.disk.outbox.some((p) => p.envelope.seq === e.seq)
        )
        const late = rng.pick(releasedHere)
        if (Option.isSome(late) && rng.chance(0.08)) {
          const original = late.value
          const marker = client.disk.released.get(original.mutationId)
          if (marker !== undefined) {
            // INV-CLI-4: the marker answers with the seq it records; no handler, no new intent.
            stats.markerHits++
            note(`${client.clientId}: retry enqueue of released ${original.mutationId} (marker)`)
            const lastSeq = client.disk.lastSeq
            const again = yield* client.engine.mutate(original.mutationId, original.name, original.args)
            if (again.seq !== marker.seq || client.disk.lastSeq !== lastSeq) {
              return yield* violated("enqueue of a released id adds no intent", original.mutationId)
            }
            return
          }
          // Marker pruned: the duplicate gets a fresh seq. The server still knows the id: its
          // ledger entry answers it, or, once expired, its kept identity answers ClientExpired
          // (SPEC §11), so it never executes again, with or without ledger expiry.
          {
            note(`${client.clientId}: retry enqueue of released ${original.mutationId} (pruned)`)
            return yield* ignoreExpected(
              withClock(
                client,
                client.engine.mutate(original.mutationId, original.name, original.args).pipe(
                  Effect.tap(record),
                  Effect.tap((dup) =>
                    Effect.sync(() => {
                      stats.lateRetries++
                      duplicates.add(dupKey(dup.clientId, dup.seq))
                    })
                  )
                )
              )
            )
          }
        }
        const retry = client.disk.outbox.at(-1)
        if (retry !== undefined && rng.chance(0.05)) {
          note(`${client.clientId}: retry enqueue ${retry.envelope.mutationId}`)
          const again = yield* client.engine.mutate(retry.envelope.mutationId, "increment", { id: "ignored", by: 99 })
          if (again.seq !== retry.envelope.seq)
            return yield* violated("enqueue is idempotent on mutationId", again.mutationId)
          return
        }
        const { name, args } = randomMutation(client)
        // Caller-generated, unordered idempotency key (SPEC §2).
        const mutationId = Ids.MutationId.make(
          `m${rng.int(2 ** 30).toString(36)}-${client.clientId}-${client.disk.lastSeq}`
        )
        note(`${client.clientId}: mutate ${name} ${JSON.stringify(args)}`)
        yield* ignoreExpected(withClock(client, client.engine.mutate(mutationId, name, args).pipe(Effect.tap(record))))
      })

    const pushStep = (client: Client) =>
      Effect.gen(function* () {
        if (!client.online) return
        note(`${client.clientId}: push`)
        yield* ignoreExpected(countingBackstop(client, client.engine.push))
      })

    const pullStep = (client: Client) =>
      Effect.gen(function* () {
        if (!client.online) return
        note(`${client.clientId}: pull from ${client.disk.meta.cursor} (${client.disk.staged.length} staged)`)
        yield* ignoreExpected(pull(client))
      })

    /** A pull suspended on the network while local mutations and pushes commit (P1 shape). */
    const interleavedStep = (client: Client) =>
      Effect.gen(function* () {
        if (!client.online) return
        const arrived = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        client.ctl.gate = Option.some({ arrived, release })
        note(`${client.clientId}: pull (suspended) from ${client.disk.meta.cursor}`)
        const fiber = yield* Effect.forkChild(ignoreExpected(pull(client)))
        yield* Deferred.await(arrived)
        if (rng.chance(0.3)) {
          note(`server: GC log ≤ head ${server.head()} (pull will need a resnapshot)`)
          server.gcLog(server.head())
          if (options.ledgerGc === true) server.gcLedger(server.head())
        }
        for (let i = 0; i <= rng.int(3); i++) {
          stats.interleaved++
          yield* rng.chance(0.6) ? mutateStep(client) : pushStep(client)
        }
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(fiber)
      })

    /** A crash in the middle of a pull install or push-backstop transaction, then a reload. */
    const crashStep = (client: Client) =>
      Effect.gen(function* () {
        if (!client.online) return
        client.disk.crashAfterWrites = Option.some(rng.int(6))
        const op: Effect.Effect<void, PullFailure | PushFailure> = rng.chance(0.6) ? pullStep(client) : pushStep(client)
        note(`${client.clientId}: crash armed`)
        yield* op.pipe(
          Effect.catchDefect((defect) =>
            defect instanceof MemorySyncStorage.SimulatedCrash
              ? Effect.sync(() => {
                  stats.crashes++
                  note("  ↳ crashed mid-transaction")
                })
              : Effect.die(defect)
          )
        )
        client.disk.crashAfterWrites = Option.none()
        yield* reload(client)
      })

    /**
     * A new app version with a different `dataHash` (INV-CLI-12, browser `migrate`): the replica is
     * emptied, the cursor nulled and pending undo emptied; outbox and push watermark are kept.
     * The next install must restore nothing and replay every unsettled pending entry.
     */
    const rebuildStep = (client: Client) =>
      Effect.gen(function* () {
        const { disk } = client
        note(`${client.clientId}: schema rebuild (${disk.outbox.length} pending)`)
        stats.rebuilds++
        disk.replica.replace(new Map())
        disk.meta = { ...disk.meta, cursor: null, resnapshot: false }
        disk.staged = []
        disk.outbox = disk.outbox.map((entry) => ({ ...entry, undo: [] }))
        client.rebuiltThrough = Option.some({ seq: disk.lastSeq, pending: disk.outbox.length })
        yield* reload(client)
      })

    const clientStep = (client: Client) =>
      Effect.gen(function* () {
        const roll = rng.int(100)
        if (roll < 38) return yield* mutateStep(client)
        if (roll < 56) return yield* pushStep(client)
        if (roll < 76) return yield* pullStep(client)
        if (roll < 81) return yield* interleavedStep(client)
        if (roll < 84) return yield* crashStep(client)
        if (roll < 87) {
          note(`${client.clientId}: released-row retention passed (markers pruned)`)
          return MemorySyncStorage.pruneReleased(client.disk)
        }
        if (roll < 93) {
          if (client.disk.staged.length > 0) stats.reloadsMidSet++
          note(`${client.clientId}: reload (${client.disk.staged.length} staged)`)
          return yield* reload(client)
        }
        if (roll < 95) {
          client.online = !client.online
          return note(`${client.clientId}: ${client.online ? "online" : "offline"}`)
        }
        if (roll < 97) return yield* rebuildStep(client)
        client.skew = rng.int(2) === 0 ? 0 : (rng.int(2) === 0 ? -1 : 1) * rng.int(3_600_000)
        note(`${client.clientId}: clock skew ${client.skew}ms`)
      })

    for (step = 1; step <= options.steps; step++) {
      const roll = rng.int(100)
      if (options.restores === true && roll >= 97) {
        const pending = new Set(
          clients.flatMap((c) => c.disk.outbox.map((e) => `${c.clientId}#${e.envelope.mutationId}`))
        )
        // Released intent is outside the recovery guarantee; pending intent must still drain.
        for (const envelope of committed) {
          const key = `${envelope.clientId}#${envelope.mutationId}`
          if (!pending.has(key)) lostReleased.add(key)
        }
        backup()
        stats.restores++
        note(`server: restore to ${server.head()}, rotate ${server.epoch()}`)
        for (const client of clients) yield* checkClient(client)
      } else if (roll < 85) {
        const client = Option.getOrThrow(rng.pick(clients))
        yield* clientStep(client)
        yield* checkClient(client)
      } else if (roll < 95) {
        yield* serverWrite
        if (options.restores === true && rng.chance(0.25)) backup = server.checkpoint()
      } else {
        const horizon = server.head() - BigInt(5 + rng.int(20))
        if (horizon > 0n) {
          note(`server: GC log${options.ledgerGc === true ? " + ledger" : ""} ≤ ${horizon}`)
          server.gcLog(horizon)
          if (options.ledgerGc === true) server.gcLedger(horizon)
        }
      }
      yield* TestClock.adjust(`${1 + rng.int(1000)} millis`)
    }

    // Quiescence: no faults, everyone online, push and pull until stable.
    step = options.steps + 1
    faults = false
    note("quiescence")
    for (const client of clients) client.online = true
    for (let round = 0; round < 30; round++) {
      for (const client of clients) {
        yield* countingBackstop(client, client.engine.push)
        yield* pull(client)
        yield* checkClient(client)
      }
      const settled = clients.every((c) => c.disk.outbox.length === 0 && c.disk.meta.cursor === server.head())
      if (settled) break
    }

    for (const client of clients) {
      if (client.disk.outbox.length > 0) {
        return yield* violated(
          "no intent lost: outbox drains after quiescence",
          `${client.clientId} still pending ${client.disk.outbox.map((e) => e.envelope.mutationId).join(",")}`
        )
      }
      const expected = canonical(yield* view(server.store.tables(), client.principal))
      const actual = canonical(client.disk.replica.tables())
      if (actual !== expected)
        return yield* violated(`${client.clientId} replica = server ∩ readable scopes`, diff(actual, expected))
    }
    for (const envelope of committed) {
      const key = `${envelope.clientId}#${envelope.mutationId}`
      const count = server.executions.get(key) ?? 0
      const ledger = server.ledgerEntry(envelope.clientId, envelope.mutationId)
      const client = clients.find((c) => c.clientId === envelope.clientId)
      const exhausted =
        client?.disk.notices.filter(
          (n) => n.envelope.seq === envelope.seq && n._tag === "Rejected" && n.error._tag === "TransientExhausted"
        ) ?? []
      if (exhausted.length > 0) {
        if (exhausted.length !== 1 || count !== 0 || Option.isSome(ledger)) {
          return yield* violated("exhausted intent is unexecuted and noticed exactly once", key)
        }
        stats.exhausted++
        continue
      }
      if (count !== 1 && !(count === 0 && lostReleased.has(key)))
        return yield* violated("every committed intent executes exactly once", `${key} executed ${count}×`)
      const notices = client?.disk.notices.filter((n) => n.envelope.seq === envelope.seq).length ?? 0
      const rejected = Option.isSome(ledger) && ledger.value.outcome._tag === "Rejected"
      const unknown = client?.disk.notices.some((n) => n.envelope.seq === envelope.seq && n._tag === "OutcomeUnknown")
      // After restore, a released identity may be re-enqueued and receive a different verdict.
      // Its old notice belongs to the old seq, not this timeline's later execution.
      if (
        Option.isSome(ledger) &&
        !(lostReleased.has(key) && ledger.value.seq !== envelope.seq) &&
        (notices !== (rejected ? 1 : 0) || unknown === true)
      ) {
        return yield* violated(
          "rejections surface as exactly one notice",
          `${key}: ${notices} notices, rejected=${rejected}`
        )
      }
      if (rejected) stats.notices++
      if (Option.isNone(ledger) && notices > 1) {
        return yield* violated("an expired mutation surfaces as at most one notice", `${key}: ${notices}`)
      }
    }
    for (const client of clients) {
      stats.expired += client.disk.notices.filter((n) => n._tag === "OutcomeUnknown").length
    }
    stats.mutations = committed.length
    const result: Stats = stats
    return result
  })
