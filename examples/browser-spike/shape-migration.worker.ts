/** OPFS migration benchmark. Synthetic transport isolates storage/engine costs, not server throughput. */
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { Array as Arr, Cause, Effect, Exit, Schema, Stream, String as Str } from "effect"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as SqlClient from "effect/sql/SqlClient"
import { AccessScope, Ids, Mutation, Registry, SyncTable } from "../../src/core/index.ts"
import * as Engine from "../../src/client/sync/Engine.ts"
import * as Storage from "../../src/client/sync/SqliteSyncStorage.ts"
import * as OpfsSqlite from "../../src/client/worker/OpfsSqlite.ts"
import { Protocol } from "../../src/core/index.ts"
import { randomUUIDv4 } from "../../src/client/Crypto.ts"

const minute = Schema.NullOr(Schema.Int)
const Airport = Schema.Struct({
  code: Schema.String,
  name: Schema.String,
  position: Schema.Struct({ latitude: Schema.Finite, longitude: Schema.Finite }),
  elevation: Schema.Int,
  timezone: Schema.String,
  runways: Schema.Array(Schema.Struct({ name: Schema.String, length: Schema.Int, surface: Schema.String })),
  country: Schema.Struct({ code: Schema.String, name: Schema.String })
})
const fields = {
  id: Schema.String.pipe(Schema.brand("FlightId")),
  accessScopeId: AccessScope.AccessScopeId,
  m00: minute,
  m01: minute,
  m02: minute,
  m03: minute,
  m04: minute,
  m05: minute,
  m06: minute,
  m07: minute,
  m08: minute,
  m09: minute,
  m10: minute,
  m11: minute,
  m12: minute,
  m13: minute,
  m14: minute,
  m15: minute,
  m16: minute,
  m17: minute,
  m18: minute,
  m19: minute,
  m20: minute,
  m21: minute,
  m22: minute,
  m23: minute,
  m24: minute,
  m25: minute,
  m26: minute,
  m27: minute,
  m28: minute,
  m29: minute,
  departure: Airport,
  arrival: Airport,
  events: Schema.Array(Schema.Json),
  remarks: Schema.String
}
const before = SyncTable.make("flights", fields, { indexes: { by_minutes: ["m01"] } })
const after = SyncTable.make("flights", { ...fields, blockMinutes: minute }, { indexes: { by_minutes: ["m01"] } })
const oldRegistry = Registry.make([before])
const newRegistry = Registry.make([after])
const actor = AccessScope.PrincipalId.make("pilot")
const clientId = Ids.ClientId.make("migration-benchmark")
const mutations = Mutation.makeRegistry([Mutation.make("noop", { args: Schema.Null, handler: () => Effect.void })])
// oxlint-disable-next-line effecttsgo/schema-sync -- Pure synthetic fixture sizing and transport serialization, not an engine API.
const json = Schema.encodeSync(Schema.fromJsonString(Schema.Json))
const decodeResponse = Schema.decodeUnknownEffect(Schema.fromJsonString(Protocol.PullResponse))
const airport = (code: string) => ({
  code,
  name: `${code} International`,
  position: { latitude: 40.6413, longitude: -73.7781 },
  elevation: 13,
  timezone: "America/New_York",
  runways: [{ name: "04L", length: 3682, surface: "asphalt" }],
  country: { code: "US", name: "United States" }
})
const flight = (i: number): SyncTable.EncodedRow => {
  const row: { [column: string]: Schema.Json } = {
    id: `flight-${i}`,
    access_scope_id: "logbook",
    departure: airport("KJFK"),
    arrival: airport("KBOS"),
    events: Arr.makeBy(8, (n) => ({
      type: n % 2 === 0 ? "takeoff" : "landing",
      time: 1_780_000_000_000 + i * 60000 + n * 1000,
      night: n % 3 === 0
    })),
    remarks: ""
  }
  for (let n = 0; n < 30; n++) row[`m${Str.padStart(2, "0")(String(n))}`] = n % 4 === 0 ? null : (i % 180) + n
  row["remarks"] = Str.repeat(2377 - json(row).length)("r")
  return row
}

const open = (directory: string) =>
  Effect.gen(function* () {
    const serve = yield* OpfsSqlite.open({ poolDirectory: directory, dbName: "flights.db" })
    const channel = new MessageChannel()
    yield* Effect.forkScoped(serve(channel.port1))
    const reactivity = yield* Reactivity.make
    return yield* SqliteClient.make({ worker: Effect.succeed(channel.port2) }).pipe(
      Effect.provideService(Reactivity.Reactivity, reactivity)
    )
  })

const trial = (count: number, legacy: boolean, delayMs: number) =>
  Effect.gen(function* () {
    const directory = `shape-bench-${yield* randomUUIDv4}`
    return yield* Effect.gen(function* () {
      const rows = Arr.makeBy(count, flight)
      yield* Effect.gen(function* () {
        const sql = yield* open(directory)
        yield* Storage.migrate(oldRegistry).pipe(Effect.provideService(SqlClient.SqlClient, sql))
        const oldStorage = yield* Storage.make(sql, oldRegistry)
        yield* oldStorage.transaction(
          Effect.gen(function* () {
            yield* oldStorage.rows.writeMany(before, rows)
            yield* oldStorage.setMeta({ cursor: Ids.Position.make(1n), pushedThrough: 0, resnapshot: false })
          })
        )
        const oldEngine = yield* Engine.make({
          registry: oldRegistry,
          storage: oldStorage,
          actor,
          clientId,
          mutations,
          transport: { push: () => Effect.die("offline"), pull: () => Stream.die("offline") }
        })
        yield* oldEngine.mutate(Ids.MutationId.make("queued"), "noop", null)
        if (legacy) yield* sql`DELETE FROM echo_meta WHERE key = 'replica_manifests'`
      }).pipe(Effect.scoped)
      // Reopen the persisted OPFS replica as a deploy does. Module download/election is excluded.
      const started = performance.now()
      const sql = yield* open(directory)
      const storage = yield* Storage.make(sql, newRegistry)
      const migrationStarted = performance.now()
      yield* Storage.migrate(newRegistry, Engine.replayMigrated({ storage, mutations, actor })).pipe(
        Effect.provideService(SqlClient.SqlClient, sql)
      )
      const migrationMs = performance.now() - migrationStarted
      const offline = yield* storage.transaction(
        sql<{
          n: number
          placeholders: number
        }>`SELECT count(*) AS n, sum(block_minutes IS NULL) AS placeholders FROM flights`
      )
      const offlineVisibleMs = performance.now() - started
      if (offline[0]?.n !== (legacy ? 0 : count)) return yield* Effect.die("offline row count differs")
      let firstPushMs: number | null = null
      let maxLockLowerMs = 0
      let maxLockUpperMs = 0
      const measured: Engine.SyncStorage = {
        ...storage,
        transaction: (self) =>
          Effect.suspend(() => {
            const requested = performance.now()
            let transactionStart = requested
            return storage
              .transaction(
                Effect.gen(function* () {
                  transactionStart = performance.now()
                  return yield* self
                })
              )
              .pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    maxLockLowerMs = Math.max(maxLockLowerMs, performance.now() - transactionStart)
                    maxLockUpperMs = Math.max(maxLockUpperMs, performance.now() - requested)
                  })
                )
              )
          })
      }
      const engine = yield* Engine.make({
        registry: newRegistry,
        storage: measured,
        actor,
        clientId,
        mutations,
        transport: {
          push: (request) =>
            Effect.sync(() => {
              firstPushMs ??= performance.now() - started
              return {
                results: Arr.map(request.mutations, (m) => ({
                  mutationId: m.mutationId,
                  position: Ids.Position.make(2n),
                  outcome: { _tag: "Applied" as const }
                }))
              }
            }),
          pull: (request) => {
            if (request.cursor !== null) return Stream.die("refresh must start from null")
            return Stream.fromIterable(Arr.makeBy(Math.ceil(count / 400), (i) => i)).pipe(
              Stream.mapEffect((page) =>
                Effect.gen(function* () {
                  yield* Effect.sleep(delayMs)
                  const changes = Arr.map(rows.slice(page * 400, (page + 1) * 400), (row) => ({
                    _tag: "upsert",
                    table: "flights",
                    row: { ...row, block_minutes: 73 }
                  }))
                  const last = (page + 1) * 400 >= count
                  return yield* decodeResponse(
                    json(
                      last
                        ? { _tag: "PullDone", changes, acks: [], nextCursor: "2", expiredThrough: 0 }
                        : { _tag: "PullPage", changes, acks: [], continuation: `page-${page}` }
                    )
                  ).pipe(Effect.orDie)
                })
              )
            )
          }
        }
      })
      yield* engine.push
      const refreshStarted = performance.now()
      yield* engine.pull
      const refreshMs = performance.now() - refreshStarted
      const installed = yield* storage.transaction(
        sql<{ n: number; repaired: number }>`SELECT count(*) AS n, sum(block_minutes = 73) AS repaired FROM flights`
      )
      if (installed[0]?.n !== count || installed[0]?.repaired !== count)
        return yield* Effect.die("refresh lost flights or placeholders")
      const refreshedVisibleMs = performance.now() - started
      yield* engine.push
      // Outside timing, verify complete payloads, not just counts and the new column.
      for (const actual of yield* storage.transaction(storage.rows.scan(after))) {
        const id = actual["id"]
        if (typeof id !== "string") return yield* Effect.die("missing flight id")
        const expected = rows[Number(Str.slice("flight-".length)(id))]
        if (
          expected === undefined ||
          SyncTable.canonicalJson(actual) !== SyncTable.canonicalJson({ ...expected, block_minutes: 73 })
        )
          return yield* Effect.die(`flight ${id} differs`)
      }
      return {
        count,
        legacy,
        rowJsonBytes: json(rows[0] ?? {}).length,
        delayMsPerPage: delayMs,
        pages: Math.ceil(count / 400),
        migrationMs,
        offlineRows: offline[0]?.n,
        firstVisibleMs: legacy ? refreshedVisibleMs : offlineVisibleMs,
        refreshMs,
        maxLockLowerMs,
        maxLockUpperMs,
        firstPushMs,
        validatedRows: installed[0]?.repaired
      }
    }).pipe(
      Effect.scoped,
      Effect.ensuring(
        Effect.promise(() => navigator.storage.getDirectory()).pipe(
          Effect.flatMap((root) => Effect.promise(() => root.removeEntry(directory, { recursive: true })))
        )
      )
    )
  })

const Request = Schema.Struct({ count: Schema.Int, legacy: Schema.Boolean, delayMs: Schema.Int })
self.addEventListener("message", (event: MessageEvent<unknown>) => {
  Effect.runFork(
    Effect.gen(function* () {
      const request = yield* Schema.decodeUnknownEffect(Request)(event.data)
      const result = yield* Effect.exit(trial(request.count, request.legacy, request.delayMs))
      self.postMessage(Exit.isSuccess(result) ? { result: result.value } : { error: Cause.pretty(result.cause) }, {
        transfer: []
      })
    })
  )
})
