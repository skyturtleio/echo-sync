/**
 * Worker-side SQLite host on OPFS, speaking the same port protocol as `@effect/sql-sqlite-wasm`'s
 * `OpfsWorker` so the stock `SqliteClient.layer({ worker })` can drive it.
 *
 * Why not `OpfsWorker.run`: it registers AccessHandlePoolVFS under the fixed name "opfs", and that VFS
 * uses its name as its OPFS directory and opens an exclusive synchronous access handle on *every*
 * file in it. All databases on the origin would share one pool, so a live leader for one
 * `<appId>/<profile>` makes every other identity's leader fail to open ("Access Handles cannot be
 * created if there is another open Access Handle"). Here the pool directory is per identity.
 *
 * The protocol (`OpfsWorkerMessage` in `@effect/sql-sqlite-wasm/src/internal/opfsWorker.ts`) is
 * internal to that package: re-verify this file on every `@effect/sql-sqlite-wasm` bump.
 */
// Consumers type-check this file from source, so the ambient types it needs must travel with it.
// oxlint-disable-next-line typescript/triple-slash-reference -- ambient module for an untyped JS file
/// <reference path="./wa-sqlite-vfs.d.ts" />
/// <reference lib="webworker" />
// oxlint-disable-next-line import/no-namespace -- wa-sqlite's API is a namespace of functions and constants
import * as WaSqlite from "@effect/wa-sqlite"
import SQLiteESMFactory from "@effect/wa-sqlite/dist/wa-sqlite.mjs"
import { AccessHandlePoolVFS } from "@effect/wa-sqlite/src/examples/AccessHandlePoolVFS.js"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Platform from "./platform.ts"

const Param = Schema.Union([Schema.String, Schema.Number, Schema.Null, Schema.BigInt, Schema.Uint8Array])
const Message = Schema.Union([
  Schema.Tuple([Schema.Number, Schema.String, Schema.Array(Param)]),
  Schema.Tuple([Schema.Literal("import"), Schema.Number, Schema.Uint8Array]),
  Schema.Tuple([Schema.Literal("export"), Schema.Number]),
  Schema.Tuple([Schema.Literal("update_hook")]),
  Schema.Tuple([Schema.Literal("close")])
])
const isMessage = Schema.is(Message)

const errorPayload = (e: unknown) => {
  const message = e instanceof Error ? e.message : String(e)
  return typeof e === "object" && e !== null && "code" in e && typeof e.code === "number"
    ? { message, code: e.code }
    : message
}

/** AccessHandlePoolVFS's own default pool size. */
const POOL_CAPACITY = 6

export interface OpfsSqliteConfig {
  /** OPFS directory holding this database's access-handle pool; must not be shared with any other database. */
  readonly poolDirectory: string
  readonly dbName: string
}

/**
 * Open the pool and the database in the current scope. Every way opening can fail surfaces here, before
 * anything waits on the port protocol: `SqliteClient` waits for "ready" uninterruptibly, so a host that
 * failed after the client started waiting would hang its caller (and its locks) forever.
 *
 * Returns `serve(port)`, which answers protocol messages on `port` until the client sends "close".
 */
export const open = (
  config: OpfsSqliteConfig
): Effect.Effect<(port: MessagePort) => Effect.Effect<void>, Platform.BrowserStorageError, Scope.Scope> =>
  Effect.gen(function* () {
    const module: unknown = yield* Effect.promise(() => SQLiteESMFactory())
    const sqlite3 = WaSqlite.Factory(module)
    const vfs = yield* Effect.acquireRelease(
      Platform.storageCall("opening the OPFS pool", () => AccessHandlePoolVFS.create(config.poolDirectory, module)),
      (pool) => Effect.promise(() => pool.close())
    )
    // The VFS only adds capacity when the directory is empty. A candidate that died mid-way through
    // creating its pool leaves fewer files than SQLite needs (db + journal + temp files), and the next
    // leader would fail with SQLITE_CANTOPEN on its first write. Top the pool up instead.
    const missing = POOL_CAPACITY - vfs.getCapacity()
    if (missing > 0) {
      yield* Platform.storageCall(`adding ${missing} files to the OPFS pool`, () => vfs.addCapacity(missing))
    }
    // Platform edge: wa-sqlite reads `mxPathname` (a plain field on its VFS base class, default 64) when
    // the VFS is registered. The pool's own header holds far longer paths; see `Platform.vfsMaxPathname`.
    vfs.mxPathname = Platform.vfsMaxPathname
    sqlite3.vfs_register(vfs, false)
    const db = yield* Effect.acquireRelease(
      Effect.try({
        try: () => sqlite3.open_v2(config.dbName, undefined, config.poolDirectory),
        catch: (cause) => new Platform.BrowserStorageError({ operation: "opening the database", cause })
      }),
      (handle) => Effect.sync(() => sqlite3.close(handle))
    )

    return (port: MessagePort) =>
      Effect.callback<void>((resume) => {
        const onMessage = (event: MessageEvent) => {
          const message: unknown = event.data
          if (!isMessage(message)) return
          let id = -1
          try {
            switch (message[0]) {
              case "close":
                port.close()
                return resume(Effect.void)
              case "update_hook":
                sqlite3.update_hook(db, (_op, _db, table, rowid) => {
                  if (table) port.postMessage(["update_hook", table, Number(rowid)])
                })
                return
              case "import": {
                const [, messageId, data] = message
                id = messageId
                sqlite3.deserialize(db, "main", data, data.length, data.length, 1 | 2)
                port.postMessage([id, undefined, undefined])
                return
              }
              case "export": {
                id = message[1]
                const data = sqlite3.serialize(db, "main")
                port.postMessage([id, undefined, data], [data.buffer])
                return
              }
              default: {
                const [messageId, sql, params] = message
                id = messageId
                const rows: Array<Array<unknown>> = []
                const columns: Array<Array<string>> = []
                for (const stmt of sqlite3.statements(db, sql)) {
                  let names: Array<string> | undefined
                  sqlite3.bind_collection(stmt, [...params])
                  while (sqlite3.step(stmt) === WaSqlite.SQLITE_ROW) {
                    names ??= sqlite3.column_names(stmt)
                    rows.push(sqlite3.row(stmt))
                    columns.push(names)
                  }
                }
                port.postMessage([id, undefined, [columns, rows]])
                return
              }
            }
          } catch (e) {
            port.postMessage([id, errorPayload(e), undefined])
          }
        }
        port.addEventListener("message", onMessage)
        port.start()
        port.postMessage(["ready", undefined, undefined])
        return Effect.sync(() => port.removeEventListener("message", onMessage))
      })
  })
