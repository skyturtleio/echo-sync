import { Array as Arr, Option, Order } from "effect"
import { ScopeGrants } from "./ScopeGrants.ts"
import { canonicalJson, hash, SyncTableDeclarationError, type AnySyncTable } from "./SyncTable.ts"

/**
 * The set of attached tables: the app's tables plus the engine's system tables. Sync names are
 * the identity shared with clients (`log.table_name`, `Upsert.table`), so they must be unique.
 */
export interface SyncRegistry {
  /** System tables first, then app tables in declaration order (DDL order). */
  readonly tables: ReadonlyArray<AnySyncTable>
  /** Independently versioned global reference tables; never part of scope replication. */
  readonly catalogs: ReadonlyArray<AnySyncTable>
  readonly get: (name: string) => Option.Option<AnySyncTable>
  /** Combined hashes; a `dataHash` change means "rebuild replica, keep outbox" (INV-CLI-12). */
  readonly dataHash: string
  readonly indexHash: string
}

export const make = (tables: ReadonlyArray<AnySyncTable>): SyncRegistry => {
  const all: ReadonlyArray<AnySyncTable> = [ScopeGrants, ...tables]
  const byName = new Map<string, AnySyncTable>()
  for (const table of all) {
    if (byName.has(table.name)) throw new SyncTableDeclarationError(`SyncRegistry: duplicate table "${table.name}"`)
    byName.set(table.name, table)
  }
  for (const table of all) {
    if (table.kind === "catalog" && byName.has(`${table.name}__next`)) {
      throw new SyncTableDeclarationError(`SyncRegistry: ${table.name}__next is reserved for catalog installation`)
    }
  }
  const sorted = Arr.sort(byName.keys(), Order.String)
  const manifests = Arr.flatMap(sorted, (name) => {
    const table = byName.get(name)
    return table === undefined || table.kind === "catalog" ? [] : [table.manifest]
  })
  return {
    tables: Arr.filter(all, (table) => table.kind === "sync"),
    catalogs: Arr.filter(all, (table) => table.kind === "catalog"),
    get: (name) => Option.fromNullishOr(byName.get(name)).pipe(Option.filter((table) => table.kind === "sync")),
    dataHash: hash(canonicalJson(Arr.map(manifests, (m) => ({ name: m.name, dataHash: m.dataHash })))),
    indexHash: hash(canonicalJson(Arr.map(manifests, (m) => ({ name: m.name, indexHash: m.indexHash }))))
  }
}
