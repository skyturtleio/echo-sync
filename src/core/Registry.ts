import { Option } from "effect"
import { ScopeGrants } from "./ScopeGrants.ts"
import { canonicalJson, hash, SyncTableDeclarationError, type AnySyncTable } from "./SyncTable.ts"

/**
 * The set of attached tables: the app's tables plus the engine's system tables. Sync names are
 * the identity shared with clients (`log.table_name`, `Upsert.table`), so they must be unique.
 */
export interface SyncRegistry {
  /** System tables first, then app tables in declaration order (DDL order). */
  readonly tables: ReadonlyArray<AnySyncTable>
  readonly get: (name: string) => Option.Option<AnySyncTable>
  /** Combined hashes; a `dataHash` change means "rebuild replica, keep outbox" (DESIGN §9). */
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
  const sorted = [...byName.keys()].toSorted()
  const manifests = sorted.flatMap((name) => {
    const table = byName.get(name)
    return table === undefined ? [] : [table.manifest]
  })
  return {
    tables: all,
    get: (name) => Option.fromNullishOr(byName.get(name)),
    dataHash: hash(canonicalJson(manifests.map((m) => ({ name: m.name, dataHash: m.dataHash })))),
    indexHash: hash(canonicalJson(manifests.map((m) => ({ name: m.name, indexHash: m.indexHash }))))
  }
}
