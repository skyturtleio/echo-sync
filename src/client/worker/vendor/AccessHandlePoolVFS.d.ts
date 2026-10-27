// The vendored example remains JavaScript to keep ownership changes reviewable against upstream.
// oxlint-disable-next-line import/no-unassigned-import -- Load the dependency's global SQLiteVFS declaration for source consumers.
import "@effect/wa-sqlite"

// oxlint-disable-next-line typescript/no-empty-object-type, typescript/no-unsafe-declaration-merging
export interface AccessHandlePoolVFS extends Omit<SQLiteVFS, "isReady"> {}
// oxlint-disable-next-line typescript/no-unsafe-declaration-merging
export class AccessHandlePoolVFS {
  /** Constructs an uninitialized pool without starting native operations. */
  constructor(name: string, module: unknown)
  static create(name: string, module: unknown): Promise<AccessHandlePoolVFS>
  /** Opens the directory and acquires its handles; failure closes partial and late acquisitions. */
  isReady(): Promise<boolean>
  /** Runtime spelling (the dependency's SQLiteVFS declaration spells it mxPathName). */
  mxPathname: number
  getCapacity(): number
  addCapacity(n: number): Promise<number>
  /** Terminal close; pending acquisitions close their own late results without delaying this call. */
  close(): Promise<void>
}
