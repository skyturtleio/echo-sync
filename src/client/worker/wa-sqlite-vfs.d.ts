// Types for the one untyped wa-sqlite module the leader uses (the package ships its VFS examples as JS only).
declare module "@effect/wa-sqlite/src/examples/AccessHandlePoolVFS.js" {
  // The VFS methods themselves are wa-sqlite's `SQLiteVFS`; merged into the class below.
  // oxlint-disable-next-line typescript/no-empty-object-type, typescript/no-unsafe-declaration-merging
  export interface AccessHandlePoolVFS extends SQLiteVFS {}
  // oxlint-disable-next-line typescript/no-unsafe-declaration-merging
  export class AccessHandlePoolVFS {
    /** Opens (creating if needed) the OPFS directory `name` and takes a sync access handle on every file in it. */
    static create(name: string, module: unknown): Promise<AccessHandlePoolVFS>
    /** Releases every access handle in the pool. */
    close(): Promise<void>
  }
}
