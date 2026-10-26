import { String as Str } from "effect"

let handles: Array<FileSystemSyncAccessHandle> = []

addEventListener(
  "message",
  (event: MessageEvent<{ readonly _tag: "Hold"; readonly directory: string } | { readonly _tag: "Release" }>) => {
    if (event.data._tag === "Release") {
      for (const handle of handles) handle.close()
      handles = []
      postMessage({ _tag: "Released" })
      close()
      return
    }
    const path = event.data.directory
    // oxlint-disable-next-line effecttsgo/async-function -- The test holds actual raw OPFS handles outside the engine to exercise browser contention and worker-death cleanup.
    void (async () => {
      try {
        const root = await navigator.storage.getDirectory()
        let directory = root
        for (const part of Str.split(path, "/")) directory = await directory.getDirectoryHandle(part)
        // `values()` is in the WHATWG spec and every engine that has OPFS, but not in TS's DOM lib yet.
        // oxlint-disable-next-line typescript/consistent-type-assertions -- platform edge, see above
        const iterable = directory as unknown as { values(): AsyncIterable<FileSystemHandle> }
        for await (const entry of iterable.values()) {
          if (entry instanceof FileSystemFileHandle) handles.push(await entry.createSyncAccessHandle())
        }
        postMessage({ _tag: "Held", count: handles.length })
      } catch (error) {
        postMessage({ _tag: "Failed", message: String(error) })
      }
    })()
  }
)
