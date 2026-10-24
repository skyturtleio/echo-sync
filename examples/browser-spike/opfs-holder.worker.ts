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
    void (async () => {
      try {
        const root = await navigator.storage.getDirectory()
        let directory = root
        for (const part of event.data.directory.split("/")) directory = await directory.getDirectoryHandle(part)
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
