/**
 * WebKit driver for the browser scenarios (WEBKIT=1, see AGENTS.md "WebKit smoke").
 *
 * agent-browser drives Chromium over CDP only. This script answers the same `ab` subcommands that
 * lib.sh, scenarios.sh and slice.sh use, against Playwright's WebKit build:
 *
 *   bun webkit/driver.ts serve                    # daemon: one WebKit browser, one context (one profile)
 *   bun webkit/driver.ts open <url> | tab new <url> | tab --json | tab <id> | tab close [<id>]
 *   bun webkit/driver.ts eval --json --stdin | reload | close
 *
 * Output shapes match agent-browser's `--json` (`{ data: { tabs } }`, `{ data: { result } }`).
 * Playwright is not a repo dependency: it is loaded from $ECHO_PLAYWRIGHT_DIR (default
 * ~/.cache/echo-webkit, where `bun add playwright` and `bunx playwright install --with-deps webkit` put it).
 * Dev harness only.
 */
import { Array as Arr, Clock, Config, Console, Effect, Schema, String as Str } from "effect"
// oxlint-disable-next-line effecttsgo/node-builtin-import -- External Playwright daemon creates its native persistent-profile directory before browser startup.
import { mkdtemp } from "node:fs/promises"
import { homedir, tmpdir } from "node:os"
// oxlint-disable-next-line effecttsgo/node-builtin-import -- Resolve the separately installed Playwright entrypoint and native profile path.
import { join } from "node:path"

const port = Number(Effect.runSync(Config.String("ECHO_WEBKIT_PORT").pipe(Config.withDefault("5299"))))
const command = process.argv.slice(2)

/** The slice of Playwright's API this driver uses (Playwright is loaded at runtime, untyped). */
interface Page {
  goto(url: string): Promise<unknown>
  evaluate(expression: string): Promise<unknown>
  reload(): Promise<unknown>
  close(): Promise<void>
  bringToFront(): Promise<void>
  on(event: "console", listener: (message: { type(): string; text(): string }) => void): void
  on(event: "pageerror", listener: (error: Error) => void): void
  on(event: "worker", listener: (worker: WorkerHandle) => void): void
}
interface WorkerHandle {
  url(): string
  on(event: "console", listener: (message: { type(): string; text(): string }) => void): void
  on(event: "close", listener: () => void): void
}
interface Context {
  newPage(): Promise<Page>
  close(): Promise<void>
}
interface Playwright {
  webkit: {
    launchPersistentContext(userDataDir: string, options: { headless: boolean; args: Array<string> }): Promise<Context>
  }
}
const isPlaywright = (value: unknown): value is Playwright =>
  typeof value === "object" && value !== null && "webkit" in value

// oxlint-disable-next-line effecttsgo/async-function -- External Playwright's Promise API owns this driver lifecycle, outside the engine.
const serve = async () => {
  const dir = Effect.runSync(
    Config.String("ECHO_PLAYWRIGHT_DIR").pipe(Config.withDefault(join(homedir(), ".cache", "echo-webkit")))
  )
  const loaded: unknown = await import(join(dir, "node_modules", "playwright", "index.mjs"))
  if (!isPlaywright(loaded)) throw new Error(`no Playwright in ${dir}`)
  // Extra MiniBrowser arguments (space-separated), e.g. `--features=+StorageAPI`.
  const browserArgs = Arr.filter(
    Str.split(Effect.runSync(Config.String("ECHO_WEBKIT_ARGS").pipe(Config.withDefault(""))), " "),
    (a) => a !== ""
  )
  // A persistent context, like a normal (non-private) Safari window: WebKit's ephemeral contexts behave as
  // private browsing, where OPFS is unavailable (microsoft/playwright#18235). A fresh profile per daemon.
  const profile = await mkdtemp(join(tmpdir(), "echo-webkit-"))
  const context = await loaded.webkit.launchPersistentContext(profile, { headless: true, args: browserArgs })
  const tabs = new Map<string, Page>()
  let next = 1
  let active: string | undefined
  // Page console output and uncaught errors go to the daemon's stderr (ECHO_WEBKIT_LOG=1).
  const log = Effect.runSync(Config.String("ECHO_WEBKIT_LOG").pipe(Config.withDefault("0"))) === "1"
  // oxlint-disable-next-line effecttsgo/async-function -- Tab creation and navigation implement Playwright's external Promise contract.
  const open = async (url: string) => {
    const page = await context.newPage()
    const id = `t${next++}`
    if (log) {
      page.on("console", (m) => Effect.runSync(Console.error(`${id} console.${m.type()} ${m.text()}`)))
      page.on("pageerror", (e) => Effect.runSync(Console.error(`${id} pageerror ${e.message}`)))
      page.on("worker", (worker) => {
        const name = `${id} worker ${new URL(worker.url()).pathname}`
        Effect.runSync(Console.error(`${name} started`))
        worker.on("console", (m) => Effect.runSync(Console.error(`${name} console.${m.type()} ${m.text()}`)))
        worker.on("close", () => Effect.runSync(Console.error(`${name} closed`)))
      })
    }
    tabs.set(id, page)
    active = id
    await page.goto(url)
    return id
  }
  const current = () => {
    const page = active === undefined ? undefined : tabs.get(active)
    if (page === undefined) throw new Error("no active tab")
    return page
  }
  // oxlint-disable-next-line effecttsgo/async-function -- Command dispatch adapts the external Playwright Promise methods without adding sync semantics.
  const run = async (args: ReadonlyArray<string>, stdin: string): Promise<unknown> => {
    const [verb, a1, a2] = args
    switch (verb) {
      case "open":
        return { data: { tabId: await open(a1 ?? "about:blank") } }
      case "tab":
        if (a1 === "new") return { data: { tabId: await open(a2 ?? "about:blank") } }
        if (a1 === "--json")
          return { data: { tabs: Arr.map([...tabs.keys()], (tabId) => ({ tabId, active: tabId === active })) } }
        if (a1 === "close") {
          const id = a2 ?? active
          const page = id === undefined ? undefined : tabs.get(id)
          if (id === undefined || page === undefined) return { data: { closed: false } }
          tabs.delete(id)
          await page.close()
          if (active === id) active = [...tabs.keys()].at(-1)
          return { data: { closed: true } }
        }
        if (a1 !== undefined && tabs.has(a1)) {
          active = a1
          await current().bringToFront()
          return { data: { tabId: a1 } }
        }
        throw new Error(`unknown tab ${a1 ?? ""}`)
      case "eval": {
        // JSON-encoded in the page, as CDP's returnByValue does: Playwright's own serializer would turn an
        // Error subclass (a Schema.TaggedError notice) into { name, message, stack } and drop its fields.
        const encoded = await current().evaluate(`(async () => JSON.stringify(await (${stdin})) ?? "null")()`)
        const result: unknown =
          typeof encoded === "string"
            ? await Effect.runPromise(Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(encoded))
            : null
        return { data: { result } }
      }
      case "reload":
        await current().reload()
        return { data: {} }
      case "close":
        // oxlint-disable-next-line effecttsgo/global-timers -- Let Bun flush the command response before the browser daemon exits.
        setTimeout(() => void context.close().finally(() => process.exit(0)), 10)
        return { data: {} }
      default:
        throw new Error(`unsupported command ${verb ?? ""}`)
    }
  }
  // One command at a time, like a single CLI session.
  let queue: Promise<unknown> = Promise.resolve()
  Bun.serve({
    port,
    hostname: "127.0.0.1",
    idleTimeout: 0,
    // oxlint-disable-next-line effecttsgo/async-function -- Bun's HTTP callback serializes commands against external Playwright Promises.
    fetch: async (request) => {
      if (request.method !== "POST") return new Response("webkit driver")
      // oxlint-disable-next-line typescript/no-unsafe-assignment -- loopback-only driver receives its own CLI's JSON
      const { args, stdin }: { args: Array<string>; stdin: string } = await request.json()
      // A command that never settles (a hung page) fails after this long instead of stalling the run.
      const limit = Number(
        Effect.runSync(Config.String("ECHO_WEBKIT_COMMAND_TIMEOUT_MS").pipe(Config.withDefault("180000")))
      )
      const result = queue.then(() => {
        const started = Effect.runSync(Clock.currentTimeMillis)
        Effect.runSync(Console.error(`> ${Arr.join(args, " ")}`))
        return Effect.runPromise(Effect.tryPromise(() => run(args, stdin)).pipe(Effect.timeout(limit))).finally(() =>
          Effect.runSync(
            Console.error(`< ${Arr.join(args, " ")} ${Effect.runSync(Clock.currentTimeMillis) - started} ms`)
          )
        )
      })
      queue = result.catch(() => undefined)
      return result.then(
        (value) => Response.json(value),
        (error: unknown) => Response.json({ error: String(error) }, { status: 500 })
      )
    }
  })
  Effect.runSync(Console.log(`webkit driver on ${port}`))
}

// oxlint-disable-next-line effecttsgo/async-function -- CLI entrypoint sends commands through Bun's native HTTP/stdio Promise APIs.
const client = async () => {
  const stdin = command.includes("--stdin") ? await Bun.stdin.text() : ""
  // oxlint-disable-next-line effecttsgo/global-fetch -- This loopback-only browser driver checks native HTTP status and writes the daemon's response verbatim.
  const response = await fetch(`http://127.0.0.1:${port}/`, {
    method: "POST",
    body: JSON.stringify({ args: command, stdin })
  })
  const text = await response.text()
  process.stdout.write(`${text}\n`)
  if (!response.ok) process.exit(1)
}

await (command[0] === "serve" ? serve() : client())
