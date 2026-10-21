// Counter SharedWorker for sharedworker-lifetime.sh: replies to every message with how many it has seen.
let n = 0
self.addEventListener("connect", (event) => {
  const port = event.ports[0]
  port.addEventListener("message", () => port.postMessage({ n: ++n }))
  port.start()
})
