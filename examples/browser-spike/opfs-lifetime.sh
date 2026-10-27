#!/usr/bin/env bash
# Vite must be running; these native OPFS controls need neither PostgreSQL nor the example server.
set -euo pipefail
export AGENT_BROWSER_SESSION=echo-opfs-lifetime
trap 'agent-browser close >/dev/null 2>&1 || true' EXIT
BASE=${1:-http://127.0.0.1:5199}
agent-browser open "$BASE/bootstrap.html" >/dev/null
agent-browser set viewport 1280 720 2 >/dev/null
result=$(agent-browser eval --json 'new Promise((resolve, reject) => {
  const worker = new Worker("/opfs-lifetime.worker.js", { type: "module" });
  const timer = setTimeout(() => { worker.terminate(); reject(new Error("OPFS controls timed out")); }, 15000);
  worker.onmessage = (event) => { clearTimeout(timer); worker.terminate(); resolve(event.data); };
  worker.onerror = (event) => { clearTimeout(timer); worker.terminate(); reject(new Error(event.message)); };
})')
echo "$result" | jq -e '.success and .data.result.ok and (.data.result.checks | length == 4)' >/dev/null || {
  echo "$result"
  exit 1
}
echo "$result" | jq -r '.data.result.checks[] | "PASS " + .'
