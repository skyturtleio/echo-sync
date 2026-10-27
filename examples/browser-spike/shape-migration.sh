#!/usr/bin/env bash
# Real OPFS + engine; synthetic JSON transport, 400 rows/page and optional per-page delay.
# Reopens each seeded database before timing. Excludes worker download/election and server work.
# Legacy control removes replica_manifests to run the unchanged former rebuild path.
# firstVisibleMs: reopen start -> verified count query (after refresh for the legacy control).
# migrationMs: the whole migration; an upper bound on its SQLite transaction hold.
# maxLockLowerMs/UpperMs: max body-through-commit / admission-through-commit durations.
# With no concurrent requests these bracket the longest refresh/push storage-lock hold,
# apart from sub-ms timer/publication overhead. Full payload validation is outside timing.
# WEBKIT=1 uses an already-running webkit/driver.ts daemon at ECHO_WEBKIT_PORT.
set -euo pipefail
BASE=${1:-http://127.0.0.1:5199}
SIZES=${SIZES:-2400,17000}
TRIALS=${TRIALS:-3}
DELAY_MS=${DELAY_MS:-75}
[[ "$SIZES" =~ ^[0-9]+(,[0-9]+)*$ && "$TRIALS" =~ ^[0-9]+$ && "$DELAY_MS" =~ ^[0-9]+$ ]] || exit 2
export AGENT_BROWSER_SESSION=shape-$$
if [ "${WEBKIT:-0}" = 1 ]; then
  ab() { bun examples/browser-spike/webkit/driver.ts "$@"; }
else
  ab() { agent-browser "$@"; }
fi
cleanup() { ab close >/dev/null 2>&1 || true; }
trap cleanup EXIT
ab open "$BASE/bootstrap.html" >/dev/null
if [ "${WEBKIT:-0}" != 1 ]; then ab set viewport 1280 720 2 >/dev/null; fi
sleep 2
ab eval --stdin >/dev/null <<EOF
(() => {
  window.shapeBenchmark = { state: 'running', results: [], error: '', userAgent: navigator.userAgent };
  (async () => {
    try {
      for (const count of [$SIZES]) for (let trial = 0; trial < $TRIALS; trial++) for (const legacy of [true, false]) {
        const result = await new Promise((resolve, reject) => {
          const worker = new Worker('/shape-migration.worker.ts', { type: 'module' });
          worker.onerror = e => { worker.terminate(); reject(new Error(e.message)); };
          worker.onmessage = e => { worker.terminate(); e.data.error ? reject(new Error(e.data.error)) : resolve(e.data.result); };
          worker.postMessage({ count, legacy, delayMs: $DELAY_MS });
        });
        window.shapeBenchmark.results.push({ trial, ...result });
      }
      window.shapeBenchmark.state = 'done';
    } catch (e) { window.shapeBenchmark.error = String(e); window.shapeBenchmark.state = 'failed'; }
  })();
  return 'started';
})()
EOF
for _ in $(seq 1 900); do
  result=$(printf 'window.shapeBenchmark' | ab eval --json --stdin | jq -c '.data.result')
  state=$(jq -r '.state' <<<"$result")
  case "$state" in
    done) jq --arg revision "$(git rev-parse HEAD)" --arg diff "$(git diff | sha256sum | cut -d ' ' -f1)" '. + {revision: $revision, diffSha256: $diff}' <<<"$result"; exit 0 ;;
    failed) echo "$result"; exit 1 ;;
    running) sleep 1 ;;
    *) echo "benchmark page lost: $result" >&2; exit 1 ;;
  esac
done
echo 'benchmark timed out' >&2
exit 1
