#!/usr/bin/env bash
# Default: 10k + 100k, three empty-OPFS trials each. Smoke: SIZES=100 TRIALS=1 bash .../bootstrap.sh
# Prerequisites: supervised echo-server (:5200), echo-spike (:5199), PostgreSQL, agent-browser, jq.
# Seeding is outside timing; fresh server scope/principal AND replica identity on every trial.
# Server rows/logs are retained in the disposable harness DB; completed replicas are reset.
# Use a fresh ECHO_SLICE_DB for each before/after campaign to match server row populations.
# firstUsableQueryMs: before LeaderClient.make (includes worker/wasm/OPFS startup, pull,
# staging and install) through receipt + field validation of the first ten rows in a
# live query whose same-transaction count equals the seeded total. Not page navigation time.
# longestInstallMs: max NativeSpan end-start for echo.install before querying metrics
# immediately after that first usable result; includes FIFO admission wait and the complete
# transaction through commit, excludes network and prior page staging. This is an upper bound
# on permit hold, not a direct measurement of it, and includes failed/interrupted attempts.
# Full row validation and reset are outside both metrics. Each trial has empty OPFS but
# browser HTTP/module caches and PostgreSQL buffers are NOT cleared between trials.
# ECHO_BENCH_RTT_MS=0|75 asserts the proxy's advertised added RTT (default 0).
# Restart server, proxy and Vite from the recorded clean revision before each campaign.
set -euo pipefail
if [ -n "$(git status --porcelain)" ]; then
  echo 'benchmark receipts require a clean committed source tree' >&2
  exit 2
fi
BASE=${1:-http://127.0.0.1:5199}
SIZES=${SIZES:-10000,100000}
TRIALS=${TRIALS:-3}
[[ "$SIZES" =~ ^[0-9]+(,[0-9]+)*$ && "$TRIALS" =~ ^[0-9]+$ ]] || { echo 'invalid SIZES/TRIALS' >&2; exit 2; }
SESSION="boot-$$"
ab() { agent-browser --session "$SESSION" "$@"; }
cleanup() { ab close >/dev/null 2>&1 || true; }
trap cleanup EXIT
revision=$(git rev-parse HEAD)
harness_tree=$(git rev-parse HEAD:examples/browser-spike)
expected_rtt=${ECHO_BENCH_RTT_MS:-0}
[[ "$expected_rtt" = 0 || "$expected_rtt" = 75 ]] || { echo 'invalid ECHO_BENCH_RTT_MS' >&2; exit 2; }
response_headers=$(curl -fsS -D - -o /dev/null "$BASE/admin/stats")
proxy_rtt=$(awk 'tolower($1) == "x-echo-benchmark-rtt-ms:" {gsub("\r", "", $2); print $2}' <<<"$response_headers")
proxy_sha256=$(awk 'tolower($1) == "x-echo-benchmark-proxy-sha256:" {gsub("\r", "", $2); print $2}' <<<"$response_headers")
[[ "${proxy_rtt:-0}" = "$expected_rtt" ]] || { echo 'proxy RTT differs from ECHO_BENCH_RTT_MS' >&2; exit 2; }
if [ -n "$proxy_rtt" ]; then
  local_proxy_sha256=$(sha256sum examples/browser-spike/delay-proxy.ts | cut -d ' ' -f 1)
  [[ "$proxy_sha256" = "$local_proxy_sha256" ]] || { echo 'running proxy differs from clean source' >&2; exit 2; }
fi
metadata=$(jq -cn --arg revision "$revision" --arg harnessTree "$harness_tree" \
  --argjson addedRttMs "$expected_rtt" --arg proxySha256 "$proxy_sha256" \
  '{sourceRevision: $revision, harnessTree: $harnessTree, cleanSource: true, addedRttMs: $addedRttMs,
    proxySha256: (if $proxySha256 == "" then null else $proxySha256 end),
    delayModel: "half before upstream dispatch, half before response headers; no body delay"}')
ab open "$BASE/bootstrap.html" >/dev/null
ab set viewport 1280 720 2 >/dev/null
for _ in $(seq 1 120); do
  ready=$(ab eval --json 'window.bootstrapBenchmark?.state ?? "loading"' | jq -r '.data.result')
  [ "$ready" = ready ] && break
  sleep 1
done
[ "$ready" = ready ] || { echo 'page not ready' >&2; exit 1; }
echo "Chromium OPFS + SharedWorker + leader + real server; four-column todos (not eight-column tasks kit)" >&2
echo "revision=$(git rev-parse HEAD); sizes=$SIZES; trials=$TRIALS" >&2
ab eval 'navigator.userAgent' >&2
ab eval "window.bootstrapBenchmark.start([$SIZES], $TRIALS); 'started'" >/dev/null
# Never await a whole trial in CDP eval: short polls survive long installs.
deadline=$((SECONDS + ${TIMEOUT_SECONDS:-14400}))
while [ "$SECONDS" -lt "$deadline" ]; do
  result=$(ab eval --json 'window.bootstrapBenchmark' | jq -c '.data.result')
  state=$(jq -r '.state' <<<"$result")
  case "$state" in
    done)
      if [ -n "$(git status --porcelain)" ] || [ "$(git rev-parse HEAD)" != "$revision" ]; then
        echo 'source changed during benchmark; receipt refused' >&2
        exit 2
      fi
      jq -c --argjson metadata "$metadata" '. + {metadata: $metadata}' <<<"$result"
      cleanup; trap - EXIT; exit 0 ;;
    failed) jq -c --argjson metadata "$metadata" '. + {metadata: $metadata}' <<<"$result"; exit 1 ;;
    running) sleep 2 ;;
    *) echo "page reloaded or benchmark lost: $result" >&2; exit 1 ;;
  esac
done
echo 'benchmark timed out; browser closed, incomplete replica may remain in its ephemeral profile' >&2
exit 1
