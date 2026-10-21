# Shared by scenarios.sh and slice.sh: helpers, the sync mode, a fresh agent-browser session and a warm-up tab.
# Sourced with the base URL as $1.
set -uo pipefail

BASE=${1:-http://127.0.0.1:5199}
SYNC_MODE=${ECHO_SYNC:-real}
if [ "$SYNC_MODE" = real ] && ! curl -sf "$BASE/admin/stats" >/dev/null; then
  echo "the slice server is not reachable at $BASE/admin (start examples/server/main.ts, or set ECHO_SYNC=off)" >&2
  exit 2
fi
RUN=$(date +%s)-$$
export AGENT_BROWSER_SESSION=echo-${SESSION_PREFIX:-scenarios}-$RUN
fails=0
total=0

ab() { agent-browser "$@"; }
# Evaluate the async JS body on stdin in the active tab (after window.echo exists); prints the JSON result.
ev() {
  local body
  body=$(cat)
  printf '(async () => { for (let i = 0; i < 400 && !window.echo; i++) await new Promise((r) => setTimeout(r, 50));\n%s\n})()' "$body" |
    ab eval --json --stdin | jq -c '.data.result'
}
# Every tab syncs with the real slice server (examples/server/main.ts, proxied at /sync) unless ECHO_SYNC=off
# or the URL already names a syncUrl (the fault-injection scenarios point at the fake server).
open_tab() {
  local url=$1
  if [ "$SYNC_MODE" = real ] && [[ "$url" == *"?app="* ]] && [[ "$url" != *syncUrl=* ]]; then url="$url&syncUrl=$BASE/sync"; fi
  ab tab new "$url" >/dev/null
  ab tab --json | jq -r '.data.tabs[] | select(.active) | .tabId'
}
check() {
  total=$((total + 1))
  if [ "$(jq -r '.pass' <<<"$2")" = "true" ]; then
    echo "PASS $1"
    # ECHO_EVIDENCE=1 also prints what a passing check observed (the slice script sets it).
    [ "${ECHO_EVIDENCE:-0}" = 1 ] && echo "     $(jq -c 'del(.pass)' <<<"$2")"
  else
    echo "FAIL $1: $2"
    fails=$((fails + 1))
  fi
}
# Status once this tab is connected to a leader *and* that leader answers leaderInfo with its own id.
SERVED='await echo.until(async () => { const s = await echo.status(); const i = await echo.info(); return s.leaderId !== undefined && i.ok && i.value.leaderId === s.leaderId })
const s = await echo.status(); const i = await echo.info(); return { ...s, served: i.ok ? i.value.leaderId : null }'

ab open about:blank >/dev/null
# Warm-up: the first page load lets Vite finish dependency optimization before any assertion runs.
W=$(open_tab "$BASE/?app=warmup-$RUN")
ev <<<"$SERVED" >/dev/null
ab tab close "$W" >/dev/null
