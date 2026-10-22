#!/usr/bin/env bash
# Native worker seam privacy and teardown; leaves the substrate/slice suites unchanged.
set -euo pipefail
export AGENT_BROWSER_SESSION=echo-telemetry
trap 'agent-browser close >/dev/null 2>&1 || true' EXIT
BASE=${1:-http://127.0.0.1:5199}
agent-browser open "$BASE" >/dev/null
agent-browser set viewport 1280 720 2 >/dev/null
result=$(agent-browser eval --json "import('/observability.ts').then(m => m.run())")
jq -e '
  .success and (.data.result as $r | $r.secret as $secret | $r.result as $t |
    ([ $t.spans[].name ] | contains(["echo.leader.election", "echo.enqueue", "echo.push", "echo.pull", "echo.install", "echo.rebase", "echo.pokes"])) and
    (all($t.spans[]; (.name | IN("echo.leader.election", "echo.enqueue", "echo.push", "echo.pull", "echo.install", "echo.rebase", "echo.pokes")) and .events == [] and .links == 0 and .annotations == 0 and .exit._tag == "Success" and (.exit | has("value") | not))) and
    ([$t.spans[] | select(.name == "echo.enqueue") | .attributes[] | select(.[0] == "echo.outcome") | .[1]] | contains(["saved", "rejected", "defect"])) and
    ([$t.spans, $t.metrics, [$t.logs[] | select(.diagnostics | not)]] | tostring | contains($secret) | not) and
    ([$t.logs[] | select(.diagnostics)] | tostring | contains($secret)) and
    (all($t.metrics[]; .attributes.test_runtime == "host")) and
    (all($t.held.held[]; (.name | startswith("echo:" + $r.appId + ":native:leader") | not))) and
    (all($t.held.pending[]; (.name | startswith("echo:" + $r.appId + ":") | not))) and
    ($t.spans == $r.late.spans) and ($t.logs == $r.late.logs) and
    ($r.router.watched and $r.router.removed and $r.router.stopped)
  )' <<<"$result" >/dev/null
echo 'PASS native OPFS host seam: typed/defect canaries diagnostics-only, safe spans/metrics, scoped teardown and late callback fence'
