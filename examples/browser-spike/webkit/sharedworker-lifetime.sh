#!/usr/bin/env bash
# Does a SharedWorker survive the close of the tab that created it while another tab is still connected?
# The router (SPEC §12) relies on it. Chromium: yes. Playwright WebKit (Linux): no. See
# AGENTS.md "WebKit smoke".
#
#   [WEBKIT=1] examples/browser-spike/webkit/sharedworker-lifetime.sh [base-url]
SESSION_PREFIX=swlife
ECHO_SYNC=off
# shellcheck source=../lib.sh
source "$(dirname "$0")/../lib.sh"
# Connects (once per tab) to a counter SharedWorker and pings it; "no reply" after 3 s.
PING='const w = (window.__sw ??= new SharedWorker("/webkit/sharedworker-probe.js", { name: "probe-'"$RUN"'" }))
w.port.start()
const reply = new Promise((r) => { w.port.onmessage = (m) => r(m.data) })
w.port.postMessage("ping")
return await Promise.race([reply, new Promise((r) => setTimeout(() => r("no reply"), 3000))])'
A=$(open_tab "$BASE/?app=swlife-$RUN"); a=$(ev <<<"$PING")
B=$(open_tab "$BASE/?app=swlife-$RUN"); b=$(ev <<<"$PING")
ab tab close "$A" >/dev/null
ab tab "$B" >/dev/null; after=$(ev <<<"$PING")
check "a SharedWorker survives the close of the tab that created it: the other tab's port still reaches the same instance" "$(jq -nc --argjson a "$a" --argjson b "$b" --argjson after "$after" '
  { pass: ($a.n == 1 and $b.n == 2 and ($after | type) == "object" and $after.n == 3), creator: $a, other: $b, otherAfterCreatorClosed: $after }')"
ab tab close "$B" >/dev/null
ab close >/dev/null
echo "$((total - fails))/$total passed"
exit $((fails > 0))
