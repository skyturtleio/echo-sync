#!/usr/bin/env bash
# Browser invariant scenarios (DESIGN §14) for the leader/router/outbox substrate, driven by agent-browser
# against the Vite harness. Each scenario uses a fresh appId, so it starts from an empty replica.
#
#   bun run spike &                              # or: amp orb service start spike --command 'bun run spike'
#   examples/browser-spike/scenarios.sh [base-url]
#
# Prints one PASS/FAIL line per invariant; exits non-zero if any failed.
set -uo pipefail

BASE=${1:-http://127.0.0.1:5199}
RUN=$(date +%s)-$$
export AGENT_BROWSER_SESSION=echo-scenarios-$RUN
fails=0

ab() { agent-browser "$@"; }
# Evaluate the async JS body on stdin in the active tab (after window.echo exists); prints the JSON result.
ev() {
  local body
  body=$(cat)
  printf '(async () => { for (let i = 0; i < 200 && !window.echo; i++) await new Promise((r) => setTimeout(r, 50));\n%s\n})()' "$body" |
    ab eval --json --stdin | jq -c '.data.result'
}
open_tab() {
  ab tab new "$1" >/dev/null
  ab tab --json | jq -r '.data.tabs[] | select(.active) | .tabId'
}
check() {
  if [ "$(jq -r '.pass' <<<"$2")" = "true" ]; then echo "PASS $1"; else
    echo "FAIL $1: $2"
    fails=$((fails + 1))
  fi
}
sleep_ms() { sleep "$(awk "BEGIN { print $1 / 1000 }")"; }

ab open about:blank >/dev/null

# ── single DB owner across N tabs, then across leader handover ───────────────────────────────────────
APP="owner-$RUN"
TABS=()
for _ in 1 2 3 4; do TABS+=("$(open_tab "$BASE/?app=$APP")"); done
statuses="[]"
for t in "${TABS[@]}"; do
  ab tab "$t" >/dev/null
  s=$(ev <<<'const s = await echo.ready(); const i = await echo.info(); return { ...s, served: i.ok ? i.value.leaderId : null }')
  statuses=$(jq -c --arg t "$t" --argjson s "$s" '. + [$s + {tab: $t}]' <<<"$statuses")
done
check "single owner across 4 tabs (one leader id, exactly one leader tab, every tab served by it)" "$(jq -c '
  { pass: ((map(.leaderId) | unique | length) == 1 and (map(select(.isLeaderTab)) | length) == 1
           and all(.[]; .served == .leaderId and .leaderId != null)), statuses: map({tab, leaderId, isLeaderTab}) }' <<<"$statuses")"

LEADER_TAB=$(jq -r 'map(select(.isLeaderTab))[0].tab' <<<"$statuses")
OLD_LEADER=$(jq -r '.[0].leaderId' <<<"$statuses")
FOLLOWER=$(jq -r 'map(select(.isLeaderTab | not))[0].tab' <<<"$statuses")

ab tab "$LEADER_TAB" >/dev/null
ev <<<'await echo.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
for (const b of ["a", "b", "c"]) await echo.exec("INSERT INTO items (body) VALUES (?)", [b])
return await echo.enqueue({ mutationId: "before-handover", name: "t", args: {} })' >/dev/null

# In a follower: an open subscription, a long-running request, and a retried enqueue queued behind it,
# all in flight when the leader tab closes.
ab tab "$FOLLOWER" >/dev/null
ev <<<'window.sub = echo.subscribe(["SELECT count(*) AS n FROM items"], ["items"])
await new Promise((r) => setTimeout(r, 300))
window.slow = echo.exec("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 1000000000) SELECT count(*) FROM c")
await new Promise((r) => setTimeout(r, 300))
window.retried = echo.enqueueRetrying({ mutationId: "retried-across-handover", name: "t", args: {} })
return true' >/dev/null
ab tab close "$LEADER_TAB" >/dev/null

statuses="[]"
for t in "${TABS[@]}"; do
  [ "$t" = "$LEADER_TAB" ] && continue
  ab tab "$t" >/dev/null
  s=$(ev <<<"const old = '$OLD_LEADER'
for (let i = 0; i < 200 && ((await echo.status()).leaderId ?? old) === old; i++) await new Promise((r) => setTimeout(r, 50))
const s = await echo.status(); const i = await echo.info(); return { ...s, served: i.ok ? i.value.leaderId : null }")
  statuses=$(jq -c --arg t "$t" --argjson s "$s" '. + [$s + {tab: $t}]' <<<"$statuses")
done
check "single owner after handover (3 tabs agree on one new leader, exactly one leader tab)" "$(jq -c --arg old "$OLD_LEADER" '
  { pass: (((map(.leaderId) | unique) as $ids | ($ids | length) == 1 and $ids[0] != $old and $ids[0] != null)
          and (map(select(.isLeaderTab)) | length) == 1 and all(.[]; .served == .leaderId)),
    statuses: map({tab, leaderId, isLeaderTab}) }' <<<"$statuses")"

ab tab "$FOLLOWER" >/dev/null
check "handover: in-flight request fails with LeaderUnavailable{LeaderLost}" "$(ev <<<'const r = await window.slow
return { pass: !r.ok && r.error._tag === "LeaderUnavailable" && r.error.reason === "LeaderLost", r }')"
check "handover: open subscription terminates with LeaderUnavailable{LeaderLost}" "$(ev <<<'const e = JSON.stringify(sub.ended)
return { pass: sub.emissions.length >= 1 && e.includes("LeaderUnavailable") && e.includes("LeaderLost"), ended: sub.ended }')"
check "handover: retried enqueue converges to exactly one outbox entry" "$(ev <<<'const r = await window.retried
const rows = await echo.exec("SELECT seq FROM echo_outbox WHERE mutation_id = ?", ["retried-across-handover"])
return { pass: r.ok && rows.ok && rows.value.length === 1 && rows.value[0].seq === r.value.seq, r, rows }')"
check "handover: replica and outbox persist" "$(ev <<<'const items = await echo.exec("SELECT count(*) AS n FROM items")
const ob = await echo.exec("SELECT mutation_id FROM echo_outbox ORDER BY seq")
return { pass: items.ok && items.value[0].n === 3 && ob.ok && ob.value[0]?.mutation_id === "before-handover", items, ob }')"
for t in "${TABS[@]}"; do [ "$t" != "$LEADER_TAB" ] && ab tab close "$t" >/dev/null; done

# ── saved means durable ──────────────────────────────────────────────────────────────────────────────
APP="durable-$RUN"
T=$(open_tab "$BASE/?app=$APP")
ev <<<'return await echo.enqueue({ mutationId: "survives-reload", name: "t", args: { n: 1 } })' >/dev/null
ab reload >/dev/null
check "saved means durable: enqueued entry visible after reload" "$(ev <<<'await echo.ready()
const r = await echo.exec("SELECT mutation_id, args FROM echo_outbox")
return { pass: r.ok && r.value.some((x) => x.mutation_id === "survives-reload" && x.args === "{\"n\":1}"), r }')"

# Hard kill: closing the only tab terminates the leader worker mid-stream of enqueues. Every enqueue
# that had resolved before the kill must be in the outbox afterwards.
resolved=$(ev <<<'window.resolved = []
for (let i = 0; i < 400; i++) { const id = "kill-" + i; echo.enqueue({ mutationId: id, name: "t", args: {} }).then((r) => r.ok && resolved.push(id)) }
await new Promise((r) => setTimeout(r, 150))
return resolved')
ab tab close "$T" >/dev/null
T=$(open_tab "$BASE/?app=$APP")
check "saved means durable: every enqueue resolved before a hard kill is in the outbox ($(jq length <<<"$resolved") resolved)" "$(ev <<<"const resolved = $resolved
await echo.ready()
const r = await echo.exec(\"SELECT mutation_id, seq FROM echo_outbox WHERE mutation_id LIKE 'kill-%' ORDER BY seq\")
const have = new Set(r.value.map((x) => x.mutation_id))
const seqs = r.value.map((x) => x.seq)
return { pass: resolved.length > 0 && resolved.every((id) => have.has(id)) && seqs.every((s, i) => i === 0 || s === seqs[i - 1] + 1),
  resolved: resolved.length, present: r.value.length }")"
ab tab close "$T" >/dev/null

# ── gap-free outbox ordering under concurrent tabs ───────────────────────────────────────────────────
APP="order-$RUN"
A=$(open_tab "$BASE/?app=$APP")
B=$(open_tab "$BASE/?app=$APP")
START=$(($(date +%s%3N) + 6000))
for t in "$A" "$B"; do
  ab tab "$t" >/dev/null
  ev <<<"await echo.ready()
const tag = '$t'
window.run = (async () => {
  while (Date.now() < $START) await new Promise((r) => setTimeout(r, 1))
  const out = []
  for (let i = 0; i < 150; i++) {
    const r = await echo.enqueue({ mutationId: tag + '-' + i, name: 't', args: { i } })
    out.push(r.ok ? r.value.seq : null)
    if (i === 75) { const d = await echo.enqueue({ mutationId: tag + '-0', name: 't', args: {} }); out.dup = [out[0], d.ok ? d.value.seq : null] }
  }
  return out
})()
return true" >/dev/null
done
seqs="{}"
for t in "$A" "$B"; do
  ab tab "$t" >/dev/null
  s=$(ev <<<'const out = await window.run; return { seqs: [...out], dup: out.dup }')
  seqs=$(jq -c --arg t "$t" --argjson s "$s" '. + {($t): $s}' <<<"$seqs")
done
all=$(ev <<<'const r = await echo.exec("SELECT seq FROM echo_outbox ORDER BY seq"); return r.value.map((x) => x.seq)')
check "gap-free ordering: 2 tabs x 150 concurrent enqueues -> one contiguous seq range, per-tab order kept, re-enqueue idempotent" "$(jq -c --argjson all "$all" '
  (to_entries | map(.value.seqs) | add) as $mine |
  # How often consecutive seqs belong to different tabs: proves the tabs really interleaved.
  ([$all[] as $x | (to_entries[] | select(.value.seqs | index($x)) | .key)]
    | [range(1; length) as $i | select(.[$i] != .[$i - 1])] | length) as $switches |
  { pass: (($mine | length) == 300 and ($mine | map(select(. == null)) | length) == 0
          and ($mine | sort) == $all and ($all | length) == 300 and ($all[-1] - $all[0]) == 299
          and all(to_entries[]; .value.seqs as $s | [range(1; $s | length)] | all(.[]; $s[.] > $s[. - 1]))
          and all(to_entries[]; .value.dup[0] == .value.dup[1]) and $switches > 10),
    tabSwitches: $switches,
    range: [$all[0], $all[-1]] }' <<<"$seqs")"
ab tab close "$A" >/dev/null
ab tab close "$B" >/dev/null

# ── reactive query re-emits after a cross-tab write ─────────────────────────────────────────────────
# (The pair_a/pair_b agreement is a sanity check only: removing the read transaction does not make it fail,
# because the single-connection semaphore already serializes the reader's statements. See docs.)
APP="reactive-$RUN"
A=$(open_tab "$BASE/?app=$APP")
ev <<<'await echo.exec("CREATE TABLE pair_a (id INTEGER PRIMARY KEY)")
await echo.exec("CREATE TABLE pair_b (id INTEGER PRIMARY KEY)")
await echo.exec("CREATE TRIGGER pair AFTER INSERT ON pair_a BEGIN INSERT INTO pair_b (id) VALUES (new.id); END")
return true' >/dev/null
B=$(open_tab "$BASE/?app=$APP")
ev <<<'window.sub = echo.subscribe(["SELECT count(*) AS n FROM pair_a", "SELECT count(*) AS n FROM pair_b"], ["pair_a", "pair_b"])
await new Promise((r) => setTimeout(r, 300)); return sub.emissions.length' >/dev/null
ab tab "$A" >/dev/null
ev <<<'await Promise.all(Array.from({ length: 6 }, async () => { for (let i = 0; i < 50; i++) await echo.exec("INSERT INTO pair_a DEFAULT VALUES") })); return true' >/dev/null
ab tab "$B" >/dev/null
check "reactivity: a live query in tab B re-emits after writes from tab A and converges to the final value" "$(ev <<<'await new Promise((r) => setTimeout(r, 500))
const pairs = sub.emissions.map((e) => [e.results[0][0].n, e.results[1][0].n])
const last = pairs[pairs.length - 1]
return { pass: pairs.length > 1 && last[0] === 300 && pairs.every(([a, b]) => a === b),
  emissions: pairs.length, mismatched: pairs.filter(([a, b]) => a !== b).length, last }')"
ab tab close "$A" >/dev/null
ab tab close "$B" >/dev/null

# ── no-SharedWorker fallback fails closed, never a second owner ──────────────────────────────────────
APP="fallback-$RUN"
A=$(open_tab "$BASE/?app=$APP&noSharedWorker")
a=$(ev <<<'const s = await echo.ready(); await echo.exec("CREATE TABLE t (x)"); await echo.exec("INSERT INTO t VALUES (1)"); return s')
B=$(open_tab "$BASE/?app=$APP&noSharedWorker")
check "fallback: second tab without SharedWorker fails closed with SecondaryTabWithoutRouter" "$(ev <<<"const r = await echo.exec('SELECT 1'); const s = await echo.status()
return { pass: $(jq -r '.mode == "single-tab" and .isLeaderTab' <<<"$a") && s.mode === 'single-tab' && !r.ok && r.error.reason === 'SecondaryTabWithoutRouter' && s.leaderId === undefined, r, s }")"
ab tab close "$A" >/dev/null
ab tab "$B" >/dev/null
check "fallback: after the owning tab closes, the waiting tab takes over the same replica" "$(ev <<<'const s = await echo.ready(); const r = await echo.exec("SELECT x FROM t")
return { pass: s.isLeaderTab && r.ok && r.value.length === 1, s, r }')"
ab tab close "$B" >/dev/null

ab close >/dev/null
echo "$fails failed"
exit $((fails > 0))
