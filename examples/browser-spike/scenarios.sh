#!/usr/bin/env bash
# Browser invariant scenarios (DESIGN §14) for the leader/router/outbox substrate, driven by agent-browser
# against the Vite harness. Each scenario uses a fresh appId, so it starts from an empty replica.
#
#   bun run spike &                              # or: amp orb service start spike --command 'bun run spike'
#   examples/browser-spike/scenarios.sh [base-url]
#
# Prints one PASS/FAIL line per invariant and a summary; exits non-zero if any failed.
# Determinism: no assertion depends on a sleep. Every check awaits the condition it asserts (echo.until),
# and concurrent phases start from a BroadcastChannel barrier, not a wall-clock time.
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

# ── single DB owner across N tabs, then across leader handover ───────────────────────────────────────
APP="owner-$RUN"
TABS=()
for _ in 1 2 3 4; do TABS+=("$(open_tab "$BASE/?app=$APP")"); done
statuses="[]"
for t in "${TABS[@]}"; do
  ab tab "$t" >/dev/null
  s=$(ev <<<"$SERVED")
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

# In a follower: an open subscription (with its first emission), a request that keeps the leader busy for
# minutes, and an enqueue queued behind it. All three are in flight when the leader tab closes.
ab tab "$FOLLOWER" >/dev/null
ev <<<'window.sub = echo.subscribe(["SELECT count(*) AS n FROM items"], ["items"])
await echo.until(() => sub.emissions.length >= 1)
window.slow = echo.exec("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 1000000000) SELECT count(*) FROM c")
window.retried = echo.enqueueRetrying({ mutationId: "retried-across-handover", name: "t", args: {} })
return true' >/dev/null
ab tab close "$LEADER_TAB" >/dev/null

statuses="[]"
for t in "${TABS[@]}"; do
  [ "$t" = "$LEADER_TAB" ] && continue
  ab tab "$t" >/dev/null
  s=$(ev <<<"const old = '$OLD_LEADER'
await echo.until(async () => { const s = await echo.status(); const i = await echo.info(); return s.leaderId !== undefined && s.leaderId !== old && i.ok && i.value.leaderId === s.leaderId })
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
check "handover: open subscription terminates with LeaderUnavailable{LeaderLost}" "$(ev <<<'await echo.until(() => sub.ended !== undefined)
const e = JSON.stringify(sub.ended)
return { pass: sub.emissions.length >= 1 && e.includes("LeaderUnavailable") && e.includes("LeaderLost"), ended: sub.ended }')"
check "handover: an enqueue lost with the leader is retried and lands exactly once" "$(ev <<<'const r = await window.retried
const rows = await echo.exec("SELECT seq FROM echo_outbox WHERE mutation_id = ?", ["retried-across-handover"])
return { pass: r.ok && r.attempts >= 2 && rows.ok && rows.value.length === 1 && rows.value[0].seq === r.value.seq, r, rows }')"
check "handover: replica and outbox persist" "$(ev <<<'const items = await echo.exec("SELECT count(*) AS n FROM items")
const ob = await echo.exec("SELECT mutation_id FROM echo_outbox ORDER BY seq")
return { pass: items.ok && items.value[0].n === 3 && ob.ok && ob.value[0]?.mutation_id === "before-handover", items, ob }')"

# A dead leader's announcement replayed to the router after its successor took over (a delayed relay).
# The router must ignore it: for 2 s, every surviving tab must keep its route to the new leader.
NEW_LEADER=$(jq -r '.[0].leaderId' <<<"$statuses")
# Also hold the dead epoch in shared mode, as a slow awaitLockReleased watcher would right after the death:
# a shared holder must not count as the leader being alive.
ab tab "$FOLLOWER" >/dev/null
ev <<<"const epoch = 'echo:$APP:default:leader:$OLD_LEADER'
await new Promise((granted) => { window.sharedEpoch = navigator.locks.request(epoch, { mode: 'shared' }, () => { granted(); return new Promise((r) => (window.dropSharedEpoch = r)) }) })
echo.forgeLeaderAnnouncement('$OLD_LEADER'); return true" >/dev/null
stable="[]"
for t in "${TABS[@]}"; do
  [ "$t" = "$LEADER_TAB" ] && continue
  ab tab "$t" >/dev/null
  r=$(ev <<<"const want = '$NEW_LEADER', end = Date.now() + 2000; let samples = 0, bad = null
while (Date.now() < end && bad === null) {
  const r = await Promise.race([echo.exec('SELECT 1 AS one'), new Promise((res) => setTimeout(() => res({ ok: false, error: 'timeout' }), 1000))])
  const s = await echo.status()
  if (!r.ok || s.leaderId !== want) bad = { r, leaderId: s.leaderId }
  samples++
}
return { bad, samples }")
  stable=$(jq -c --argjson r "$r" '. + [$r]' <<<"$stable")
done
ab tab "$FOLLOWER" >/dev/null
ev <<<'window.dropSharedEpoch(); await window.sharedEpoch; return true' >/dev/null
check "router ignores a stale announcement from a dead leader, even while its epoch is held shared (every tab keeps the live route for 2 s)" "$(jq -c '{ pass: all(.[]; .bad == null and .samples > 5), tabs: . }' <<<"$stable")"
for t in "${TABS[@]}"; do [ "$t" != "$LEADER_TAB" ] && ab tab close "$t" >/dev/null; done

# ── commit before lost response, with simultaneous same-id retries from two tabs ─────────────────────
# Tab L's leader worker ends itself instead of sending a committed enqueue's response (harness fault
# `crashAfterCommit`). F and G enqueue the same mutationId at the same moment and retry on LeaderLost.
APP="crash-$RUN"
L=$(open_tab "$BASE/?app=$APP&crashAfterCommit")
l=$(ev <<<"$SERVED")
F=$(open_tab "$BASE/?app=$APP")
ev <<<"$SERVED" >/dev/null
G=$(open_tab "$BASE/?app=$APP")
ev <<<"$SERVED" >/dev/null
for t in "$F" "$G"; do
  ab tab "$t" >/dev/null
  ev <<<"window.r = echo.barrier('crash-$RUN').then(() => echo.enqueueRetrying({ mutationId: 'crash-after-commit-1', name: 't', args: {} })); return true" >/dev/null
done
ab tab "$L" >/dev/null
ev <<<"echo.release('crash-$RUN'); return true" >/dev/null
results="{}"
for t in "$F" "$G"; do
  ab tab "$t" >/dev/null
  results=$(jq -c --arg t "$t" --argjson r "$(ev <<<'return await window.r')" '. + {($t): $r}' <<<"$results")
done
after=$(ev <<<"const rows = await echo.exec(\"SELECT seq FROM echo_outbox WHERE mutation_id = 'crash-after-commit-1'\")
const next = await echo.enqueue({ mutationId: 'after-crash', name: 't', args: {} })
return { rows: rows.value, next: next.value.seq, leader: (await echo.status()).leaderId }")
check "crash after commit: both tabs' retries return the committed seq, one row, no seq consumed" "$(jq -c --argjson l "$l" --argjson a "$after" '
  (to_entries | map(.value)) as $r |
  { pass: (all($r[]; .ok) and ($r | map(.value.seq) | unique | length) == 1 and ($a.rows | length) == 1
          and $a.rows[0].seq == $r[0].value.seq and $a.next == $r[0].value.seq + 1
          and ($r | map(.attempts) | max) >= 2 and $a.leader != $l.leaderId and $l.isLeaderTab),
    attempts: ($r | map(.attempts)), seqs: ($r | map(.value.seq)), rows: $a.rows, next: $a.next }' <<<"$results")"
for t in "$L" "$F" "$G"; do ab tab close "$t" >/dev/null; done

# ── worker-only crash: the tab outlives its leader worker and must stop claiming leadership ──────────
# Tab L's worker ends itself after committing L's own enqueue (`crashAfterCommit`); tab L stays open, F's
# worker is elected, and L reaches F's leader through the router. Every emission of L's continuous status
# stream that names another epoch must have `isLeaderTab` false, and L must end following F's leader.
# The `holdEpochWatch` run delays L's epoch-lock callbacks until F's leader is connected, so L learns of
# its own epoch's end last (the order a slow lock callback produces).
worker_crash() { # $1 = label, $2 = extra page flags
  APP="workercrash-${#2}-$RUN"
  L=$(open_tab "$BASE/?app=$APP&crashAfterCommit$2")
  l=$(ev <<<"$SERVED")
  F=$(open_tab "$BASE/?app=$APP")
  ev <<<"$SERVED" >/dev/null
  ab tab "$L" >/dev/null
  ev <<<"echo.watchStatus(); await echo.enqueue({ mutationId: 'crash-after-commit-worker-only', name: 't', args: {} }); return true" >/dev/null
  ab tab "$F" >/dev/null
  f=$(ev <<<"return await echo.until(async () => { const s = await echo.status(); return s.isLeaderTab && s.leaderId !== undefined ? s : undefined }, 15000) ?? null")
  ab tab "$L" >/dev/null
  # Connected to F's leader; with `holdEpochWatch`, L has still not seen its own epoch end.
  connected=$(ev <<<"return (await echo.until(() => echo.statusLog.some((s) => s.leaderId === '$(jq -r .leaderId <<<"$f")'), 15000)) === true")
  ev <<<"echo.releaseEpochWatch(); await new Promise((r) => setTimeout(r, 300)); return true" >/dev/null
  log=$(ev <<<"return echo.statusLog")
  check "worker-only crash ($1): every status emission naming another epoch has isLeaderTab false, and the tab follows the new leader" "$(jq -c --argjson l "$l" --argjson f "$f" --argjson c "$connected" --argjson log "$log" '
    ($log | map(select(.leaderId != null and .leaderId != $l.leaderId))) as $others |
    { pass: ($l.isLeaderTab and $f != null and $f.isLeaderTab and $c and ($log | any(.isLeaderTab))
            and ($others | length) > 0 and ($others | all(.isLeaderTab | not))
            and ($log[-1].leaderId == $f.leaderId) and ($log[-1].isLeaderTab | not)),
      log: ($log | map({ isLeaderTab, leader: (.leaderId // "-")[0:8] })), old: $l.leaderId[0:8], new: ($f.leaderId // "-")[0:8] }' <<<"{}")"
  for t in "$L" "$F"; do ab tab close "$t" >/dev/null; done
}
worker_crash "lock callback on time" ""
worker_crash "own epoch end seen last" "&holdEpochWatch"

# ── a candidate that cannot open OPFS releases its locks; the next tab becomes leader ────────────────
APP="failopen-$RUN"
LOCK="echo:$APP:default:leader"
A=$(open_tab "$BASE/?app=$APP&failOpen")
ev <<<"return await echo.until(async () => (await navigator.locks.query()).held.some((l) => l.name === '$LOCK'))" >/dev/null
B=$(open_tab "$BASE/?app=$APP")
ev <<<"return await echo.until(async () => (await navigator.locks.query()).pending.some((l) => l.name === '$LOCK'))" >/dev/null
ev <<<"echo.release('fail-open'); return true" >/dev/null
b=$(ev <<<"$SERVED")
ab tab "$A" >/dev/null
a=$(ev <<<"$SERVED")
check "OPFS startup failure: the failed candidate releases the leader lock, the next tab leads and serves both" "$(jq -nc --argjson a "$a" --argjson b "$b" '
  { pass: ($b.isLeaderTab and ($a.isLeaderTab | not) and $b.leaderId != null and $a.served == $b.leaderId), a: $a, b: $b }')"
ab tab close "$A" >/dev/null
ab tab close "$B" >/dev/null

# ── saved means durable ──────────────────────────────────────────────────────────────────────────────
APP="durable-$RUN"
T=$(open_tab "$BASE/?app=$APP")
ev <<<'return await echo.enqueue({ mutationId: "survives-reload", name: "t", args: { n: 1 } })' >/dev/null
ab reload >/dev/null
check "saved means durable: enqueued entry visible after reload" "$(ev <<<'await echo.ready()
const r = await echo.exec("SELECT mutation_id, args FROM echo_outbox")
return { pass: r.ok && r.value.some((x) => x.mutation_id === "survives-reload" && x.args === "{\"n\":1}"), r }')"

# Hard kill: closing the only tab terminates the leader worker mid-burst. Every enqueue that resolved
# before the kill must be in the outbox afterwards, with contiguous seqs.
resolved=$(ev <<<'window.resolved = []
for (let i = 0; i < 400; i++) { const id = "kill-" + i; echo.enqueue({ mutationId: id, name: "t", args: {} }).then((r) => r.ok && resolved.push(id)) }
await echo.until(() => resolved.length >= 10)
return resolved')
ab tab close "$T" >/dev/null
T=$(open_tab "$BASE/?app=$APP")
check "saved means durable: every enqueue resolved before a hard kill is in the outbox ($(jq length <<<"$resolved") resolved)" "$(ev <<<"const resolved = $resolved
await echo.ready()
const r = await echo.exec(\"SELECT mutation_id, seq FROM echo_outbox WHERE mutation_id LIKE 'kill-%' ORDER BY seq\")
const have = new Set(r.value.map((x) => x.mutation_id))
const seqs = r.value.map((x) => x.seq)
return { pass: resolved.length >= 10 && resolved.every((id) => have.has(id)) && seqs.every((s, i) => i === 0 || s === seqs[i - 1] + 1),
  resolved: resolved.length, present: r.value.length }")"
ab tab close "$T" >/dev/null

# ── gap-free outbox ordering under concurrent tabs ───────────────────────────────────────────────────
APP="order-$RUN"
A=$(open_tab "$BASE/?app=$APP")
ev <<<"$SERVED" >/dev/null
B=$(open_tab "$BASE/?app=$APP")
ev <<<"$SERVED" >/dev/null
for t in "$A" "$B"; do
  ab tab "$t" >/dev/null
  ev <<<"const tag = '$t'
window.run = echo.barrier('order-$RUN').then(async () => {
  const out = []
  for (let i = 0; i < 150; i++) {
    const r = await echo.enqueue({ mutationId: tag + '-' + i, name: 't', args: { i } })
    out.push(r.ok ? r.value.seq : null)
    if (i === 75) { const d = await echo.enqueue({ mutationId: tag + '-0', name: 't', args: {} }); out.dup = [out[0], d.ok ? d.value.seq : null] }
  }
  return out
})
return true" >/dev/null
done
C=$(open_tab "$BASE/?app=$APP")
ev <<<"$SERVED" >/dev/null
ev <<<"echo.release('order-$RUN'); return true" >/dev/null
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
for t in "$A" "$B" "$C"; do ab tab close "$t" >/dev/null; done

# ── reactive query re-emits after a cross-tab write ─────────────────────────────────────────────────
# (The pair_a/pair_b agreement is a sanity check only: "no write interleaves between the statements of one
# emission" holds by construction (withTransaction leases the single connection), and removing the
# transaction does not make this check fail. See docs/browser-substrate.md.)
APP="reactive-$RUN"
A=$(open_tab "$BASE/?app=$APP")
ev <<<'await echo.exec("CREATE TABLE pair_a (id INTEGER PRIMARY KEY)")
await echo.exec("CREATE TABLE pair_b (id INTEGER PRIMARY KEY)")
await echo.exec("CREATE TRIGGER pair AFTER INSERT ON pair_a BEGIN INSERT INTO pair_b (id) VALUES (new.id); END")
return true' >/dev/null
B=$(open_tab "$BASE/?app=$APP")
ev <<<'window.sub = echo.subscribe(["SELECT count(*) AS n FROM pair_a", "SELECT count(*) AS n FROM pair_b"], ["pair_a", "pair_b"])
return await echo.until(() => sub.emissions.length >= 1)' >/dev/null
ab tab "$A" >/dev/null
ev <<<'await Promise.all(Array.from({ length: 6 }, async () => { for (let i = 0; i < 50; i++) await echo.exec("INSERT INTO pair_a DEFAULT VALUES") })); return true' >/dev/null
ab tab "$B" >/dev/null
check "reactivity: a live query in tab B re-emits after writes from tab A and converges to the final value" "$(ev <<<'await echo.until(() => sub.emissions.at(-1)?.results[0][0].n === 300)
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

# ── sync loop: a fatal push halts push only; export stays available; a new leader re-trips once ────────
# The harness's fake SyncRpcs server (fakeSync.ts) refuses every push with MalformedEnvelope, answers pull
# with an empty PullDone, and ends its pokes stream after one poke. Counters are per client id.
APP="sync-$RUN"
SYNC="syncUrl=$BASE/fake-sync/rpc"
A=$(open_tab "$BASE/?app=$APP&$SYNC")
HALTED='await echo.until(async () => { const s = await echo.syncStatus(); return s.ok && s.value._tag === "Some" && s.value.value.push._tag === "Halted" })'
STATS='const stats = async () => fetch("/fake-sync/stats?clientId=" + (await echo.info()).value.clientId).then((r) => r.json())'
check "sync: MalformedEnvelope halts push; pull and pokes reconnects continue; pending export unaffected" "$(ev <<<"await echo.ready()
const e = await echo.enqueue({ mutationId: 'halt-me', name: 't', args: {} })
const halted = $HALTED
$STATS
const at = await stats()
// Two more pokes connections means two more wakes, so two more pull cycles; no further push.
const later = await echo.until(async () => { const s = await stats(); return s.pokes >= at.pokes + 2 && s.pulls > at.pulls ? s : undefined })
const exp = await echo.exportPending()
const status = await echo.syncStatus()
return { pass: e.ok && halted === true && later !== undefined && later.pushes === 1 && at.pushes === 1
  && exp.ok && exp.value.map((x) => x.envelope.mutationId).join() === 'halt-me' && status.value.value.lastError === null,
  at, later, exp: exp.ok ? exp.value.length : exp, status }")"
B=$(open_tab "$BASE/?app=$APP&$SYNC")
before=$(ev <<<"$SERVED")
ab tab close "$A" >/dev/null
ab tab "$B" >/dev/null
check "sync: after handover the new leader pushes once, halts again, and still exports the entry" "$(ev <<<"await echo.until(async () => { const s = await echo.status(); return s.isLeaderTab && s.leaderId !== $(jq '.leaderId' <<<"$before") })
const halted = $HALTED
$STATS
const s = await stats(); const exp = await echo.exportPending()
return { pass: halted === true && s.pushes === 2 && exp.ok && exp.value.map((x) => x.envelope.mutationId).join() === 'halt-me', s, exp }")"
ab tab close "$B" >/dev/null

# ── database path length (DESIGN §9): the OPFS pool, not wa-sqlite's default mxPathname, is the bound ──
# A UUID-length profile (the HeyBabe trial's identity shape) opens and keeps a write across a reload.
UUID_PROFILE=$(cat /proc/sys/kernel/random/uuid)
T=$(open_tab "$BASE/?app=heybabe-trial&profile=$UUID_PROFILE")
w=$(ev <<<'await echo.ready(); return await echo.enqueue({ mutationId: "uuid-profile-1", name: "t", args: {} })')
ab reload >/dev/null
check "a 36-character profile opens its replica and keeps a write across a reload" "$(ev <<<"await echo.ready()
const r = await echo.exec(\"SELECT mutation_id FROM echo_outbox WHERE mutation_id = 'uuid-profile-1'\")
const i = await echo.info()
return { pass: $(jq '.ok' <<<"$w") && r.ok && r.value.length === 1 && i.ok, write: $w, rows: r, dbName: i.ok ? i.value.dbName : i }")"
ab tab close "$T" >/dev/null

# The bound, from the pinned AccessHandlePoolVFS: a stored path ("/" + SQLite's file name) must stay under
# its 512-byte header, and the longest file name is "<db>-journal". So the longest db name is
# 512 - 1 - 1 - 8 = 502 characters, i.e. appId + profile = 502 - len("echo-sync//.db") = 488.
APP="bound-$RUN"
PROFILE=$(printf 'p%.0s' $(seq 1 $((488 - ${#APP}))))
T=$(open_tab "$BASE/?app=$APP&profile=$PROFILE")
w=$(ev <<<'await echo.ready(); return await echo.enqueue({ mutationId: "longest-path-1", name: "t", args: {} })')
ab reload >/dev/null
longest=$(ev <<<"await echo.ready()
const r = await echo.exec(\"SELECT mutation_id FROM echo_outbox WHERE mutation_id = 'longest-path-1'\")
const i = await echo.info()
return { ok: $(jq '.ok' <<<"$w") && r.ok && r.value.length === 1, dbNameLength: i.ok ? i.value.dbName.length : null }")
ab tab close "$T" >/dev/null
T=$(open_tab "$BASE/?app=$APP&profile=${PROFILE}p")
over=$(ev <<<"const r = await echo.info(); const s = await echo.status()
const root = await navigator.storage.getDirectory()
const app = await root.getDirectoryHandle('echo-sync').then((d) => d.getDirectoryHandle('$APP')).catch(() => undefined)
const names = []
if (app !== undefined) for await (const name of app.keys()) names.push(name.length)
return { mode: s.mode, reason: r.ok ? null : r.error.reason, message: r.ok ? null : r.error.message, poolNameLengths: names }")
check "the longest database path the pool holds opens and round-trips; one character more is refused before OPFS is touched" "$(jq -nc --argjson l "$longest" --argjson o "$over" --arg p "${#PROFILE}" '
  { pass: ($l.ok and $l.dbNameLength == 502 and $o.mode == "unsupported" and $o.reason == "Unsupported"
          and ($o.message | contains("502")) and ($o.poolNameLengths | all(. == ($p | tonumber)))),
    longest: $l, over: $o }')"
ab tab close "$T" >/dev/null

# ── identities that would escape the storage namespace are refused up front ──────────────────────────
D=$(open_tab "$BASE/?app=.")
check "identity '.' is refused with LeaderUnavailable{Unsupported}, nothing is opened" "$(ev <<<'const r = await echo.info(); const s = await echo.status()
return { pass: s.mode === "unsupported" && !r.ok && r.error.reason === "Unsupported", s, r }')"
ab tab close "$D" >/dev/null

ab close >/dev/null
echo "$((total - fails))/$total passed"
exit $((fails > 0))
