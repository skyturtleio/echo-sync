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
  ev <<<"echo.watchStatus(); echo.watchSyncStatus(); await echo.until(() => echo.syncStatusLog.length > 0); await echo.enqueue({ mutationId: 'crash-after-commit-worker-only', name: 't', args: {} }); return true" >/dev/null
  ab tab "$F" >/dev/null
  f=$(ev <<<"return await echo.until(async () => { const s = await echo.status(); return s.isLeaderTab && s.leaderId !== undefined ? s : undefined }, 15000) ?? null")
  ab tab "$L" >/dev/null
  # Connected to F's leader; with `holdEpochWatch`, L has still not seen its own epoch end.
  connected=$(ev <<<"return (await echo.until(() => echo.statusLog.some((s) => s.leaderId === '$(jq -r .leaderId <<<"$f")'), 15000)) === true")
  ev <<<"echo.releaseEpochWatch(); await new Promise((r) => setTimeout(r, 300)); return true" >/dev/null
  log=$(ev <<<"return echo.statusLog")
  # syncStatus (DESIGN §9.1): the stream never ends, and it emits again once the tab follows F's leader.
  sync=$(ev <<<"const f = echo.statusLog.findIndex((s) => s.leaderId === '$(jq -r '.leaderId // empty' <<<"$f")')
await echo.until(() => echo.syncStatusLog.some((e) => e.afterStatus > f), 15000)
return { firstOnNew: f, entries: echo.syncStatusLog.map((e) => ({ after: e.afterStatus, pending: e.value.pendingMutations, phase: e.value.phase, synced: e.value.lastSyncedAt !== null })), ended: echo.syncWatch.ended ?? null }")
  check "worker-only crash ($1): every status emission naming another epoch has isLeaderTab false, the tab follows the new leader, and syncStatus survives into it" "$(jq -c --argjson l "$l" --argjson f "$f" --argjson c "$connected" --argjson log "$log" --argjson sync "$sync" '
    ($log | map(select(.leaderId != null and .leaderId != $l.leaderId))) as $others |
    { pass: ($l.isLeaderTab and $f != null and $f.isLeaderTab and $c and ($log | any(.isLeaderTab))
            and ($others | length) > 0 and ($others | all(.isLeaderTab | not))
            and ($log[-1].leaderId == $f.leaderId) and ($log[-1].isLeaderTab | not)
            and $sync.ended == null and $sync.firstOnNew >= 0
            and ($sync.entries | any(.after <= $sync.firstOnNew)) and ($sync.entries | any(.after > $sync.firstOnNew))),
      sync: $sync,
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
# 512 - 1 - 1 - 8 = 502 characters, i.e. appId + profile = 502 - len("echo-sync//.db") = 488. Each of them is
# also an OPFS directory name, at most 255 UTF-8 bytes (WebKit's limit, DESIGN §16 item 1): 233 + 255.
pad() { printf '%s' "$1"; printf "${2}%.0s" $(seq 1 $(($3 - ${#1}))); }
APP=$(pad "bound-$RUN" a 233)
PROFILE=$(pad "" p 255)
T=$(open_tab "$BASE/?app=$APP&profile=$PROFILE")
w=$(ev <<<'await echo.ready(); return await echo.enqueue({ mutationId: "longest-path-1", name: "t", args: {} })')
ab reload >/dev/null
longest=$(ev <<<"await echo.ready()
const r = await echo.exec(\"SELECT mutation_id FROM echo_outbox WHERE mutation_id = 'longest-path-1'\")
const i = await echo.info()
return { ok: $(jq '.ok' <<<"$w") && r.ok && r.value.length === 1, dbNameLength: i.ok ? i.value.dbName.length : null }")
ab tab close "$T" >/dev/null
# A refused identity: its status, error, and whether echo-sync/<app> exists in OPFS at all.
refused() {
  ev <<<"const r = await echo.info(); const s = await echo.status()
const root = await navigator.storage.getDirectory()
const app = await root.getDirectoryHandle('echo-sync').then((d) => d.getDirectoryHandle('$1')).catch(() => undefined)
return { mode: s.mode, reason: r.ok ? null : r.error.reason, message: r.ok ? null : r.error.message, appDirExists: app !== undefined }"
}
# One character over the combined bound (234 + 255), and a 256-byte profile whose whole path fits.
T=$(open_tab "$BASE/?app=${APP}a&profile=$PROFILE")
over=$(refused "${APP}a")
ab tab close "$T" >/dev/null
SEG_APP="seg-$RUN"
T=$(open_tab "$BASE/?app=$SEG_APP&profile=${PROFILE}p")
seg=$(refused "$SEG_APP")
ab tab close "$T" >/dev/null
check "the longest database path the pool holds (appId 233 + profile 255) opens and round-trips; one character more, or a 256-byte profile with a short path, is refused Unsupported before OPFS is touched" "$(jq -nc --argjson l "$longest" --argjson o "$over" --argjson g "$seg" '
  { pass: ($l.ok and $l.dbNameLength == 502
          and $o.mode == "unsupported" and $o.reason == "Unsupported" and ($o.message | contains("502")) and ($o.appDirExists | not)
          and $g.mode == "unsupported" and $g.reason == "Unsupported" and ($g.message | contains("profile is 256 bytes")) and ($g.appDirExists | not)),
    longest: $l, over: $o, segment: $g }')"

# ── resetReplica (DESIGN §9.1): guarded by the leader lock and the outbox, deletes only its identity ──
# Profile "a" syncs with the fake server, whose push always halts, so its entry stays pending. Profile "b"
# is a sibling identity of the same app. The resets run from a tab of an unrelated app (no client for "a").
# A's page never hears the reset signal (`ignoreReset`, as an old build or a frozen tab), so its leader lives
# on through the first reset (DESIGN §16.24).
APP="reset-$RUN"
A=$(open_tab "$BASE/?app=$APP&profile=a&syncUrl=$BASE/fake-sync/rpc&ignoreReset")
a0=$(ev <<<'await echo.ready(); const e = await echo.enqueue({ mutationId: "pending-a", name: "t", args: {} }); const i = await echo.info(); return { ok: e.ok, clientId: i.ok ? i.value.clientId : null }')
B=$(open_tab "$BASE/?app=$APP&profile=b&syncUrl=$BASE/fake-sync/rpc")
b0=$(ev <<<'await echo.ready(); await echo.exec("CREATE TABLE keep (v TEXT)"); await echo.exec("INSERT INTO keep VALUES (?)", ["b-row"]); const i = await echo.info(); return { clientId: i.ok ? i.value.clientId : null }')
ab tab close "$B" >/dev/null
R=$(open_tab "$BASE/?app=resetter-$RUN")
ev <<<'await echo.ready(); return true' >/dev/null
TARGET="{ appId: '$APP', profile: 'a' }"
# Retries only ReplicaInUse, for the moment between a closed tab and the browser releasing its worker's lock.
RESET="const reset = async (discard) => { let r; for (let i = 0; i < 100; i++) { r = await echo.resetReplica($TARGET, discard); if (r.ok || r.error._tag !== 'ReplicaInUse') return r; await new Promise((res) => setTimeout(res, 50)) } return r }"
check "a leader that ignores the reset signal yields ReplicaInUse after closeTimeout (1 s), deletes nothing, and leaves no queued leader request" "$(ev <<<"const before = await echo.opfsList('echo-sync/$APP/a')
const r = await echo.resetReplica($TARGET, false, undefined, 1000)
const after = await echo.opfsList('echo-sync/$APP/a')
const queued = await echo.until(async () => { const l = await echo.locksFor($TARGET); return l.pending.includes('exclusive:echo:$APP:a:leader') ? undefined : l }, 3000)
return { pass: !r.ok && r.error._tag === 'ReplicaInUse' && r.ms >= 1000 && before !== null && before.length > 0 && JSON.stringify(after) === JSON.stringify(before)
  && queued !== undefined && queued.held.includes('exclusive:echo:$APP:a:leader'), r, before, after, locks: queued ?? null }")"
ab tab "$A" >/dev/null
alive=$(ev <<<'const r = await echo.exec("SELECT mutation_id FROM echo_outbox WHERE released_at IS NULL"); return r.ok && r.value.length === 1')
ab tab close "$A" >/dev/null
ab tab "$R" >/dev/null
check "reset with one pending write fails PendingWrites{1} and deletes nothing" "$(ev <<<"$RESET
const before = await echo.opfsList('echo-sync/$APP/a')
const r = await reset(false)
const after = await echo.opfsList('echo-sync/$APP/a')
return { pass: $alive && $(jq '.ok' <<<"$a0") && !r.ok && r.error._tag === 'PendingWrites' && r.error.count === 1 && before !== null && JSON.stringify(after) === JSON.stringify(before), r, before, after }")"
check "reset with discardPending removes echo-sync/<appId>/a and leaves sibling profile b intact" "$(ev <<<"$RESET
const r = await reset(true)
const app = await echo.opfsList('echo-sync/$APP')
const b = await echo.opfsList('echo-sync/$APP/b')
return { pass: r.ok && app !== null && !app.includes('a') && app.includes('b') && b !== null && b.length > 0, r, app, b }")"
ab tab close "$R" >/dev/null
A=$(open_tab "$BASE/?app=$APP&profile=a&syncUrl=$BASE/fake-sync/rpc")
check "a leader started after the reset has an empty replica and a fresh clientId" "$(ev <<<"await echo.ready()
const i = await echo.info(); const ob = await echo.exec('SELECT count(*) AS n FROM echo_outbox')
return { pass: i.ok && i.value.clientId !== $(jq '.clientId' <<<"$a0") && ob.ok && ob.value[0].n === 0, clientId: i.ok ? i.value.clientId : i, before: $(jq '.clientId' <<<"$a0"), ob }")"
ab tab close "$A" >/dev/null
B=$(open_tab "$BASE/?app=$APP&profile=b&syncUrl=$BASE/fake-sync/rpc")
check "the sibling identity keeps its rows and clientId through the reset" "$(ev <<<"await echo.ready()
const i = await echo.info(); const k = await echo.exec('SELECT v FROM keep')
return { pass: i.ok && i.value.clientId === $(jq '.clientId' <<<"$b0") && k.ok && k.value[0]?.v === 'b-row', i, k }")"
ab tab close "$B" >/dev/null

# Profile "c": a replica with a pending write whose database no longer opens (header overwritten). Without
# discardPending the outbox cannot be read, so the reset fails closed; with it, the database is never opened.
C=$(open_tab "$BASE/?app=$APP&profile=c&syncUrl=$BASE/fake-sync/rpc")
ev <<<'await echo.ready(); await echo.enqueue({ mutationId: "pending-c", name: "t", args: {} }); return true' >/dev/null
ab tab close "$C" >/dev/null
R=$(open_tab "$BASE/?app=resetter2-$RUN")
TARGET_C="{ appId: '$APP', profile: 'c' }"
RESET_C="const resetC = async (discard) => { let r; for (let i = 0; i < 100; i++) { r = await echo.resetReplica($TARGET_C, discard); if (r.ok || r.error._tag !== 'ReplicaInUse') return r; await new Promise((res) => setTimeout(res, 50)) } return r }"
check "reset of a replica whose database does not open fails ResetFailed without discardPending and deletes nothing" "$(ev <<<"$RESET_C
await echo.until(async () => !(await navigator.locks.query()).held.some((l) => l.name === 'echo:$APP:c:leader'))
const corrupted = await echo.corruptReplica('$APP', 'c')
const before = await echo.opfsList('echo-sync/$APP/c')
const r = await resetC(false)
const after = await echo.opfsList('echo-sync/$APP/c')
return { pass: corrupted && !r.ok && r.error._tag === 'ResetFailed' && before !== null && JSON.stringify(after) === JSON.stringify(before), corrupted, r, before, after }")"
check "reset with discardPending never opens the database: the unopenable replica is removed" "$(ev <<<"$RESET_C
const r = await resetC(true)
const app = await echo.opfsList('echo-sync/$APP')
return { pass: r.ok && app !== null && !app.includes('c') && app.includes('b'), r, app }")"
check "a spawnLeader or worker.postMessage that throws surfaces as ResetFailed, not a defect" "$(ev <<<"const s = await echo.resetReplica($TARGET_C, false, 'spawnThrows')
const p = await echo.resetReplica($TARGET_C, false, 'postThrows')
return { pass: !s.ok && s.error._tag === 'ResetFailed' && s.error.message.includes('injected spawnLeader failure')
  && !p.ok && p.error._tag === 'ResetFailed' && p.error.message.includes('injected postMessage failure'), s, p }")"
ab tab close "$R" >/dev/null

# ── coordinated reset (DESIGN §16.24): resetReplica closes the identity's clients itself ─────────────
# Two tabs hold clients of one identity (the leader in one of them) and a third holds a sibling profile. One
# of the two calls resetReplica with no app-side preparation. On main this fails ReplicaInUse (nothing closes
# the leader), or, in single-tab mode, the queued candidate is elected and re-creates the replica.
# `RESET_STATE` reports this page's client after the signal: its status, a `mutate` attempt, its open raw
# subscription and its `syncStatus` stream (if it had them). A raw subscription ends on any leader change, so in
# a tab whose leader died before its own copy of the signal arrived it may legitimately end `LeaderLost`;
# `syncStatus` resubscribes across `LeaderLost` and must end `Reset` either way.
RESET_STATE='await echo.until(() => (window.sub === undefined || window.sub.ended !== undefined) && (echo.syncStatusLog.length === 0 || echo.syncWatch.ended !== undefined), 5000)
const s = await echo.status(); const m = await echo.mutate()
return { status: s, last: echo.statusLog.at(-1) ?? null, statusEnded: echo.statusWatch.ended ?? null, mutate: m,
  hadSub: window.sub !== undefined, sub: window.sub?.ended ?? null, hadSync: echo.syncStatusLog.length > 0, sync: echo.syncWatch.ended ?? null }'
IS_RESET='(.status.reset and (.status.isLeaderTab | not) and .status.leaderId == null and .last.reset and .statusEnded == null
  and (.mutate.ok | not) and .mutate.error.reason == "Reset" and .mutate.ms < 1000
  and ((.hadSub | not) or (.sub | tostring | test("\"(Reset|LeaderLost)\"")))
  and ((.hadSync | not) or (.sync | tostring | contains("\"Reset\""))))'
coordinated_reset() { # $1 = label, $2 = which tab resets (leader|follower), $3 = extra page flags
  APP="coord-$2-${#3}-$RUN"
  P=$(open_tab "$BASE/?app=$APP$3")
  p=$(ev <<<"$SERVED")
  p_client=$(ev <<<'return (await echo.info()).value.clientId')
  ev <<<'await echo.exec("CREATE TABLE coord_rows (id INTEGER PRIMARY KEY)"); await echo.exec("INSERT INTO coord_rows DEFAULT VALUES")
window.sub = echo.subscribe(["SELECT count(*) AS n FROM coord_rows"], ["coord_rows"]); await echo.until(() => sub.emissions.length >= 1); echo.watchStatus(); echo.watchSyncStatus(); await echo.until(() => echo.syncStatusLog.length > 0); return true' >/dev/null
  Q=$(open_tab "$BASE/?app=$APP$3")
  if [[ "$3" == *noSharedWorker* ]]; then
    # Single-tab mode: Q's candidate queues behind P's leader and Q is not served.
    q=$(ev <<<"await echo.until(async () => (await echo.locksFor()).pending.includes('exclusive:echo:$APP:default:leader')); echo.watchStatus(); return await echo.status()")
  else
    q=$(ev <<<"$SERVED")
    ev <<<'window.sub = echo.subscribe(["SELECT count(*) AS n FROM coord_rows"], ["coord_rows"]); await echo.until(() => sub.emissions.length >= 1); echo.watchStatus(); echo.watchSyncStatus(); await echo.until(() => echo.syncStatusLog.length > 0); return true' >/dev/null
  fi
  S=$(open_tab "$BASE/?app=$APP&profile=other$3")
  s0=$(ev <<<"$SERVED")
  if [ "$2" = leader ]; then X=$P; Y=$Q; else X=$Q; Y=$P; fi
  ab tab "$X" >/dev/null
  r=$(ev <<<"const r = await echo.resetReplica(); return { ...r, dir: await echo.opfsList('echo-sync/$APP/default'), app: await echo.opfsList('echo-sync/$APP') }")
  x=$(ev <<<"$RESET_STATE")
  ab tab "$Y" >/dev/null
  y=$(ev <<<"$RESET_STATE")
  check "coordinated reset ($1): resetReplica from the $2 tab, with no app-side preparation, removes only this profile's replica" \
    "$(jq -nc --argjson r "$r" --argjson p "$p" '{ pass: ($r.ok and $r.dir == null and ($r.app | any(.[]; . == "other")) and $p.isLeaderTab), r: $r }')"
  check "coordinated reset ($1): both clients enter the reset state (status says reset and stays open, mutate fails Reset at once, syncStatus ends Reset)" \
    "$(jq -nc --argjson x "$x" --argjson y "$y" "{ pass: ((\$x | $IS_RESET) and (\$y | $IS_RESET)), resetting: \$x, other: \$y }")"
  # Absence over a window: nothing re-elects, re-creates the directory, or holds or queues a lock of the identity.
  quiet=$(ev <<<"const until = Date.now() + 1500; let seen = null
while (Date.now() < until && seen === null) {
  const l = await echo.locksFor(); const d = await echo.opfsList('echo-sync/$APP/default'); const s = await echo.status()
  if (l.held.length > 0 || l.pending.length > 0 || d !== null || !s.reset || s.leaderId !== undefined) seen = { l, d, s }
  await new Promise((res) => setTimeout(res, 50))
}
return { seen }")
  check "coordinated reset ($1): no leader is elected and the identity holds or queues no Web Lock (navigator.locks.query) until a tab builds a new client" \
    "$(jq -nc --argjson q "$quiet" '{ pass: ($q.seen == null), quiet: $q }')"
  ab tab "$S" >/dev/null
  s1=$(ev <<<"const s = await echo.status(); const i = await echo.info(); const e = await echo.exec('SELECT 1 AS one'); return { ...s, served: i.ok ? i.value.leaderId : null, exec: e.ok }")
  check "coordinated reset ($1): the client of another profile of the same app is unaffected" \
    "$(jq -nc --argjson a "$s0" --argjson b "$s1" '{ pass: (($b.reset | not) and $b.leaderId == $a.leaderId and $b.served == $a.leaderId and $b.exec), before: $a, after: $b }')"
  # A new client (Y reloads): elected on a fresh, empty replica. X's old client stays reset.
  ab tab "$Y" >/dev/null
  ab reload >/dev/null
  fresh=$(ev <<<"$SERVED")
  f=$(ev <<<"const i = await echo.info(); const ob = await echo.exec('SELECT count(*) AS n FROM echo_outbox'); const t = await echo.exec(\"SELECT name FROM sqlite_master WHERE name = 'coord_rows'\")
return { clientId: i.ok ? i.value.clientId : null, outbox: ob.ok ? ob.value[0].n : null, items: t.ok ? t.value.length : null }")
  ab tab "$X" >/dev/null
  xs=$(ev <<<'return await echo.status()')
  old=$(jq -nc --argjson p "$p" '$p.leaderId')
  check "coordinated reset ($1): a client built after the signal elects a leader on a fresh, empty replica; the reset client stays reset" \
    "$(jq -nc --argjson n "$fresh" --argjson f "$f" --argjson x "$xs" --argjson old "$old" --argjson c "$p_client" '{ pass: ($n.isLeaderTab and $n.leaderId != $old and $f.clientId != $c and ($n.reset | not) and $f.outbox == 0 and $f.items == 0 and $x.reset and $x.leaderId == null), fresh: $n, replica: $f, resetClient: $x }')"
  for t in "$P" "$Q" "$S"; do ab tab close "$t" >/dev/null; done
}
coordinated_reset "leader tab resets" leader ""
coordinated_reset "follower tab resets" follower ""
coordinated_reset "single-tab mode, queued candidate" follower "&noSharedWorker"

# PendingWrites refusal: the clients stay reset, the lock is free, nothing is deleted, and the following
# discardPending reset succeeds at once (no ReplicaInUse, no retry).
APP="coord-pending-$RUN"
P=$(open_tab "$BASE/?app=$APP&syncUrl=$BASE/fake-sync/rpc")
ev <<<'await echo.ready(); await echo.enqueue({ mutationId: "pending-coord", name: "t", args: {} }); echo.watchStatus(); return true' >/dev/null
Q=$(open_tab "$BASE/?app=$APP&syncUrl=$BASE/fake-sync/rpc")
ev <<<"$SERVED" >/dev/null
ev <<<'echo.watchStatus(); return true' >/dev/null
r=$(ev <<<"const before = await echo.opfsList('echo-sync/$APP/default'); const r = await echo.resetReplica()
const free = await echo.until(async () => { const l = await echo.locksFor(); return l.held.length === 0 && l.pending.length === 0 ? l : undefined }, 3000)
return { r, before, after: await echo.opfsList('echo-sync/$APP/default'), free: free ?? await echo.locksFor() }")
q=$(ev <<<"$RESET_STATE")
ab tab "$P" >/dev/null
p=$(ev <<<"$RESET_STATE")
check "coordinated reset: PendingWrites{1} deletes nothing, leaves both clients reset and every lock of the identity free" \
  "$(jq -nc --argjson r "$r" --argjson p "$p" --argjson q "$q" "{ pass: ((\$r.r.ok | not) and \$r.r.error._tag == \"PendingWrites\" and \$r.r.error.count == 1
    and \$r.before != null and (\$r.before | length) > 0 and \$r.after == \$r.before and \$r.free.held == [] and \$r.free.pending == []
    and (\$p | $IS_RESET) and (\$q | $IS_RESET)), r: \$r, p: \$p, q: \$q }")"
check "coordinated reset: a following discardPending reset succeeds on its first call (no ReplicaInUse)" "$(ev <<<"const r = await echo.resetReplica(undefined, true)
const d = await echo.opfsList('echo-sync/$APP/default')
return { pass: r.ok && d === null, r, d }")"
for t in "$P" "$Q"; do ab tab close "$t" >/dev/null; done

# ── presence (DESIGN §9.1, §12): entry exactly while the tab stream is open; survives handover ────────
# Needs the real server (PresenceRpcs is mounted with SyncRpcs on /sync). L is the leader tab; F and W follow.
if [ "$SYNC_MODE" = real ]; then
  APP="presence-$RUN"
  ROOM="room-$RUN"
  curl -sf "$BASE/admin/grant?scope=$ROOM&principal=spike-user&role=writer" >/dev/null
  L=$(open_tab "$BASE/?app=$APP")
  l=$(ev <<<"$SERVED")
  F=$(open_tab "$BASE/?app=$APP")
  ftab=$(ev <<<"$SERVED" | jq -r .tabId)
  W=$(open_tab "$BASE/?app=$APP")
  ev <<<"$SERVED" >/dev/null
  LATEST="const latest = () => echo.presence.get('$ROOM')?.snapshots.at(-1) ?? []"
  ev <<<"echo.presenceOpen('$ROOM', 'w'); return true" >/dev/null
  ab tab "$F" >/dev/null
  ev <<<"echo.presenceOpen('$ROOM', 'f0'); await echo.until(() => (echo.presence.get('$ROOM')?.snapshots.length ?? 0) > 0); return (await echo.track('$ROOM', 'f1')).ok" >/dev/null
  ab tab "$W" >/dev/null
  shown=$(ev <<<"$LATEST
return (await echo.until(() => latest().includes('$ftab=\"f1\"'), 15000)) === true")
  ab tab "$F" >/dev/null
  ev <<<"echo.presence.get('$ROOM').close(); return true" >/dev/null
  ab tab "$W" >/dev/null
  gone=$(ev <<<"$LATEST
return (await echo.until(() => echo.presence.get('$ROOM').snapshots.length > 0 && !latest().some((e) => e.startsWith('$ftab=')), 15000)) === true")
  check "presence: a tab's entry is listed while its stream is open (with its tracked state) and gone once it closes" \
    "$(jq -nc --argjson s "$shown" --argjson g "$gone" '{ pass: ($s and $g), shown: $s, gone: $g }')"

  # F joins again and tracks f3; then the leader tab closes. The new leader re-joins F with f3 (not f2).
  ab tab "$F" >/dev/null
  ev <<<"echo.presenceOpen('$ROOM', 'f2'); await echo.until(() => (echo.presence.get('$ROOM')?.snapshots.length ?? 0) > 0); return (await echo.track('$ROOM', 'f3')).ok" >/dev/null
  ab tab "$W" >/dev/null
  ev <<<"$LATEST
await echo.until(() => latest().includes('$ftab=\"f3\"'), 15000); window.beforeHandover = echo.presence.get('$ROOM').snapshots.length; return true" >/dev/null
  ab tab close "$L" >/dev/null
  ab tab "$W" >/dev/null
  after=$(ev <<<"$LATEST
const old = $(jq '.leaderId' <<<"$l")
const s = await echo.until(async () => { const s = await echo.status(); const i = await echo.info(); return s.leaderId !== undefined && s.leaderId !== old && i.ok && i.value.leaderId === s.leaderId ? s : undefined }, 20000)
const n = window.beforeHandover
// A snapshot produced after the handover (the re-joins on the new leader) still lists F with f3, and W itself.
const ok = await echo.until(() => echo.presence.get('$ROOM').snapshots.length > n && latest().includes('$ftab=\"f3\"') && latest().some((e) => e.endsWith('=\"w\"')), 20000)
return { newLeader: s?.leaderId ?? null, ok: ok === true, latest: latest(), hist: echo.presence.get('$ROOM').snapshots.slice(n - 1), ended: echo.presence.get('$ROOM').ended ?? null }")
  ab tab "$F" >/dev/null
  fend=$(ev <<<"return echo.presence.get('$ROOM').ended ?? null")
  check "presence across handover: both streams stay open and the new leader re-joins F with its latest tracked state" \
    "$(jq -nc --argjson a "$after" --argjson f "$fend" '{ pass: ($a.ok and $a.newLeader != null and $a.ended == null and $f == null), after: $a, fEnded: $f }')"
  for t in "$F" "$W"; do ab tab close "$t" >/dev/null; done
fi

# ── router killed while tabs are connected: every tab is served again ────────────────────────────────
# WebKit ends the router SharedWorker with its creating tab, and the next `new SharedWorker` starts an empty
# instance. Here Chromium's DevTools protocol ends it while two tabs are connected; a third tab then relaunches
# it. The new instance broadcasts RouterStarted, every tab re-sends Hello and the leader tab re-announces.
# WEBKIT=1 has no CDP; the WebKit handover scenarios (router creator = leader tab closes) cover it there.
if [ "${WEBKIT:-0}" = 1 ]; then
  echo "SKIP router killed while tabs connected (Chromium only: needs CDP to end the SharedWorker)"
else
  APP="router-kill-$RUN"
  RA=$(open_tab "$BASE/?app=$APP")
  ra=$(ev <<<"$SERVED")
  RB=$(open_tab "$BASE/?app=$APP")
  rb=$(ev <<<"$SERVED")
  ev <<<"window.rsub = echo.subscribe(['SELECT 1 AS n'], []); await echo.until(() => rsub.emissions.length >= 1); return true" >/dev/null
  killed=$(bun "$(dirname "$0")/cdp/kill-router.ts" "$(ab get cdp-url)" "echo:$APP:default:router")
  RC=$(open_tab "$BASE/?app=$APP")
  rc=$(ev <<<"$SERVED")
  ab tab "$RB" >/dev/null
  rb2=$(ev <<<"const s = await echo.status(); const i = await echo.info()
return { ...s, served: i.ok ? i.value.leaderId : null, subEnded: rsub.ended ?? null, subEmissions: rsub.emissions.length }")
  check "router killed while tabs connected: a new tab is served by the same leader; connected tabs keep their connection and open subscription" \
    "$(jq -nc --argjson k "${killed:-null}" --argjson a "$ra" --argjson b "$rb" --argjson c "$rc" --argjson b2 "$rb2" '
      { pass: ($k.killed > 0 and $k.left == 0 and $a.leaderId != null and $c.served == $a.leaderId
               and $b2.leaderId == $a.leaderId and $b2.served == $a.leaderId and $b2.subEnded == null),
        killed: $k, a: $a, b: $b, c: $c, bAfter: $b2 }')"
  # Then the leader tab closes: the survivors (one of them the relaunched router's creator) agree on a new leader.
  RL=$(jq -r --arg a "$RA" --arg b "$RB" 'if .isLeaderTab then $a else $b end' <<<"$ra")
  ab tab close "$RL" >/dev/null
  after="[]"
  for t in "$RA" "$RB" "$RC"; do
    [ "$t" = "$RL" ] && continue
    ab tab "$t" >/dev/null
    s=$(ev <<<"const old = $(jq '.leaderId' <<<"$ra")
await echo.until(async () => { const s = await echo.status(); const i = await echo.info(); return s.leaderId !== undefined && s.leaderId !== old && i.ok && i.value.leaderId === s.leaderId })
const s = await echo.status(); const i = await echo.info(); return { ...s, served: i.ok ? i.value.leaderId : null }")
    after=$(jq -c --argjson s "$s" '. + [$s]' <<<"$after")
  done
  check "router killed, then the leader tab closes: both remaining tabs are served by one new leader" "$(jq -c --arg old "$(jq -r '.leaderId' <<<"$ra")" '
    { pass: (((map(.leaderId) | unique) as $ids | ($ids | length) == 1 and $ids[0] != $old and $ids[0] != null)
            and (map(select(.isLeaderTab)) | length) == 1 and all(.[]; .served == .leaderId)), statuses: . }' <<<"$after")"
  for t in "$RA" "$RB" "$RC"; do [ "$t" = "$RL" ] || ab tab close "$t" >/dev/null; done

  # A successor elected before its first Connected: B's Hello to the first router is held, that router is
  # killed and the leader tab A closes, so B is elected without ever having been connected (no epoch end to
  # react to). B must reconnect on its own election: relaunch the router, Hello, re-announce.
  APP="router-elect-$RUN"
  EA=$(open_tab "$BASE/?app=$APP")
  ea=$(ev <<<"$SERVED")
  EB=$(open_tab "$BASE/?app=$APP&holdRouterOut")
  ev <<<'await echo.until(() => echo.routerHeld().out >= 1); return true' >/dev/null
  killed=$(bun "$(dirname "$0")/cdp/kill-router.ts" "$(ab get cdp-url)" "echo:$APP:default:router")
  ab tab close "$EA" >/dev/null
  ab tab "$EB" >/dev/null
  eb=$(ev <<<"const elected = await echo.until(async () => (await echo.status()).isLeaderTab)
echo.flushRouter()
const served = await echo.until(async () => {
  const s = await echo.status()
  const i = await Promise.race([echo.info(), new Promise((r) => setTimeout(() => r({ ok: false }), 1000))])
  return s.leaderId !== undefined && i.ok && i.value.leaderId === s.leaderId ? s : undefined
})
return { elected: elected === true, served: served ?? null, spawns: echo.routerSpawns() }")
  check "router killed before a successor's first Connected: the newly elected leader tab reconnects and is served by its own leader" \
    "$(jq -nc --argjson k "${killed:-null}" --argjson a "$ea" --argjson b "$eb" '
      { pass: ($k.killed > 0 and $b.elected and $b.served != null and $b.served.isLeaderTab and $b.served.leaderId != $a.leaderId),
        killed: $k, a: $a, b: $b }')"
  ab tab close "$EB" >/dev/null

  # A Connected for the epoch a tab is already connected to, delivered late from a dead router instance:
  # B's first Connected (from R0) is buffered, R0 is killed, C relaunches it (R1) and R1 links B to the same
  # leader. The late R0 Connected is then delivered; it must not displace B's live connection.
  APP="router-dup-$RUN"
  DA=$(open_tab "$BASE/?app=$APP")
  da=$(ev <<<"$SERVED")
  DB=$(open_tab "$BASE/?app=$APP&holdRouterIn")
  ev <<<'await echo.until(() => echo.routerHeld().in >= 1); return true' >/dev/null
  killed=$(bun "$(dirname "$0")/cdp/kill-router.ts" "$(ab get cdp-url)" "echo:$APP:default:router")
  DC=$(open_tab "$BASE/?app=$APP")
  ev <<<"$SERVED" >/dev/null
  ab tab "$DB" >/dev/null
  db=$(ev <<<"$SERVED")
  db2=$(ev <<<"window.dsub = echo.subscribe(['SELECT 1 AS n'], []); await echo.until(() => dsub.emissions.length >= 1)
const held = echo.routerHeld().in
echo.flushRouter()
const i = await echo.info()
// Displacement fails the open subscription with LeaderLost; give it a moment to surface.
await echo.until(() => dsub.ended !== undefined, 1500)
const s = await echo.status()
return { held, info: i.ok ? i.value.leaderId : null, leaderId: s.leaderId ?? null, subEnded: dsub.ended ?? null, emissions: dsub.emissions.length }")
  check "a late Connected for the leader a tab is already connected to (from a dead router instance) is ignored: the open subscription stays open" \
    "$(jq -nc --argjson k "${killed:-null}" --argjson a "$da" --argjson b "$db" --argjson b2 "$db2" '
      { pass: ($k.killed > 0 and $b.served == $a.leaderId and $b2.held >= 1 and $b2.info == $a.leaderId
               and $b2.leaderId == $a.leaderId and $b2.subEnded == null),
        killed: $k, a: $a, b: $b, after: $b2 }')"
  for t in "$DA" "$DB" "$DC"; do ab tab close "$t" >/dev/null; done

  # A late Connected for an *older* epoch: F's first Connected(A) (from R0) is buffered, R0 is killed, C
  # relaunches it and F is served by A through R1; then A's tab closes and F is served by the successor B.
  # The buffered Connected(A) is delivered last: it names a dead epoch and must not displace B.
  APP="router-old-$RUN"
  OA=$(open_tab "$BASE/?app=$APP")
  oa=$(ev <<<"$SERVED")
  OF=$(open_tab "$BASE/?app=$APP&holdRouterIn")
  ev <<<'await echo.until(() => echo.routerHeld().in >= 1); return true' >/dev/null
  killed=$(bun "$(dirname "$0")/cdp/kill-router.ts" "$(ab get cdp-url)" "echo:$APP:default:router")
  OC=$(open_tab "$BASE/?app=$APP")
  ev <<<"$SERVED" >/dev/null
  ab tab "$OF" >/dev/null
  of=$(ev <<<"$SERVED")
  ab tab close "$OA" >/dev/null
  ab tab "$OF" >/dev/null
  of2=$(ev <<<"const old = $(jq '.leaderId' <<<"$oa")
const b = await echo.until(async () => { const s = await echo.status(); const i = await echo.info(); return s.leaderId !== undefined && s.leaderId !== old && i.ok && i.value.leaderId === s.leaderId ? s.leaderId : undefined })
window.osub = echo.subscribe(['SELECT 1 AS n'], []); await echo.until(() => osub.emissions.length >= 1)
const held = echo.routerHeld().in
echo.flushRouter()
const i = await Promise.race([echo.info(), new Promise((r) => setTimeout(() => r({ ok: false, error: 'timeout' }), 5000))])
// Displacement fails the open subscription with LeaderLost; give it a moment to surface.
await echo.until(() => osub.ended !== undefined, 1500)
const s = await echo.status()
return { b: b ?? null, held, info: i.ok ? i.value.leaderId : i.error, leaderId: s.leaderId ?? null, subEnded: osub.ended ?? null }")
  check "a late Connected for a dead older epoch (from a dead router instance) is ignored: the successor keeps serving the tab and its open subscription" \
    "$(jq -nc --argjson k "${killed:-null}" --argjson a "$oa" --argjson f "$of" --argjson f2 "$of2" '
      { pass: ($k.killed > 0 and $f.served == $a.leaderId and $f2.b != null and $f2.b != $a.leaderId and $f2.held >= 1
               and $f2.info == $f2.b and $f2.leaderId == $f2.b and $f2.subEnded == null),
        killed: $k, a: $a, f: $f, after: $f2 }')"
  for t in "$OF" "$OC"; do ab tab close "$t" >/dev/null; done
fi

# ── identities that would escape the storage namespace are refused up front ──────────────────────────
D=$(open_tab "$BASE/?app=.")
check "identity '.' is refused with LeaderUnavailable{Unsupported}, nothing is opened" "$(ev <<<'const r = await echo.info(); const s = await echo.status()
return { pass: s.mode === "unsupported" && !r.ok && r.error.reason === "Unsupported", s, r }')"
ab tab close "$D" >/dev/null

ab close >/dev/null
echo "$((total - fails))/$total passed"
exit $((fails > 0))
