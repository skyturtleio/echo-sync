#!/usr/bin/env bash
# Vertical-slice scenarios: the real browser leader against the real server (examples/server/main.ts on
# Postgres), driven by agent-browser through the Vite harness. See docs/vertical-slice.md.
#
#   examples/browser-spike/slice.sh [base-url]
#
# Needs the slice server and the harness running. Scenario 5 stops and restarts the server with
# $ECHO_SERVER_STOP / $ECHO_SERVER_START (default: the `echo-server` orb service).
# Prints one PASS/FAIL line per invariant and a summary; exits non-zero if any failed. Every check awaits
# the condition it asserts (echo.until); the only fixed wait is the server's downtime in scenario 5.
ECHO_SYNC=real
SESSION_PREFIX=slice
ECHO_EVIDENCE=${ECHO_EVIDENCE:-1}
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
server_stop() { eval "${ECHO_SERVER_STOP:-amp orb service stop echo-server >/dev/null}"; }
server_start() { eval "${ECHO_SERVER_START:-amp orb service start echo-server --command 'cd $ROOT && bun examples/server/main.ts' >/dev/null}"; }
admin() { curl -sf "$BASE/admin/$1"; }

APP="slice-$RUN"
SCOPE="team-$RUN"
for p in alice bob; do admin "grant?scope=$SCOPE&principal=$p&role=writer" >/dev/null; done

# JS prelude: the scope's todos in this tab's replica, this tab's client id, and a todos.add envelope.
PRE="const scope = '$SCOPE'
const todos = async (s = scope) => { const r = await echo.exec('SELECT id FROM todos WHERE access_scope_id = ? ORDER BY id', [s]); return r.ok ? r.value.map((x) => x.id) : ['error'] }
const clientId = async () => (await echo.info()).value.clientId
const add = (id, title, s = scope) => echo.enqueue({ mutationId: id, name: 'todos.add', args: { id, scope: s, title } })
const released = (id) => echo.until(async () => { const r = await echo.exec('SELECT released_at FROM echo_outbox WHERE mutation_id = ?', [id]); return r.ok && r.value[0]?.released_at != null }, 40000)
const seen = (id) => sub.emissions.some((e) => e.results[0].some((r) => r.id === id))"

# ── (2) two clients on one scope: A's mutation reaches B through push, poke and pull ──────────────────
A=$(open_tab "$BASE/?app=$APP&profile=a&principal=alice")
ev <<<"$SERVED" >/dev/null
B=$(open_tab "$BASE/?app=$APP&profile=b&principal=bob")
ev <<<"$SERVED" >/dev/null
ev <<<"window.sub = echo.subscribe([\"SELECT id FROM todos WHERE access_scope_id = '$SCOPE' ORDER BY id\"], ['todos'])
return await echo.until(() => sub.emissions.length >= 1)" >/dev/null
ab tab "$A" >/dev/null
a=$(ev <<<"$PRE
const e = await add('deliver-$RUN', 'hello')
return { e, released: (await released('deliver-$RUN')) === true, clientId: await clientId(), notices: (await echo.notices()).value }")
ab tab "$B" >/dev/null
b=$(ev <<<"$PRE
const ok = await echo.until(() => seen('deliver-$RUN'), 20000)
return { ok: ok === true, emissions: sub.emissions.length, clientId: await clientId() }")
ledger=$(admin "ledger?mutationId=deliver-$RUN")
check "two clients: A's todos.add lands in B's live query via poke/pull, no refresh; ledger acks A exactly once" "$(jq -nc --argjson a "$a" --argjson b "$b" --argjson l "$ledger" '
  { pass: ($a.e.ok and $a.released and ($a.notices | length) == 0 and $b.ok and $a.clientId != $b.clientId
          and ($l | length) == 1 and $l[0].client_id == $a.clientId and $l[0].outcome._tag == "Applied"),
    a: $a, b: $b, ledger: $l }')"

# ── (3) a server-side domain rejection: typed notice in A, no effect anywhere ─────────────────────────
ab tab "$A" >/dev/null
a=$(ev <<<"$PRE
const e = await add('reject-$RUN', 'server-rejects-$RUN')
const optimistic = (await todos()).includes('reject-$RUN')
const notice = await echo.until(async () => (await echo.notices()).value.find((n) => n.notice.envelope.mutationId === 'reject-$RUN'), 20000)
const after = await todos()
// A later mutation: once B shows it, B has pulled past the rejection's ledger position.
await add('marker-$RUN', 'after the rejection')
return { e: e.ok, optimistic, notice: notice?.notice, after }")
ab tab "$B" >/dev/null
b=$(ev <<<"$PRE
const ok = await echo.until(() => seen('marker-$RUN'), 20000)
return { ok: ok === true, everSawRejected: seen('reject-$RUN'), now: await todos() }")
ledger=$(admin "ledger?mutationId=reject-$RUN")
server=$(admin "todos?scope=$SCOPE")
check "real mutator rejection: A gets the typed Rejected{DomainError} notice and its optimistic row is undone; B and the server never see it" "$(jq -nc --argjson a "$a" --argjson b "$b" --argjson l "$ledger" --argjson s "$server" '
  { pass: ($a.e and $a.notice._tag == "Rejected" and $a.notice.error._tag == "DomainError"
          and $a.notice.error.error.reason == "blocked-title" and ($a.after | index("reject-'"$RUN"'")) == null
          and $b.ok and ($b.everSawRejected | not) and ($b.now | index("reject-'"$RUN"'")) == null
          and ($s | map(.id) | index("reject-'"$RUN"'")) == null
          and ($l | length) == 1 and $l[0].outcome._tag == "Rejected"),
    optimisticInA: $a.optimistic, notice: $a.notice, b: $b, ledger: $l }')"

# ── (4) revoke purges the scope from both replicas; re-grant walks it back in ────────────────────────
for p in alice bob; do admin "revoke?scope=$SCOPE&principal=$p" >/dev/null; done
GONE="const ok = await echo.until(async () => { const t = await todos(); const g = await echo.exec('SELECT id FROM scope_grants WHERE access_scope_id = ?', [scope]); return t.length === 0 && g.ok && g.value.length === 0 }, 20000)"
ab tab "$A" >/dev/null
a=$(ev <<<"$PRE
$GONE
return { ok: ok === true }")
ab tab "$B" >/dev/null
b=$(ev <<<"$PRE
$GONE
const live = await echo.until(() => sub.emissions.at(-1).results[0].length === 0, 5000)
return { ok: ok === true, liveQueryEmpty: live === true }")
check "revoke: both replicas drop the scope's rows and grants, and B's live query re-emits empty" "$(jq -nc --argjson a "$a" --argjson b "$b" '{ pass: ($a.ok and $b.ok and $b.liveQueryEmpty), a: $a, b: $b }')"
for p in alice bob; do admin "grant?scope=$SCOPE&principal=$p&role=writer" >/dev/null; done
BACK="const ok = await echo.until(async () => { const t = await todos(); return t.includes('deliver-$RUN') && t.includes('marker-$RUN') ? t : undefined }, 20000)"
ab tab "$A" >/dev/null
a=$(ev <<<"$PRE
$BACK
return { rows: ok ?? (await todos()) }")
ab tab "$B" >/dev/null
b=$(ev <<<"$PRE
$BACK
return { rows: ok ?? (await todos()), live: sub.emissions.at(-1).results[0].map((r) => r.id) }")
check "re-grant: the scope is walked again and both replicas hold exactly the server's rows" "$(jq -nc --argjson a "$a" --argjson b "$b" --argjson s "$(admin "todos?scope=$SCOPE")" '
  ($s | map(.id)) as $want | { pass: ($a.rows == $want and $b.rows == $want and $b.live == $want), want: $want, a: $a, b: $b }')"

# ── (5) server restart mid-session: nothing enqueued while it was down is lost or doubled ─────────────
server_stop
ab tab "$A" >/dev/null
a=$(ev <<<"$PRE
for (const i of [1, 2, 3]) await add('down-' + i + '-$RUN', 'while the server is down')
const failing = await echo.until(async () => { const s = await echo.syncStatus(); return s.ok && s.value.value.lastError !== null }, 15000)
return { failing: failing === true }")
sleep 3 # downtime, so both clients' pokes streams and backoffs really see the server gone
server_start
for _ in $(seq 1 50); do admin "stats" >/dev/null && break; sleep 0.2; done
back=$(date +%s%3N)
ab tab "$B" >/dev/null
# Short evals in a loop: one CDP evaluate cannot wait out the clients' backoff.
for _ in $(seq 1 24); do
  b=$(ev <<<"$PRE
const ok = await echo.until(() => [1, 2, 3].every((i) => seen('down-' + i + '-$RUN')), 5000)
return { ok: ok === true, clientId: await clientId() }")
  [ "$(jq -r .ok <<<"$b")" = true ] && break
done
b=$(jq -c --argjson ms "$(($(date +%s%3N) - back))" '. + { catchUpMs: $ms }' <<<"$b")
ab tab "$A" >/dev/null
a2=$(ev <<<"$PRE
const ok = await Promise.all([1, 2, 3].map((i) => released('down-' + i + '-$RUN')))
const s = await echo.syncStatus()
return { released: ok.every((x) => x === true), lastError: s.value.value.lastError, push: s.value.value.push._tag }")
ledgers=$(for i in 1 2 3; do admin "ledger?mutationId=down-$i-$RUN"; done | jq -sc 'map(length)')
bstats=$(admin "stats?clientId=$(jq -r .clientId <<<"$b")")
check "server restart: clients reconnect via the pokes backoff, catch up from the initial poke, and the 3 offline enqueues land exactly once" "$(jq -nc --argjson a "$a" --argjson a2 "$a2" --argjson b "$b" --argjson l "$ledgers" --argjson st "$bstats" '
  { pass: ($a.failing and $b.ok and $a2.released and $a2.push == "Active" and $l == [1, 1, 1]
          and ($st.rpcs.pokes // 0) >= 1),
    ledgerRows: $l, bRpcsAfterRestart: $st.rpcs, bCatchUpMs: $b.catchUpMs, a: $a2 }')"
ab tab close "$A" >/dev/null
ab tab close "$B" >/dev/null

# ── (6) the leader tab closes while its push is in flight: the next leader pushes it, once ────────────
C1=$(open_tab "$BASE/?app=$APP&profile=c&principal=alice")
ev <<<"$SERVED" >/dev/null
C2=$(open_tab "$BASE/?app=$APP&profile=c&principal=alice")
ev <<<"$SERVED" >/dev/null
ab tab "$C1" >/dev/null
c1=$(ev <<<"$PRE
const s = await echo.status()
const e = await add('inflight-$RUN', 'slow: the server stalls this push 2 s')
const started = await echo.until(async () => (await (await fetch('/admin/stats?todo=inflight-$RUN')).json()).started >= 1, 15000)
return { leaderTab: s.isLeaderTab, leaderId: s.leaderId, e: e.ok, started: started === true, clientId: await clientId() }")
ab tab close "$C1" >/dev/null
ab tab "$C2" >/dev/null
c2=$(ev <<<"$PRE
const old = $(jq '.leaderId' <<<"$c1")
await echo.until(async () => { const s = await echo.status(); return s.isLeaderTab && s.leaderId !== old })
const ok = await released('inflight-$RUN')
return { released: ok === true, clientId: await clientId(), notices: (await echo.notices()).value.length }")
ledger=$(admin "ledger?mutationId=inflight-$RUN")
started=$(admin "stats?todo=inflight-$RUN" | jq .started)
check "leader closed mid-push: the new leader pushes the entry, the ledger has exactly one row, and it is released" "$(jq -nc --argjson c1 "$c1" --argjson c2 "$c2" --argjson l "$ledger" --argjson n "$started" '
  { pass: ($c1.leaderTab and $c1.e and $c1.started and $c2.released and $c2.notices == 0 and $c2.clientId == $c1.clientId
          and ($l | length) == 1 and $l[0].client_id == $c1.clientId and $l[0].outcome._tag == "Applied"),
    serverHandlerStarts: $n, ledger: $l }')"
ab tab close "$C2" >/dev/null

ab close >/dev/null
echo "$((total - fails))/$total passed"
exit $((fails > 0))
