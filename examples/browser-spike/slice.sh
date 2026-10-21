#!/usr/bin/env bash
# Vertical-slice scenarios: the real browser leader against the real server (examples/server/main.ts on
# Postgres), driven by agent-browser through the Vite harness. See docs/vertical-slice.md.
#
#   [SLICE_ORDER="base offline conflict deleteedit"] [SLICE_CONTROL=<fault>] examples/browser-spike/slice.sh [base-url]
#
# SLICE_ORDER lists the sections to run, in order; SLICE_CONTROL injects one deliberate fault that must
# make its check fail (both documented in docs/vertical-slice.md).
#
# Needs the slice server and the harness running. Scenario 5 stops and restarts the server with
# $ECHO_SERVER_STOP / $ECHO_SERVER_START (default: the `echo-server` orb service).
# Prints one PASS/FAIL line per invariant and a summary; exits non-zero if any failed. Every check awaits
# the condition it asserts (echo.until); the only fixed wait is the server's downtime in scenario 5. Scenarios
# 3 and 6 hold the server's push handler at a named gate (app.ts) until they open it through /admin/open.
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

section_base() {
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
const e = await add('reject-$RUN', 'server-rejects-$RUN [gate:reject-$RUN]')
// The server's handler is parked at the gate, so the rejection cannot have arrived yet: the row must be
// in A's replica now (optimistic apply), with no notice.
const parked = await echo.until(async () => (await (await fetch('/admin/stats?todo=reject-$RUN')).json()).started >= 1, 15000)
const optimistic = (await todos()).includes('reject-$RUN')
const noticeWhileGated = (await echo.notices()).value.some((n) => n.notice.envelope.mutationId === 'reject-$RUN')
await fetch('/admin/open?gate=reject-$RUN')
const notice = await echo.until(async () => (await echo.notices()).value.find((n) => n.notice.envelope.mutationId === 'reject-$RUN'), 20000)
const after = await todos()
// A later mutation: once B shows it, B has pulled past the rejection's ledger position.
await add('marker-$RUN', 'after the rejection')
return { e: e.ok, parked: parked === true, optimistic, noticeWhileGated, notice: notice?.notice, after }")
ab tab "$B" >/dev/null
b=$(ev <<<"$PRE
const ok = await echo.until(() => seen('marker-$RUN'), 20000)
return { ok: ok === true, everSawRejected: seen('reject-$RUN'), now: await todos() }")
ledger=$(admin "ledger?mutationId=reject-$RUN")
server=$(admin "todos?scope=$SCOPE")
check "real mutator rejection: A applies the row optimistically while the server holds the push, then gets the typed Rejected{DomainError} notice and the row is undone; B and the server never see it" "$(jq -nc --argjson a "$a" --argjson b "$b" --argjson l "$ledger" --argjson s "$server" '
  { pass: ($a.e and $a.parked and $a.optimistic and ($a.noticeWhileGated | not) and $a.notice._tag == "Rejected" and $a.notice.error._tag == "DomainError"
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
const want = JSON.stringify(ok ?? [])
const live = await echo.until(() => JSON.stringify(sub.emissions.at(-1).results[0].map((r) => r.id)) === want, 20000)
return { rows: ok ?? (await todos()), live: sub.emissions.at(-1).results[0].map((r) => r.id), liveCaughtUp: live === true }")
check "re-grant: the scope is walked again and both replicas hold exactly the server's rows" "$(jq -nc --argjson a "$a" --argjson b "$b" --argjson s "$(admin "todos?scope=$SCOPE")" '
  ($s | map(.id)) as $want | { pass: ($a.rows == $want and $b.rows == $want and $b.liveCaughtUp and $b.live == $want), want: $want, a: $a, b: $b }')"

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
const e = await add('inflight-$RUN', 'in flight [gate:inflight-$RUN]')
// Parked at the gate inside the server's push transaction: this push is in flight until the gate opens.
const started = await echo.until(async () => (await (await fetch('/admin/stats?todo=inflight-$RUN')).json()).started >= 1, 15000)
const id = await clientId()
const pushes = (await (await fetch('/admin/stats?clientId=' + id)).json()).rpcs.push ?? 0
return { leaderTab: s.isLeaderTab, leaderId: s.leaderId, e: e.ok, started: started === true, clientId: id, pushes }")
ab tab close "$C1" >/dev/null
ab tab "$C2" >/dev/null
c2=$(ev <<<"$PRE
const old = $(jq '.leaderId' <<<"$c1")
const succeeded = await echo.until(async () => { const s = await echo.status(); return s.isLeaderTab && s.leaderId !== old })
// The gate is still closed: the entry is unreleased, so the new leader must push it again itself.
const id = await clientId()
const repushed = await echo.until(async () => ((await (await fetch('/admin/stats?clientId=' + id)).json()).rpcs.push ?? 0) > $(jq '.pushes' <<<"$c1"), 30000)
const pendingWhileGated = (await echo.exportPending()).value.map((x) => x.envelope.mutationId)
await fetch('/admin/open?gate=inflight-$RUN')
const ok = await released('inflight-$RUN')
return { succeeded: succeeded === true, repushed: repushed === true, pendingWhileGated, released: ok === true, clientId: id, notices: (await echo.notices()).value.length }")
ledger=$(admin "ledger?mutationId=inflight-$RUN")
started=$(admin "stats?todo=inflight-$RUN" | jq .started)
check "leader closed while its push is held on the server: the new leader sends its own push of the entry, the ledger has exactly one row, and it is released" "$(jq -nc --argjson c1 "$c1" --argjson c2 "$c2" --argjson l "$ledger" --argjson n "$started" '
  { pass: ($c1.leaderTab and $c1.e and $c1.started and $c1.pushes >= 1 and $c2.succeeded and $c2.repushed
          and $c2.pendingWhileGated == ["inflight-'"$RUN"'"] and $c2.released and $c2.notices == 0 and $c2.clientId == $c1.clientId
          and ($l | length) == 1 and $l[0].client_id == $c1.clientId and $l[0].outcome._tag == "Applied"),
    pushesBeforeClose: $c1.pushes, serverHandlerStarts: $n, ledger: $l }')"
ab tab close "$C2" >/dev/null

}

# ── Offline convergence (DESIGN §14, docs/vertical-slice.md): two independent clients, each its own profile
# and client id, with the harness network gate (`echo.setOffline`: the leader's requests fail as an
# unreachable server does and its open streams abort). Every wait is an awaited condition. SLICE_CONTROL
# names one deliberate fault that must make exactly its check fail (see docs/vertical-slice.md).
ctl() { [ "${SLICE_CONTROL:-}" = "$1" ]; }
now() { date +%s%3N; }
# Re-evaluates a JS body returning { ok, ... } in short evals until ok (one CDP evaluate cannot wait out a
# reconnect backoff); prints the last result.
evu() {
  local body out
  body=$(cat)
  for _ in $(seq 1 "${1:-12}"); do
    out=$(ev <<<"$body")
    [ "$(jq -r .ok <<<"$out")" = true ] && break
  done
  echo "$out"
}
# JS prelude for scope $1: replica rows, the live query's latest rows, the server's rows, the ledger,
# the gate, and waits on each.
npre() {
  echo "const scope = '$1'
const cols = 'SELECT id, access_scope_id, title, note FROM todos WHERE access_scope_id = ? ORDER BY id'
const rows = async () => { const r = await echo.exec(cols, [scope]); return r.ok ? r.value : ['error'] }
const server = async () => (await fetch('/admin/todos?scope=' + scope)).json()
const ledger = async (id) => (await fetch('/admin/ledger?mutationId=' + id)).json()
const clientId = async () => (await echo.info()).value.clientId
const mut = (mutationId, name, args) => echo.enqueue({ mutationId, name, args })
const row = (id, title, note = null) => ({ id, access_scope_id: scope, title, note })
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b)
const watch = () => { window.live = echo.subscribe([\"SELECT id, access_scope_id, title, note FROM todos WHERE access_scope_id = '$1' ORDER BY id\"], ['todos']); return echo.until(() => window.live.emissions.length >= 1) }
const live = () => window.live.emissions.at(-1)?.results[0] ?? null
const released = (id, ms = 5000) => echo.until(async () => { const r = await echo.exec('SELECT released_at FROM echo_outbox WHERE mutation_id = ?', [id]); return r.ok && r.value[0]?.released_at != null }, ms)
const pending = async () => (await echo.exportPending()).value.map((x) => x.envelope.mutationId)
const notice = (id) => echo.notices().then((n) => n.value.find((x) => x.notice.envelope.mutationId === id)?.notice ?? null)
// Offline is in effect once a sync attempt fails with a network error (not a halted push).
const offline = async () => (await echo.until(async () => { await echo.requestSync(); const s = await echo.syncStatus(); return s.ok && s.value.value.lastError?.kind === 'network' }, 15000)) === true
const converged = (want, ms = 5000) => echo.until(async () => same(await rows(), want) && same(live(), want), ms)"
}
# Opens a tab for profile $1 as principal $2, served by its own leader; prints the tab id.
client_tab() {
  local tab
  tab=$(open_tab "$BASE/?app=$APP&profile=$1&principal=$2")
  ev <<<"$SERVED" >/dev/null
  echo "$tab"
}

# ── S6 offline edit, reload while offline, reconnect: Postgres, A and B converge; A acked once ───────
section_offline() {
  local S="off-$RUN" t0 id="o1-$RUN" upd="upd-o1-$RUN" OA OB a0 a1 a2 a3 b ledger srv
  for p in alice bob; do admin "grant?scope=$S&principal=$p&role=writer" >/dev/null; done
  t0=$(now)
  OA=$(client_tab oa alice)
  OB=$(client_tab ob bob)
  ab tab "$OB" >/dev/null
  ev <<<"$(npre "$S")
return await watch()" >/dev/null
  ab tab "$OA" >/dev/null
  a0=$(ev <<<"$(npre "$S")
const e = await mut('$id', 'todos.add', { id: '$id', scope, title: 'before' })
return { e: e.ok, released: (await released('$id', 30000)) === true, clientId: await clientId() }")
  ab tab "$OB" >/dev/null
  ev <<<"$(npre "$S")
return await converged([row('$id', 'before')], 20000)" >/dev/null
  ab tab "$OA" >/dev/null
  ctl skip-offline || ev <<<"echo.setOffline(true); return true" >/dev/null
  a1=$(ev <<<"$(npre "$S")
const wentOffline = await offline()
const e = await mut('$upd', 'todos.update', { id: '$id', title: 'edited offline' })
// Wait for at least one more failed attempt with the entry pending, then check nothing reached the server.
await offline()
return { wentOffline, e: e.ok, optimistic: await rows(), pending: await pending(), ledgerWhileOffline: (await ledger('$upd')).length }")
  # Control drop-outbox: a harness fault that loses the queued mutation across the reload.
  ctl drop-outbox && ev <<<"return await echo.exec(\"DELETE FROM echo_outbox WHERE mutation_id = '$upd'\")" >/dev/null
  ab reload >/dev/null
  a2=$(ev <<<"$(npre "$S")
await echo.ready()
await watch()
const stillOffline = await offline()
return { stillOffline, afterReload: await rows(), live: live(), pending: await pending(), ledgerWhileOffline: (await ledger('$upd')).length }")
  ctl stay-offline || ev <<<"echo.setOffline(false); return true" >/dev/null
  a3=$(evu 12 <<<"$(npre "$S")
const want = [row('$id', 'edited offline')]
const ok = (await released('$upd')) === true && (await converged(want)) === true
return { ok, rows: await rows(), live: live(), notice: await notice('$upd'), clientId: await clientId() }")
  ab tab "$OB" >/dev/null
  b=$(evu 6 <<<"$(npre "$S")
const ok = (await converged([row('$id', 'edited offline')])) === true
return { ok, rows: await rows(), live: live(), emissions: window.live.emissions.length, clientId: await clientId() }")
  ledger=$(admin "ledger?mutationId=$upd")
  srv=$(admin "todos?scope=$S")
  check "offline edit survives a reload while offline and converges on reconnect: the queued todos.update and its optimistic row are still there after the reload, nothing reaches the server while offline, then Postgres, A's and B's live queries all hold the edited row and the ledger acks A exactly once" "$(jq -nc \
    --argjson a0 "$a0" --argjson a1 "$a1" --argjson a2 "$a2" --argjson a3 "$a3" --argjson b "$b" --argjson l "$ledger" --argjson s "$srv" \
    --arg id "$id" --arg upd "$upd" --arg S "$S" --argjson ms "$(($(now) - t0))" '
    [{ id: $id, access_scope_id: $S, title: "edited offline", note: null }] as $want
    | { pass: ($a0.e and $a0.released and $a1.wentOffline and $a1.e and $a1.optimistic == $want and $a1.pending == [$upd]
            and $a1.ledgerWhileOffline == 0 and $a2.stillOffline and $a2.afterReload == $want and $a2.live == $want
            and $a2.pending == [$upd] and $a2.ledgerWhileOffline == 0 and $a3.ok and $a3.notice == null
            and $b.ok and $b.clientId != $a3.clientId and $s == $want
            and ($l | length) == 1 and $l[0].client_id == $a3.clientId and $l[0].outcome._tag == "Applied"),
        ms: $ms, offline: $a1, afterReload: $a2, a: $a3, b: $b, server: $s, ledger: $l }')"
  ab tab close "$OA" >/dev/null
  ab tab close "$OB" >/dev/null
}

# ── S7 same row, both offline: B reconnects first, then A; the server reruns B's then A's mutators ───
section_conflict() {
  local S="cf-$RUN" t0 CA CB a0 a1 b1 b2 a2 a3 b3 srv ledgers
  local c1="c1-$RUN" c2="c2-$RUN" c3="c3-$RUN"
  for p in alice bob; do admin "grant?scope=$S&principal=$p&role=writer" >/dev/null; done
  t0=$(now)
  CA=$(client_tab ca alice)
  CB=$(client_tab cb bob)
  ab tab "$CA" >/dev/null
  a0=$(ev <<<"$(npre "$S")
await watch()
for (const [id, t] of [['$c1', 't1'], ['$c2', 't2'], ['$c3', 't3']]) await mut(id, 'todos.add', { id, scope, title: t })
return { released: (await Promise.all(['$c1', '$c2', '$c3'].map((id) => released(id, 30000)))).every((x) => x === true) }")
  ab tab "$CB" >/dev/null
  ev <<<"$(npre "$S")
await watch()
return await converged([row('$c1', 't1'), row('$c2', 't2'), row('$c3', 't3')], 20000)" >/dev/null
  # Both offline, both edit all three rows: c1 different fields, c2 the same field, c3 A's edit is one the
  # server's real mutator rejects (blocked title; control no-reject: an ordinary title).
  ab tab "$CB" >/dev/null
  b1=$(ev <<<"$(npre "$S")
echo.setOffline(true)
const wentOffline = await offline()
await mut('u-b-$c1', 'todos.update', { id: '$c1', title: 'title from B' })
await mut('u-b-$c2', 'todos.update', { id: '$c2', title: 'B loses' })
await mut('u-b-$c3', 'todos.update', { id: '$c3', title: 'B stays' })
return { wentOffline, optimistic: await rows() }")
  ab tab "$CA" >/dev/null
  a1=$(ev <<<"$(npre "$S")
echo.setOffline(true)
const wentOffline = await offline()
await mut('u-a-$c1', 'todos.update', { id: '$c1', note: 'note from A' })
await mut('u-a-$c2', 'todos.update', { id: '$c2', title: 'A wins' })
await mut('u-a-$c3', 'todos.update', { id: '$c3', title: 'server-rejects A' })
await offline()
return { wentOffline, optimistic: await rows(), ledgers: (await Promise.all(['$c1', '$c2', '$c3'].map((c) => ledger('u-a-' + c)))).map((l) => l.length) }")
  # Control no-reject: the server's blocked-title check is off, so A's unchanged edit is applied.
  ctl no-reject && admin "rejections?enabled=0" >/dev/null
  # Reconnect B first and wait until all its pushes are acknowledged, then A (control a-first: the reverse).
  first=$CB second=$CA fp=b sp=a
  if ctl a-first; then first=$CA second=$CB fp=a sp=b; fi
  for pair in "$first:$fp" "$second:$sp"; do
    ab tab "${pair%%:*}" >/dev/null
    ev <<<"echo.setOffline(false); return true" >/dev/null
    out=$(evu 12 <<<"$(npre "$S")
const ok = (await Promise.all(['$c1', '$c2', '$c3'].map((c) => released('u-${pair##*:}-' + c)))).every((x) => x === true)
return { ok }")
    [ "${pair##*:}" = b ] && b2=$out || a2=$out
    # Control b-offline-after-push: B drops off again once its own pushes are in, so it never pulls A's.
    if [ "${pair##*:}" = b ] && ctl b-offline-after-push; then ev <<<"echo.setOffline(true); return true" >/dev/null; fi
  done
  ab tab "$CA" >/dev/null
  a3=$(evu 6 <<<"$(npre "$S")
const want = [row('$c1', 'title from B', 'note from A'), row('$c2', 'A wins'), row('$c3', 'B stays')]
const ok = (await converged(want)) === true
return { ok, rows: await rows(), live: live(), pending: await pending(), notice: await notice('u-a-$c3') }")
  ctl no-reject && admin "rejections?enabled=1" >/dev/null
  # Barrier: a marker committed after A's rejection. Once B's live query shows it, B has pulled past the
  # rejection's ledger position, so its emission history covers everything the rejection could cause.
  ev <<<"$(npre "$S")
await mut('mk-$c3', 'todos.add', { id: 'mk-$c3', scope, title: 'after the rejection' })
return await released('mk-$c3', 30000)" >/dev/null
  ab tab "$CB" >/dev/null
  # Control transient-rejected: B's replica shows A's rejected title for one emission, then B's again, so
  # B's final state is still correct.
  ctl transient-rejected && ev <<<"$(npre "$S")
const set = (t) => echo.exec('UPDATE todos SET title = ? WHERE id = ?', [t, '$c3'])
await set('server-rejects A')
await echo.until(() => live().some((r) => r.id === '$c3' && r.title === 'server-rejects A'))
await set('B stays')
return await echo.until(() => live().some((r) => r.id === '$c3' && r.title === 'B stays'))" >/dev/null
  b3=$(evu 6 <<<"$(npre "$S")
const want = [row('$c1', 'title from B', 'note from A'), row('$c2', 'A wins'), row('$c3', 'B stays'), row('mk-$c3', 'after the rejection')]
const ok = (await converged(want)) === true
const has = (pred) => window.live.emissions.some((e) => e.results[0].some(pred))
return { ok, rows: await rows(), live: live(), pending: await pending(), notices: (await echo.notices()).value.length,
  emissions: window.live.emissions.length, sawMarker: has((r) => r.id === 'mk-$c3'),
  sawRejected: has((r) => r.id === '$c3' && r.title === 'server-rejects A') }")
  srv=$(admin "todos?scope=$S")
  ledgers='{}'
  for who in a b; do
    for c in c1 c2 c3; do
      ledgers=$(jq -c --arg k "$who-$c" --argjson v "$(admin "ledger?mutationId=u-$who-$c-$RUN")" '. + { ($k): $v }' <<<"$ledgers")
    done
  done
  ms=$(($(now) - t0))
  common=(--argjson a1 "$a1" --argjson b1 "$b1" --argjson a3 "$a3" --argjson b3 "$b3" --argjson s "$srv" --argjson l "$ledgers" --arg S "$S" --argjson ms "$ms")
  # $l keys: "b-c1", "a-c1", ... (mutation author and row). One ledger row each; B's position before A's.
  check "same row, different fields, both offline: B then A reconnect and the server row carries B's title and A's note; Postgres and both live queries hold exactly that row, nothing pending" "$(jq -nc "${common[@]}" --arg id "$c1" '
    [{ id: $id, access_scope_id: $S, title: "title from B", note: "note from A" }] as $want
    | ([$s[] | select(.id == $id)]) as $srow
    | { pass: ($a1.wentOffline and $b1.wentOffline and ($a1.ledgers | all(. == 0))
            and ($a1.optimistic | map(select(.id == $id))) == [{ id: $id, access_scope_id: $S, title: "t1", note: "note from A" }]
            and ($b1.optimistic | map(select(.id == $id))) == [{ id: $id, access_scope_id: $S, title: "title from B", note: null }]
            and $srow == $want and ($a3.rows | map(select(.id == $id))) == $want and ($b3.rows | map(select(.id == $id))) == $want
            and ($a3.live | map(select(.id == $id))) == $want and ($b3.live | map(select(.id == $id))) == $want
            and $a3.pending == [] and $b3.pending == []
            and ($l["b-c1"] | length) == 1 and ($l["a-c1"] | length) == 1 and ($l["b-c1"][0].position | tonumber) < ($l["a-c1"][0].position | tonumber)
            and $l["b-c1"][0].outcome._tag == "Applied" and $l["a-c1"][0].outcome._tag == "Applied"),
        ms: $ms, server: $srow, a: ($a3.rows | map(select(.id == $id))), b: ($b3.rows | map(select(.id == $id))), bLive: ($b3.live | map(select(.id == $id))) }')"
  check "same row, same field, both offline: B then A reconnect, the server applies B's mutator then A's so A's title stands; B's replica and live query drop its own optimistic title (no residue) and match Postgres" "$(jq -nc "${common[@]}" --arg id "$c2" '
    [{ id: $id, access_scope_id: $S, title: "A wins", note: null }] as $want
    | ([$s[] | select(.id == $id)]) as $srow
    | { pass: (($b1.optimistic | map(select(.id == $id) | .title)) == ["B loses"] and $srow == $want
            and ($a3.rows | map(select(.id == $id))) == $want and ($b3.rows | map(select(.id == $id))) == $want
            and ($a3.live | map(select(.id == $id))) == $want and ($b3.live | map(select(.id == $id))) == $want
            and ($l["b-c2"] | length) == 1 and ($l["a-c2"] | length) == 1 and ($l["b-c2"][0].position | tonumber) < ($l["a-c2"][0].position | tonumber)),
        ms: $ms, server: $srow, b: ($b3.rows | map(select(.id == $id))), positions: { b: $l["b-c2"][0].position, a: $l["a-c2"][0].position } }')"
  check "same row, A's edit rejected by the real server mutator: A gets the typed Rejected{DomainError blocked-title} notice and its optimistic title is undone to B's, which stays in Postgres and both replicas; no emission of B's live query, through a marker committed after the rejection, ever shows A's rejected title" "$(jq -nc "${common[@]}" --arg id "$c3" '
    [{ id: $id, access_scope_id: $S, title: "B stays", note: null }] as $want
    | ([$s[] | select(.id == $id)]) as $srow
    | { pass: (($a1.optimistic | map(select(.id == $id) | .title)) == ["server-rejects A"] and $srow == $want
            and ($a3.rows | map(select(.id == $id))) == $want and ($b3.rows | map(select(.id == $id))) == $want
            and ($a3.live | map(select(.id == $id))) == $want and ($b3.live | map(select(.id == $id))) == $want
            and $a3.notice._tag == "Rejected" and $a3.notice.error._tag == "DomainError" and $a3.notice.error.error.reason == "blocked-title"
            and $b3.sawMarker and ($b3.sawRejected | not)
            and $b3.notices == 0 and ($l["a-c3"] | length) == 1 and $l["a-c3"][0].outcome._tag == "Rejected" and $l["b-c3"][0].outcome._tag == "Applied"),
        ms: $ms, server: $srow, a: ($a3.rows | map(select(.id == $id))), notice: $a3.notice,
        bEmissions: $b3.emissions, bSawMarker: $b3.sawMarker, bEverShowedRejected: $b3.sawRejected }')"
  ab tab close "$CA" >/dev/null
  ab tab close "$CB" >/dev/null
}

# ── S8 offline delete vs online edit, and offline edit vs online delete: no phantom rows ────────────
section_deleteedit() {
  local S="de-$RUN" t0 DA DB a1 b1 a2 b2 srv l1 l2 d1="d1-$RUN" d2="d2-$RUN"
  for p in alice bob; do admin "grant?scope=$S&principal=$p&role=writer" >/dev/null; done
  t0=$(now)
  DA=$(client_tab da alice)
  DB=$(client_tab db bob)
  ab tab "$DA" >/dev/null
  ev <<<"$(npre "$S")
await watch()
for (const [id, t] of [['$d1', 't1'], ['$d2', 't2']]) await mut(id, 'todos.add', { id, scope, title: t })
return (await Promise.all(['$d1', '$d2'].map((id) => released(id, 30000)))).every((x) => x === true)" >/dev/null
  ab tab "$DB" >/dev/null
  ev <<<"$(npre "$S")
await watch()
return await converged([row('$d1', 't1'), row('$d2', 't2')], 20000)" >/dev/null
  # A, offline: deletes d1 and edits d2.
  ab tab "$DA" >/dev/null
  a1=$(ev <<<"$(npre "$S")
echo.setOffline(true)
const wentOffline = await offline()
await mut('del-$d1', 'todos.delete', { id: '$d1' })
await mut('upd-$d2', 'todos.update', { id: '$d2', title: 'A edit offline' })
return { wentOffline, optimistic: await rows() }")
  # B, online: edits d1 and deletes d2 (control skip-b-delete: B leaves d2 alone).
  ab tab "$DB" >/dev/null
  b1=$(ev <<<"$(npre "$S")
await mut('upd-$d1', 'todos.update', { id: '$d1', title: 'B edit' })
$(ctl skip-b-delete || echo "await mut('del-$d2', 'todos.delete', { id: '$d2' })")
const ok = (await released('upd-$d1', 30000)) === true && $(ctl skip-b-delete && echo true || echo "(await released('del-$d2', 30000)) === true")
return { ok, server: await server() }")
  ab tab "$DA" >/dev/null
  ctl a-stays-offline || ev <<<"echo.setOffline(false); return true" >/dev/null
  a2=$(evu 12 <<<"$(npre "$S")
const delReleased = (await released('del-$d1')) === true
const ok = delReleased && (await released('upd-$d2')) === true && (await converged([])) === true
return { ok, delReleased, rows: await rows(), live: live(), pending: await pending(), delNotice: await notice('del-$d1'), updNotice: await notice('upd-$d2') }")
  ab tab "$DB" >/dev/null
  b2=$(evu 6 <<<"$(npre "$S")
const ok = (await echo.until(async () => (await rows()).length === 0 && live().length === 0)) === true
return { ok, rows: await rows(), live: live(), sawEdit: window.live.emissions.some((e) => e.results[0].some((r) => r.id === '$d1' && r.title === 'B edit')) }")
  srv=$(admin "todos?scope=$S")
  l1=$(admin "ledger?mutationId=del-$d1")
  l2=$(admin "ledger?mutationId=upd-$d2")
  common=(--argjson a1 "$a1" --argjson b1 "$b1" --argjson a2 "$a2" --argjson b2 "$b2" --argjson s "$srv" --argjson ms "$(($(now) - t0))")
  check "offline delete vs online edit: B's edit lands first, A's queued delete then removes the row; Postgres, A and B (whose live query showed the edit) all end without it, A gets no notice" "$(jq -nc "${common[@]}" --argjson l "$l1" --arg id "$d1" '
    { pass: ($a1.wentOffline and ($a1.optimistic | map(.id) | index($id)) == null and $b1.ok
            and ($b1.server | map(select(.id == $id) | .title)) == ["B edit"] and $a2.delReleased and $b2.sawEdit
            and ($s | map(select(.id == $id))) == [] and ($a2.rows | map(select(.id == $id))) == [] and ($b2.rows | map(select(.id == $id))) == []
            and ($a2.live | map(select(.id == $id))) == [] and ($b2.live | map(select(.id == $id))) == [] and $a2.delNotice == null
            and ($l | length) == 1 and $l[0].outcome._tag == "Applied"),
        ms: $ms, server: $s, a: $a2.rows, b: $b2.rows, ledger: $l }')"
  check "offline edit vs online delete: A's queued todos.update of the row B deleted is rejected RowNotFound, and A's optimistic edit does not resurrect it: no phantom row in A, B or Postgres" "$(jq -nc "${common[@]}" --argjson l "$l2" --arg id "$d2" '
    { pass: (($a1.optimistic | map(select(.id == $id) | .title)) == ["A edit offline"] and $a2.ok and $b2.ok
            and ($s | map(select(.id == $id))) == [] and ($a2.rows | map(select(.id == $id))) == [] and ($b2.rows | map(select(.id == $id))) == []
            and ($a2.live | map(select(.id == $id))) == [] and ($b2.live | map(select(.id == $id))) == [] and $a2.pending == []
            and $a2.updNotice._tag == "Rejected" and $a2.updNotice.error._tag == "RowNotFound"
            and ($l | length) == 1 and $l[0].outcome._tag == "Rejected"),
        ms: $ms, server: $s, a: $a2.rows, notice: $a2.updNotice, ledger: $l }')"
  ab tab close "$DA" >/dev/null
  ab tab close "$DB" >/dev/null
}

for section in ${SLICE_ORDER:-base offline conflict deleteedit}; do
  "section_$section"
done

ab close >/dev/null
echo "$((total - fails))/$total passed"
exit $((fails > 0))
