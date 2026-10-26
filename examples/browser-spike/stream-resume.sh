#!/usr/bin/env bash
# Actual Chromium/OPFS recovery: abort an active HTTP pull, reload offline, then resume.
# Requires the example server and Vite; uses the existing per-identity fetch/stream gate.
set -euo pipefail
BASE=${1:-http://127.0.0.1:5199}
SESSION="resume-$$"
APP="stream-resume-$(date +%s)-$$"
ab() { agent-browser --session "$SESSION" "$@"; }
ev() {
  local result
  ab eval "window.resumeResult = undefined; (async () => { try { return await (async () => { $(cat); })(); } catch (e) { return {error:String(e)}; } })().then(value => { window.resumeResult = value; }); 'started'" >/dev/null
  for _ in $(seq 1 90); do
    result=$(ab eval --json 'window.resumeResult ?? null' | jq -cer 'if .success == false then error(.error) else .data.result end' ) || true
    if [ "$result" != null ] && [ -n "$result" ]; then
      jq -cer 'if .error then error(.error) else . end' <<<"$result"
      return
    fi
    sleep 1
  done
  echo 'browser recovery step timed out' >&2
  return 1
}
trap 'ab close >/dev/null 2>&1 || true' EXIT
seed=$(curl -fsS -X POST "$BASE/admin/bootstrap-seed?count=100000")
principal=$(jq -r .principal <<<"$seed")
scope=$(jq -r .scope <<<"$seed")
ab open "$BASE/bootstrap.html" >/dev/null
ab set viewport 1280 720 2 >/dev/null
ab eval "localStorage.setItem('echo-test:net:$APP/trial', 'offline')" >/dev/null
ab open "$BASE/?app=$APP&profile=trial&principal=$principal&syncUrl=$BASE/sync" >/dev/null
ab wait --fn '!!window.echo' >/dev/null
before=$(ev <<'JS'
const read = () => echo.exec("SELECT (SELECT count(*) FROM echo_staged) AS pages, (SELECT json_extract(page, '$.continuation') FROM echo_staged ORDER BY ord DESC LIMIT 1) AS tail, (SELECT json_extract(value, '$.cursor') FROM echo_meta WHERE key='sync_meta') AS cursor, (SELECT count(*) FROM todos) AS rows").then(r => { if (!r.ok) throw new Error(JSON.stringify(r.error)); return r.value[0]; });
await read();
echo.setOffline(false);
await echo.requestSync();
const staged = await echo.until(async () => (await read()).pages > 0, 30000);
if (!staged) throw new Error('no durable page observed: ' + JSON.stringify(await echo.syncStatus()));
echo.setOffline(true);
const offline = await echo.until(async () => (await echo.syncStatus()).value?.value?.lastError?.kind === 'network', 15000);
if (!offline) throw new Error('network gate did not abort sync');
const saved = await read();
if (saved.pages < 1 || !saved.tail || saved.cursor !== null || saved.rows !== 0) throw new Error('partial set exposed or not staged: ' + JSON.stringify(saved));
return saved;
JS
)
ab reload >/dev/null
ab wait --fn '!!window.echo' >/dev/null
reopened=$(ev <<'JS'
const r = await echo.exec("SELECT (SELECT count(*) FROM echo_staged) AS pages, (SELECT json_extract(page, '$.continuation') FROM echo_staged ORDER BY ord DESC LIMIT 1) AS tail, (SELECT json_extract(value, '$.cursor') FROM echo_meta WHERE key='sync_meta') AS cursor, (SELECT count(*) FROM todos) AS rows");
if (!r.ok) throw new Error(JSON.stringify(r.error));
return r.value[0];
JS
)
[[ "$(jq -cS . <<<"$before")" = "$(jq -cS . <<<"$reopened")" ]] || { echo 'FAIL staging changed across offline reload' >&2; exit 1; }
result=$(ev <<JS
const query = async (sql, params = []) => { const r = await echo.exec(sql, params); if (!r.ok) throw new Error(JSON.stringify(r.error)); return r.value; };
echo.setOffline(false);
const complete = await echo.until(async () => (await query('SELECT count(*) AS n FROM todos'))[0].n === 100000, 60000);
if (!complete) throw new Error('resume did not complete');
const bad = (await query("WITH numbered AS (SELECT *, CAST(substr(id,-6) AS INTEGER) AS n FROM todos) SELECT count(*) AS n FROM numbered WHERE n < 1 OR n > 100000 OR id != ? || '-' || printf('%06d',n) OR access_scope_id != ? OR title != 'Todo ' || n OR note IS NOT CASE WHEN n % 3 = 0 THEN NULL ELSE 'Note ' || n || ' café' END", ['$scope','$scope']))[0].n;
const staged = (await query('SELECT count(*) AS n FROM echo_staged'))[0].n;
if (bad !== 0 || staged !== 0) throw new Error('invalid final rows or uncleared staging');
return { validatedRows: 100000, invalidRows: bad, stagedPages: staged };
JS
)
jq -cn --argjson before "$before" --argjson reopened "$reopened" --argjson result "$result" \
  '{pass:true,before:$before,afterOfflineReload:$reopened,result:$result}'
echo 'PASS active HTTP abort + offline OPFS reload + durable continuation resume; all 100000 rows validated' >&2
