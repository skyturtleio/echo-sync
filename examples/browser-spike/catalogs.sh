#!/usr/bin/env bash
# Global catalog discovery, publication, offline persistence; CATALOG_BENCH=1 measures 85k rows.
source "$(dirname "$0")/lib.sh"
export ECHO_EVIDENCE=1
APP="catalog-$RUN"
curl -sf "$BASE/admin/catalog-seed?count=3" >/dev/null
T=$(open_tab "$BASE/?app=$APP&principal=catalog-reader")
ev <<<"$SERVED" >/dev/null
check "catalog downloads without grants and publishes a version" "$(ev <<<'await echo.until(async () => { const r = await echo.exec("SELECT count(*) AS n FROM airport_reference"); return r.ok && r.value[0].n === 3 })
const v = await echo.exec("SELECT value FROM echo_meta WHERE key = ?", ["catalog:airport_reference"])
window.oldCatalogVersion = v.value[0].value
window.catalogSub = echo.subscribe(["SELECT count(*) AS n FROM airport_reference"], ["airport_reference"])
await echo.until(() => catalogSub.emissions.length > 0)
return { pass: v.ok && typeof oldCatalogVersion === "string", version: oldCatalogVersion }')"
curl -sf "$BASE/admin/catalog-seed?count=3" >/dev/null
check "identical reimport keeps the advertised version" "$(ev <<<'await echo.requestSync()
await echo.until(async () => { const r = await echo.exec("SELECT value FROM echo_meta WHERE key = ?", ["catalog:airport_reference"]); return r.ok })
const r = await echo.exec("SELECT value FROM echo_meta WHERE key = ?", ["catalog:airport_reference"])
return { pass: r.value[0].value === oldCatalogVersion }')"
COUNT=7
[ "${CATALOG_BENCH:-0}" = 1 ] && COUNT=85000
curl -sf "$BASE/admin/catalog-seed?count=$COUNT" >/dev/null
check "catalog-only commit pokes the leader and invalidates an open query after swap" "$(ev <<<"await echo.until(() => catalogSub.emissions.some(e => e.results[0][0].n === $COUNT), 120000)
const r = await echo.exec('SELECT value FROM echo_meta WHERE key = ?', ['catalog:airport_reference'])
window.catalogVersion = r.value[0].value
return { pass: catalogVersion !== oldCatalogVersion, count: $COUNT, emissions: catalogSub.emissions.length }")"
if [ "${CATALOG_BENCH:-0}" = 1 ]; then
  check "85000-row OPFS catalog benchmark" "$(ev <<<'const b = await echo.catalogBenchmark(catalogVersion); return { pass: b.ok && b.result.rows === 85000 && b.result.queries >= 100 && !b.result.queryError, ...b }')"
  check "hard kill after a committed shadow chunk preserves the published catalog" "$(ev <<<'const id = "crash-" + Date.now()
const killed = await echo.catalogBenchmark(catalogVersion, "crash", id)
const reopened = await echo.catalogBenchmark(catalogVersion, "recover", id)
return { pass: killed.paused === 500 && reopened.ok && reopened.result.rows === 3 && reopened.result.version === "previous" && reopened.result.abandoned === 1 && reopened.result.shadows === 0, killed, reopened }')"
fi
ev <<<'await echo.setOffline(true); return true' >/dev/null
ab reload >/dev/null
ev <<<"$SERVED" >/dev/null
check "catalog survives offline reload" "$(ev <<<"
const r = await echo.exec('SELECT count(*) AS n FROM airport_reference')
return { pass: r.ok && r.value[0].n === $COUNT, rows: r }")"
ab tab close "$T" >/dev/null
ab close >/dev/null
echo "$((total - fails))/$total passed"
exit $((fails > 0))
