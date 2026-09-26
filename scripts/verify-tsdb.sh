#!/usr/bin/env bash
# verify-tsdb.sh —— ddl.md §8 用例 8–11 脚本化复跑（DAT-98 / IMPL-4 验收口径）。
#
#   用例 8  telemetry 幂等 upsert（ON CONFLICT DO UPDATE，last-write-wins）
#   用例 9  cagg 聚合正确性（avg 忽略 NULL、bad_count、bit_or quality_mask）
#   用例 10 TSDB 角色矩阵（ddl.md §5.4：ingest 窄写 / api 只读 / algo 读+天气写）
#   用例 11 压缩策略 + 压缩后补传（自动解压；cagg 手动刷新收敛，runbook 路径）
#
# 前置：迁移链已 goose up（角色经 bootstrap 建立）。角色一律经 TCP 口令认证真实
# 登录（非 SET ROLE 模拟，ddl.md §8.1 口径）。脚本幂等可重跑：探针数据先清后插。
#
# 环境变量：TSDB_HOST TSDB_PORT TSDB_SUPER_PASSWORD
#           TSDB_INGEST_PASSWORD TSDB_API_PASSWORD TSDB_ALGO_PASSWORD
set -uo pipefail

: "${TSDB_HOST:?need TSDB_HOST}" "${TSDB_PORT:?need TSDB_PORT}"
: "${TSDB_SUPER_PASSWORD:?}" "${TSDB_INGEST_PASSWORD:?}" "${TSDB_API_PASSWORD:?}" "${TSDB_ALGO_PASSWORD:?}"

DB=thermio_ts
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); echo "PASS $1"; }
bad() { FAIL=$((FAIL+1)); echo "FAIL $1"; }

_psql() { # _psql <user> <password> [psql args...]
  local user=$1 pw=$2; shift 2
  PGPASSWORD="$pw" psql -h "$TSDB_HOST" -p "$TSDB_PORT" -U "$user" -d "$DB" -v ON_ERROR_STOP=1 "$@"
}
admin()  { _psql postgres "$TSDB_SUPER_PASSWORD" "$@"; }
ingest() { _psql tsdb_ingest "$TSDB_INGEST_PASSWORD" "$@"; }
api()    { _psql tsdb_api "$TSDB_API_PASSWORD" "$@"; }
algo()   { _psql tsdb_algo "$TSDB_ALGO_PASSWORD" "$@"; }

# probe <label> <ok|deny> <rolefn> [psql args...]
# 注意：macOS bash 3.2 下 $var 紧邻多字节字符会被并入变量名，一律 ${var} 包裹。
probe() {
  local label=$1 expect=$2 role=$3; shift 3
  if out=$("$role" "$@" 2>&1); then
    if [[ $expect == ok ]]; then ok "$label"; else bad "${label}（预期拒绝，实际成功）"; fi
  else
    if [[ $expect == deny ]]; then ok "${label}（被拒）"; else bad "${label}（预期成功，实际失败）: $out"; fi
  fi
}
near() { awk -v a="$1" -v b="$2" 'BEGIN{exit !(a-b<1e-9 && b-a<1e-9)}'; }

# 探针数据清场（重跑幂等）：保留策略是唯一删除通道，清理走管理员
admin -c "DELETE FROM telemetry WHERE point_id IN (101, 102, 103)" >/dev/null

UPSERT="INSERT INTO telemetry (point_id, ts, value, quality)
        VALUES (101, date_trunc('hour', now() - interval '2 hours'), %s, 0)
        ON CONFLICT (point_id, ts) DO UPDATE
        SET value = EXCLUDED.value, value_text = EXCLUDED.value_text, quality = EXCLUDED.quality"

# ── 用例 8：幂等 upsert（last-write-wins）───────────────────────────────
echo "== 用例 8 telemetry 幂等 upsert =="
ingest -c "${UPSERT/\%s/7.42}" >/dev/null || bad "用例8 首次 upsert 失败"
ingest -c "${UPSERT/\%s/7.44}" >/dev/null || bad "用例8 第二次 upsert 失败"
n=$(ingest -tAc "SELECT count(*) FROM telemetry WHERE point_id = 101")
v=$(ingest -tAc "SELECT value FROM telemetry WHERE point_id = 101 ORDER BY ts DESC LIMIT 1")
if [[ $n == 1 ]] && near "$v" 7.44; then
  ok "upsert last-write-wins（7.42 → 7.44，1 行，终值 ${v}）"
else
  bad "用例8 行数=${n:-空} 终值=${v:-空}，want 1 行 / 7.44"
fi

# ── 用例 9：cagg 聚合正确性 ─────────────────────────────────────────────
echo "== 用例 9 cagg 聚合正确性（avg 忽略 NULL / bad_count / quality_mask）=="
# NULL+quality=4 行放在桶中间：last 取最新行的 value，NULL 行在最后会污染 last
ingest >/dev/null <<'SQL'
INSERT INTO telemetry (point_id, ts, value, quality) VALUES
  (102, date_trunc('hour', now() - interval '2 hours') + interval '1 min', 7.4, 0),
  (102, date_trunc('hour', now() - interval '2 hours') + interval '2 min', NULL, 4),
  (102, date_trunc('hour', now() - interval '2 hours') + interval '3 min', 7.5, 0),
  (102, date_trunc('hour', now() - interval '2 hours') + interval '4 min', 7.6, 0)
ON CONFLICT (point_id, ts) DO NOTHING;
SQL
admin -c "CALL refresh_continuous_aggregate('telemetry_5min',
          date_trunc('hour', now() - interval '2 hours'),
          date_trunc('hour', now() - interval '2 hours') + interval '5 minutes')" >/dev/null
row=$(api -tAc "SELECT avg,min,max,last,stddev,sample_count,bad_count,quality_mask
                FROM telemetry_5min
                WHERE point_id = 102 AND bucket = date_trunc('hour', now() - interval '2 hours')")
IFS='|' read -r avg min max last stddev sc bc qm <<< "$row"
[[ ${sc:-x} == 4 ]]       && ok "sample_count=4（NULL 行计入分母）"  || bad "sample_count=${sc:-空} ≠ 4"
[[ ${bc:-x} == 1 ]]       && ok "bad_count=1（quality<>0 计 1 行）"  || bad "bad_count=${bc:-空} ≠ 1"
[[ ${qm:-x} == 4 ]]       && ok "quality_mask=4（bit_or）"           || bad "quality_mask=${qm:-空} ≠ 4"
near "${avg:-9e9}" 7.5    && ok "avg=7.5（NULL 忽略）"               || bad "avg=${avg:-空} ≠ 7.5"
near "${min:-9e9}" 7.4    && ok "min=7.4"                           || bad "min=${min:-空} ≠ 7.4"
near "${max:-9e9}" 7.6    && ok "max=7.6"                           || bad "max=${max:-空} ≠ 7.6"
near "${last:-9e9}" 7.6   && ok "last=7.6"                          || bad "last=${last:-空} ≠ 7.6"
near "${stddev:-9e9}" 0.1 && ok "stddev=0.1"                        || bad "stddev=${stddev:-空} ≠ 0.1"

# ── 用例 10：TSDB 角色矩阵（ddl.md §5.4 / §7.1 #20）────────────────────
echo "== 用例 10 角色矩阵（TCP 口令认证真实登录）=="
probe "tsdb_ingest 可 SELECT telemetry"               ok   ingest -c "SELECT 1 FROM telemetry LIMIT 1"
probe "tsdb_ingest 可 upsert telemetry"               ok   ingest -c "${UPSERT/\%s/7.44}"
probe "tsdb_ingest 不可读 telemetry_5min"             deny ingest -tAc "SELECT count(*) FROM telemetry_5min"
probe "tsdb_ingest 不可读 telemetry_1h"               deny ingest -tAc "SELECT count(*) FROM telemetry_1h"
probe "tsdb_ingest 不可读 weather_actual"             deny ingest -tAc "SELECT count(*) FROM weather_actual"
probe "tsdb_ingest 不可读 weather_forecast"           deny ingest -tAc "SELECT count(*) FROM weather_forecast"
probe "tsdb_ingest 无 DELETE（保留策略是唯一删除通道）" deny ingest -c "DELETE FROM telemetry WHERE point_id = -1"
probe "tsdb_api 可读 telemetry"                       ok   api -tAc "SELECT count(*) FROM telemetry"
probe "tsdb_api 可读 telemetry_5min"                  ok   api -tAc "SELECT count(*) FROM telemetry_5min"
probe "tsdb_api 可读 telemetry_1h"                    ok   api -tAc "SELECT count(*) FROM telemetry_1h"
probe "tsdb_api 可读 weather_actual"                  ok   api -tAc "SELECT count(*) FROM weather_actual"
probe "tsdb_api 可读 weather_forecast"                ok   api -tAc "SELECT count(*) FROM weather_forecast"
probe "tsdb_api 不可写 telemetry"                     deny api -c "INSERT INTO telemetry (point_id, ts, value, quality) VALUES (-1, now(), 0, 0)"
probe "tsdb_api 不可写 weather_actual"                deny api -c "INSERT INTO weather_actual (station_id, obs_ts, source) VALUES ('x', now(), 'x')"
probe "tsdb_algo 可读 telemetry"                      ok   algo -tAc "SELECT count(*) FROM telemetry"
probe "tsdb_algo 可读 telemetry_5min"                 ok   algo -tAc "SELECT count(*) FROM telemetry_5min"
probe "tsdb_algo 可写 weather_actual（upsert）"       ok   algo -c "INSERT INTO weather_actual (station_id, obs_ts, temp_c, source) VALUES ('probe', now(), 1.5, 'probe') ON CONFLICT (station_id, obs_ts) DO UPDATE SET temp_c = EXCLUDED.temp_c"
probe "tsdb_algo 可 UPDATE weather_actual"            ok   algo -c "UPDATE weather_actual SET temp_c = temp_c WHERE station_id = 'probe'"
probe "tsdb_algo 不可写 telemetry"                    deny algo -c "INSERT INTO telemetry (point_id, ts, value, quality) VALUES (-1, now(), 0, 0)"

# ── 用例 11：压缩 + 压缩后补传（ddl.md §8.1 用例 11）───────────────────
echo "== 用例 11 压缩策略 + 压缩后补传 =="
ingest >/dev/null <<'SQL'
INSERT INTO telemetry (point_id, ts, value, quality) VALUES
  (103, date_trunc('hour', now() - interval '10 days') + interval '1 min', 7.4, 0),
  (103, date_trunc('hour', now() - interval '10 days') + interval '2 min', 7.5, 0),
  (103, date_trunc('hour', now() - interval '10 days') + interval '3 min', 7.6, 0)
ON CONFLICT (point_id, ts) DO NOTHING;
SQL
admin >/dev/null <<'SQL'
SELECT compress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass)
FROM timescaledb_information.chunks
WHERE hypertable_name = 'telemetry' AND NOT is_compressed
  AND range_start < now() - interval '9 days';
SQL
n=$(admin -tAc "SELECT count(*) FROM timescaledb_information.chunks
                WHERE hypertable_name = 'telemetry' AND is_compressed
                  AND range_start < now() - interval '9 days'")
[[ ${n:-0} -ge 1 ]] && ok "10 天前 chunk 已压缩（is_compressed=t，${n} 个）" || bad "无已压缩 chunk"
# 补传落已压缩 chunk：自动解压（tsdb_ingest 真实写路径；quality=256 = backfill 位）
probe "压缩后补传成功（自动解压）" ok ingest -c "INSERT INTO telemetry (point_id, ts, value, quality)
        VALUES (103, date_trunc('hour', now() - interval '10 days') + interval '4 min', 6.5, 256)
        ON CONFLICT (point_id, ts) DO NOTHING"
# 物化收敛走 runbook 手动刷新（补传超 5 天不自动覆盖，ingest.md §8）
admin -c "CALL refresh_continuous_aggregate('telemetry_5min',
          date_trunc('hour', now() - interval '10 days'),
          date_trunc('hour', now() - interval '10 days') + interval '5 minutes')" >/dev/null
row=$(api -tAc "SELECT avg, sample_count FROM telemetry_5min
                WHERE point_id = 103 AND bucket = date_trunc('hour', now() - interval '10 days')")
IFS='|' read -r avg103 sc103 <<< "$row"
[[ ${sc103:-x} == 4 ]] && ok "cagg 收敛：4 行落 1 桶" || bad "补传后 sample_count=${sc103:-空} ≠ 4"
near "${avg103:-9e9}" 7.25 && ok "桶均值 7.25（(7.4+7.5+7.6+6.5)/4）" || bad "补传后 avg=${avg103:-空} ≠ 7.25"

echo
echo "verify-tsdb: PASS=$PASS FAIL=$FAIL"
[[ $FAIL -eq 0 ]]
