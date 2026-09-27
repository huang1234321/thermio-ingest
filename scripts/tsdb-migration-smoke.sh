#!/usr/bin/env bash
# tsdb-migration-smoke.sh —— 一次性干净 TSDB 上的迁移链冒烟（CI 与本地同一路径）。
#
#   bootstrap 角色 → goose up → ddl.md §8 用例 8–12 复跑 → goose down-to 0 → goose up
#
# 角色口令运行时随机生成，不落源码（SEC-KEY-01）。环境隔离纪律（2026-09-26 小黄
# 指示）：只连自起的一次性容器或本项目 deploy 独立栈，禁止复用宿主机/其他项目
# 已有服务。
#
# 环境变量：TSDB_HOST TSDB_PORT TSDB_SUPER_PASSWORD（管理员口令）
# 可选：    GOOSE_BIN（默认 goose）
set -euo pipefail
cd "$(dirname "$0")/.."

: "${TSDB_HOST:?need TSDB_HOST}" "${TSDB_PORT:?need TSDB_PORT}" "${TSDB_SUPER_PASSWORD:?}"

GOOSE_BIN="${GOOSE_BIN:-goose}"
MIG_DIR=db/migrations/tsdb
DB=thermio_ts
admin_dsn="postgres://postgres:${TSDB_SUPER_PASSWORD}@${TSDB_HOST}:${TSDB_PORT}/${DB}?sslmode=disable"

export TSDB_INGEST_PASSWORD TSDB_API_PASSWORD TSDB_ALGO_PASSWORD
TSDB_INGEST_PASSWORD="$(openssl rand -hex 16)"
TSDB_API_PASSWORD="$(openssl rand -hex 16)"
TSDB_ALGO_PASSWORD="$(openssl rand -hex 16)"

# public 下用户对象计数：3 表（telemetry/weather_actual/weather_forecast）+ 2 cagg
# （TSDB 2.x 的 continuous aggregate 对外是普通视图 relkind 'v'）
obj_count() {
  PGPASSWORD="$TSDB_SUPER_PASSWORD" psql -h "$TSDB_HOST" -p "$TSDB_PORT" -U postgres -d "$DB" -tAc \
    "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public' AND c.relkind IN ('r','m','v')
       AND c.relname IN ('telemetry','telemetry_5min','telemetry_1h','weather_actual','weather_forecast')"
}

echo "== 1/5 bootstrap 角色（db/bootstrap/tsdb-roles.sql，ddl.md §10）=="
PGPASSWORD="$TSDB_SUPER_PASSWORD" psql -h "$TSDB_HOST" -p "$TSDB_PORT" -U postgres -d "$DB" \
  -v ON_ERROR_STOP=1 \
  -v ingest_password="$TSDB_INGEST_PASSWORD" \
  -v api_password="$TSDB_API_PASSWORD" \
  -v algo_password="$TSDB_ALGO_PASSWORD" \
  -f db/bootstrap/tsdb-roles.sql

# 双 cagg real-time 契约（ddl.md §11.2 v1.5 / DAT-158）：materialized_only 必须 false。
# TS ≥2.7 引擎默认是 true（近窗不可见），0002 显式关 + 0005 存量收口后在此永久把关。
cagg_realtime_check() {
  local out
  out=$(PGPASSWORD="$TSDB_SUPER_PASSWORD" psql -h "$TSDB_HOST" -p "$TSDB_PORT" -U postgres -d "$DB" -tAc \
    "SELECT string_agg(view_name || '=' || materialized_only::text, ', ' ORDER BY view_name)
     FROM timescaledb_information.continuous_aggregates
     WHERE view_name IN ('telemetry_5min','telemetry_1h')")
  [[ "$out" == "telemetry_1h=false, telemetry_5min=false" ]] \
    || { echo "FAIL: cagg real-time 契约破坏（want 双 false，got ${out:-空}）"; exit 1; }
  echo "PASS: 双 cagg materialized_only=false（real-time 开）"
}

echo "== 2/5 goose up（管理员执行，ddl.md §5.4）=="
"$GOOSE_BIN" -dir "$MIG_DIR" postgres "$admin_dsn" up
n=$(obj_count)
[[ "$n" == 5 ]] || { echo "FAIL: up 后用户对象 $n ≠ 5（3 表 + 2 cagg）"; exit 1; }
echo "PASS: up 后 5 用户对象（3 hypertable + 2 cagg）"
cagg_realtime_check

echo "== 3/5 ddl.md §8 用例 8–12 复跑（scripts/verify-tsdb.sh）=="
scripts/verify-tsdb.sh

echo "== 4/5 goose down-to 0（回滚干净）=="
# 先停后台策略 job（cagg 刷新/压缩/保留）：Down 摘策略与刚触发的后台 worker 并发
# 会偶发 40P01 死锁（瞬态锁竞争，非数据问题）；停调度 + 失败重试一次兜底。
PGPASSWORD="$TSDB_SUPER_PASSWORD" psql -h "$TSDB_HOST" -p "$TSDB_PORT" -U postgres -d "$DB" \
  -v ON_ERROR_STOP=1 -tA \
  -c "SELECT alter_job(job_id, scheduled => false) FROM timescaledb_information.jobs WHERE job_id IS NOT NULL" >/dev/null
if ! "$GOOSE_BIN" -dir "$MIG_DIR" postgres "$admin_dsn" down-to 0; then
  echo "down 首次失败（疑为后台 job 锁竞争），等待 5s 重试一次"
  sleep 5
  "$GOOSE_BIN" -dir "$MIG_DIR" postgres "$admin_dsn" down-to 0
fi
n=$(obj_count)
[[ "$n" == 0 ]] || { echo "FAIL: down-to 0 后用户对象 $n ≠ 0"; exit 1; }
echo "PASS: down 后 0 用户对象（策略 job 随对象清除）"

echo "== 5/5 goose up（重建，round-trip 闭环）=="
"$GOOSE_BIN" -dir "$MIG_DIR" postgres "$admin_dsn" up
n=$(obj_count)
[[ "$n" == 5 ]] || { echo "FAIL: 重建后用户对象 $n ≠ 5"; exit 1; }
echo "PASS: 重建后 5 用户对象"
cagg_realtime_check

echo
echo "SMOKE OK：bootstrap → up → 用例 8–12 → down-to 0 → up 全部通过"
