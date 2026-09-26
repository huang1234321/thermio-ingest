#!/usr/bin/env bash
# IMPL-5 集成测试编排：自起 thermio 伞仓 deploy/docker-compose.dev.yml 独立栈
# （EMQX+Kafka+TSDB+PG；环境隔离纪律——容器名前缀 thermio-、独立 compose project/
# network/volume，禁止复用宿主机或其他项目既有服务；宿主端口被占时改本项目映射）。
#
# 用法：scripts/integration.sh [umbrella-repo-path]
#   umbrella 默认 ../thermio（与 thermio-ingest 同级检出），或设 THERMIO_UMBRELLA。
# 流程：选空闲端口 → compose up（子集服务）→ TSDB 角色 bootstrap + goose 迁移链
#       → PG 测试 schema → go test -tags integration → 退出码透传 → teardown（-v 清卷）。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UMBRELLA="${1:-${THERMIO_UMBRELLA:-$(cd "$REPO_ROOT/.." && pwd)/thermio}}"
COMPOSE_FILE="$UMBRELLA/deploy/docker-compose.dev.yml"
if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "ERROR: 找不到 ${COMPOSE_FILE}（传伞仓路径或设 THERMIO_UMBRELLA）" >&2
  exit 2
fi
command -v nc >/dev/null || { echo "ERROR: 需要 nc（端口探测）" >&2; exit 2; }

# ── 独立端口选择（宿主 5432/5433 等常被占：改本项目映射，不动既有服务）────
PICKED=" "
# pick_port <目标变量名> <起始候选>：跳过本机占用与本轮已选（无子 shell，
# 变量经 printf -v 回写——$() 会丢状态）。
pick_port() {
  local __var="$1" port="$2"
  while [[ "$PICKED" == *" $port "* ]] || nc -z 127.0.0.1 "$port" 2>/dev/null; do
    port=$((port + 1))
  done
  PICKED="$PICKED$port "
  printf -v "$__var" '%s' "$port"
}
pick_port EMQX_PORT 18830
pick_port KAFKA_PORT 19092
pick_port PG_PORT 15432
pick_port TSDB_PORT 15433
echo "ports: emqx=$EMQX_PORT kafka=$KAFKA_PORT pg=$PG_PORT tsdb=$TSDB_PORT"

PG_PASSWORD="it_$(openssl rand -hex 6)"
TSDB_PASSWORD="it_$(openssl rand -hex 6)"
TSDB_INGEST_PASSWORD="it_ingest_$(openssl rand -hex 6)"
TSDB_API_PASSWORD="it_api_$(openssl rand -hex 6)"
TSDB_ALGO_PASSWORD="it_algo_$(openssl rand -hex 6)"

ENV_FILE="$REPO_ROOT/build/it.env"
mkdir -p "$REPO_ROOT/build"
cat >"$ENV_FILE" <<EOF
EMQX_MQTT_PORT=$EMQX_PORT
KAFKA_EXTERNAL_PORT=$KAFKA_PORT
PG_PORT=$PG_PORT
PG_PASSWORD=$PG_PASSWORD
TSDB_PORT=$TSDB_PORT
TSDB_PASSWORD=$TSDB_PASSWORD
EOF

# ── 独立 compose project（防同胞会话互踩）─────────────────────────────────
# 伞仓 compose 固定 container_name: thermio-* 与 project name thermio-dev——
# 并发集成测试（同一台宿主上的其他 agent 会话）会互相 down 掉对方的容器。
# 本脚本以唯一 project 名运行：剥离固定 container_name/网络名（compose 生成
# <project>-<service> 名），容器/网络/卷全部 project 作用域，互不可见。
# 同时把 Kafka EXTERNAL advertised listener 改到实际映射端口（原文件钉死
# localhost:9092；本栈动态选端口后客户端元据会指错端口）。
# 镜像/配置/健康检查仍以伞仓 compose 为唯一蓝本（环境隔离纪律不变）。
RUN_ID="it$(date +%s)"
PROJECT="thermio-it-$RUN_ID"
IT_COMPOSE_FILE="$REPO_ROOT/build/docker-compose.it.yml"
sed -E -e '/^[[:space:]]*container_name:/d' \
       -e '/^[[:space:]]*name: thermio-dev$/d' \
       -e "s#EXTERNAL://localhost:9092#EXTERNAL://localhost:$KAFKA_PORT#g" \
       "$COMPOSE_FILE" > "$IT_COMPOSE_FILE"
COMPOSE="docker compose -p $PROJECT --env-file $ENV_FILE -f $IT_COMPOSE_FILE"
trap '$COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true' EXIT

echo "== 启动独立栈（project=${PROJECT}：emqx kafka kafka-init postgres timescaledb）"
$COMPOSE up -d --wait emqx kafka kafka-init postgres timescaledb

PG_CONTAINER="$PROJECT-postgres-1"
TSDB_CONTAINER="$PROJECT-timescaledb-1"

PG_DSN="postgres://thermio:$PG_PASSWORD@127.0.0.1:$PG_PORT/thermio?sslmode=disable"
TSDB_ADMIN_DSN="postgres://thermio_ts:$TSDB_PASSWORD@127.0.0.1:$TSDB_PORT/thermio_ts?sslmode=disable"
TSDB_INGEST_DSN="postgres://tsdb_ingest:$TSDB_INGEST_PASSWORD@127.0.0.1:$TSDB_PORT/thermio_ts?sslmode=disable"

echo "== TSDB 角色 bootstrap（ddl.md §10：三角色全量，口令随机）+ goose 迁移链 0001–0004（§11）"
docker exec -i "$TSDB_CONTAINER" psql -U thermio_ts -d thermio_ts -v ON_ERROR_STOP=1 \
  -v ingest_password="$TSDB_INGEST_PASSWORD" \
  -v api_password="$TSDB_API_PASSWORD" \
  -v algo_password="$TSDB_ALGO_PASSWORD" <<'SQL'
CREATE ROLE tsdb_ingest LOGIN PASSWORD :'ingest_password';
CREATE ROLE tsdb_api    LOGIN PASSWORD :'api_password';
CREATE ROLE tsdb_algo   LOGIN PASSWORD :'algo_password';
GRANT CONNECT ON DATABASE thermio_ts TO tsdb_ingest, tsdb_api, tsdb_algo;
SQL
( cd "$REPO_ROOT" && go run github.com/pressly/goose/v3/cmd/goose@v3.24.0 \
    -dir db/migrations/tsdb postgres "$TSDB_ADMIN_DSN" up )

echo "== PG 测试 schema（internal/it/testdata/pg_schema.sql）"
docker exec -i "$PG_CONTAINER" psql -U thermio -d thermio -v ON_ERROR_STOP=1 \
  < "$REPO_ROOT/internal/it/testdata/pg_schema.sql"

echo "== go test -tags integration ./internal/it/"
( cd "$REPO_ROOT" &&
  IT_EMQX_URL="tcp://127.0.0.1:$EMQX_PORT" \
  IT_KAFKA_BROKERS="127.0.0.1:$KAFKA_PORT" \
  IT_PG_DSN="$PG_DSN" \
  IT_TSDB_DSN="$TSDB_INGEST_DSN" \
  go test -tags integration -count=1 -timeout 10m -v ./internal/it/ )
