# thermio-ingest

thermio 数据管道服务（Go，ADR-001/016）：**MQTT in → TimescaleDB / Kafka out** 的
接口面极窄管道——不消费领域契约、不做业务逻辑，只认 `point_id / 单位 / 量程 /
质量位`。设计全文见伞仓 `docs/design/ingest.md`；TSDB DDL 蓝本见 `docs/design/ddl.md`
（v1.2）。

## 当前状态

IMPL-5 管线 MVP（DAT-108）：MQTT → TSDB/Kafka 全链路——共享订阅、§3.2 字段规则、
配置缓存（增量 30s + 全量 1h + 失败保旧）、单位归一 v1、L1b 质量标记、stale 扫描、
批量写入（TSDB → Kafka → PUBACK）、背压、DLQ 全 reason、指标全套、优雅退出。
后置项（ingest.md §5.3/§9）：L2 统计检测（bit6/7）、DLQ 重放工具（v2）、gw-sim（IMPL-6）。

## 处理管线（ingest.md §5/§7）

```
EMQX $share/ingest/thermio/gw/+/up/data（QoS1 手动 ACK）
  → 有界 intake（10k，满则阻塞 → TCP 背压，§7.3）
  → decode worker 池（§3.2 校验 + 网关/点位解析 + 单位归一 + 质量标记）
  → rowGate（BUFFER_MAX_ROWS 行占位；打满 → worker 阻塞 → 背压上传）
  → 单 writer（批内 (point_id, ts) 排序；2000 行/500ms 先到先发）
     TSDB 批量 upsert → Kafka produce（raw/quality/DLQ）→ PUBACK → 释放占位
```

## 布局

```
cmd/ingestd            守护进程入口（组装 + 优雅退出）
internal/
  config/              env 配置（§11，fail-fast）
  mqtt/                EMQX 共享订阅接入（§2）
  decode/              信封解码与 §3.2 字段规则
  points/              point/gateway 配置缓存（§6）
  units/               单位归一转换表 v1（§5.2，公式评审入库）
  quality/             quality 位掩码 v1 冻结位表（§4）+ stale 状态机（§6.3）
  tsdb/                telemetry 批量 upsert（§7.1，重试退避）
  kafkaproducer/       franz-go producer（§7.4，key=gateway_id 同分区保序）
  dlq/                 死信原因封闭集 + 消息体（§9）
  metrics/             Prometheus 指标全套（§10，OBS-MT-04）
  pipeline/            阶段编排/背压/写顺序/优雅退出（§5/§7/§12）
  it/                  集成测试（build tag integration，自起独立栈）
db/
  bootstrap/           TSDB 角色 bootstrap（迁移前置，不入版本链）
  migrations/tsdb/     迁移链 0001–0004（蓝本 ddl.md v1.2 §11）
scripts/               迁移冒烟 + 集成测试编排（自起独立 compose 栈）
```

依赖钉版本（ADR-016 指定）：`paho.mqtt.golang` / `franz-go` / `pgx v5`；
指标 `prometheus/client_golang`（§10）。

## 配置（§11，全部环境变量）

| 变量 | 说明 | 默认 |
|---|---|---|
| `MQTT_BROKER_URL` / `MQTT_USERNAME` / `MQTT_PASSWORD` | EMQX 地址与 ingest 服务账号 | — |
| `MQTT_SHARED_GROUP` | 共享订阅组 | `ingest` |
| `KAFKA_BROKERS` | KRaft 节点（逗号分隔） | — |
| `PG_DSN` | 业务库（thermio_ingest 只读角色，只读 point/gateway） | — |
| `TSDB_DSN` | 遥测库（tsdb_ingest 角色） | — |
| `BATCH_MAX_ROWS` / `BATCH_FLUSH_INTERVAL` / `BUFFER_MAX_ROWS` | §7.2 | 2000 / 500ms / 100000 |
| `CACHE_REFRESH_INTERVAL` / `CACHE_FULL_RESYNC_INTERVAL` | §6.2 | 30s / 1h |
| `METRICS_ADDR` | /metrics 监听 | `:9091` |

## 开发

```bash
go build ./... && go vet ./... && go test ./...

# 静态二进制（私有化交付形态）
CGO_ENABLED=0 go build -o bin/ingestd ./cmd/ingestd

# TSDB 迁移冒烟（一次性干净容器）
scripts/tsdb-migration-smoke.sh

# 集成测试（IMPL-5 验收口径）：自起伞仓 deploy/docker-compose.dev.yml 独立栈
# （EMQX+Kafka+TSDB+PG；唯一 compose project + 动态端口，不与并发运行互踩，
#  禁止复用宿主机/其他项目既有服务）。
scripts/integration.sh            # 伞仓默认在 ../thermio，或传路径 / 设 THERMIO_UMBRELLA
```

CI（`.github/workflows/ci.yml`）：lint + test + vet + 静态构建 + TSDB 迁移冒烟
（集成测试需真实 compose 栈，本机/交付环境执行，不进 CI）。

环境隔离（2026-09-26 指示）：涉及数据库/中间件的开发、测试、联调一律用本项目
deploy compose 独立栈或一次性干净容器（跑完即清）。
