# thermio-ingest

thermio 数据管道服务（Go，ADR-001/016）：**MQTT in → TimescaleDB / Kafka out** 的
接口面极窄管道——不消费领域契约、不做业务逻辑，只认 `point_id / 单位 / 量程 /
质量位`。设计全文见伞仓 `docs/design/ingest.md`；TSDB DDL 蓝本见 `docs/design/ddl.md`
（v1.2）。

## 当前状态

IMPL-4 建仓骨架（DAT-98）：包结构 + 钉版本依赖 + CI + TSDB 迁移链落盘。
管线实现（共享订阅 → 解码 → 单位归一 → 质量标记 → 批量写入）= IMPL-5。

## 布局

```
cmd/ingestd            守护进程入口
internal/
  mqtt/                EMQX 共享订阅接入（ingest.md §2）
  decode/              信封解码与 §3.2 字段规则
  points/              point/gateway 配置缓存（§6）
  units/               单位归一转换表 v1（§5.2）
  quality/             quality 位掩码 v1 冻结位表（§4）
  tsdb/                telemetry 批量 upsert（§7）
  kafkaproducer/       franz-go producer（§7.4）
  dlq/                 死信原因封闭集（§9）
  metrics/             指标名（§10，OBS-MT-04）
db/
  bootstrap/           TSDB 角色 bootstrap（迁移前置，不入版本链）
  migrations/tsdb/     迁移链 0001–0004（蓝本 ddl.md v1.2 §11）
scripts/               迁移冒烟 + ddl.md §8 用例 8–11 复跑
```

依赖钉版本（ADR-016 指定）：`paho.mqtt.golang` / `franz-go` / `pgx v5`。

## 开发

```bash
go build ./... && go vet ./... && go test ./...

# 静态二进制（私有化交付形态）
CGO_ENABLED=0 go build -o bin/ingestd ./cmd/ingestd

# TSDB 迁移冒烟（一次性干净容器；环境隔离纪律：不连宿主机/其他项目已有服务）
scripts/tsdb-migration-smoke.sh
```

CI（`.github/workflows/ci.yml`）：lint + test + vet + 静态构建 + TSDB 迁移冒烟。

环境隔离（2026-09-26 指示）：涉及数据库/中间件的开发、测试、联调一律用本项目
deploy compose 独立栈或一次性干净容器（跑完即清）。
