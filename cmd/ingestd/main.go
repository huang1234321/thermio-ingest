// Command ingestd 是 thermio 遥测管道守护进程（MQTT in → TimescaleDB / Kafka out，
// ADR-016：Go 不越过 Kafka 缝进入应用族）。
//
// 本文件为 IMPL-4 建仓骨架：只组装并打印组件清单即退出。
// 管线接线（共享订阅 → 解码 → 点位解析 → 单位归一 → 质量标记 → 批量写入）
// 随 IMPL-5（ingest.md §5/§12）逐步接入各 internal 包。
package main

import (
	"log/slog"
	"os"
)

// version 由构建时注入：-ldflags "-X main.version=v1.2.3"。
var version = "dev"

// component 描述 internal 包的职责锚点（ingest.md §12 包结构）。
type component struct {
	Package string
	Role    string
}

func components() []component {
	return []component{
		{"internal/mqtt", "EMQX 共享订阅接入（§2）"},
		{"internal/decode", "信封解码与 §3.2 字段规则校验"},
		{"internal/points", "point/gateway 配置缓存（§6）"},
		{"internal/units", "单位归一内置转换表 v1（§5.2）"},
		{"internal/quality", "quality 位掩码 v1 冻结位表（§4）"},
		{"internal/tsdb", "telemetry 批量 upsert（§7）"},
		{"internal/kafkaproducer", "franz-go producer（§7.4）"},
		{"internal/dlq", "死信原因封闭集（§9）"},
		{"internal/metrics", "指标名与打点（§10）"},
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)) // 结构化 JSON（§10）
	logger.Info("ingestd skeleton", "version", version,
		"status", "skeleton (IMPL-4); pipeline wiring lands with IMPL-5 (ingest.md §12)")
	for _, c := range components() {
		logger.Info("component", "package", c.Package, "role", c.Role)
	}
}
