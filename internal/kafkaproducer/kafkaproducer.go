// Package kafkaproducer 封装 franz-go producer（ingest.md §7.4：topic
// thermio.telemetry.raw / thermio.telemetry.quality / thermio.dlq，key=gateway_id
// 同网关同分区保序，headers 带 tenant_id/trace_id/gw_seq）。生产循环与优雅退出
// （冲 producer）随 IMPL-5 落地；本文件提供项目级选项基线。
package kafkaproducer

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultOpts 返回 producer 选项基线（ingest.md §7.2/§7.4）：
// at-least-once + 幂等 producer（franz-go 默认启用幂等生产与 AllISR 确认，
// 除非显式 DisableIdempotentWrite，此处不关）；linger 100ms 批效率。
func DefaultOpts(brokers []string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ProducerLinger(100 * time.Millisecond),
	}
}
