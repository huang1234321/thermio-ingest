package metrics

import (
	"testing"
)

// 指标名唯一且全部带 ingest_ 域前缀（ingest.md §10 / OBS-MT-04）。
// §10 表 12 项 + §3.2/§8 要求的 seq 缺口计数 = 13（只增不改）。
func TestNames(t *testing.T) {
	all := []string{
		MQTLMessagesTotal, PointsTotal, DLQMessagesTotal,
		PipelineLatencyMS, TSDBWriteLatencyMS, TSDBWriteFailuresTotal,
		KafkaProduceLatencyMS, BufferRows, BackpressureActive,
		ConfigCachePoints, ConfigRefreshAgeSeconds, UnregisteredPointsTotal,
		SeqGapsTotal,
	}
	seen := make(map[string]bool, len(all))
	for _, n := range all {
		if seen[n] {
			t.Errorf("指标名重复: %s", n)
		}
		seen[n] = true
	}
	if len(all) != 13 {
		t.Errorf("指标共 %d 条, want 13（§10 表 12 项 + seq 缺口计数）", len(all))
	}
}

// New 注册全套 collector 且 /metrics handler 可用。
func TestNewRegistry(t *testing.T) {
	m := New(
		func() float64 { return 42 },
		func() float64 { return 1 },
		func() float64 { return 7 },
		func() float64 { return 3 },
	)
	m.MQTTMessages.Inc()
	m.Points.WithLabelValues("ok").Add(10)
	m.Points.WithLabelValues("dlq").Add(2)
	m.DLQ.WithLabelValues("TS_INVALID").Inc()
	m.SeqGaps.Inc()

	if m.Handler() == nil {
		t.Error("Handler 不应为 nil")
	}
}
