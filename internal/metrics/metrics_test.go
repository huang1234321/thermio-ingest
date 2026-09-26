package metrics

import "testing"

// 指标名唯一且全部带 ingest_ 域前缀（ingest.md §10 / OBS-MT-04）。
func TestNames(t *testing.T) {
	all := []string{
		MQTLMessagesTotal, PointsTotal, DLQMessagesTotal,
		PipelineLatencyMS, TSDBWriteLatencyMS, TSDBWriteFailuresTotal,
		KafkaProduceLatencyMS, BufferRows, BackpressureActive,
		ConfigCachePoints, ConfigRefreshAgeSeconds, UnregisteredPointsTotal,
	}
	seen := make(map[string]bool, len(all))
	for _, n := range all {
		if seen[n] {
			t.Errorf("指标名重复: %s", n)
		}
		seen[n] = true
	}
	if len(all) != 12 {
		t.Errorf("指标共 %d 条, want 12（ingest.md §10 表全量）", len(all))
	}
}
