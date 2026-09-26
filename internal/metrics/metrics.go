// Package metrics 冻结指标名（ingest.md §10，OBS-MT-04：命名带域前缀）。
// prom-client 注册与打点随 IMPL-5 落地；ADR-014 触发器口径（写 P99 > 500ms 等）
// 依赖这些名字保持稳定。
package metrics

// 指标名（ingest.md §10 表）。
const (
	MQTLMessagesTotal       = "ingest_mqtt_messages_total"       // counter
	PointsTotal             = "ingest_points_total"              // counter {result=ok|dlq}
	DLQMessagesTotal        = "ingest_dlq_messages_total"        // counter {reason}
	PipelineLatencyMS       = "ingest_pipeline_latency_ms"       // histogram
	TSDBWriteLatencyMS      = "ingest_tsdb_write_latency_ms"     // histogram（P99 > 500ms 触发扩容/分片）
	TSDBWriteFailuresTotal  = "ingest_tsdb_write_failures_total" // counter
	KafkaProduceLatencyMS   = "ingest_kafka_produce_latency_ms"  // histogram
	BufferRows              = "ingest_buffer_rows"               // gauge
	BackpressureActive      = "ingest_backpressure_active"       // gauge
	ConfigCachePoints       = "ingest_config_cache_points"       // gauge
	ConfigRefreshAgeSeconds = "ingest_config_refresh_age_s"      // gauge
	UnregisteredPointsTotal = "ingest_unregistered_points_total" // counter
)
