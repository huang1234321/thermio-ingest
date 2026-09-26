// Package metrics 冻结指标名（ingest.md §10，OBS-MT-04：命名带域前缀）并持有
// Prometheus 注册与打点。独立 Registry（不用全局 DefaultRegisterer）——测试可
// 多实例并行；ADR-014 触发器口径（写 P99 > 500ms 等）依赖这些名字保持稳定。
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// 指标名（ingest.md §10 表；新增只增不改）。
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
	// §3.2 seq 规则「缺口只记指标与 WARN」的指标落点（§8 面板数据源，增量新增）。
	SeqGapsTotal = "ingest_gw_seq_gaps_total" // counter
)

// Metrics 全套 collector。零值不可用，经 New 构建。
type Metrics struct {
	Registry *prometheus.Registry

	MQTTMessages prometheus.Counter
	Points       *prometheus.CounterVec // result=ok|dlq
	DLQ          *prometheus.CounterVec // reason
	SeqGaps      prometheus.Counter
	Unregistered prometheus.Counter
	TSDBFailures prometheus.Counter

	PipelineLatency     prometheus.Histogram
	TSDBWriteLatency    prometheus.Histogram
	KafkaProduceLatency prometheus.Histogram

	BufferRows         prometheus.GaugeFunc
	BackpressureActive prometheus.GaugeFunc
	ConfigCachePoints  prometheus.GaugeFunc
	ConfigRefreshAge   prometheus.GaugeFunc
}

// gaugeSources GaugeFunc 的数据源（pipeline 注入闭包，避免锁竞争入口分散）。
type gaugeSources struct {
	bufferRows         func() float64
	backpressureActive func() float64
	configCachePoints  func() float64
	configRefreshAge   func() float64
}

// New 注册全套指标并返回。gauge 数据源由调用方闭包提供（背压状态、缓冲行数、
// 配置缓存健康都是 pipeline 内部状态）。
func New(bufferRows, backpressureActive, configCachePoints, configRefreshAge func() float64) *Metrics {
	src := gaugeSources{bufferRows, backpressureActive, configCachePoints, configRefreshAge}
	m := &Metrics{
		Registry: prometheus.NewRegistry(),

		MQTTMessages: prometheus.NewCounter(prometheus.CounterOpts{Name: MQTLMessagesTotal, Help: "MQTT 接收消息数（RED-R）"}),
		Points:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: PointsTotal, Help: "点位级吞吐与去向"}, []string{"result"}),
		DLQ:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: DLQMessagesTotal, Help: "死信分原因（OBS-MT-02：每条 reason 有 runbook）"}, []string{"reason"}),
		SeqGaps:      prometheus.NewCounter(prometheus.CounterOpts{Name: SeqGapsTotal, Help: "网关 seq 缺口数（QoS1 丢包兜底检测，§3.2）"}),
		Unregistered: prometheus.NewCounter(prometheus.CounterOpts{Name: UnregisteredPointsTotal, Help: "未注册点早期信号（注册漏配）"}),
		TSDBFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: TSDBWriteFailuresTotal, Help: "TSDB 批量写失败（重试耗尽）"}),

		PipelineLatency:     prometheus.NewHistogram(prometheus.HistogramOpts{Name: PipelineLatencyMS, Help: "MQTT 收到 → TSDB 落库（ms）", Buckets: prometheus.ExponentialBuckets(1, 2, 14)}),
		TSDBWriteLatency:    prometheus.NewHistogram(prometheus.HistogramOpts{Name: TSDBWriteLatencyMS, Help: "TSDB 批量写耗时（ms）；P99>500 触发扩容/分片（ADR-014）", Buckets: prometheus.ExponentialBuckets(1, 2, 14)}),
		KafkaProduceLatency: prometheus.NewHistogram(prometheus.HistogramOpts{Name: KafkaProduceLatencyMS, Help: "Kafka produce 耗时（ms）", Buckets: prometheus.ExponentialBuckets(1, 2, 14)}),

		BufferRows:         prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: BufferRows, Help: "批量缓冲当前行数"}, src.bufferRows),
		BackpressureActive: prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: BackpressureActive, Help: "背压状态（1=缓冲达上限，停止取MQTT消息）"}, src.backpressureActive),
		ConfigCachePoints:  prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: ConfigCachePoints, Help: "配置缓存点位数"}, src.configCachePoints),
		ConfigRefreshAge:   prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: ConfigRefreshAgeSeconds, Help: "配置缓存年龄（秒）；刷新失败持续增长"}, src.configRefreshAge),
	}

	m.Registry.MustRegister(m.MQTTMessages, m.Points, m.DLQ, m.SeqGaps, m.Unregistered, m.TSDBFailures,
		m.PipelineLatency, m.TSDBWriteLatency, m.KafkaProduceLatency,
		m.BufferRows, m.BackpressureActive, m.ConfigCachePoints, m.ConfigRefreshAge)
	// Go 运行时基线（部署.md 资源预算口径：ingest 128MB）。
	m.Registry.MustRegister(collectors.NewGoCollector())
	return m
}

// Handler /metrics HTTP handler（main 与集成测试共用）。
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
