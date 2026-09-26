// Package config 汇集 ingestd 的全部环境变量配置（ingest.md §11：全部环境变量，
// SEC-KEY-01；默认值与 §7.2/§6.2 对齐）。解析失败 fail-fast——配置错误不进运行时。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config ingestd 运行配置。零值不可用，经 Load 构建。
type Config struct {
	// MQTT（§2/§11）：EMQX 地址与 ingest 自身服务账号（内部凭证，非设备凭证）。
	MQTTBrokerURL string
	MQTTUsername  string
	MQTTPassword  string
	MQTTClientID  string
	MQTTGroup     string // 共享订阅组，默认 ingest

	KafkaBrokers []string

	// PGDSN：业务库连接（thermio_ingest 旁路只读角色，只读 point/gateway 两表）。
	PGDSN string
	// TSDBDSN：遥测库连接（tsdb_ingest 角色）。
	TSDBDSN string

	// 批量与背压（§7.2，全部可 env 覆盖）。
	BatchMaxRows       int
	BatchFlushInterval time.Duration
	BufferMaxRows      int

	// 配置缓存刷新（§6.2）。
	CacheRefreshInterval    time.Duration
	CacheFullResyncInterval time.Duration

	// 派生常量（ingest.md 冻结值，env 可覆盖仅为测试便利）。
	StaleScanInterval time.Duration // §6.3 后台扫描周期 30s
	TsSkewThreshold   time.Duration // bit5 阈值 10min
	BackfillThreshold time.Duration // bit8 阈值 10min
	RetentionCutoff   time.Duration // §7.1 拒写线 2 年
	MaxQueueMessages  int           // §5.1 有界内存队列 10k 消息

	// MetricsAddr：Prometheus /metrics 监听地址（ADR-017；deploy prometheus.yml 预留 :9091）。
	MetricsAddr string
}

// Load 从环境变量构建配置；必填项缺失或数值非法即报错（启动即败，不带病运行）。
func Load() (Config, error) { return load(os.Getenv) }

// load 与测试注入的 env 读取函数解耦。
func load(getenv func(string) string) (Config, error) {
	c := Config{
		MQTTBrokerURL: getenv("MQTT_BROKER_URL"),
		MQTTUsername:  getenv("MQTT_USERNAME"),
		MQTTPassword:  getenv("MQTT_PASSWORD"),
		MQTTClientID:  envOr(getenv, "MQTT_CLIENT_ID", "svc-ingest"),
		MQTTGroup:     envOr(getenv, "MQTT_SHARED_GROUP", "ingest"),
		KafkaBrokers:  splitCSV(getenv("KAFKA_BROKERS")),
		PGDSN:         getenv("PG_DSN"),
		TSDBDSN:       getenv("TSDB_DSN"),
		MetricsAddr:   envOr(getenv, "METRICS_ADDR", ":9091"),
	}

	var err error
	if c.BatchMaxRows, err = envInt(getenv, "BATCH_MAX_ROWS", 2000); err != nil {
		return c, err
	}
	if c.BatchFlushInterval, err = envDur(getenv, "BATCH_FLUSH_INTERVAL", 500*time.Millisecond); err != nil {
		return c, err
	}
	if c.BufferMaxRows, err = envInt(getenv, "BUFFER_MAX_ROWS", 100000); err != nil {
		return c, err
	}
	if c.CacheRefreshInterval, err = envDur(getenv, "CACHE_REFRESH_INTERVAL", 30*time.Second); err != nil {
		return c, err
	}
	if c.CacheFullResyncInterval, err = envDur(getenv, "CACHE_FULL_RESYNC_INTERVAL", time.Hour); err != nil {
		return c, err
	}
	if c.StaleScanInterval, err = envDur(getenv, "STALE_SCAN_INTERVAL", 30*time.Second); err != nil {
		return c, err
	}
	if c.TsSkewThreshold, err = envDur(getenv, "TS_SKEW_THRESHOLD", 10*time.Minute); err != nil {
		return c, err
	}
	if c.BackfillThreshold, err = envDur(getenv, "BACKFILL_THRESHOLD", 10*time.Minute); err != nil {
		return c, err
	}
	if c.RetentionCutoff, err = envDur(getenv, "RETENTION_CUTOFF", 2*365*24*time.Hour); err != nil {
		return c, err
	}
	if c.MaxQueueMessages, err = envInt(getenv, "MQTT_QUEUE_MESSAGES", 10000); err != nil {
		return c, err
	}

	switch {
	case c.MQTTBrokerURL == "":
		return c, fmt.Errorf("config: MQTT_BROKER_URL 必填（EMQX 地址）")
	case len(c.KafkaBrokers) == 0:
		return c, fmt.Errorf("config: KAFKA_BROKERS 必填（逗号分隔）")
	case c.PGDSN == "":
		return c, fmt.Errorf("config: PG_DSN 必填（thermio_ingest 只读角色）")
	case c.TSDBDSN == "":
		return c, fmt.Errorf("config: TSDB_DSN 必填（tsdb_ingest 角色）")
	case c.BatchMaxRows <= 0 || c.BufferMaxRows <= 0 || c.BufferMaxRows < c.BatchMaxRows:
		return c, fmt.Errorf("config: 批量/缓冲参数非法（BATCH_MAX_ROWS=%d BUFFER_MAX_ROWS=%d，需 0 < BATCH ≤ BUFFER）",
			c.BatchMaxRows, c.BufferMaxRows)
	case c.BatchFlushInterval <= 0 || c.CacheRefreshInterval <= 0 || c.CacheFullResyncInterval <= 0 || c.StaleScanInterval <= 0:
		return c, fmt.Errorf("config: 周期参数必须为正")
	case c.MaxQueueMessages <= 0:
		return c, fmt.Errorf("config: MQTT_QUEUE_MESSAGES 必须为正")
	}
	return c, nil
}

// MustLoad 仅供 cmd 入口：配置错误直接终止进程（启动期 fail-fast，非库语义）。
func MustLoad() Config {
	c, err := load(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return c
}

func envOr(getenv func(string) string, key, def string) string {
	if v := strings.TrimSpace(getenv(key)); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envInt(getenv func(string) string, key string, def int) (int, error) {
	v := strings.TrimSpace(getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q 不是整数: %w", key, v, err)
	}
	return n, nil
}

func envDur(getenv func(string) string, key string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(getenv(key))
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q 不是时长: %w", key, v, err)
	}
	return d, nil
}
