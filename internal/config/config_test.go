package config

import (
	"testing"
	"time"
)

func fullEnv() map[string]string {
	return map[string]string{
		"MQTT_BROKER_URL": "tcp://localhost:1883",
		"KAFKA_BROKERS":   "localhost:9092, localhost:9093",
		"PG_DSN":          "postgres://u:p@h/db",
		"TSDB_DSN":        "postgres://u:p@h/ts",
	}
}

func envFunc(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// §11：默认值齐备（BATCH 2000/500ms/BUFFER 100k；缓存 30s/1h）。
func TestDefaults(t *testing.T) {
	c, err := load(envFunc(fullEnv()))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.BatchMaxRows != 2000 || c.BatchFlushInterval != 500*time.Millisecond || c.BufferMaxRows != 100000 {
		t.Errorf("批量默认值错误: %+v", c)
	}
	if c.CacheRefreshInterval != 30*time.Second || c.CacheFullResyncInterval != time.Hour {
		t.Errorf("缓存刷新默认值错误: %+v", c)
	}
	if c.MQTTGroup != "ingest" || c.MQTTClientID != "svc-ingest" || c.MetricsAddr != ":9091" {
		t.Errorf("杂项默认值错误: %+v", c)
	}
	if len(c.KafkaBrokers) != 2 || c.KafkaBrokers[0] != "localhost:9092" || c.KafkaBrokers[1] != "localhost:9093" {
		t.Errorf("KAFKA_BROKERS 解析错误: %v", c.KafkaBrokers)
	}
}

// 必填缺失逐项报错（启动 fail-fast）。
func TestRequiredMissing(t *testing.T) {
	for _, key := range []string{"MQTT_BROKER_URL", "KAFKA_BROKERS", "PG_DSN", "TSDB_DSN"} {
		m := fullEnv()
		delete(m, key)
		if _, err := load(envFunc(m)); err == nil {
			t.Errorf("%s 缺失应报错", key)
		}
	}
}

// 覆盖与非法值：BUFFER < BATCH 拒绝；非数字/非时长报错。
func TestOverridesAndInvalid(t *testing.T) {
	m := fullEnv()
	m["BATCH_MAX_ROWS"] = "100"
	m["BUFFER_MAX_ROWS"] = "50"
	if _, err := load(envFunc(m)); err == nil {
		t.Error("BUFFER < BATCH 应报错")
	}
	m["BUFFER_MAX_ROWS"] = "500"
	c, err := load(envFunc(m))
	if err != nil {
		t.Fatalf("合法覆盖: %v", err)
	}
	if c.BatchMaxRows != 100 || c.BufferMaxRows != 500 {
		t.Errorf("覆盖未生效: %+v", c)
	}
	m["BATCH_MAX_ROWS"] = "abc"
	if _, err := load(envFunc(m)); err == nil {
		t.Error("非整数应报错")
	}
	m["BATCH_MAX_ROWS"] = "100"
	m["BATCH_FLUSH_INTERVAL"] = "notaduration"
	if _, err := load(envFunc(m)); err == nil {
		t.Error("非时长应报错")
	}
}
