package mqtt

import "testing"

// 共享订阅 filter 形态：$share/{group}/thermio/gw/+/up/data（ingest.md §2）。
func TestSharedTopic(t *testing.T) {
	cases := []struct {
		group string
		want  string
	}{
		{"ingest", "$share/ingest/thermio/gw/+/up/data"},
		{"ingest-2", "$share/ingest-2/thermio/gw/+/up/data"},
	}
	for _, c := range cases {
		if got := SharedTopic(c.group); got != c.want {
			t.Errorf("SharedTopic(%q) = %q, want %q", c.group, got, c.want)
		}
	}
}

// 客户端选项基线：会话不清除 + 手动 ACK（ingest.md §2 / §7.1）。
func TestNewClientOptions(t *testing.T) {
	o := NewClientOptions("tcp://emqx:1883", "svc-ingest-1", "svc-ingest", "secret")
	if o.CleanSession {
		t.Error("CleanSession = true, want false（会话不清除，session expiry ≥ 1h）")
	}
	if !o.AutoAckDisabled {
		t.Error("AutoAckDisabled = false, want true（PUBACK 在 TSDB → Kafka 之后）")
	}
}
