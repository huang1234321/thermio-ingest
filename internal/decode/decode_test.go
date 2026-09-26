package decode

import (
	"strings"
	"testing"
	"time"

	"github.com/huang1234321/thermio-ingest/internal/dlq"
)

// validEnvelope §3.1 示例的最小合法信封，负路径用例在其上单字段变异。
func validEnvelope() string {
	return `{
	  "msg_type": "telemetry_batch", "ver": 1, "gw": "GW2026001", "seq": 4821,
	  "sent_at": "2026-09-26T14:03:05.250+08:00",
	  "points": [
	    {"name": "CHW_SUPPLY_TEMP_1", "value": 7.42, "ts": "2026-09-26T14:03:04+08:00", "quality": "good", "unit": "degC"},
	    {"name": "CHW_PUMP_1_STATUS", "value_text": "running", "ts": "2026-09-26T14:03:04+08:00"},
	    {"name": "CHW_PUMP_1_POWER", "value": null, "ts": "2026-09-26T14:03:04+08:00", "quality": "bad", "unit": "kW"}
	  ]
	}`
}

// mutate 在合法信封上做一次 JSON 片段替换。
func mutate(t *testing.T, from, to string) []byte {
	t.Helper()
	s := strings.Replace(validEnvelope(), from, to, 1)
	if s == validEnvelope() && from != to {
		t.Fatalf("变异未命中：%q 不在信封中", from)
	}
	return []byte(s)
}

// §3.2 信封字段规则正路径：示例信封整条通过，字段值正确落位。
func TestDecodePositive(t *testing.T) {
	env, perrs, eerr := Decode([]byte(validEnvelope()))
	if eerr != nil {
		t.Fatalf("正路径被拒: %v", eerr)
	}
	if len(perrs) != 0 {
		t.Fatalf("正路径出现点位级失败: %+v", perrs)
	}
	if env.Gw != "GW2026001" || env.Seq != 4821 || env.Ver != 1 || env.MsgType != MsgTypeTelemetryBatch {
		t.Errorf("信封字段落位错误: %+v", env)
	}
	if want := "2026-09-26T14:03:05.25+08:00"; env.SentAt.Format(time.RFC3339Nano) != want {
		t.Errorf("sent_at = %v, want %s", env.SentAt, want)
	}
	if len(env.Points) != 3 {
		t.Fatalf("points 数 = %d, want 3", len(env.Points))
	}
	p0 := env.Points[0]
	if p0.Name != "CHW_SUPPLY_TEMP_1" || p0.Value == nil || *p0.Value != 7.42 || p0.Quality != "good" || p0.Unit != "degC" {
		t.Errorf("点位 0 落位错误: %+v", p0)
	}
	if env.Points[1].ValueText == nil || *env.Points[1].ValueText != "running" {
		t.Errorf("点位 1 value_text 落位错误: %+v", env.Points[1])
	}
	if env.Points[2].Value != nil {
		t.Errorf("点位 2 显式 null 应解码为 Value=nil（bit2 null_value 路径）: %+v", env.Points[2])
	}
}

// §3.2 信封级负路径：每条规则一条用例，reason 逐条断言（IMPL-5 验收）。
func TestDecodeEnvelopeNegative(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		reason  string
	}{
		{"非 JSON", []byte("{not json"), dlq.MalformedJSON},
		{"msg_type 未知", mutate(t, `"telemetry_batch"`, `"status_batch"`), dlq.UnknownMsgType},
		{"msg_type 缺失", mutate(t, `"msg_type": "telemetry_batch",`, ``), dlq.MalformedJSON},
		{"ver 不认识", mutate(t, `"ver": 1`, `"ver": 2`), dlq.UnsupportedVer},
		{"ver 缺失", mutate(t, `"ver": 1,`, ``), dlq.MalformedJSON},
		{"gw 缺失", mutate(t, `"gw": "GW2026001",`, ``), dlq.MalformedJSON},
		{"seq 缺失", mutate(t, `"seq": 4821,`, ``), dlq.MalformedJSON},
		{"seq 负数", mutate(t, `"seq": 4821`, `"seq": -1`), dlq.MalformedJSON},
		{"sent_at 缺失", mutate(t, `"sent_at": "2026-09-26T14:03:05.250+08:00",`, ``), dlq.MalformedJSON},
		{"sent_at 无时区", mutate(t, `2026-09-26T14:03:05.250+08:00`, `2026-09-26T14:03:05.250`), dlq.MalformedJSON},
		{"points 缺失", mutate(t, `"points": [`, `"points_x": [`), dlq.MalformedJSON},
	}
	// points 空（1..500 下界）：独立构造。
	env0 := `{"msg_type":"telemetry_batch","ver":1,"gw":"GW","seq":1,"sent_at":"2026-09-26T14:03:05+08:00","points":[]}`
	if _, _, eerr := Decode([]byte(env0)); eerr == nil || eerr.Reason != dlq.MalformedJSON {
		t.Errorf("points 空: got %+v, want MALFORMED_JSON", eerr)
	}

	for _, c := range cases {
		_, _, eerr := Decode(c.payload)
		if eerr == nil {
			t.Errorf("%s: 未拒绝, want %s", c.name, c.reason)
			continue
		}
		if eerr.Reason != c.reason {
			t.Errorf("%s: reason = %s, want %s（detail=%s）", c.name, eerr.Reason, c.reason, eerr.Detail)
		}
	}
}

// §3.2 超长规则：>500 点与 >256KB 都是整消息 PAYLOAD_TOO_LARGE（防重投风暴）。
func TestDecodeTooLarge(t *testing.T) {
	// 501 个点。
	batch := func(n int) string {
		var sb strings.Builder
		sb.WriteString(`{"msg_type":"telemetry_batch","ver":1,"gw":"GW","seq":1,"sent_at":"2026-09-26T14:03:05+08:00","points":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(`{"name":"P_` + itoa(i) + `","value":1,"ts":"2026-09-26T14:03:04+08:00"}`)
		}
		sb.WriteString(`]}`)
		return sb.String()
	}
	if _, _, eerr := Decode([]byte(batch(MaxPoints + 1))); eerr == nil || eerr.Reason != dlq.PayloadTooLarge {
		t.Errorf("501 点: got %+v, want PAYLOAD_TOO_LARGE", eerr)
	}
	// 500 点恰好在界内。
	env, perrs, eerr := Decode([]byte(batch(MaxPoints)))
	if eerr != nil || env == nil || len(env.Points) != MaxPoints || len(perrs) != 0 {
		t.Errorf("500 点应通过: eerr=%v points=%d perrs=%d", eerr, len(env.Points), len(perrs))
	}
	// 256KB+1：非 JSON 也先按尺寸拒绝。
	big := make([]byte, MaxPayloadBytes+1)
	if _, _, eerr := Decode(big); eerr == nil || eerr.Reason != dlq.PayloadTooLarge {
		t.Errorf("256KB+1: got %+v, want PAYLOAD_TOO_LARGE", eerr)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// §3.2 点位级负路径：失败只废该点（PointError），同消息其余点通过。
func TestDecodePointNegative(t *testing.T) {
	base := `"ver":1,"gw":"GW","seq":1,"sent_at":"2026-09-26T14:03:05+08:00"`
	cases := []struct {
		name   string
		point  string
		reason string
	}{
		{"name 缺失", `{"value":1,"ts":"2026-09-26T14:03:04+08:00"}`, dlq.MalformedJSON},
		{"name 空", `{"name":"","value":1,"ts":"2026-09-26T14:03:04+08:00"}`, dlq.MalformedJSON},
		{"name 超长", `{"name":"` + strings.Repeat("A", 65) + `","value":1,"ts":"2026-09-26T14:03:04+08:00"}`, dlq.MalformedJSON},
		{"name 非法字符", `{"name":"BAD NAME!","value":1,"ts":"2026-09-26T14:03:04+08:00"}`, dlq.MalformedJSON},
		{"name 汉字", `{"name":"温度","value":1,"ts":"2026-09-26T14:03:04+08:00"}`, dlq.MalformedJSON},
		{"value 字符串", `{"name":"P1","value":"7.4","ts":"2026-09-26T14:03:04+08:00"}`, dlq.MalformedJSON},
		{"value 与 value_text 并存", `{"name":"P1","value":1,"value_text":"x","ts":"2026-09-26T14:03:04+08:00"}`, dlq.MalformedJSON},
		{"value_text 非字符串", `{"name":"P1","value_text":3,"ts":"2026-09-26T14:03:04+08:00"}`, dlq.MalformedJSON},
		{"ts 缺失", `{"name":"P1","value":1}`, dlq.TSInvalid},
		{"ts 不可解析", `{"name":"P1","value":1,"ts":"2026-09-26 14:03:04"}`, dlq.TSInvalid},
		{"ts 无时区", `{"name":"P1","value":1,"ts":"2026-09-26T14:03:04"}`, dlq.TSInvalid},
	}
	for _, c := range cases {
		payload := `{"msg_type":"telemetry_batch",` + base + `,"points":[` + c.point + `]}`
		env, perrs, eerr := Decode([]byte(payload))
		if eerr != nil {
			t.Errorf("%s: 整消息被拒（应点级失败）: %v", c.name, eerr)
			continue
		}
		if len(perrs) != 1 || perrs[0].Reason != c.reason {
			t.Errorf("%s: perrs = %+v, want 单条 %s", c.name, perrs, c.reason)
			continue
		}
		if len(env.Points) != 0 {
			t.Errorf("%s: 失败点不应进入信封（got %d 点）", c.name, len(env.Points))
		}
	}
}

// 点级失败与合法点共存：只有坏点进 PointError。
func TestDecodeMixedPoints(t *testing.T) {
	payload := `{"msg_type":"telemetry_batch","ver":1,"gw":"GW","seq":1,
	  "sent_at":"2026-09-26T14:03:05+08:00",
	  "points":[
	    {"name":"GOOD_1","value":1,"ts":"2026-09-26T14:03:04+08:00"},
	    {"name":"BAD_TS","value":2},
	    {"name":"GOOD_2","value_text":"on","ts":"2026-09-26T14:03:04+08:00"}
	  ]}`
	env, perrs, eerr := Decode([]byte(payload))
	if eerr != nil {
		t.Fatalf("整消息被拒: %v", eerr)
	}
	if len(env.Points) != 2 || env.Points[0].Name != "GOOD_1" || env.Points[1].Name != "GOOD_2" {
		t.Errorf("合法点落位错误: %+v", env.Points)
	}
	if len(perrs) != 1 || perrs[0].PointName != "BAD_TS" || perrs[0].Reason != dlq.TSInvalid {
		t.Errorf("点级失败 = %+v, want BAD_TS/TS_INVALID", perrs)
	}
}

// quality 缺省 good；未知 quality 值不拒（§3.2 lenient：非 good → device_bad 位）。
func TestDecodeQualityLenient(t *testing.T) {
	payload := `{"msg_type":"telemetry_batch","ver":1,"gw":"GW","seq":1,
	  "sent_at":"2026-09-26T14:03:05+08:00",
	  "points":[
	    {"name":"P_DEFAULT","value":1,"ts":"2026-09-26T14:03:04+08:00"},
	    {"name":"P_WEIRD","value":1,"ts":"2026-09-26T14:03:04+08:00","quality":"garbage"}
	  ]}`
	env, _, eerr := Decode([]byte(payload))
	if eerr != nil {
		t.Fatalf("未知 quality 不应拒收: %v", eerr)
	}
	if env.Points[0].Quality != "good" {
		t.Errorf("quality 缺省 = %q, want good", env.Points[0].Quality)
	}
	if env.Points[1].Quality != "garbage" {
		t.Errorf("未知 quality 应原样保留: %q", env.Points[1].Quality)
	}
}
