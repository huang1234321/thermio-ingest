package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestEnvelopeContractShape 校验信封字段名/顺序/类型与 ingest.md §3.1 示例逐字段一致。
func TestEnvelopeContractShape(t *testing.T) {
	v := 7.42
	running := "running"
	env := newEnvelope("GW2026001", 4821,
		time.Date(2026, 9, 26, 14, 3, 5, 250_000_000, time.FixedZone("CST", 8*3600)),
		[]Point{
			{Name: "CHW_SUPPLY_TEMP_1", Value: &v, TS: "2026-09-26T14:03:04+08:00", Quality: "good", Unit: "degC"},
			{Name: "CHW_PUMP_1_STATUS", ValueText: &running, TS: "2026-09-26T14:03:04+08:00", Quality: "good"},
			{Name: "CHW_PUMP_1_POWER", Value: nil, TS: "2026-09-26T14:03:04+08:00", Quality: "bad", Unit: "kW"},
		})
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"msg_type", "ver", "gw", "seq", "sent_at", "points"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("envelope missing contract key %q", key)
		}
	}
	if string(raw["msg_type"]) != `"telemetry_batch"` {
		t.Errorf("msg_type = %s", raw["msg_type"])
	}
	if string(raw["ver"]) != "1" {
		t.Errorf("ver = %s", raw["ver"])
	}
	// 字段顺序 = 契约示例顺序（encoding/json 按结构体声明序输出）。
	wantOrder := `{"msg_type":"telemetry_batch","ver":1,"gw":"GW2026001","seq":4821,"sent_at":`
	if !strings.HasPrefix(string(b), wantOrder) {
		t.Errorf("field order drift:\n got %s\nwant prefix %s", b, wantOrder)
	}

	var pts []map[string]json.RawMessage
	if err := json.Unmarshal(raw["points"], &pts); err != nil {
		t.Fatal(err)
	}
	if len(pts) != 3 {
		t.Fatalf("points = %d", len(pts))
	}
	// null 值点：value 序列化为 null（契约示例第三点形态）。
	if string(pts[2]["value"]) != "null" {
		t.Errorf("null point value = %s", pts[2]["value"])
	}
	// 枚态点：value_text 通道、无 unit。
	if _, ok := pts[1]["value_text"]; !ok {
		t.Error("enum point missing value_text")
	}
	if _, ok := pts[1]["unit"]; ok {
		t.Error("enum point should not carry unit")
	}
}

// TestValidateContractNameCharset 点位 name 字符集/长度规则（ingest.md §3.2）。
func TestValidateContractNameCharset(t *testing.T) {
	v := 1.0
	base := Point{Name: "OK_NAME.1:x-y_9", Value: &v, TS: "2026-09-26T14:03:04Z", Quality: "good"}
	env := newEnvelope("GW", 1, time.Now(), []Point{base})
	if err := env.validateContract(); err != nil {
		t.Fatalf("legal name rejected: %v", err)
	}
	for _, bad := range []string{"", strings.Repeat("A", 65), "bad name", "名字", "a/b"} {
		p := base
		p.Name = bad
		e := newEnvelope("GW", 1, time.Now(), []Point{p})
		if err := e.validateContract(); err == nil {
			t.Errorf("illegal name %q accepted", bad)
		}
	}
}

// TestValidateContractBounds 点数上限与 payload 上限的发端自检。
func TestValidateContractBounds(t *testing.T) {
	v := 0.0
	pts := make([]Point, 501)
	for i := range pts {
		pts[i] = Point{Name: "P", Value: &v, TS: "2026-09-26T14:03:04Z", Quality: "good"}
	}
	tooMany := newEnvelope("GW", 1, time.Now(), pts)
	if err := tooMany.validateContract(); err == nil {
		t.Error("501 points accepted")
	}

	small := make([]Point, 1)
	small[0] = Point{Name: "PAD", Value: &v, TS: "2026-09-26T14:03:04Z", Quality: "good"}
	env := newEnvelope("GW", 1, time.Now(), small)
	env.Pad = strings.Repeat("X", 256<<10)
	if err := env.validateContract(); err == nil {
		t.Error(">256KB payload accepted")
	}
}

// TestFormatTS 时区带偏移且可被 RFC3339 解析。
func TestFormatTS(t *testing.T) {
	tz := time.FixedZone("plus8", 8*3600)
	got := FormatTS(time.Date(2026, 9, 26, 14, 3, 4, 0, tz))
	if got != "2026-09-26T14:03:04+08:00" {
		t.Errorf("FormatTS = %q", got)
	}
	if _, err := time.Parse(time.RFC3339, got); err != nil {
		t.Errorf("not RFC3339: %v", err)
	}
}
