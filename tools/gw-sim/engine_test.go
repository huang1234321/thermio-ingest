package main

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestOutboxFIFOAndCap FIFO 保序 + 容量上限丢最旧 + 丢弃计数。
func TestOutboxFIFOAndCap(t *testing.T) {
	ob := newOutbox(10)
	for i := 0; i < 5; i++ {
		ob.add(outboxEntry{topic: "t", payload: []byte{byte(i)}, rows: 3})
	}
	// rows=3×5=15 > 10：丢最旧两条（剩 3 条 9 行），队头是原第 3 条。
	if got := ob.droppedLoad(); got != 2 {
		t.Fatalf("dropped = %d want 2", got)
	}
	e, ok := ob.peek()
	if !ok || e.payload[0] != 2 {
		t.Fatalf("peek = %+v (FIFO broken)", e)
	}
	ob.pop()
	e, _ = ob.peek()
	if e.payload[0] != 3 {
		t.Fatalf("FIFO order broken: %v", e.payload)
	}
}

// TestOutboxDropPolicyKeepsLatest 单条超容量的极端情况：至少保住最新一条。
func TestOutboxDropPolicyKeepsLatest(t *testing.T) {
	ob := newOutbox(1)
	ob.add(outboxEntry{topic: "t", payload: []byte{1}, rows: 5})
	ob.add(outboxEntry{topic: "t", payload: []byte{2}, rows: 5})
	e, ok := ob.peek()
	if !ok || e.payload[0] != 2 {
		t.Fatalf("latest entry not retained: %+v ok=%v", e, ok)
	}
}

// TestProfileValidateDefaults 默认值填充与非法值拒绝。
func TestProfileValidateDefaults(t *testing.T) {
	p := &Profile{Name: "x", SampleIntervalS: 60}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	if p.BatchPoints != 500 || p.Gateways.Count != 1 || p.Gateways.PasswordEnv != "GWSIM_MQTT_PASSWORD" {
		t.Errorf("defaults not applied: %+v", p.Gateways)
	}
	if p.Downlink.Readback != "echo" {
		t.Errorf("readback default = %s", p.Downlink.Readback)
	}

	bad := &Profile{Name: "x", SampleIntervalS: 60, BatchPoints: 501}
	if err := bad.Validate(); err == nil {
		t.Error("batch_points > 500 accepted")
	}
	bad2 := &Profile{Name: "x", SampleIntervalS: 60, Faults: FaultsCfg{BadQualityRate: 1.5}}
	if err := bad2.Validate(); err == nil {
		t.Error("rate > 1 accepted")
	}
	bad3 := &Profile{Name: "x", SampleIntervalS: 60, Downlink: DownlinkCfg{Readback: "nope"}}
	if err := bad3.Validate(); err == nil {
		t.Error("unknown readback mode accepted")
	}
	bad4 := &Profile{Name: "x", SampleIntervalS: 60, Backfill: BackfillCfg{Enabled: true}}
	if err := bad4.Validate(); err == nil {
		t.Error("backfill without days accepted")
	}
}

// TestReplaceAll 模板占位符替换。
func TestReplaceAll(t *testing.T) {
	if got := replaceAll("{serial}@{tenant}", "{serial}", "GWSIM001"); got != "GWSIM001@{tenant}" {
		t.Errorf("replaceAll = %q", got)
	}
	if got := replaceAll(replaceAll("{serial}@{tenant}", "{serial}", "GWSIM001"), "{tenant}", "t-a"); got != "GWSIM001@t-a" {
		t.Errorf("replaceAll = %q", got)
	}
}

// TestSamplingEnvelopeCompliance 采样产出的信封满足契约自检（正常 profile）。
func TestSamplingEnvelopeCompliance(t *testing.T) {
	p := &Profile{
		Name: "ut", SampleIntervalS: 60, Seed: 7,
		Points: PointsCfg{Count: 50, NumericRatio: 0.9, Base: 20, Amplitude: 5, Noise: 0.1,
			Units: []string{"degC", "degF"}, Setpoints: 2, SetpointBase: 21},
		Gateways: GatewaysCfg{Count: 1},
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	g := newGatewaySim(p, 0, "tcp://localhost:1883", "pw", newRunStats(), testLogger)
	now := time.Date(2026, 9, 26, 14, 0, 0, 0, time.UTC)

	total := 0
	for tick := 0; tick < 3; tick++ {
		at := now.Add(time.Duration(tick) * time.Minute)
		pts := make([]Point, 0)
		for _, pd := range g.numPoints {
			pts = append(pts, g.samplePoint(pd, at))
		}
		for _, pd := range g.spPoints {
			pts = append(pts, g.samplePoint(pd, at))
		}
		// 分批组装应通过契约自检。
		for s := 0; s < len(pts); s += p.BatchPoints {
			e := s + p.BatchPoints
			if e > len(pts) {
				e = len(pts)
			}
			env := newEnvelope(g.serial, 1, now, pts[s:e])
			if err := env.validateContract(); err != nil {
				t.Fatalf("tick %d: %v", tick, err)
			}
			total += e - s
		}
	}
	if total != 3*52 {
		t.Errorf("total points = %d", total)
	}
	// 单位轮转生效（degC/degF 均出现——喂单位归一路径）。
	units := map[string]bool{}
	for _, pd := range g.numPoints {
		units[pd.unit] = true
	}
	if !units["degC"] || !units["degF"] {
		t.Errorf("unit rotation broken: %v", units)
	}
}

// TestPublishBatchSequenceNormalPath publishBatch 全链（无故障、无 broker）：
// 契约自检通过 + NextSeq 单调。
func TestPublishBatchSequenceNormalPath(t *testing.T) {
	p := &Profile{
		Name: "ut", SampleIntervalS: 60, Seed: 3,
		Points:   PointsCfg{Count: 4, NumericRatio: 1, Base: 20, Units: []string{"degC"}},
		Gateways: GatewaysCfg{Count: 1},
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	g := newGatewaySim(p, 0, "tcp://localhost:1883", "pw", newRunStats(), testLogger)
	v := 1.0
	for i := 1; i <= 3; i++ {
		pts := []Point{{Name: "SIM_0000", Value: &v, TS: FormatTS(time.Now()), Quality: "good", Unit: "degC"}}
		before := g.seq
		// publishBatch 在无连接时入 outbox，不 panic、不丢消息。
		g.publishBatch(pts, time.Now(), false)
		if g.seq != before+1 {
			t.Fatalf("seq %d after %d", g.seq, before)
		}
	}
	if n := g.ob.len(); n != 3 {
		t.Fatalf("outbox = %d want 3 (offline buffered)", n)
	}
}

// DAT-165：offset 命名与 overrides 单点差异化（含 kind 切换）。
func TestBuildPointsOffsetAndOverrides(t *testing.T) {
	v150 := 150.0
	zero := 0.0
	p := &Profile{
		Name: "cold-plant", SampleIntervalS: 5,
		Points: PointsCfg{
			Count: 4, NumericRatio: 0.75, Units: []string{"degC"},
			Base: 20, Offset: 100,
			Overrides: map[string]PointOverride{
				"SIM_0100": {Base: &v150, Unit: "kW"},                       // 数值覆盖
				"SIM_0103": {Kind: "enum", EnumValues: []string{"stopped"}}, // 轮转数值位改枚态
				"SIM_0101": {Base: &zero, Amplitude: &zero},                 // 常值 0
			},
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	g := newGatewaySim(p, 0, "tcp://x", "pw", newRunStats(), logger)
	if len(g.numPoints) != 4 {
		t.Fatalf("want 4 points, got %d", len(g.numPoints))
	}
	byName := map[string]pointDef{}
	for _, d := range g.numPoints {
		byName[d.name] = d
	}
	if d := byName["SIM_0100"]; d.base != 150.0 || d.unit != "kW" {
		t.Fatalf("override not applied: %+v", d)
	}
	if d := byName["SIM_0101"]; d.base != 0 || d.amplitude != 0 {
		t.Fatalf("zero override not applied: %+v", d)
	}
	if d := byName["SIM_0103"]; d.kind != "enum" || len(d.enumValues) != 1 || d.enumValues[0] != "stopped" {
		t.Fatalf("enum override not applied: %+v", d)
	}
	if _, ok := byName["SIM_0000"]; ok {
		t.Fatal("offset ignored: SIM_0000 should not exist")
	}
}
