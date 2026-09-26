package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func testEnv(seq int64) *Envelope {
	v := 7.0
	txt := "running"
	env := newEnvelope("GWSIM001", seq, time.Now(), []Point{
		{Name: "SIM_0000", Value: &v, TS: "2026-09-26T14:03:04+08:00", Quality: "good", Unit: "degC"},
		{Name: "SIM_0001", ValueText: &txt, TS: "2026-09-26T14:03:04+08:00", Quality: "good"},
	})
	return &env
}

// TestInjectorDisabledZeroTouch 全旋钮关闭时信封零改动（正常采集路径）。
func TestInjectorDisabledZeroTouch(t *testing.T) {
	inj := NewInjector(FaultsCfg{}, 1)
	before := testEnv(1)
	after := *before
	payload, ok, err := inj.Apply(&after, time.Now())
	if err != nil || !ok {
		t.Fatalf("apply: %v %v", ok, err)
	}
	if jb, _ := json.Marshal(*before); string(jb) != string(payload) {
		t.Errorf("payload changed with no faults:\n%s\n%s", jb, payload)
	}
	if len(inj.Stats()) != 0 {
		t.Errorf("stats not empty: %v", inj.Stats())
	}
}

// TestInjectorSeqGap 每 N 条跳号、其余连续。
func TestInjectorSeqGap(t *testing.T) {
	inj := NewInjector(FaultsCfg{SeqGapEvery: 3, SeqGapSize: 5}, 1)
	seq := int64(0)
	seqs := make([]int64, 0, 7)
	for i := 0; i < 7; i++ {
		env := testEnv(0)
		seq = inj.NextSeq(seq)
		env.Seq = seq
		if _, _, err := inj.Apply(env, time.Now()); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	// 期望：1,2,(3+5)=8,9,10,(11+5)=16,17
	want := []int64{1, 2, 8, 9, 10, 16, 17}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("seqs = %v want %v", seqs, want)
		}
	}
	if inj.Stats()["seq_gap"] != 2 {
		t.Errorf("seq_gap count = %d", inj.Stats()["seq_gap"])
	}
}

// TestInjectorEnvelopeFaults 信封级故障逐项：gw 错配/未知点/坏 ts/超保留/未知类型/越级版本。
func TestInjectorEnvelopeFaults(t *testing.T) {
	now := time.Date(2026, 9, 26, 14, 0, 0, 0, time.UTC)

	t.Run("gw_mismatch", func(t *testing.T) {
		inj := NewInjector(FaultsCfg{GWMismatchEvery: 1}, 1)
		env := testEnv(1)
		if _, _, err := inj.Apply(env, now); err != nil {
			t.Fatal(err)
		}
		if env.GW == "GWSIM001" {
			t.Error("gw not tampered")
		}
	})

	t.Run("unknown_points", func(t *testing.T) {
		inj := NewInjector(FaultsCfg{UnknownPointsEvery: 1, UnknownPointsCount: 2}, 1)
		env := testEnv(1)
		if _, _, err := inj.Apply(env, now); err != nil {
			t.Fatal(err)
		}
		if len(env.Points) != 4 {
			t.Fatalf("points = %d", len(env.Points))
		}
		if !strings.HasPrefix(env.Points[2].Name, "UNREGISTERED_PT_") {
			t.Errorf("unexpected appended name %q", env.Points[2].Name)
		}
	})

	t.Run("invalid_ts", func(t *testing.T) {
		inj := NewInjector(FaultsCfg{InvalidTSEvery: 1}, 1)
		env := testEnv(1)
		if _, _, err := inj.Apply(env, now); err != nil {
			t.Fatal(err)
		}
		if _, err := time.Parse(time.RFC3339, env.Points[0].TS); err == nil {
			t.Error("ts still parseable")
		}
	})

	t.Run("ts_beyond_retention", func(t *testing.T) {
		inj := NewInjector(FaultsCfg{TSBeyondRetentionEvery: 1}, 1)
		env := testEnv(1)
		if _, _, err := inj.Apply(env, now); err != nil {
			t.Fatal(err)
		}
		got, err := time.Parse(time.RFC3339, env.Points[0].TS)
		if err != nil {
			t.Fatal(err)
		}
		if now.Sub(got) < 2*365*24*time.Hour {
			t.Errorf("ts not beyond retention: %v", got)
		}
	})

	t.Run("unknown_msg_type_and_ver", func(t *testing.T) {
		inj := NewInjector(FaultsCfg{UnknownMsgTypeEvery: 1, UnsupportedVerEvery: 1}, 1)
		env := testEnv(1)
		if _, _, err := inj.Apply(env, now); err != nil {
			t.Fatal(err)
		}
		if env.MsgType != "firmware_status" || env.Ver != 2 {
			t.Errorf("msg_type=%q ver=%d", env.MsgType, env.Ver)
		}
	})
}

// TestInjectorOversize 两种超限形态：>500 点 / >256KB。
func TestInjectorOversize(t *testing.T) {
	now := time.Now()
	t.Run("points", func(t *testing.T) {
		inj := NewInjector(FaultsCfg{OversizeEvery: 1, OversizeMode: "points"}, 1)
		env := testEnv(1)
		payload, _, err := inj.Apply(env, now)
		if err != nil {
			t.Fatal(err)
		}
		var probe struct {
			Points []json.RawMessage `json:"points"`
		}
		if err := json.Unmarshal(payload, &probe); err != nil {
			t.Fatal(err)
		}
		if len(probe.Points) <= 500 {
			t.Errorf("points = %d (want >500)", len(probe.Points))
		}
	})
	t.Run("bytes", func(t *testing.T) {
		inj := NewInjector(FaultsCfg{OversizeEvery: 1, OversizeMode: "bytes"}, 1)
		env := testEnv(1)
		payload, _, err := inj.Apply(env, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(payload) <= 256<<10 {
			t.Errorf("payload = %d bytes (want >256KB)", len(payload))
		}
	})
}

// TestInjectorMalformed 截断字节（不可解析 JSON）。
func TestInjectorMalformed(t *testing.T) {
	inj := NewInjector(FaultsCfg{MalformedEvery: 1}, 1)
	env := testEnv(1)
	payload, _, err := inj.Apply(env, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if json.Valid(payload) {
		t.Errorf("payload still valid JSON: %s", payload)
	}
}

// TestInjectorRates 点级 rate 注入命中与计数（种子确定 → 可复现）。
func TestInjectorRates(t *testing.T) {
	inj := NewInjector(FaultsCfg{NullValueRate: 1.0, BadQualityRate: 0.5}, 42)
	env := testEnv(1)
	if _, _, err := inj.Apply(env, time.Now()); err != nil {
		t.Fatal(err)
	}
	if env.Points[0].Value != nil {
		t.Error("null rate 1.0 did not null the numeric value")
	}
	if env.Points[0].Unit != "" {
		t.Error("null point should drop unit")
	}
	if got := inj.Stats()["null_value"]; got != 1 {
		t.Errorf("null_value stat = %d", got)
	}
}

// TestInjectorTSkew 时钟偏移同时施加于 ts 与 sent_at，且量值守恒。
func TestInjectorTSSkew(t *testing.T) {
	inj := NewInjector(FaultsCfg{TSkewS: 900}, 1) // 15min > 10min 阈值
	env := testEnv(1)
	before := env.Points[0].TS
	if _, _, err := inj.Apply(env, time.Now()); err != nil {
		t.Fatal(err)
	}
	tb, _ := time.Parse(tsLayout, before)
	ta, err := time.Parse(tsLayout, env.Points[0].TS)
	if err != nil {
		t.Fatal(err)
	}
	if ta.Sub(tb) != 900*time.Second {
		t.Errorf("ts skew = %v", ta.Sub(tb))
	}
}
