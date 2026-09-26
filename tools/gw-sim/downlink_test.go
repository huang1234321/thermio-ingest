package main

import (
	"encoding/json"
	"testing"
	"time"

	"log/slog"
	"os"
)

var testLogger = slog.New(slog.NewJSONHandler(os.Stderr, nil))

func mkDown(cfg DownlinkCfg) *DownlinkState {
	d := NewDownlinkState(cfg, 0, 100, 1, testLogger)
	d.InitSetpoint("SIM_SP_0001", 20)
	return d
}

func writeCmd(t *testing.T, d *DownlinkState, raw string, now time.Time) WriteAck {
	t.Helper()
	resp := d.HandleMessage([]byte(raw), "GWSIM001", now)
	if resp == nil {
		t.Fatal("no ack")
	}
	var ack WriteAck
	if err := json.Unmarshal(resp, &ack); err != nil {
		t.Fatalf("not write_ack: %v", err)
	}
	return ack
}

// TestWriteAckEcho 写成功 → 寄存器更新 → 回读一致（默认链路）。
func TestWriteAckEcho(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo"})
	now := time.Now()
	ack := writeCmd(t, d, `{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","sent_at":"x","writes":[{"name":"SIM_SP_0001","value":7.5}]}`, now)
	if ack.Results[0].Status != "ok" || *ack.Results[0].Written != 7.5 {
		t.Fatalf("ack = %+v", ack.Results[0])
	}
	resp := d.HandleMessage(mustJSON(t, ReadCmd{MsgType: msgTypeReadCmd, Ver: 1, CmdID: "c2", Names: []string{"SIM_SP_0001"}}), "GWSIM001", now.Add(5*time.Second))
	var rAck ReadAck
	if err := json.Unmarshal(resp, &rAck); err != nil {
		t.Fatal(err)
	}
	if *rAck.Values[0].Value != 7.5 {
		t.Errorf("readback = %v want 7.5", *rAck.Values[0].Value)
	}
}

// TestWriteAckRejectOutOfRange 越界写 → rejected（值域防线）。
func TestWriteAckRejectOutOfRange(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo"})
	ack := writeCmd(t, d, `{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","writes":[{"name":"SIM_SP_0001","value":999}]}`, time.Now())
	if ack.Results[0].Status != "rejected" {
		t.Errorf("status = %s", ack.Results[0].Status)
	}
}

// TestWriteAckUnknownPoint 未注册点 → unknown_point。
func TestWriteAckUnknownPoint(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo"})
	ack := writeCmd(t, d, `{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","writes":[{"name":"NOPE","value":1}]}`, time.Now())
	if ack.Results[0].Status != "unknown_point" {
		t.Errorf("status = %s", ack.Results[0].Status)
	}
}

// TestWriteAckFailRate 注入通讯失败 → failed。
func TestWriteAckFailRate(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo", WriteFailRate: 1.0})
	ack := writeCmd(t, d, `{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","writes":[{"name":"SIM_SP_0001","value":7.5}]}`, time.Now())
	if ack.Results[0].Status != "failed" {
		t.Errorf("status = %s", ack.Results[0].Status)
	}
}

// TestReadbackMismatchModes 回读不一致注入三种形态（IMPL-18 回滚演练）。
func TestReadbackMismatchModes(t *testing.T) {
	now := time.Now()
	cases := []struct {
		mode string
		cfg  DownlinkCfg
		want float64
	}{
		{"pinned", DownlinkCfg{Enabled: true, Readback: "pinned", PinnedValue: 20}, 20},
		{"stale", DownlinkCfg{Enabled: true, Readback: "stale"}, 20},                // 旧值 = 初始 20
		{"offset", DownlinkCfg{Enabled: true, Readback: "offset", Offset: 2.5}, 10}, // 7.5+2.5
	}
	for _, c := range cases {
		t.Run(c.mode, func(t *testing.T) {
			d := mkDown(c.cfg)
			ack := writeCmd(t, d, `{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","writes":[{"name":"SIM_SP_0001","value":7.5}]}`, now)
			if ack.Results[0].Status != "ok" {
				t.Fatalf("write should still ack ok: %+v", ack.Results[0])
			}
			resp := d.HandleMessage(mustJSON(t, ReadCmd{MsgType: msgTypeReadCmd, Ver: 1, CmdID: "c2", Names: []string{"SIM_SP_0001"}}), "GWSIM001", now.Add(time.Second))
			var rAck ReadAck
			if err := json.Unmarshal(resp, &rAck); err != nil {
				t.Fatal(err)
			}
			if got := *rAck.Values[0].Value; got != c.want {
				t.Errorf("readback(%s) = %v want %v", c.mode, got, c.want)
			}
		})
	}
}

// TestReadbackTelemetryConsistency pinned/stale 模式下遥测跟随设备值（写入未生效
// 在 up/data 亦可见——E2E 断言面）。
func TestReadbackTelemetryConsistency(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "stale"})
	writeCmd(t, d, `{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","writes":[{"name":"SIM_SP_0001","value":7.5}]}`, time.Now())
	if v, _ := d.RegisterValue("SIM_SP_0001"); v != 20 {
		t.Errorf("telemetry register = %v want 20 (stale)", v)
	}
}

// TestHandleMessageUnknownType 未识别 msg_type → 忽略（不崩溃）。
func TestHandleMessageUnknownType(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true})
	if resp := d.HandleMessage([]byte(`{"msg_type":"fw_upgrade","ver":1}`), "GWSIM001", time.Now()); resp != nil {
		t.Errorf("unexpected ack: %s", resp)
	}
	if resp := d.HandleMessage([]byte(`{`), "GWSIM001", time.Now()); resp != nil {
		t.Errorf("garbage produced ack: %s", resp)
	}
}

// TestAckEnvelopeShape 应答信封字段（v0 下行契约）。
func TestAckEnvelopeShape(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo"})
	resp := d.HandleMessage([]byte(`{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","writes":[{"name":"SIM_SP_0001","value":7.5}]}`), "GWSIM001", time.Now())
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(resp, &raw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"msg_type", "ver", "cmd_id", "gw", "sent_at", "results"} {
		if _, ok := raw[k]; !ok {
			t.Errorf("write_ack missing %q", k)
		}
	}
	if string(raw["gw"]) != `"GWSIM001"` {
		t.Errorf("gw = %s", raw["gw"])
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
