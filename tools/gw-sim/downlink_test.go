package main

import (
	"encoding/json"
	"testing"
	"time"

	"log/slog"
	"os"
)

var testLogger = slog.New(slog.NewJSONHandler(os.Stderr, nil))

// gwClientID 模拟 topic clientid（§4.3：应答 gw 字段必须与之一致）。
const gwClientID = "GW-SIM-001"

func mkDown(cfg DownlinkCfg) *DownlinkState {
	d := NewDownlinkState(cfg, 0, 100, 1, testLogger)
	d.InitSetpoint("SIM_SP_0001", 20)
	return d
}

// writeCmd 定稿信封单点写（point_ref + value）。
func writeCmd(t *testing.T, d *DownlinkState, raw string, now time.Time) WriteAck {
	t.Helper()
	resp := d.HandleMessage([]byte(raw), gwClientID, now)
	if resp == nil {
		t.Fatal("no ack")
	}
	var ack WriteAck
	if err := json.Unmarshal(resp, &ack); err != nil {
		t.Fatalf("not write_ack: %v", err)
	}
	return ack
}

// readCmd 定稿信封单点回读。
func readCmd(t *testing.T, d *DownlinkState, pointRef string, now time.Time) ReadResult {
	t.Helper()
	resp := d.HandleMessage(mustJSON(t, WriteCmd{
		MsgType: msgTypeReadCmd, Ver: 1, CmdID: "c2", PointRef: pointRef,
		IssuedAt: now.Format(time.RFC3339),
	}), gwClientID, now)
	if resp == nil {
		t.Fatal("no read_result")
	}
	var res ReadResult
	if err := json.Unmarshal(resp, &res); err != nil {
		t.Fatalf("not read_result: %v", err)
	}
	return res
}

// TestWriteAckEcho 写成功 → 寄存器更新 → 回读一致（默认链路）。
func TestWriteAckEcho(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo"})
	now := time.Now()
	ack := writeCmd(t, d, `{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","point_ref":"SIM_SP_0001","value":7.5,"issued_at":"`+now.Format(time.RFC3339)+`","expires_in_s":30}`, now)
	if ack.Result != ackAccepted || ack.Code != nil {
		t.Fatalf("ack = %+v", ack)
	}
	if ack.GW != gwClientID {
		t.Fatalf("gw = %s want %s (§4.3 与 topic clientid 一致)", ack.GW, gwClientID)
	}
	res := readCmd(t, d, "SIM_SP_0001", now.Add(5*time.Second))
	if res.Quality != qualityGood || res.Value == nil || *res.Value != 7.5 {
		t.Fatalf("read_result = %+v", res)
	}
}

// TestWriteAckRejectOutOfRange 越界写 → rejected + WRITE_REFUSED（值域防线）。
func TestWriteAckRejectOutOfRange(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo"})
	now := time.Now()
	ack := writeCmd(t, d, `{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","point_ref":"SIM_SP_0001","value":999,"issued_at":"`+now.Format(time.RFC3339)+`","expires_in_s":30}`, now)
	if ack.Result != ackRejected || ack.Code == nil || *ack.Code != codeWriteRefused {
		t.Errorf("ack = %+v", ack)
	}
}

// TestWriteAckUnknownPoint 未注册点 → rejected + POINT_UNKNOWN。
func TestWriteAckUnknownPoint(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo"})
	now := time.Now()
	ack := writeCmd(t, d, `{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","point_ref":"NOPE","value":1,"issued_at":"`+now.Format(time.RFC3339)+`","expires_in_s":30}`, now)
	if ack.Result != ackRejected || ack.Code == nil || *ack.Code != codePointUnknown {
		t.Errorf("ack = %+v", ack)
	}
}

// TestWriteAckExpired 指令过期 → rejected + CMD_EXPIRED。
func TestWriteAckExpired(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo"})
	stale := time.Now().Add(-2 * time.Minute)
	ack := writeCmd(t, d, `{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","point_ref":"SIM_SP_0001","value":7.5,"issued_at":"`+stale.Format(time.RFC3339)+`","expires_in_s":30}`, time.Now())
	if ack.Result != ackRejected || ack.Code == nil || *ack.Code != codeCmdExpired {
		t.Errorf("ack = %+v", ack)
	}
}

// TestWriteAckFailRate 注入通讯失败 → 不应答（§4.4 超时路径，回读仲裁兜底）。
func TestWriteAckFailRate(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo", WriteFailRate: 1.0})
	now := time.Now()
	resp := d.HandleMessage([]byte(`{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","point_ref":"SIM_SP_0001","value":7.5,"issued_at":"`+now.Format(time.RFC3339)+`","expires_in_s":30}`), gwClientID, now)
	if resp != nil {
		t.Errorf("injected failure should stay silent, got %s", resp)
	}
}

// TestReadResultUnknownPoint 未注册点回读 → value null + quality bad。
func TestReadResultUnknownPoint(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo"})
	res := readCmd(t, d, "NOPE", time.Now())
	if res.Value != nil || res.Quality != qualityBad {
		t.Errorf("res = %+v", res)
	}
}

// TestReadResultExpired 过期 read_cmd → value null + quality bad（与 write 同判，
// control-safety §4 信封 expires_in_s 对两形态生效——DAT-211 对齐；不以新读数
// 背书旧指令）。helper readCmd 不带 expires_in_s，此处走原始报文。
func TestReadResultExpired(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo"})
	stale := time.Now().Add(-2 * time.Minute)
	resp := d.HandleMessage([]byte(`{"msg_type":"read_cmd","ver":1,"cmd_id":"c9","point_ref":"SIM_SP_0001","issued_at":"`+stale.Format(time.RFC3339)+`","expires_in_s":30}`), gwClientID, time.Now())
	if resp == nil {
		t.Fatal("no read_result")
	}
	var res ReadResult
	if err := json.Unmarshal(resp, &res); err != nil {
		t.Fatalf("not read_result: %v", err)
	}
	if res.Value != nil || res.Quality != qualityBad {
		t.Errorf("expired read_result = %+v, want value nil + quality bad", res)
	}
	// 时效窗口内的同指令仍正常回读（对照：过期判定不误伤新鲜指令）。
	fresh := readCmd(t, d, "SIM_SP_0001", time.Now())
	if fresh.Quality != qualityGood || fresh.Value == nil {
		t.Errorf("fresh read_result = %+v, want good", fresh)
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
			ack := writeCmd(t, d, `{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","point_ref":"SIM_SP_0001","value":7.5,"issued_at":"`+now.Format(time.RFC3339)+`","expires_in_s":30}`, now)
			if ack.Result != ackAccepted {
				t.Fatalf("write should still ack accepted: %+v", ack)
			}
			res := readCmd(t, d, "SIM_SP_0001", now.Add(time.Second))
			if res.Value == nil || *res.Value != c.want {
				t.Errorf("readback(%s) = %+v want %v", c.mode, res, c.want)
			}
		})
	}
}

// TestReadbackTelemetryConsistency pinned/stale 模式下遥测跟随设备值（写入未生效
// 在 up/data 亦可见——E2E 断言面）。
func TestReadbackTelemetryConsistency(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "stale"})
	now := time.Now()
	writeCmd(t, d, `{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","point_ref":"SIM_SP_0001","value":7.5,"issued_at":"`+now.Format(time.RFC3339)+`","expires_in_s":30}`, now)
	if v, _ := d.RegisterValue("SIM_SP_0001"); v != 20 {
		t.Errorf("telemetry register = %v want 20 (stale)", v)
	}
}

// TestHandleMessageUnknownType 未识别 msg_type → 忽略（不崩溃）。
func TestHandleMessageUnknownType(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true})
	if resp := d.HandleMessage([]byte(`{"msg_type":"fw_upgrade","ver":1}`), gwClientID, time.Now()); resp != nil {
		t.Errorf("unexpected ack: %s", resp)
	}
	if resp := d.HandleMessage([]byte(`{`), gwClientID, time.Now()); resp != nil {
		t.Errorf("garbage produced ack: %s", resp)
	}
}

// TestAckEnvelopeShape 应答信封字段（定稿下行契约：单点 + result/code/at）。
func TestAckEnvelopeShape(t *testing.T) {
	d := mkDown(DownlinkCfg{Enabled: true, Readback: "echo"})
	now := time.Now()
	resp := d.HandleMessage([]byte(`{"msg_type":"write_cmd","ver":1,"cmd_id":"c1","point_ref":"SIM_SP_0001","value":7.5,"issued_at":"`+now.Format(time.RFC3339)+`","expires_in_s":30}`), gwClientID, now)
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(resp, &raw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"msg_type", "ver", "cmd_id", "gw", "result", "code", "at"} {
		if _, ok := raw[k]; !ok {
			t.Errorf("write_ack missing %q", k)
		}
	}
	if string(raw["gw"]) != `"`+gwClientID+`"` {
		t.Errorf("gw = %s", raw["gw"])
	}
	// 定稿契约无批量字段（旧 v0 writes/results 面收敛为单点）。
	for _, k := range []string{"results", "sent_at"} {
		if _, ok := raw[k]; ok {
			t.Errorf("write_ack should not carry v0 field %q", k)
		}
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
