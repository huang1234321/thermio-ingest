package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/huang1234321/thermio-ingest/internal/decode"
)

// 对齐复核（DAT-109 合并门 3）：把 21 份 profile 实际产出的 wire payload 逐条
// 喂给 main 上 DAT-108 落地的 decode.Decode（单一契约真源的执行面），断言每份
// profile 的解码结果与场景意图一致。该测试锁定「gw-sim 产出 ↔ Decode 消费」
// 的契约对齐：Decode 语义变更或 gw-sim 信封漂移都会在此爆。
//
// 口径备注：gw/clientid 一致性（GW_MISMATCH）、点位注册、retention、质量位
// 均是 decode 之后的管线阶段——相关 profile 在本测试断言「decode 干净」，
// 其故障价值在管线/E2E 层兑现（README 矩阵 B 的期望观测列）。

type alignExpect struct {
	envelopeErr string // 期望出现的信封级 DLQ reason（空 = 全部消息信封级零失败）
	pointErr    string // 期望出现的点位级 DLQ reason（空 = 全部消息点位级零失败）
	cleanOnly   bool   // 额外断言：全部消息信封级+点位级零失败
}

var alignMatrix = map[string]alignExpect{
	// 信封级故障 → 对应 DLQ reason
	"malformed-json":   {envelopeErr: "MALFORMED_JSON"},
	"unknown-msg-type": {envelopeErr: "UNKNOWN_MSG_TYPE"},
	"unsupported-ver":  {envelopeErr: "UNSUPPORTED_VER"},
	"oversize-points":  {envelopeErr: "PAYLOAD_TOO_LARGE"},
	"oversize-bytes":   {envelopeErr: "PAYLOAD_TOO_LARGE"},
	// 点级故障：TS_INVALID 废该点、消息其余点存活（decode.go 点级语义）
	"invalid-ts": {pointErr: "TS_INVALID"},
	// 以下场景 decode 阶段必须全部干净（故障在更下游阶段兑现）
	"normal-10k":                 {cleanOnly: true},
	"bad-null":                   {cleanOnly: true},
	"unregistered-points":        {cleanOnly: true},
	"backfill-3d":                {cleanOnly: true},
	"backfill-8d-compressed":     {cleanOnly: true},
	"backpressure":               {cleanOnly: true},
	"emqx-restart-window":        {cleanOnly: true},
	"ingest-rolling-restart":     {cleanOnly: true},
	"offline-buffer-retransmit":  {cleanOnly: true},
	"seq-gap":                    {cleanOnly: true},
	"ts-skew":                    {cleanOnly: true},
	"gw-mismatch":                {cleanOnly: true}, // GW_MISMATCH 在管线阶段（clientid↔gw）
	"ts-beyond-retention":        {cleanOnly: true}, // retention 防线在写路径（ingest.md §7.1）
	"downlink-echo":              {cleanOnly: true},
	"downlink-readback-mismatch": {cleanOnly: true},
}

// TestProfilesAgainstDecodeDecode 全 profile → decode.Decode 对齐。
func TestProfilesAgainstDecodeDecode(t *testing.T) {
	entries, err := os.ReadDir("profiles")
	if err != nil {
		t.Fatal(err)
	}
	ran := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".json")
		exp, ok := alignMatrix[name]
		if !ok {
			t.Errorf("profile %q not covered in alignMatrix（矩阵缺行）", name)
			continue
		}
		t.Run(name, func(t *testing.T) {
			p, err := LoadProfile(filepath.Join("profiles", e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			g := newGatewaySim(p, 0, "tcp://unused:1883", "pw", newRunStats(), testLogger)

			// 3 轮采样：every=2 的故障必然触发；产出经 deliver 入 outbox
			// （无连接），outbox 字节即 wire 字节（含故障注入后的定格形态）。
			for i := 1; i <= 3; i++ {
				g.sampleCycle(t.Context(), time.Now(), false)
			}
			var payloads [][]byte
			for {
				entry, ok := g.ob.peek()
				if !ok {
					break
				}
				payloads = append(payloads, entry.payload)
				g.ob.pop()
			}
			if len(payloads) == 0 {
				t.Fatal("no messages produced")
			}

			envErrSeen, ptErrSeen, cleanMsgs := 0, 0, 0
			for _, raw := range payloads {
				env, ptErrs, envErr := decode.Decode(raw)
				if envErr != nil {
					envErrSeen++
					if exp.envelopeErr == "" {
						t.Errorf("unexpected envelope error: %v", envErr)
					} else if envErr.Reason != exp.envelopeErr {
						t.Errorf("envelope error reason = %s want %s（detail: %s）",
							envErr.Reason, exp.envelopeErr, envErr.Detail)
					}
					continue
				}
				// 信封合法：字段语义与 gw-sim 源头一致
				if env.MsgType != decode.MsgTypeTelemetryBatch || env.Ver != decode.Ver1 {
					t.Errorf("decoded msg_type/ver = %q/%d", env.MsgType, env.Ver)
				}
				if env.Gw == "" || env.Seq < 0 {
					t.Errorf("decoded gw/seq = %q/%d", env.Gw, env.Seq)
				}
				for _, pe := range ptErrs {
					ptErrSeen++
					if exp.pointErr == "" {
						t.Errorf("unexpected point error: %+v", pe)
					} else if pe.Reason != exp.pointErr {
						t.Errorf("point error reason = %s want %s", pe.Reason, exp.pointErr)
					}
				}
				if len(ptErrs) == 0 {
					cleanMsgs++
				}
			}

			switch {
			case exp.envelopeErr != "":
				if envErrSeen == 0 {
					t.Errorf("fault %s never fired（injector stats: %v）", exp.envelopeErr, g.inj.Stats())
				}
			case exp.pointErr != "":
				if ptErrSeen == 0 {
					t.Errorf("fault %s never fired（injector stats: %v）", exp.pointErr, g.inj.Stats())
				}
				if cleanMsgs == 0 {
					t.Error("TS_INVALID 应为点级：同批其余点必须存活")
				}
			default: // cleanOnly
				if envErrSeen != 0 || ptErrSeen != 0 {
					t.Errorf("clean profile decoded dirty: envelopeErr=%d pointErr=%d", envErrSeen, ptErrSeen)
				}
			}
		})
		ran++
	}
	if ran != len(alignMatrix) {
		t.Errorf("profiles ran %d, matrix rows %d（两侧应对齐）", ran, len(alignMatrix))
	}
}

// TestDecodeSemanticsPinballs 逐条钉死与 gw-sim 产出相关的 Decode 语义细节
// （对齐复核的人工结论部分，测试化防漂移）。
func TestDecodeSemanticsPinballs(t *testing.T) {
	now := time.Now()

	t.Run("sent_at millisecond layout accepted", func(t *testing.T) {
		v := 1.0
		env := newEnvelope("GWSIM001", 1, now, []Point{{Name: "SIM_0000", Value: &v, TS: FormatTS(now), Quality: "good"}})
		raw, err := marshalEnvelope(env)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, derr := decode.Decode(raw); derr != nil {
			t.Fatalf("millisecond sent_at rejected: %v", derr)
		}
	})

	t.Run("explicit null value decodes as nil (bit2 source)", func(t *testing.T) {
		env := newEnvelope("GWSIM001", 1, now, []Point{{Name: "SIM_0000", Value: nil, TS: FormatTS(now), Quality: "bad"}})
		raw, _ := marshalEnvelope(env)
		denv, ptErrs, derr := decode.Decode(raw)
		if derr != nil || len(ptErrs) != 0 {
			t.Fatalf("null value point rejected: %v %+v", derr, ptErrs)
		}
		if denv.Points[0].Value != nil {
			t.Error("explicit null should decode to nil Value")
		}
	})

	t.Run("enum point value_text passthrough", func(t *testing.T) {
		running := "running"
		env := newEnvelope("GWSIM001", 1, now, []Point{{Name: "SIM_0001", ValueText: &running, TS: FormatTS(now), Quality: "good"}})
		raw, _ := marshalEnvelope(env)
		denv, ptErrs, derr := decode.Decode(raw)
		if derr != nil || len(ptErrs) != 0 || *denv.Points[0].ValueText != "running" {
			t.Fatalf("enum point mishandled: %v %+v", derr, ptErrs)
		}
	})

	t.Run("oversize bytes trips size guard before parse", func(t *testing.T) {
		v := 0.0
		env := newEnvelope("GWSIM001", 1, now, []Point{{Name: "PAD", Value: &v, TS: FormatTS(now), Quality: "good"}})
		env.Pad = strings.Repeat("X", decode.MaxPayloadBytes)
		raw, _ := marshalEnvelope(env)
		_, _, derr := decode.Decode(raw)
		if derr == nil || derr.Reason != "PAYLOAD_TOO_LARGE" {
			t.Fatalf("oversize bytes: %v", derr)
		}
	})

	t.Run("skewed ts still RFC3339-valid (bit5 is pipeline stage)", func(t *testing.T) {
		v := 20.0
		env := newEnvelope("GWSIM001", 1, now, []Point{{Name: "SIM_0000", Value: &v, TS: FormatTS(now.Add(15 * time.Minute)), Quality: "good"}})
		raw, _ := marshalEnvelope(env)
		if _, ptErrs, derr := decode.Decode(raw); derr != nil || len(ptErrs) != 0 {
			t.Fatalf("skewed-but-valid ts rejected: %v %+v", derr, ptErrs)
		}
	})
}
