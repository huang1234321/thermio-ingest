package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/huang1234321/thermio-ingest/internal/decode"
)

// Injector 按故障旋钮对信封/字节流做确定性破坏。rate 类按点独立投掷（rng 可种子
// 复现）；every 类按网关内消息计数触发。全部关闭时信封零改动（正常采集）。
type Injector struct {
	cfg FaultsCfg
	rng *rand.Rand
	// msgCount 已组装消息数（从 1 计）。
	msgCount int64
	// stats 注入计数（按 DLQ reason 口径归并，进运行摘要）。
	stats map[string]int64
}

// NewInjector 每网关一个（种子 = profile.Seed + 网关序号，隔离且可复现）。
func NewInjector(cfg FaultsCfg, seed int64) *Injector {
	return &Injector{
		cfg:   cfg,
		rng:   rand.New(rand.NewSource(seed)),
		stats: make(map[string]int64),
	}
}

// Stats 返回注入计数快照。
func (in *Injector) Stats() map[string]int64 {
	out := make(map[string]int64, len(in.stats))
	for k, v := range in.stats {
		out[k] = v
	}
	return out
}

// NextSeq 给出下一条消息的 seq（单调递增；缺口旋钮在此生效——只跳号不拒收，
// 契约 §3.2：seq 缺口记指标与 WARN）。
func (in *Injector) NextSeq(seq int64) int64 {
	next := seq + 1
	if in.cfg.SeqGapEvery > 0 && in.msgCount > 0 &&
		int((in.msgCount+1)%int64(in.cfg.SeqGapEvery)) == 0 {
		next += int64(in.cfg.SeqGapSize)
		in.stats["seq_gap"]++
	}
	return next
}

// Apply 在组装阶段对点位施加 rate 类故障（null/quality）；并按 every 类破坏
// 信封级字段（gw 错配、未知点、坏 ts、msg_type/ver 越界）。返回可能被替换的
// 发送字节（malformed/oversize 直接替换整条 payload）。tsSkewS 在此统一施加
// （网关时钟偏移同时影响 ts 与 sent_at）。
func (in *Injector) Apply(env *Envelope, now time.Time) ([]byte, bool, error) {
	in.msgCount++
	n := in.msgCount

	// 时钟偏移：网关钟慢/快，ts 与 sent_at 同步偏移（>600s 触发 bit5 ts_skew）。
	if in.cfg.TSkewS != 0 {
		skew := time.Duration(in.cfg.TSkewS) * time.Second
		for i := range env.Points {
			if t, err := time.Parse(tsLayout, env.Points[i].TS); err == nil {
				env.Points[i].TS = FormatTS(t.Add(skew))
			}
		}
		if s, err := time.Parse(sentAtLayout, env.SentAt); err == nil {
			env.SentAt = s.Add(skew).Format(sentAtLayout)
		}
	}

	// 点级 rate 类：null 值 / quality 标记。
	for i := range env.Points {
		if env.Points[i].Value != nil && in.cfg.NullValueRate > 0 &&
			in.rng.Float64() < in.cfg.NullValueRate {
			env.Points[i].Value = nil
			env.Points[i].Unit = ""
			in.stats["null_value"]++
		}
		switch {
		case in.cfg.BadQualityRate > 0 && in.rng.Float64() < in.cfg.BadQualityRate:
			env.Points[i].Quality = "bad"
			in.stats["bad_quality"]++
		case in.cfg.UncertainQualityRate > 0 && in.rng.Float64() < in.cfg.UncertainQualityRate:
			env.Points[i].Quality = "uncertain"
			in.stats["uncertain_quality"]++
		}
	}

	// gw 字段错配（GW_MISMATCH：payload.gw ≠ topic clientid 归属网关）。
	if in.cfg.GWMismatchEvery > 0 && n%int64(in.cfg.GWMismatchEvery) == 0 {
		env.GW = env.GW + "-OTHER"
		in.stats["gw_mismatch"]++
	}

	// 夹带未注册点（UNREGISTERED_POINT）。
	if in.cfg.UnknownPointsEvery > 0 && n%int64(in.cfg.UnknownPointsEvery) == 0 && len(env.Points) > 0 {
		for k := 0; k < in.cfg.UnknownPointsCount; k++ {
			v := 1.0
			env.Points = append(env.Points, Point{
				Name:    fmt.Sprintf("UNREGISTERED_PT_%03d", k),
				Value:   &v,
				TS:      env.Points[0].TS,
				Quality: "good",
			})
		}
		in.stats["unknown_point"]++
	}

	// ts 不可解析（TS_INVALID）。
	if in.cfg.InvalidTSEvery > 0 && n%int64(in.cfg.InvalidTSEvery) == 0 && len(env.Points) > 0 {
		env.Points[0].TS = "not-a-timestamp"
		in.stats["invalid_ts"]++
	}

	// ts 超保留期（TS_BEYOND_RETENTION：ts < now − 2 年）。
	if in.cfg.TSBeyondRetentionEvery > 0 && n%int64(in.cfg.TSBeyondRetentionEvery) == 0 && len(env.Points) > 0 {
		env.Points[0].TS = FormatTS(now.AddDate(-3, 0, 0))
		in.stats["ts_beyond_retention"]++
	}

	// msg_type / ver 越界（UNKNOWN_MSG_TYPE / UNSUPPORTED_VER）。
	if in.cfg.UnknownMsgTypeEvery > 0 && n%int64(in.cfg.UnknownMsgTypeEvery) == 0 {
		env.MsgType = "firmware_status"
		in.stats["unknown_msg_type"]++
	}
	if in.cfg.UnsupportedVerEvery > 0 && n%int64(in.cfg.UnsupportedVerEvery) == 0 {
		env.Ver = 2
		in.stats["unsupported_ver"]++
	}

	// 超限（PAYLOAD_TOO_LARGE：>500 点 或 >256KB）。
	if in.cfg.OversizeEvery > 0 && n%int64(in.cfg.OversizeEvery) == 0 && len(env.Points) > 0 {
		switch in.cfg.OversizeMode {
		case "points":
			for len(env.Points) <= decode.MaxPoints {
				v := 0.0
				env.Points = append(env.Points, Point{
					Name: fmt.Sprintf("OVERSIZE_PAD_%04d", len(env.Points)), Value: &v,
					TS: env.Points[0].TS, Quality: "good",
				})
			}
		case "bytes":
			env.Pad = strings.Repeat("X", decode.MaxPayloadBytes)
		}
		in.stats["oversize"]++
	}

	payload, err := marshalEnvelope(*env)
	if err != nil {
		return nil, false, err
	}

	// 非法 JSON（MALFORMED_JSON）：截断的字节流替换整条 payload。
	if in.cfg.MalformedEvery > 0 && n%int64(in.cfg.MalformedEvery) == 0 {
		cut := len(payload) / 2
		if cut == 0 {
			cut = 1
		}
		payload = payload[:cut]
		in.stats["malformed"]++
	}
	return payload, true, nil
}
