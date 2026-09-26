package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/huang1234321/thermio-ingest/internal/decode"
)

// Envelope 是上行遥测信封（ingest.md §3.1）。字段名与顺序即契约；value/value_text
// 二选一（指针承载 null 与缺省）。Pad 仅 oversize(bytes) 故障注入使用。
type Envelope struct {
	MsgType string  `json:"msg_type"`
	Ver     int     `json:"ver"`
	GW      string  `json:"gw"`
	Seq     int64   `json:"seq"`
	SentAt  string  `json:"sent_at"`
	Points  []Point `json:"points"`
	Pad     string  `json:"pad,omitempty"`
}

// Point 单点位样本（ingest.md §3.2 点位元素规则）。序列化形态（契约示例同形）：
// 数值量 → {"name","value"(可为 null),"ts","quality","unit"?}；
// 枚态量 → {"name","value_text","ts","quality"}（无 value/unit，不做单位归一）。
type Point struct {
	Name      string
	Value     *float64
	ValueText *string
	TS        string
	Quality   string
	Unit      string
}

// MarshalJSON 按点位类型输出契约字段（数值量的 null 值必须显式成 "value": null，
// 喂 ingest null_value 路径——omitempty 会吞掉该形态，故手写）。
func (p Point) MarshalJSON() ([]byte, error) {
	if p.ValueText != nil {
		return json.Marshal(struct {
			Name      string  `json:"name"`
			ValueText *string `json:"value_text"`
			TS        string  `json:"ts"`
			Quality   string  `json:"quality"`
		}{p.Name, p.ValueText, p.TS, p.Quality})
	}
	return json.Marshal(struct {
		Name    string   `json:"name"`
		Value   *float64 `json:"value"`
		TS      string   `json:"ts"`
		Quality string   `json:"quality"`
		Unit    string   `json:"unit,omitempty"`
	}{p.Name, p.Value, p.TS, p.Quality, p.Unit})
}

// tsLayout 点位采集时间戳（秒级，带时区，契约示例同形）。
const tsLayout = "2006-01-02T15:04:05Z07:00"

// sentAtLayout 消息发送时刻（毫秒，带时区）。
const sentAtLayout = "2006-01-02T15:04:05.000Z07:00"

// FormatTS 采集时间戳格式化（导出供测试）。
func FormatTS(t time.Time) string { return t.Format(tsLayout) }

// newEnvelope 组装一条合规信封：契约常量取 internal/decode（单一真源）。
func newEnvelope(gwSerial string, seq int64, sentAt time.Time, pts []Point) Envelope {
	return Envelope{
		MsgType: decode.MsgTypeTelemetryBatch,
		Ver:     decode.Ver1,
		GW:      gwSerial,
		Seq:     seq,
		SentAt:  sentAt.Format(sentAtLayout),
		Points:  pts,
	}
}

// validateContract 对将要发出的信封做发端自检（契约红线，ingest.md §3.2）：
// 点数 1..500、payload ≤ 256KB、name 字符集与长度。故障注入刻意破坏的字段
// （gw/ver/msg_type/ts）不在自检范围——它们就是要喂给 ingest 负路径的。
func (e *Envelope) validateContract() error {
	if len(e.Points) < 1 || len(e.Points) > decode.MaxPoints {
		return fmt.Errorf("points %d out of contract range 1..%d", len(e.Points), decode.MaxPoints)
	}
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal for size check: %w", err)
	}
	if len(b) > decode.MaxPayloadBytes {
		return fmt.Errorf("payload %d bytes exceeds contract max %d", len(b), decode.MaxPayloadBytes)
	}
	for _, p := range e.Points {
		if err := validName(p.Name); err != nil {
			return err
		}
	}
	return nil
}

// validName 校验点位 name：1..64 字符，[A-Za-z0-9_.:-]+（ingest.md §3.2）。
func validName(name string) error {
	if l := len(name); l < 1 || l > decode.NameMaxLen {
		return fmt.Errorf("point name %q length %d out of 1..%d", name, l, decode.NameMaxLen)
	}
	for _, c := range name {
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '_' || c == '.' || c == ':' || c == '-':
		default:
			return fmt.Errorf("point name %q contains illegal char %q", name, c)
		}
	}
	return nil
}

// marshalEnvelope 序列化信封（独立函数便于故障注入路径复用尺寸统计）。
func marshalEnvelope(e Envelope) ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}
	return b, nil
}
