// Package decode 负责网关上报 JSON 信封解码与 ingest.md §3.2 字段规则校验
// （未知 msg_type/ver、超长、TS_INVALID 等负路径 → DLQ）。
//
// 解码是纯函数阶段（§5.1 阶段 2）：不查缓存、不产生 IO；信封级失败整消息进
// DLQ，点位级失败（name 规则 / TS_INVALID）按点进 DLQ，同消息合法点不受牵连。
package decode

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/huang1234321/thermio-ingest/internal/dlq"
)

// 信封契约常量（ingest.md §3.2，v1）。
const (
	// MsgTypeTelemetryBatch v1 仅此消息类型；未知值 → DLQ UNKNOWN_MSG_TYPE。
	MsgTypeTelemetryBatch = "telemetry_batch"
	// Ver1 当前契约版本；不认识的版本 → DLQ UNSUPPORTED_VER（消费端不猜）。
	Ver1 = 1
	// MaxPoints 单消息点位数上限（1..500），超长 → 整消息 DLQ PAYLOAD_TOO_LARGE。
	MaxPoints = 500
	// MaxPayloadBytes 单消息 payload 上限 256 KB。
	MaxPayloadBytes = 256 << 10
	// NameMaxLen 点位 name 长度上限（1..64 字符，[A-Za-z0-9_.:-]+）。
	NameMaxLen = 64
)

// Envelope 解码后的信封（§3.1）。字段语义见 §3.2 表。
type Envelope struct {
	MsgType string
	Ver     int
	Gw      string // 出厂序列号（gateway.serial），与 topic clientid 一致性在管线阶段校验
	Seq     int64  // 网关内单调递增；缺口只记指标与 WARN，不拒收
	SentAt  time.Time
	Points  []Point
}

// Point 解码后的点位元素。Value/ValueText 为 nil 表示数值/文本缺失
// （显式 JSON null 与字段缺失同等对待 → bit2 null_value，§3.2/§4）。
type Point struct {
	Name      string
	Value     *float64
	ValueText *string
	TS        time.Time
	Quality   string // good（默认）/bad/uncertain；非 good 一律映射 device_bad 位
	Unit      string // 信息性；归一以 point.unit_raw 配置为准，不一致仅 WARN
}

// PointError 点位级失败（该点进 DLQ，消息其余点继续）。
type PointError struct {
	Index     int    // 在 points 数组中的下标（0 起）
	Reason    string // dlq.Reason 值
	PointName string // 尽力提取的点名（可能无效为空）
	Detail    string // 人读细节
}

// EnvelopeError 信封级失败（整消息进 DLQ 后 PUBACK）。
type EnvelopeError struct {
	Reason string // dlq.Reason 值
	Detail string
}

func (e *EnvelopeError) Error() string { return fmt.Sprintf("envelope: %s: %s", e.Reason, e.Detail) }

// Decode 校验并解码一条上行消息（§3.2 全部字段规则）。返回：
//   - 信封级失败：*EnvelopeError（整消息 DLQ，调用方不再看点位）；
//   - 信封合法：Envelope + 各点位级失败列表（可能为空）。
func Decode(payload []byte) (*Envelope, []PointError, *EnvelopeError) {
	// 守卫顺序：先尺寸后解析——超长消息不必付出 JSON 解析成本，且先给
	// PAYLOAD_TOO_LARGE（§3.2 防重投风暴语义）。
	if len(payload) > MaxPayloadBytes {
		return nil, nil, &EnvelopeError{Reason: dlq.PayloadTooLarge,
			Detail: fmt.Sprintf("payload %d bytes > %d", len(payload), MaxPayloadBytes)}
	}

	var raw rawEnvelope
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, nil, &EnvelopeError{Reason: dlq.MalformedJSON, Detail: "json: " + err.Error()}
	}

	// msg_type / ver：前向兼容拒绝（消费端不猜）。
	if raw.MsgType == nil {
		return nil, nil, &EnvelopeError{Reason: dlq.MalformedJSON, Detail: "msg_type 缺失"}
	}
	if *raw.MsgType != MsgTypeTelemetryBatch {
		return nil, nil, &EnvelopeError{Reason: dlq.UnknownMsgType,
			Detail: fmt.Sprintf("msg_type=%q（v1 仅 %q）", *raw.MsgType, MsgTypeTelemetryBatch)}
	}
	if raw.Ver == nil {
		return nil, nil, &EnvelopeError{Reason: dlq.MalformedJSON, Detail: "ver 缺失"}
	}
	if *raw.Ver != Ver1 {
		return nil, nil, &EnvelopeError{Reason: dlq.UnsupportedVer,
			Detail: fmt.Sprintf("ver=%d（v1 仅 1）", *raw.Ver)}
	}

	// gw / seq / sent_at：必填。
	if raw.Gw == nil || *raw.Gw == "" {
		return nil, nil, &EnvelopeError{Reason: dlq.MalformedJSON, Detail: "gw 缺失"}
	}
	if raw.Seq == nil {
		return nil, nil, &EnvelopeError{Reason: dlq.MalformedJSON, Detail: "seq 缺失"}
	}
	if *raw.Seq < 0 {
		return nil, nil, &EnvelopeError{Reason: dlq.MalformedJSON, Detail: fmt.Sprintf("seq=%d 为负（规则 int ≥ 0）", *raw.Seq)}
	}
	if raw.SentAt == nil {
		return nil, nil, &EnvelopeError{Reason: dlq.MalformedJSON, Detail: "sent_at 缺失"}
	}
	sentAt, err := time.Parse(time.RFC3339, *raw.SentAt)
	if err != nil {
		return nil, nil, &EnvelopeError{Reason: dlq.MalformedJSON,
			Detail: "sent_at 非 RFC3339 带时区: " + err.Error()}
	}

	// points：1..500；超上限整消息 PAYLOAD_TOO_LARGE（§3.2），空批次属结构错误。
	if raw.Points == nil {
		return nil, nil, &EnvelopeError{Reason: dlq.MalformedJSON, Detail: "points 缺失"}
	}
	if n := len(raw.Points); n > MaxPoints {
		return nil, nil, &EnvelopeError{Reason: dlq.PayloadTooLarge,
			Detail: fmt.Sprintf("points %d > %d", n, MaxPoints)}
	} else if n == 0 {
		return nil, nil, &EnvelopeError{Reason: dlq.MalformedJSON, Detail: "points 为空（规则 1..500）"}
	}

	env := &Envelope{MsgType: *raw.MsgType, Ver: *raw.Ver, Gw: *raw.Gw, Seq: *raw.Seq, SentAt: sentAt}
	var errs []PointError
	for i, p := range raw.Points {
		pt, perr := decodePoint(i, p)
		if perr != nil {
			errs = append(errs, *perr)
			continue
		}
		env.Points = append(env.Points, *pt)
	}
	return env, errs, nil
}

// decodePoint §3.2 点位元素规则。value/value_text 类型非法（如 value 为字符串）
// 属该点结构错误（MALFORMED_JSON，点位级）；ts 缺失/不可解析是独立 reason
// TS_INVALID——两者都是点级失败，只废该点。
func decodePoint(i int, p rawPoint) (*Point, *PointError) {
	fail := func(reason, detail string) *PointError {
		name := ""
		if p.Name != nil {
			name = *p.Name
		}
		return &PointError{Index: i, Reason: reason, PointName: name, Detail: detail}
	}

	// name：1..64 字符，[A-Za-z0-9_.:-]+。
	if p.Name == nil || *p.Name == "" {
		return nil, fail(dlq.MalformedJSON, "点位 name 缺失")
	}
	if len(*p.Name) > NameMaxLen || !validName(*p.Name) {
		return nil, fail(dlq.MalformedJSON,
			fmt.Sprintf("name=%q 违反 1..64 字符 [A-Za-z0-9_.:-]+", *p.Name))
	}

	// value：number 或 null；类型非法即点级 MALFORMED。
	var value *float64
	if p.Value != nil {
		v, ok := p.Value.(float64)
		if !ok {
			return nil, fail(dlq.MalformedJSON, fmt.Sprintf("value 类型 %T 非法（number 或 null）", p.Value))
		}
		value = &v
	}
	var text *string
	if p.ValueText != nil {
		s, ok := p.ValueText.(string)
		if !ok {
			return nil, fail(dlq.MalformedJSON, fmt.Sprintf("value_text 类型 %T 非法（string）", p.ValueText))
		}
		text = &s
	}
	// value 与 value_text 同时给 → 语义二义，按结构错误废该点。
	if value != nil && text != nil {
		return nil, fail(dlq.MalformedJSON, "value 与 value_text 同时存在（规则二选一）")
	}

	// ts：必填 RFC3339 带时区；缺失/不可解析 → TS_INVALID。
	if p.TS == nil {
		return nil, fail(dlq.TSInvalid, "ts 缺失")
	}
	ts, err := time.Parse(time.RFC3339, *p.TS)
	if err != nil {
		return nil, fail(dlq.TSInvalid, "ts 非 RFC3339 带时区: "+err.Error())
	}

	q := "good"
	if p.Quality != nil && *p.Quality != "" {
		q = *p.Quality // §3.2：非 good（含未知值）一律映射 device_bad 位，lenient
	}
	var unit string
	if p.Unit != nil {
		unit = *p.Unit
	}
	return &Point{Name: *p.Name, Value: value, ValueText: text, TS: ts, Quality: q, Unit: unit}, nil
}

func validName(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '_' || c == '.' || c == ':' || c == '-':
		default:
			return false
		}
	}
	return true
}

// rawEnvelope 与 wire 格式一一对应；指针字段区分「缺失」与「零值」（§3.2 必填判定）。
type rawEnvelope struct {
	MsgType *string    `json:"msg_type"`
	Ver     *int       `json:"ver"`
	Gw      *string    `json:"gw"`
	Seq     *int64     `json:"seq"`
	SentAt  *string    `json:"sent_at"`
	Points  []rawPoint `json:"points"`
}

type rawPoint struct {
	Name      *string `json:"name"`
	Value     any     `json:"value"`      // number 或 null
	ValueText any     `json:"value_text"` // string
	TS        *string `json:"ts"`
	Quality   *string `json:"quality"`
	Unit      *string `json:"unit"`
}
