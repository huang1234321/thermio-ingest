// Package dlq 冻结死信原因封闭集（ingest.md §9 表）与 DLQ 消息体。死信唯一出口
// 是 Kafka topic thermio.dlq（保留 30 天，ADR-004）；指标 ingest_dlq_messages_total{reason}
// 驱动告警（OBS-MT-02：每条 reason 有 runbook 对应）。重放工具（CLI）v2 交付。
package dlq

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

// Reason 封闭集（ingest.md §9）。
const (
	MalformedJSON     = "MALFORMED_JSON"      // 解码失败
	PayloadTooLarge   = "PAYLOAD_TOO_LARGE"   // >256KB / >500 点
	UnknownMsgType    = "UNKNOWN_MSG_TYPE"    // 前向兼容拒绝
	UnsupportedVer    = "UNSUPPORTED_VER"     // 前向兼容拒绝
	UnknownGateway    = "UNKNOWN_GATEWAY"     // clientid 未注册
	GWMismatch        = "GW_MISMATCH"         // payload.gw ≠ topic clientid 网关（安全关注项）
	UnregisteredPoint = "UNREGISTERED_POINT"  // (gateway_id, raw_name) 缓存未命中
	PointInactive     = "POINT_INACTIVE"      // point.status=disabled
	TSInvalid         = "TS_INVALID"          // ts 缺失/不可解析
	UnitUnconverted   = "UNIT_UNCONVERTED"    // 无转换对（数据未丢，bit4 标记）
	TSBeyondRetention = "TS_BEYOND_RETENTION" // ts < now − 2 年
	TSDBWriteFailed   = "TSDB_WRITE_FAILED"   // 重试耗尽
)

// Message thermio.dlq 消息体（ingest.md §9 示例逐字段对齐）。
type Message struct {
	Reason     string          `json:"reason"`
	ReceivedAt time.Time       `json:"received_at"`
	GatewayID  string          `json:"gateway_id,omitempty"` // topic clientid 未注册时为空
	Topic      string          `json:"topic"`
	PointName  string          `json:"point_name,omitempty"`
	PayloadB64 string          `json:"payload_b64"`
	Detail     json.RawMessage `json:"detail,omitempty"`
}

// PayloadB64 DLQ envelope 的 payload 字段编码（§9 payload_b64）。
func PayloadB64(payload []byte) string { return base64.StdEncoding.EncodeToString(payload) }

// Encode 序列化为 Kafka 值（调用方负责 key/headers）。
func (m Message) Encode() ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// DecodeMsg 反序列化 DLQ 消息（测试与 v2 重放工具共用）。
func DecodeMsg(b []byte) (Message, error) {
	var m Message
	if err := json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	return m, nil
}

// DetailJSON 把任意 detail 结构序列化为 RawMessage；失败时退化为字符串包装，
// DLQ 发射路径不得因 detail 序列化失败而丢死信。
func DetailJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		fallback, _ := json.Marshal(map[string]string{"detail_error": err.Error()})
		return fallback
	}
	return b
}
