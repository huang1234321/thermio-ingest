// Package decode 负责网关上报 JSON 信封解码与 ingest.md §3.2 字段规则校验
// （未知 msg_type/ver、超长、TS_INVALID 等负路径 → DLQ）。解码器实现随 IMPL-5
// 落地；本文件冻结信封契约常量。
package decode

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
