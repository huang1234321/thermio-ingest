// Package dlq 冻结死信原因封闭集（ingest.md §9 表）。死信唯一出口是 Kafka
// topic thermio.dlq（保留 30 天，ADR-004），消息体 reason 字段取本包常量；
// 指标 ingest_dlq_messages_total{reason} 驱动告警（OBS-MT-02：每条 reason 有
// runbook 对应）。DLQ 生产与重放工具（v2）随 IMPL-5 落地。
package dlq

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
