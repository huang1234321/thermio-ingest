// Package quality 冻结 telemetry.quality 位掩码位表（ingest.md §4，v1 冻结：
// 新增位只增不改；与 ddl.md §11.1 telemetry.quality smallint 对应）。
// L1b 置位逻辑（valid_range/null/stale/ts_skew）随 IMPL-5 落地。
package quality

// 位分配（ingest.md §4）。0 = good。
const (
	OutOfRange      uint16 = 1 << 0 // bit0  L1b：超出 point.valid_range_min/max
	DeviceBad       uint16 = 1 << 1 // bit1  L1a：网关 payload quality ≠ good
	NullValue       uint16 = 1 << 2 // bit2  L1b：数值/文本均缺失
	Stale           uint16 = 1 << 3 // bit3  L1b：超过 stale_timeout_s 无更新
	UnitUnconverted uint16 = 1 << 4 // bit4  ：无单位转换规则，原值入库（§5.2）
	TsSkew          uint16 = 1 << 5 // bit5  ：网关时钟偏移超阈值（10min）
	Jump            uint16 = 1 << 6 // bit6  预留：跳变（L2 二期）
	Drift           uint16 = 1 << 7 // bit7  预留：漂移趋势（L2 二期）
	Backfill        uint16 = 1 << 8 // bit8  信息位：ts 距接收时刻 > 10min（补传标记）
)

// Good 报告 q 是否不含任何置位（0 = good）。
func Good(q uint16) bool { return q == 0 }
