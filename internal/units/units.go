// Package units 负责单位归一（ingest.md §5.2）：只对数值量按 point.unit_raw →
// point.unit_std 查内置转换表 v1；无收录转换对时 value 原样入库 + bit4
// unit_unconverted + DLQ 副本（M&V 红线：数据永不丢）。转换不做人为舍入
// （double 原精度）；公式表随代码评审入库（枚举治理同权级）。
package units

import "strings"

// Convert 把 v 从 raw 单位归一到 std 单位。ok=false 表示无收录转换对——
// 调用方按 §5.2 走「原值入库 + bit4 + DLQ」路径，不得丢弃数据。
//
// 空单位约定（§5.2）：raw 或 std 为空 → 视为已归一直通（点表导入侧负责校验，
// ADR-015），返回原值 ok=true。
func Convert(raw, std string, v float64) (float64, bool) {
	raw, std = trim(raw), trim(std)
	switch {
	case raw == "" || std == "":
		return v, true // §5.2 空单位 = 已归一直通
	case raw == std:
		return v, true
	}
	// 同族内经族基准单位中转：raw → 族基准 → std。
	eRaw, ok := table[raw]
	if !ok {
		return v, false
	}
	eStd, ok := table[std]
	if !ok || eRaw.family != eStd.family {
		return v, false
	}
	return eStd.fromBase(eRaw.toBase(v)), true
}

type entry struct {
	family   string
	toBase   func(float64) float64 // v(该单位) → v(族基准单位)
	fromBase func(float64) float64 // v(族基准单位) → v(该单位)
}

func id(v float64) float64 { return v }

// table 内置转换表 v1（ingest.md §5.2 单位族全集）。
// 族基准：温度 degC、压力 kPa、功率 kW、能量 kWh、流量 L/s；恒等族仅同单位恒等。
// 公式（评审入库，只增不改）：
//
//	温度：degF→degC (v−32)×5/9；K→degC v−273.15（逆向即 fromBase）
//	压力：Pa ×0.001；bar ×100；psi ×6.894757293168；mmH2O ×0.00980665
//	功率：W ×0.001；MW ×1000
//	能量：Wh ×0.001
//	流量：m³/h ÷3.6（1 m³/h = 1000 L ÷ 3600 s）
var table = map[string]entry{
	// 温度（基准 degC）
	"degC": {"temperature", id, id},
	"degF": {"temperature",
		func(v float64) float64 { return (v - 32) * 5 / 9 },
		func(v float64) float64 { return v*9/5 + 32 }},
	"K": {"temperature",
		func(v float64) float64 { return v - 273.15 },
		func(v float64) float64 { return v + 273.15 }},
	// 压力（基准 kPa）
	"kPa": {"pressure", id, id},
	"Pa":  {"pressure", func(v float64) float64 { return v * 0.001 }, func(v float64) float64 { return v * 1000 }},
	"bar": {"pressure", func(v float64) float64 { return v * 100 }, func(v float64) float64 { return v / 100 }},
	"psi": {"pressure",
		func(v float64) float64 { return v * 6.894757293168 },
		func(v float64) float64 { return v / 6.894757293168 }},
	"mmH2O": {"pressure",
		func(v float64) float64 { return v * 0.00980665 },
		func(v float64) float64 { return v / 0.00980665 }},
	// 功率（基准 kW）
	"kW": {"power", id, id},
	"W":  {"power", func(v float64) float64 { return v * 0.001 }, func(v float64) float64 { return v * 1000 }},
	"MW": {"power", func(v float64) float64 { return v * 1000 }, func(v float64) float64 { return v / 1000 }},
	// 能量（基准 kWh）
	"kWh": {"energy", id, id},
	"Wh":  {"energy", func(v float64) float64 { return v * 0.001 }, func(v float64) float64 { return v * 1000 }},
	// 流量（基准 L/s）
	"L/s":  {"flow", id, id},
	"m³/h": {"flow", func(v float64) float64 { return v / 3.6 }, func(v float64) float64 { return v * 3.6 }},
	"m3/h": {"flow", func(v float64) float64 { return v / 3.6 }, func(v float64) float64 { return v * 3.6 }}, // ASCII 别名：网关模板常无法打出 ³
	// 恒等族（跨单位不换算）
	"%":   {"percent", id, id},
	"Hz":  {"frequency", id, id},
	"V":   {"voltage", id, id},
	"A":   {"current", id, id},
	"rpm": {"rotational", id, id},
}

// trim 单位匹配为精确匹配（区分大小写）：单位集合由点表导入侧治理（ADR-015），
// 本表不发明大小写别名；仅去掉首尾空白。
func trim(u string) string { return strings.TrimSpace(u) }
