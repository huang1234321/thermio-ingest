package units

import (
	"math"
	"testing"
)

// §5.2 正路径：收录转换对按公式归一（数值与 ingest.md §13 抽样口径一致，
// double 原精度比较用相对误差 < 1e-12）。
func TestConvertKnown(t *testing.T) {
	cases := []struct {
		name     string
		raw, std string
		in, want float64
	}{
		{"degF→degC 冰点", "degF", "degC", 32, 0},
		{"degF→degC 常温", "degF", "degC", 77, 25},
		{"degF→degC 负值", "degF", "degC", -40, -40},
		{"K→degC", "K", "degC", 300, 26.85},
		{"degC→degF", "degC", "degF", 25, 77},
		{"degC→K", "degC", "K", 25, 298.15},
		{"psi→kPa 一个大气压附近", "psi", "kPa", 14.5, 99.973980750936},
		{"psi→kPa 整数", "psi", "kPa", 1, 6.894757293168},
		{"bar→kPa", "bar", "kPa", 1.2, 120},
		{"Pa→kPa", "Pa", "kPa", 1234, 1.234},
		{"mmH2O→kPa", "mmH2O", "kPa", 1000, 9.80665},
		{"m³/h→L/s", "m³/h", "L/s", 3.6, 1},
		{"m³/h→L/s 小数", "m³/h", "L/s", 12.5, 3.4722222222222223},
		{"L/s→m³/h", "L/s", "m³/h", 2, 7.2},
		{"m3/h ASCII 别名→L/s", "m3/h", "L/s", 3.6, 1},
		{"W→kW", "W", "kW", 1500, 1.5},
		{"MW→kW", "MW", "kW", 0.25, 250},
		{"Wh→kWh", "Wh", "kWh", 300, 0.3},
		{"同单位恒等 kW", "kW", "kW", 7.42, 7.42},
		{"同单位恒等 %", "%", "%", 63.5, 63.5},
	}
	for _, c := range cases {
		got, ok := Convert(c.raw, c.std, c.in)
		if !ok {
			t.Errorf("%s: Convert(%s→%s) 未收录，want 收录", c.name, c.raw, c.std)
			continue
		}
		if math.Abs(got-c.want) > 1e-12*math.Max(1, math.Abs(c.want)) {
			t.Errorf("%s: Convert(%s→%s, %v) = %v, want %v", c.name, c.raw, c.std, c.in, got, c.want)
		}
	}
}

// §5.2 负路径与边界：
//   - 无收录转换对 → ok=false（调用方走原值入库 + bit4 + DLQ，不丢数）；
//   - 跨单位族（degC→kPa）与恒等族跨单位（%→Hz）均无规则；
//   - 空单位 = 已归一直通（§5.2）。
func TestConvertUnknownAndEmpty(t *testing.T) {
	unknown := [][2]string{
		{"CFM", "L/s"},  // 未收录单位
		{"degC", "kPa"}, // 跨族
		{"%", "Hz"},     // 恒等族跨单位
		{"GPM", "m³/h"}, // 未收录
	}
	for _, p := range unknown {
		if v, ok := Convert(p[0], p[1], 1); ok {
			t.Errorf("Convert(%s→%s) = (%v,true), want 未收录（原值入库 + bit4 + DLQ）", p[0], p[1], v)
		}
	}

	for _, p := range [][2]string{{"", "degC"}, {"kW", ""}, {"", ""}} {
		if v, ok := Convert(p[0], p[1], 7.5); !ok || v != 7.5 {
			t.Errorf("Convert(%q→%q) = (%v,%v), want (7.5,true) 空单位直通", p[0], p[1], v, ok)
		}
	}
}
