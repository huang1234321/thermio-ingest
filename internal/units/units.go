// Package units 负责单位归一（ingest.md §5.2）：只对数值量按 point.unit_raw →
// point.unit_std 查内置转换表 v1（温度/压力/功率/能量/流量等单位族）；无收录转换对
// 时 value 原样入库 + bit4 unit_unconverted + DLQ 副本（数据永不丢）。转换表实现随
// IMPL-5 落地，公式表随代码评审入库。
package units
