package quality

import "testing"

// 位值对齐 ingest.md §4 位表（v1 冻结：只增不改）。
func TestBitValues(t *testing.T) {
	cases := []struct {
		name string
		got  uint16
		want uint16
	}{
		{"OutOfRange bit0", OutOfRange, 1},
		{"DeviceBad bit1", DeviceBad, 2},
		{"NullValue bit2", NullValue, 4},
		{"Stale bit3", Stale, 8},
		{"UnitUnconverted bit4", UnitUnconverted, 16},
		{"TsSkew bit5", TsSkew, 32},
		{"Jump bit6 预留", Jump, 64},
		{"Drift bit7 预留", Drift, 128},
		{"Backfill bit8", Backfill, 256},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}

func TestGood(t *testing.T) {
	if !Good(0) {
		t.Error("Good(0) = false, want true（0 = good）")
	}
	for _, q := range []uint16{OutOfRange, DeviceBad, Backfill, OutOfRange | Stale} {
		if Good(q) {
			t.Errorf("Good(%d) = true, want false", q)
		}
	}
}
