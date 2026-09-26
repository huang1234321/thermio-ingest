package dlq

import "testing"

// 封闭集完整性：与 ingest.md §9 表逐条对应，值唯一。
func TestReasonSet(t *testing.T) {
	all := map[string]string{
		MalformedJSON:     "MALFORMED_JSON",
		PayloadTooLarge:   "PAYLOAD_TOO_LARGE",
		UnknownMsgType:    "UNKNOWN_MSG_TYPE",
		UnsupportedVer:    "UNSUPPORTED_VER",
		UnknownGateway:    "UNKNOWN_GATEWAY",
		GWMismatch:        "GW_MISMATCH",
		UnregisteredPoint: "UNREGISTERED_POINT",
		PointInactive:     "POINT_INACTIVE",
		TSInvalid:         "TS_INVALID",
		UnitUnconverted:   "UNIT_UNCONVERTED",
		TSBeyondRetention: "TS_BEYOND_RETENTION",
		TSDBWriteFailed:   "TSDB_WRITE_FAILED",
	}
	for got, want := range all {
		if got != want {
			t.Errorf("reason %q != %q", got, want)
		}
	}
	if len(all) != 12 {
		t.Errorf("封闭集共 %d 条, want 12（ingest.md §9 表全量）", len(all))
	}
}
