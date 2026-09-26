package quality

import (
	"testing"
	"time"
)

func fixedTimeout(time.Duration) func(int64) (time.Duration, bool) {
	return func(int64) (time.Duration, bool) { return 5 * time.Second, true }
}

// §6.3 重启首轮不判：首轮 Scan 只建基线，不产生事件；第二轮起正常判定。
func TestStaleFirstRoundNoJudge(t *testing.T) {
	tr := NewStaleTracker(fixedTimeout(5 * time.Second))
	now := time.Now()
	tr.Observe(1, now.Add(-time.Minute), now) // 早已超时的样本

	if evs := tr.Scan(now); len(evs) != 0 {
		t.Errorf("首轮扫描应不判定, got %+v", evs)
	}
	evs := tr.Scan(now)
	if len(evs) != 1 || evs[0].Event != EventStaleSet || evs[0].PointID != 1 {
		t.Errorf("第二轮应置位 stale_set, got %+v", evs)
	}
	if !tr.IsStale(1) {
		t.Error("点 1 应处于 stale 态")
	}
}

// §6.3 置位后写入带 bit3，新鲜样本解除并产出 stale_cleared；补传旧 ts 不解除。
func TestStaleCarryAndClear(t *testing.T) {
	tr := NewStaleTracker(fixedTimeout(5 * time.Second))
	now := time.Now()

	// 建 stale 态：样本 + 两轮扫描。
	tr.Observe(1, now.Add(-time.Minute), now)
	tr.Scan(now)
	tr.Scan(now)
	if !tr.IsStale(1) {
		t.Fatal("预置 stale 态失败")
	}

	// stale 期间的新鲜写入：带 bit3 + 解除事件。
	carry, cleared := tr.Observe(1, now, now)
	if !carry || cleared == nil || cleared.Event != EventStaleCleared {
		t.Errorf("新鲜样本应 carry bit3 且解除: carry=%v cleared=%+v", carry, cleared)
	}
	// 解除后的写入不再带 bit3。
	if carry, cleared := tr.Observe(1, now.Add(time.Second), now.Add(time.Second)); carry || cleared != nil {
		t.Errorf("解除后不应带 bit3: carry=%v cleared=%+v", carry, cleared)
	}

	// 重新置位后，补传旧 ts 不解除 stale。
	tr.Observe(2, now.Add(-2*time.Minute), now)
	tr.Scan(now)
	tr.Scan(now)
	if !tr.IsStale(2) {
		t.Fatal("点 2 预置 stale 失败")
	}
	carry, cleared = tr.Observe(2, now.Add(-2*time.Minute), now) // 补传（ts 仍旧）
	if !carry || cleared != nil {
		t.Errorf("补传旧 ts 应带 bit3 且不解除: carry=%v cleared=%+v", carry, cleared)
	}
	if !tr.IsStale(2) {
		t.Error("补传旧 ts 不应解除 stale")
	}
}

// lastTS 单调：乱序补传不回拨基线（stale 判定以最新样本为准）。
func TestStaleLastTSMonotonic(t *testing.T) {
	tr := NewStaleTracker(fixedTimeout(5 * time.Second))
	now := time.Now()
	tr.Observe(1, now, now)
	tr.Observe(1, now.Add(-time.Hour), now) // 乱序补传
	tr.Scan(now.Add(2 * time.Second))       // 距 fresh 样本仅 2s < 5s
	tr.Scan(now.Add(2 * time.Second))
	if tr.IsStale(1) {
		t.Error("乱序补传不应回拨 lastTS 导致误判 stale")
	}
}
