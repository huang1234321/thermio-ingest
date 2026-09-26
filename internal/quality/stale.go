// stale.go 实现 ingest.md §6.3 stale 检测状态：map<point_id, last_ts> 进程内
// 维护；后台扫描每 30s；重启后首轮不判（等一个采样周期重建基线，避免误报）。
// 置位时发质量事件并在后续写入携带 bit3，直至恢复更新。
package quality

import (
	"sync"
	"time"
)

// StaleEvent 扫描产生的状态迁移（pipeline 转 Kafka thermio.telemetry.quality）。
type StaleEvent struct {
	PointID int64
	Event   string // stale_set / stale_cleared
}

// 事件名（枚举只增不改，Kafka 值契约的一部分）。
const (
	EventStaleSet        = "stale_set"
	EventStaleCleared    = "stale_cleared"
	EventTsSkew          = "ts_skew"
	EventUnitUnconverted = "unit_unconverted"
)

// StaleTracker stale 位状态机。
//
// 语义（§6.3 的实现口径）：
//   - Observe：每个成功写入的点样本调用。若该点当前在 stale 态，本样本仍带 bit3
//     （「后续写入携带 bit3」）；若样本 ts 是新鲜的（距接收 < timeout），stale 解除
//     并产出 stale_cleared 事件——补传旧 ts 不解除 stale。
//   - Scan：后台扫描。lastTS 距 now 超过该点 stale_timeout_s 的点置 stale 并产出
//     stale_set 事件；首轮（重启后）只建基线不判定。
type StaleTracker struct {
	mu      sync.Mutex
	lastTS  map[int64]time.Time
	stale   map[int64]bool
	warmed  bool // 首轮扫描是否已完成（§6.3 重启首轮不判）
	timeout func(pointID int64) (time.Duration, bool)
}

// NewStaleTracker timeout 取每个点的 stale_timeout_s（配置缓存快照提供）；
// ok=false 的点（已从缓存消失）跳过判定。
func NewStaleTracker(timeout func(pointID int64) (time.Duration, bool)) *StaleTracker {
	return &StaleTracker{
		lastTS:  map[int64]time.Time{},
		stale:   map[int64]bool{},
		timeout: timeout,
	}
}

// Observe 记录一个样本。返回：本样本是否携带 bit3（点当前处于 stale 态）、
// 以及状态迁移事件（stale_cleared，仅新鲜样本解除时产出）。
func (t *StaleTracker) Observe(pointID int64, ts, receivedAt time.Time) (carryBit bool, cleared *StaleEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()

	carryBit = t.stale[pointID]
	if prev, ok := t.lastTS[pointID]; !ok || ts.After(prev) {
		t.lastTS[pointID] = ts
	}
	if carryBit {
		if timeout, ok := t.timeout(pointID); ok && receivedAt.Sub(ts) < timeout {
			// 新鲜样本 = 恢复更新：本样本仍带 bit3（置位期间的写入），之后解除。
			delete(t.stale, pointID)
			return true, &StaleEvent{PointID: pointID, Event: EventStaleCleared}
		}
	}
	return carryBit, nil
}

// Scan 一轮后台扫描；返回本轮新置位的 stale_set 事件。warmup 语义：进程启动后
// 首轮不判（§6.3），只标记已建基线。
func (t *StaleTracker) Scan(now time.Time) []StaleEvent {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.warmed {
		t.warmed = true
		return nil
	}
	var events []StaleEvent
	for pointID, last := range t.lastTS {
		if t.stale[pointID] {
			continue
		}
		timeout, ok := t.timeout(pointID)
		if !ok {
			continue
		}
		if now.Sub(last) > timeout {
			t.stale[pointID] = true
			events = append(events, StaleEvent{PointID: pointID, Event: EventStaleSet})
		}
	}
	return events
}

// IsStale 测试与指标辅助。
func (t *StaleTracker) IsStale(pointID int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stale[pointID]
}
