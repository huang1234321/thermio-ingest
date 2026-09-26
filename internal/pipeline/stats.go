// stats.go gauge 数据源：bufferedRows / backpressureActive / 配置缓存健康。
// 周期采样进原子变量（gauge 闭包读原子，避免与热路径抢锁）。
package pipeline

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/huang1234321/thermio-ingest/internal/points"
)

// Stats 原子 gauge 快照。
type Stats struct {
	bufferedRows     atomic.Int64
	backpressure     atomic.Int64
	configPoints     atomic.Int64
	configRefreshAge atomic.Int64
}

// NewStats 零值即用。
func NewStats() *Stats { return &Stats{} }

// Gauge 闭包组（metrics.New 接线用，OBS-MT-04 命名在 metrics 侧）。
func (s *Stats) Gauge() (bufferRows, backpressureActive, configCachePoints, configRefreshAge func() float64) {
	return func() float64 { return float64(s.bufferedRows.Load()) },
		func() float64 { return float64(s.backpressure.Load()) },
		func() float64 { return float64(s.configPoints.Load()) },
		func() float64 { return float64(s.configRefreshAge.Load()) }
}

// BackpressureActiveNow 背压即时状态（测试与告警探针用）。
func (s *Stats) BackpressureActiveNow() bool { return s.backpressure.Load() == 1 }

// BufferedRowsNow 缓冲行数即时值。
func (s *Stats) BufferedRowsNow() int64 { return s.bufferedRows.Load() }

// sampleLoop 5s 采样（Prometheus 15s 刮取，5s 足够新鲜且近零开销）。
func (s *Stats) sampleLoop(ctx context.Context, cache *points.Cache, rowGate chan struct{}) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n := int64(len(rowGate))
			s.bufferedRows.Store(n)
			if n >= int64(cap(rowGate)) {
				s.backpressure.Store(1)
			} else {
				s.backpressure.Store(0)
			}
			s.configPoints.Store(int64(cache.Size()))
			s.configRefreshAge.Store(int64(cache.RefreshAge(time.Now())))
		}
	}
}
