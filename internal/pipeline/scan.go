// scan.go stale 后台扫描（§6.3：每 30s，置位发质量事件）。
package pipeline

import (
	"context"
	"time"

	"github.com/huang1234321/thermio-ingest/internal/quality"
)

// runStaleScanner 每轮扫描置位的点发 stale_set 质量事件；首轮不判（§6.3 重启
// 基线）。ctx 取消即退出（GO-04）。
func (p *Pipeline) runStaleScanner(ctx context.Context) {
	defer p.staleWG.Done()
	ticker := time.NewTicker(p.cfg.StaleScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := p.nowFunc().UTC()
			for _, ev := range p.stale.Scan(now) {
				pc, ok := p.cache.PointByID(ev.PointID)
				if !ok {
					continue
				}
				rec := qualityRecord(pc.GatewayID, pc.TenantID, pc.PointID, now,
					quality.EventStaleSet,
					map[string]any{"stale_timeout_s": pc.StaleTimeoutS}, newTraceID())
				p.produceQualityEvent(rec)
			}
		}
	}
}
