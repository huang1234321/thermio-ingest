// scan.go stale 后台扫描（§6.3：每 30s，置位发质量事件）。
package pipeline

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
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
			// 整轮聚合单发（DAT-202 D-38：突发静默后一轮可产生千级 stale_set——
			// 逐条同步 produce 即 N×ProduceTimeout 串行阻塞面）
			var recs []*kgo.Record
			for _, ev := range p.stale.Scan(now) {
				pc, ok := p.cache.PointByID(ev.PointID)
				if !ok {
					continue
				}
				recs = append(recs, qualityRecord(pc.GatewayID, pc.TenantID, pc.PointID, now,
					quality.EventStaleSet,
					map[string]any{"stale_timeout_s": pc.StaleTimeoutS}, newTraceID()))
			}
			p.produceQualityEvents(recs)
		}
	}
}
