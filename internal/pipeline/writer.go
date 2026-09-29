// writer.go 单 writer：聚合批量（§5.1 阶段 6/7）与写入顺序（§7.1
// TSDB → Kafka → PUBACK）。批内按 (point_id, ts) 排序（索引局部性）。
package pipeline

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/huang1234321/thermio-ingest/internal/dlq"
	"github.com/huang1234321/thermio-ingest/internal/kafkaproducer"
	"github.com/huang1234321/thermio-ingest/internal/tsdb"

	"github.com/twmb/franz-go/pkg/kgo"
)

// batch 聚合中的待落库批次（整消息粒度追加，不拆分——ack 对齐）。
type batch struct {
	msgs  []*processed
	rows  []tsdb.Row
	works []rowWork     // 与 rows 同序（works[i] 即 rows[i] 的工作件）
	recs  []*kgo.Record // raw + 质量事件（TSDB 成功后才 produce）
	dlqs  []*kgo.Record
	acks  []func()
	slots int // 本批占用的行占位数（flush 后释放）
}

func (b *batch) add(p *processed, slots int) {
	b.msgs = append(b.msgs, p)
	for i := range p.rows {
		b.works = append(b.works, p.rows[i])
		b.rows = append(b.rows, p.rows[i].row)
		if p.rows[i].raw != nil {
			b.recs = append(b.recs, p.rows[i].raw)
		}
	}
	b.recs = append(b.recs, p.events...)
	b.dlqs = append(b.dlqs, p.dlqs...)
	if p.ack != nil {
		b.acks = append(b.acks, p.ack)
	}
	b.slots += slots
}

func (b *batch) empty() bool { return len(b.msgs) == 0 }

func (b *batch) reset() {
	b.msgs = b.msgs[:0]
	b.works = b.works[:0]
	b.rows = b.rows[:0]
	b.recs = b.recs[:0]
	b.dlqs = b.dlqs[:0]
	b.acks = b.acks[:0]
	b.slots = 0
}

// runWriter §5.1 阶段 6/7。decoded 关闭且排空后做 final flush 再退出（§12 冲缓冲）。
//
// 心跳与停摆观测（DAT-202 D-29）：60s 心跳日志携带全链水位（intake/decoded/
// rowGate/批内行/flush 年龄）——G4 预演实测「排空尾段停摆 + 零错误日志」形态下
// 无任何现场信号可判位（计数冻结原因不可判）。批非空且 >3 个 flush 周期未落
// 时升 WARN writer_stalled（含同一组水位），把复发变成可诊断事件。
func (p *Pipeline) runWriter() {
	defer p.writerWG.Done()
	ticker := time.NewTicker(p.cfg.BatchFlushInterval)
	defer ticker.Stop()
	heartbeat := time.NewTicker(time.Minute)
	defer heartbeat.Stop()

	var lastFlush time.Time
	b := &batch{}
	for {
		select {
		case res, ok := <-p.decoded:
			if !ok {
				p.flush(b) // final flush：优雅退出冲缓冲（§12）
				return
			}
			b.add(res, p.slotsOf(res))
			if len(b.rows) >= p.cfg.BatchMaxRows {
				p.flush(b)
				lastFlush = time.Now()
			}
		case <-ticker.C:
			if !b.empty() {
				p.flush(b)
				lastFlush = time.Now()
			}
		case <-heartbeat.C:
			pipelineState := func() (int, int, int, int) {
				return len(p.intake), len(p.decoded),
					p.cfg.BufferMaxRows - len(p.rowGate), len(b.rows)
			}
			if !b.empty() && !lastFlush.IsZero() && time.Since(lastFlush) > 3*p.cfg.BatchFlushInterval {
				intakeN, decodedN, gateFree, batchRows := pipelineState()
				p.log.Warn("writer_stalled",
					"batch_rows", batchRows,
					"since_last_flush_s", int(time.Since(lastFlush).Seconds()),
					"intake_queued", intakeN,
					"decoded_queued", decodedN,
					"rowgate_free", gateFree,
					"rowgate_capacity", p.cfg.BufferMaxRows,
					"hint", "批非空且超 3 个 flush 周期未落库——查 TSDB/Kafka 写延迟或宿主资源")
			} else {
				intakeN, decodedN, gateFree, batchRows := pipelineState()
				p.log.Info("writer_heartbeat",
					"batch_rows", batchRows,
					"since_last_flush_s", int(time.Since(lastFlush).Seconds()),
					"intake_queued", intakeN,
					"decoded_queued", decodedN,
					"rowgate_free", gateFree)
			}
		}
	}
}

// flush §7.1 写入顺序：TSDB 批量 upsert → Kafka produce（raw/quality/DLQ）
// → PUBACK → 释放行占位。TSDB 失败：整批进 DLQ TSDB_WRITE_FAILED + 失败计数，
// raw/质量事件不 produce（真相源未落，流侧可从 TSDB/DLQ 重建），仍 PUBACK
// （防重投风暴；DLQ 副本保留 30 天可重放，§9 运维介入路径）。
func (p *Pipeline) flush(b *batch) {
	if b.empty() {
		return
	}
	defer func() {
		for _, ack := range b.acks {
			ack() // 幂等；最后一步（§7.1）
		}
		p.releaseRows(b.slots)
		b.reset()
	}()

	// §5.1 阶段 6：批内 (point_id, ts) 排序；stable 保证同键 (point_id, ts)
	// 后到覆盖先到（last-write-wins，§7.2）。
	rows := make([]tsdb.Row, len(b.rows))
	copy(rows, b.rows)
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].PointID != rows[j].PointID {
			return rows[i].PointID < rows[j].PointID
		}
		return rows[i].TS.Before(rows[j].TS)
	})

	start := p.nowFunc()
	// 批写超时（DAT-121）：与 Kafka 侧 ProduceTimeout 对称——TSDB 挂起不得让
	// writer 永久阻塞（PUBACK 不发、背压顶死）；超时走下方 TSDB_WRITE_FAILED
	// 死信路径兜底（upsert 幂等，重放不重复）。
	writeCtx, writeCancel := context.WithTimeout(context.Background(), p.cfg.TsdbWriteTimeout)
	err := p.tsdb.WriteBatch(writeCtx, rows)
	writeCancel()
	p.met.TSDBWriteLatency.Observe(float64(p.nowFunc().Sub(start).Milliseconds()))

	produceCtx, cancel := context.WithTimeout(context.Background(), p.cfg.ProduceTimeout)
	defer cancel()

	if err != nil {
		p.met.TSDBFailures.Inc()
		p.met.DLQ.WithLabelValues(dlq.TSDBWriteFailed).Inc()
		if rec := p.tsdbFailDLQ(b, err); rec != nil {
			b.dlqs = append(b.dlqs, rec)
		}
		if perr := p.kafka.Produce(produceCtx, b.dlqs...); perr != nil {
			// 双写通道全断：只剩日志与指标（OBS-MT-02：TSDB_WRITE_FAILED runbook）。
			p.log.Error("dlq produce failed after tsdb write failure", "err", perr.Error())
		}
		p.log.Error("tsdb batch write failed, batch sent to dlq", "rows", len(rows), "err", err.Error())
		return
	}

	// Kafka：raw + 质量事件 + 死信（死信独立于数据面成败，一律发）。
	start = p.nowFunc()
	produceAll := append(append([]*kgo.Record{}, b.recs...), b.dlqs...)
	if perr := p.kafka.Produce(produceCtx, produceAll...); perr != nil {
		// 数据已在 TSDB（真相源不丢，ADR-002）；流缺口可从 TSDB 重放补齐。
		p.log.Error("kafka produce failed; telemetry persisted in tsdb", "records", len(produceAll), "err", perr.Error())
	}
	p.met.KafkaProduceLatency.Observe(float64(p.nowFunc().Sub(start).Milliseconds()))

	// stale 观测（§6.3）：TSDB 落库成功才推进 lastTS；解除事件即时补发。
	for i := range b.works {
		if cleared := p.staleObserve(b.works[i].stale); cleared != nil {
			p.produceQualityEvent(cleared)
		}
	}

	// pipeline latency（§10：MQTT 收到 → TSDB 落库）以批内最老消息锚定。
	oldest := b.msgs[0].receivedAt
	for _, m := range b.msgs[1:] {
		if m.receivedAt.Before(oldest) {
			oldest = m.receivedAt
		}
	}
	p.met.PipelineLatency.Observe(float64(p.nowFunc().Sub(oldest).Milliseconds()))
}

// staleObserve 写成功后的 stale 状态推进；返回 stale_cleared 事件记录（如有）。
func (p *Pipeline) staleObserve(so staleObserve) *kgo.Record {
	_, cleared := p.stale.Observe(so.pointID, so.ts, so.receivedAt)
	if cleared == nil {
		return nil
	}
	pc, ok := p.cache.PointByID(so.pointID)
	if !ok {
		return nil
	}
	return qualityRecord(pc.GatewayID, pc.TenantID, pc.PointID, so.receivedAt,
		cleared.Event, nil, newTraceID())
}

// tsdbFailDLQ 构造整批 TSDB_WRITE_FAILED 死信（payload = 行数组 JSON，可重放）。
func (p *Pipeline) tsdbFailDLQ(b *batch, err error) *kgo.Record {
	payload, merr := json.Marshal(b.rows)
	if merr != nil {
		payload = []byte("[]")
	}
	m := dlq.Message{
		Reason:     dlq.TSDBWriteFailed,
		ReceivedAt: p.nowFunc().UTC(),
		PayloadB64: dlq.PayloadB64(payload),
		Detail:     dlq.DetailJSON(map[string]any{"rows": len(b.rows), "err": err.Error()}),
	}
	var key string
	if len(b.msgs) > 0 {
		m.GatewayID = b.msgs[0].gatewayID
		m.Topic = "batch:" + b.msgs[0].clientID
		key = m.GatewayID
		if key == "" {
			key = b.msgs[0].clientID
		}
	} else {
		m.Topic = "internal:batch"
	}
	v, eerr := m.Encode()
	if eerr != nil {
		return nil
	}
	rec := &kgo.Record{Topic: kafkaproducer.TopicDLQ, Key: []byte(key), Value: v}
	if len(b.msgs) > 0 && b.msgs[0].traceID != "" {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: kafkaproducer.HeaderTraceID, Value: []byte(b.msgs[0].traceID)})
	}
	return rec
}
