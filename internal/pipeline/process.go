// process.go decode worker 与单条消息处理（§5.1 阶段 2–5）。
package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/huang1234321/thermio-ingest/internal/decode"
	"github.com/huang1234321/thermio-ingest/internal/dlq"
	"github.com/huang1234321/thermio-ingest/internal/kafkaproducer"
	"github.com/huang1234321/thermio-ingest/internal/mqtt"
	"github.com/huang1234321/thermio-ingest/internal/quality"
	"github.com/huang1234321/thermio-ingest/internal/tsdb"
	"github.com/huang1234321/thermio-ingest/internal/units"

	"github.com/twmb/franz-go/pkg/kgo"
)

// processed 一条消息处理后的全部产物（同消息不跨 flush 拆分——ack 对齐）。
type processed struct {
	gatewayID  string // 解析出的网关（Kafka key；未知网关为空）
	clientID   string // topic clientid（UNKNOWN_GATEWAY 的 Kafka key 兜底）
	traceID    string
	receivedAt time.Time // pipeline latency 观测锚点

	rows   []rowWork // TSDB 行 + Kafka raw 记录 + stale 观测件
	events []*kgo.Record
	dlqs   []*kgo.Record
	ack    func() // PUBACK（幂等）
}

// rowWork 一行遥测的全部工作件。
type rowWork struct {
	row   tsdb.Row
	raw   *kgo.Record // thermio.telemetry.raw（值 = §7.4 清洗后行）
	stale staleObserve
}

type staleObserve struct {
	pointID    int64
	ts         time.Time
	receivedAt time.Time
}

// rawPayload §7.4 值契约（枚举只增不改）。
type rawPayload struct {
	PointID   int64    `json:"point_id"`
	GatewayID string   `json:"gateway_id"`
	TenantID  string   `json:"tenant_id"`
	TS        string   `json:"ts"`
	Value     *float64 `json:"value"`
	ValueText *string  `json:"value_text"`
	Quality   int      `json:"quality"`
}

// qualityPayload §5.3 质量事件值契约。
type qualityPayload struct {
	PointID   int64          `json:"point_id"`
	GatewayID string         `json:"gateway_id"`
	TS        string         `json:"ts"`
	Event     string         `json:"event"`
	Detail    map[string]any `json:"detail,omitempty"`
}

// runDecodeWorker §5.1 阶段 2–5：解码 → 网关/点位解析 → 单位归一 → 质量标记，
// 产物经 rowGate 背压入 decoded。intake 关闭且排空后退出（GO-04）。
func (p *Pipeline) runDecodeWorker() {
	defer p.wg.Done()
	for msg := range p.intake {
		res := p.processMessage(msg)
		p.acquireRows(p.slotsOf(res))
		p.decoded <- res
	}
}

func (p *Pipeline) slotsOf(res *processed) int {
	n := len(res.rows) + len(res.dlqs) + len(res.events)
	if n == 0 {
		n = 1
	}
	return n
}

// clientIDFromTopic 解析 thermio/gw/{clientid}/up/data；形态非法返回空。
func clientIDFromTopic(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) == 5 && parts[0] == "thermio" && parts[1] == "gw" && parts[3] == "up" && parts[4] == "data" {
		return parts[2]
	}
	return ""
}

func newTraceID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 降级时间戳：trace_id 仅诊断用途，不值得失败路径（GO-02）。
		return time.Now().Format("150405.000000000")
	}
	return hex.EncodeToString(b[:])
}

// processMessage §5.1 阶段 2–5 单消息实现。信封级失败整消息 DLQ 后 ack
// （§3.2 防重投风暴语义）；点位级失败按点 DLQ，合法点不受牵连。
func (p *Pipeline) processMessage(msg mqtt.Inbound) *processed {
	// §10 RED-R 入口速率打点：此处 = 成功入队、将被处理的消息。停机窗口
	// 未入队的消息不 ACK 会重投、下轮计入，口径自洽（ADR-014 触发器数据源）。
	p.met.MQTTMessages.Inc()
	now := p.nowFunc().UTC()
	res := &processed{
		clientID:   clientIDFromTopic(msg.Topic),
		traceID:    newTraceID(),
		receivedAt: msg.ReceivedAt,
		ack:        msg.Ack,
	}
	if res.ack == nil {
		res.ack = func() {} // 测试消息可无 ack
	}

	gw, gwKnown := p.cache.GatewayByClientID(res.clientID)
	if gwKnown {
		res.gatewayID = gw.GatewayID
	}
	kafkaKey := func() []byte {
		if res.gatewayID != "" {
			return []byte(res.gatewayID)
		}
		return []byte(res.clientID)
	}
	appendDLQ := func(reason, pointName string, detail map[string]any) {
		m := dlq.Message{
			Reason:     reason,
			ReceivedAt: now,
			GatewayID:  res.gatewayID,
			Topic:      msg.Topic,
			PointName:  pointName,
			PayloadB64: dlq.PayloadB64(msg.Payload),
			Detail:     dlq.DetailJSON(detail),
		}
		v, err := m.Encode()
		if err != nil {
			return // 该结构可编码，防御分支（GO-02）
		}
		rec := &kgo.Record{Topic: kafkaproducer.TopicDLQ, Key: kafkaKey(), Value: v}
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: kafkaproducer.HeaderTraceID, Value: []byte(res.traceID)})
		if gwKnown {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: kafkaproducer.HeaderTenantID, Value: []byte(gw.TenantID)})
		}
		res.dlqs = append(res.dlqs, rec)
		p.met.DLQ.WithLabelValues(reason).Inc()
	}
	dlqDetail := func(detail string) map[string]any { return map[string]any{"detail": detail} }

	env, perrs, eerr := decode.Decode(msg.Payload)
	if eerr != nil {
		appendDLQ(eerr.Reason, "", dlqDetail(eerr.Detail))
		p.met.Points.WithLabelValues("dlq").Inc()
		return res
	}

	// 网关三连判：UNKNOWN_GATEWAY / GW_MISMATCH（§2：防伪造路由，安全关注项）。
	if !gwKnown {
		appendDLQ(dlq.UnknownGateway, "", dlqDetail("topic clientid 未注册: "+res.clientID))
		p.met.Points.WithLabelValues("dlq").Inc()
		return res
	}
	serialGWID, serialKnown := p.cache.GatewayIDBySerial(env.Gw)
	if !serialKnown || serialGWID != gw.GatewayID {
		appendDLQ(dlq.GWMismatch, "", dlqDetail("payload.gw="+env.Gw+" 与 topic clientid="+res.clientID+" 不属同一网关"))
		p.met.Points.WithLabelValues("dlq").Inc()
		return res
	}

	p.observeSeq(gw.GatewayID, env.Seq)

	// 消息级时钟偏移（bit5）：sent_at 是唯一诊断锚（§3.2）。
	skewed := absDuration(now.Sub(env.SentAt)) > p.cfg.TsSkewThreshold

	for _, pe := range perrs {
		appendDLQ(pe.Reason, pe.PointName, dlqDetail(pe.Detail))
		p.met.Points.WithLabelValues("dlq").Inc()
	}

	for i := range env.Points {
		pt := &env.Points[i]
		pc, ok := p.cache.Point(gw.GatewayID, pt.Name)
		if !ok {
			appendDLQ(dlq.UnregisteredPoint, pt.Name, dlqDetail("(gateway_id, raw_name) 缓存未命中"))
			p.met.Points.WithLabelValues("dlq").Inc()
			p.met.Unregistered.Inc()
			continue
		}
		if pc.Status != "active" {
			appendDLQ(dlq.PointInactive, pt.Name, map[string]any{"status": pc.Status})
			p.met.Points.WithLabelValues("dlq").Inc()
			continue
		}
		// §7.1 拒写线：ts < now − 2 年不落库（防落即删的僵尸 chunk）。
		if now.Sub(pt.TS) > p.cfg.RetentionCutoff {
			appendDLQ(dlq.TSBeyondRetention, pt.Name, map[string]any{"ts": pt.TS.Format(time.RFC3339Nano)})
			p.met.Points.WithLabelValues("dlq").Inc()
			continue
		}

		q := uint16(0)
		if pt.Quality != "good" {
			q |= quality.DeviceBad
		}
		value, text := pt.Value, pt.ValueText
		if value == nil && text == nil {
			q |= quality.NullValue
		}

		// 单位归一（§5.2）：失败原值入库 + bit4 + DLQ 副本，数据永不丢。
		if value != nil {
			if v, converted := units.Convert(pc.UnitRaw, pc.UnitStd, *value); converted {
				value = &v
				p.unitConverted(pc.PointID)
				// 量程判定在归一后（std 单位，§4 bit0）；未归一跳过（单位未知）。
				if pc.ValidRangeMin != nil && v < *pc.ValidRangeMin ||
					pc.ValidRangeMax != nil && v > *pc.ValidRangeMax {
					q |= quality.OutOfRange
				}
			} else {
				q |= quality.UnitUnconverted
				appendDLQ(dlq.UnitUnconverted, pt.Name,
					map[string]any{"unit_raw": pc.UnitRaw, "unit_std": pc.UnitStd, "value": *value})
				if p.markUnitFirst(pc.PointID) { // §5.3 首次出现才发质量事件
					res.events = append(res.events, qualityRecord(gw.GatewayID, pc.TenantID, pc.PointID,
						now, quality.EventUnitUnconverted, map[string]any{"unit_raw": pc.UnitRaw, "unit_std": pc.UnitStd}, res.traceID))
				}
			}
		}

		if skewed {
			q |= quality.TsSkew
			res.events = append(res.events, qualityRecord(gw.GatewayID, pc.TenantID, pc.PointID,
				now, quality.EventTsSkew,
				map[string]any{"sent_at": env.SentAt.Format(time.RFC3339Nano), "received_at": now.Format(time.RFC3339Nano)},
				res.traceID))
		}
		if now.Sub(pt.TS) > p.cfg.BackfillThreshold {
			q |= quality.Backfill // §4 信息位：补传数据标记
		}
		if p.stale.IsStale(pc.PointID) {
			q |= quality.Stale // §6.3：stale 期间的写入携带 bit3
		}

		row := tsdb.Row{PointID: pc.PointID, TS: pt.TS, Value: value, ValueText: text, Quality: q}
		res.rows = append(res.rows, rowWork{
			row:   row,
			raw:   rawRecord(gw.GatewayID, pc.TenantID, row, env.Seq, res.traceID),
			stale: staleObserve{pointID: pc.PointID, ts: pt.TS, receivedAt: msg.ReceivedAt},
		})
		p.met.Points.WithLabelValues("ok").Inc()
	}
	return res
}

// rawRecord §7.4：key=gateway_id（同网关同分区保序），headers tenant_id/trace_id/gw_seq。
func rawRecord(gatewayID, tenantID string, row tsdb.Row, seq int64, traceID string) *kgo.Record {
	val, err := json.Marshal(rawPayload{
		PointID: row.PointID, GatewayID: gatewayID, TenantID: tenantID,
		TS: row.TS.UTC().Format(time.RFC3339Nano), Value: row.Value, ValueText: row.ValueText,
		Quality: int(row.Quality),
	})
	if err != nil {
		val = []byte("{}") // 防御：字段全可编码
	}
	rec := &kgo.Record{Topic: kafkaproducer.TopicTelemetryRaw, Key: []byte(gatewayID), Value: val}
	rec.Headers = append(rec.Headers,
		kgo.RecordHeader{Key: kafkaproducer.HeaderTenantID, Value: []byte(tenantID)},
		kgo.RecordHeader{Key: kafkaproducer.HeaderTraceID, Value: []byte(traceID)},
		kgo.RecordHeader{Key: kafkaproducer.HeaderGwSeq, Value: []byte(itoa(seq))},
	)
	return rec
}

// qualityRecord 质量事件记录（§5.3 {point_id, gateway_id, ts, event, detail}）。
func qualityRecord(gatewayID, tenantID string, pointID int64, ts time.Time, event string, detail map[string]any, traceID string) *kgo.Record {
	val, err := json.Marshal(qualityPayload{
		PointID: pointID, GatewayID: gatewayID, TS: ts.UTC().Format(time.RFC3339Nano),
		Event: event, Detail: detail,
	})
	if err != nil {
		val = []byte("{}")
	}
	rec := &kgo.Record{Topic: kafkaproducer.TopicTelemetryQuality, Key: []byte(gatewayID), Value: val}
	rec.Headers = append(rec.Headers,
		kgo.RecordHeader{Key: kafkaproducer.HeaderTenantID, Value: []byte(tenantID)},
		kgo.RecordHeader{Key: kafkaproducer.HeaderTraceID, Value: []byte(traceID)},
	)
	return rec
}

// produceQualityEvents 批量直发质量事件（stale_set / stale_cleared，产生于写入
// 成功后的状态推进与后台扫描；单次 Produce 等待全部 promise 落定——聚合面，
// DAT-202 D-38：消除逐条同步 produce 的串行阻塞窗口；DAT-211 S5：失败计数按
// 实际失败条数，部分失败不高估）。
func (p *Pipeline) produceQualityEvents(recs []*kgo.Record) {
	if len(recs) == 0 {
		return
	}
	pctx, cancel := context.WithTimeout(context.Background(), p.cfg.ProduceTimeout)
	defer cancel()
	if failed, err := p.kafka.Produce(pctx, recs...); err != nil {
		// 部分失败按实际失败数计（DAT-211 S5）：整批计在高估面——批内单条
		// 失败曾虚增为 N。
		p.met.QualityProduceFailures.Add(float64(failed))
		p.log.Warn("quality events produce failed", "count", len(recs), "failed", failed, "err", err.Error())
	}
}

// observeSeq §3.2：seq 缺口只记指标与 WARN（QoS1 丢包兜底检测），不拒收；
// 断网补传的旧 seq 不告警——只看前进方向缺口。
func (p *Pipeline) observeSeq(gatewayID string, seq int64) {
	p.mu.Lock()
	last, seen := p.lastSeq[gatewayID]
	if !seen || seq > last {
		p.lastSeq[gatewayID] = seq
	}
	p.mu.Unlock()
	if seen && seq > last+1 {
		p.met.SeqGaps.Inc()
		p.log.Warn("gateway seq gap", "gateway_id", gatewayID, "last_seq", last, "seq", seq)
	}
}

// markUnitFirst unit_unconverted 首次出现判定（§5.3）；成功转换后重置。
func (p *Pipeline) markUnitFirst(pointID int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.unitSeen[pointID] {
		return false
	}
	p.unitSeen[pointID] = true
	return true
}

func (p *Pipeline) unitConverted(pointID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.unitSeen, pointID)
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
