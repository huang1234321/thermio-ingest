package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/huang1234321/thermio-ingest/internal/dlq"
	"github.com/huang1234321/thermio-ingest/internal/kafkaproducer"
	"github.com/huang1234321/thermio-ingest/internal/metrics"
	"github.com/huang1234321/thermio-ingest/internal/mqtt"
	"github.com/huang1234321/thermio-ingest/internal/points"
	"github.com/huang1234321/thermio-ingest/internal/quality"
	"github.com/huang1234321/thermio-ingest/internal/tsdb"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// ── 测试底座 ─────────────────────────────────────────────────────────────

type fakeWriter struct {
	mu       sync.Mutex
	batches  [][]tsdb.Row
	gate     chan struct{} // 非 nil 时每批写前阻塞（背压演练）
	failNext int           // >0 时接下来 N 批失败
	hang     bool          // true：写挂起直至 ctx 结束（模拟 TSDB 无响应，DAT-121）
}

func (f *fakeWriter) WriteBatch(ctx context.Context, rows []tsdb.Row) error {
	if f.hang {
		<-ctx.Done()
		return ctx.Err()
	}
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return fmt.Errorf("injected failure")
	}
	cp := make([]tsdb.Row, len(rows))
	copy(cp, rows)
	f.batches = append(f.batches, cp)
	return nil
}
func (f *fakeWriter) Close() {}
func (f *fakeWriter) rowsAll() []tsdb.Row {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []tsdb.Row
	for _, b := range f.batches {
		out = append(out, b...)
	}
	return out
}

type fakeKafka struct {
	mu                sync.Mutex
	records           []*kgo.Record
	failDirectQuality bool // true：单条质量事件直发报错（stale 路径，DAT-121）
}

func (f *fakeKafka) Produce(ctx context.Context, recs ...*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// 只模拟 stale 路径的单条直发失败：批 produce（raw/事件/死信）不受影响。
	if f.failDirectQuality && len(recs) == 1 && recs[0].Topic == kafkaproducer.TopicTelemetryQuality {
		return fmt.Errorf("injected quality produce failure")
	}
	f.records = append(f.records, recs...)
	return nil
}
func (f *fakeKafka) Flush(ctx context.Context) error { return nil }
func (f *fakeKafka) Close()                          {}
func (f *fakeKafka) byTopic(topic string) []*kgo.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*kgo.Record
	for _, r := range f.records {
		if r.Topic == topic {
			out = append(out, r)
		}
	}
	return out
}

type memLoader struct {
	gws map[string]points.GatewayConfig
	pts map[int64]points.PointConfig
}

func (l *memLoader) LoadGateways(ctx context.Context) (map[string]points.GatewayConfig, error) {
	out := map[string]points.GatewayConfig{}
	for k, v := range l.gws {
		out[k] = v
	}
	return out, nil
}
func (l *memLoader) LoadPointsSince(ctx context.Context, since time.Time) (map[int64]points.PointConfig, error) {
	out := map[int64]points.PointConfig{}
	for k, v := range l.pts {
		out[k] = v
	}
	return out, nil
}
func (l *memLoader) MaxUpdatedAt(ctx context.Context) (time.Time, error) { return time.Now(), nil }

func f64(v float64) *float64 { return &v }

// newTestPipeline 标准测试装置：1 网关 + 常用点位。
func newTestPipeline(t *testing.T, cfg Config) (*Pipeline, *points.Cache, *fakeWriter, *fakeKafka, *memLoader) {
	t.Helper()
	loader := &memLoader{
		gws: map[string]points.GatewayConfig{
			"GW-A": {GatewayID: "gid-a", TenantID: "tid-1", Serial: "SER-A"},
		},
		pts: map[int64]points.PointConfig{
			1: {PointID: 1, GatewayID: "gid-a", TenantID: "tid-1", RawName: "TEMP_F", UnitRaw: "degF", UnitStd: "degC", StaleTimeoutS: 300, Status: "active"},
			2: {PointID: 2, GatewayID: "gid-a", TenantID: "tid-1", RawName: "PUMP_STATUS", Status: "active"},
			3: {PointID: 3, GatewayID: "gid-a", TenantID: "tid-1", RawName: "NO_UNIT", UnitRaw: "", UnitStd: "", Status: "active"},
			4: {PointID: 4, GatewayID: "gid-a", TenantID: "tid-1", RawName: "CFM_FLOW", UnitRaw: "CFM", UnitStd: "L/s", Status: "active"},
			5: {PointID: 5, GatewayID: "gid-a", TenantID: "tid-1", RawName: "RANGED", UnitRaw: "degC", UnitStd: "degC", ValidRangeMin: f64(0), ValidRangeMax: f64(100), Status: "active"},
			6: {PointID: 6, GatewayID: "gid-a", TenantID: "tid-1", RawName: "DISABLED", Status: "disabled"},
		},
	}
	cache := points.NewCache(loader, slog.New(slog.DiscardHandler))
	if err := cache.Initial(context.Background()); err != nil {
		t.Fatalf("cache initial: %v", err)
	}
	fw := &fakeWriter{}
	fk := &fakeKafka{}
	cfg.BatchFlushInterval = 20 * time.Millisecond
	cfg.StaleScanInterval = 10 * time.Millisecond
	if cfg.TsSkewThreshold == 0 {
		cfg.TsSkewThreshold = 10 * time.Minute
	}
	if cfg.BackfillThreshold == 0 {
		cfg.BackfillThreshold = 10 * time.Minute
	}
	if cfg.RetentionCutoff == 0 {
		cfg.RetentionCutoff = 2 * 365 * 24 * time.Hour
	}
	if cfg.ProduceTimeout == 0 {
		cfg.ProduceTimeout = 5 * time.Second
	}
	if cfg.BufferMaxRows == 0 {
		cfg.BufferMaxRows = 1000
	}
	if cfg.BatchMaxRows == 0 {
		cfg.BatchMaxRows = 100
	}
	noGauge := func() float64 { return 0 }
	p := New(cfg, cache, fw, fk, metrics.New(noGauge, noGauge, noGauge, noGauge), slog.New(slog.DiscardHandler))
	p.nowFunc = func() time.Time { return testNow }
	return p, cache, fw, fk, loader
}

// testNow 固定时钟：与 msg() 的 sent_at（14:03:05+08:00）相距 5s，不触发 bit5；
// ts 相对 testNow 生成，保证 backfill/retention 判定确定性。
var testNow = time.Date(2026, 9, 26, 6, 3, 10, 0, time.UTC)

// tsFrom 距 testNow 的偏移（负值 = 过去）。
func tsFrom(d time.Duration) string { return testNow.Add(d).Format(time.RFC3339) }

// runAndWait 启动管线、执行 inject、优雅关停并等待收尾完成。
func runAndWait(t *testing.T, p *Pipeline, inject func(intake chan<- mqtt.Inbound)) *Pipeline {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	inject(p.Intake())
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("pipeline 未在 10s 内收尾")
	}
	return p
}

func msg(clientID string, seq int64, ptsJSON string) mqtt.Inbound {
	payload := fmt.Sprintf(`{"msg_type":"telemetry_batch","ver":1,"gw":"SER-A","seq":%d,"sent_at":"2026-09-26T14:03:05+08:00","points":[%s]}`, seq, ptsJSON)
	return mqtt.Inbound{
		Topic:      "thermio/gw/" + clientID + "/up/data",
		Payload:    []byte(payload),
		ReceivedAt: testNow,
		Ack:        func() {},
	}
}

// waitRows 轮询直到 writer 收到 ≥n 行或超时。
func waitRows(t *testing.T, fw *fakeWriter, n int) []tsdb.Row {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := fw.rowsAll(); len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待 %d 行超时（got %d）", n, len(fw.rowsAll()))
	return nil
}

// ── §7.1 写入顺序与端到端行为 ────────────────────────────────────────────

// 正路径：单位归一（degF→degC）、bit 位（device_bad/null/backfill/out_of_range/
// unit_unconverted）、raw 记录契约、ack 全到。
func TestPipelineHappyPathAndBits(t *testing.T) {
	p, _, fw, fk, _ := newTestPipeline(t, Config{})
	var acks int32
	var mu sync.Mutex
	ackFn := func() { mu.Lock(); acks++; mu.Unlock() }

	m1 := msg("GW-A", 1,
		`{"name":"TEMP_F","value":77,"ts":"2026-09-26T14:03:04+08:00"}`) // degF→degC=25
	m1.Ack = ackFn
	// 旧 ts（>10min）→ backfill 位。
	oldTS := tsFrom(-time.Hour)
	m2 := mqtt.Inbound{Topic: "thermio/gw/GW-A/up/data", Payload: []byte(fmt.Sprintf(
		`{"msg_type":"telemetry_batch","ver":1,"gw":"SER-A","seq":2,"sent_at":"2026-09-26T14:03:05+08:00","points":[
			{"name":"PUMP_STATUS","value_text":"running","ts":"%s"},
			{"name":"NO_UNIT","value":42,"ts":"%[1]s"},
			{"name":"CFM_FLOW","value":100,"ts":"%[1]s"},
			{"name":"RANGED","value":200,"ts":"%[1]s"},
			{"name":"TEMP_F","value":null,"quality":"bad","ts":"%[1]s"}
		]}`, oldTS)), ReceivedAt: testNow, Ack: ackFn}

	runAndWait(t, p, func(intake chan<- mqtt.Inbound) {
		intake <- m1
		intake <- m2
	})

	rows := fw.rowsAll()
	if len(rows) != 6 {
		t.Fatalf("总行数 = %d, want 6: %+v", len(rows), rows)
	}
	byPoint := map[int64][]tsdb.Row{}
	for _, r := range rows {
		byPoint[r.PointID] = append(byPoint[r.PointID], r)
	}
	// TEMP_F fresh: 77 degF → 25 degC, quality 0（按 ts 定位——批内已排序，
	// 不依赖插入次序）。
	var rFresh, rNull *tsdb.Row
	for i := range byPoint[1] {
		row := byPoint[1][i]
		if row.Value == nil {
			rNull = &byPoint[1][i]
		} else {
			rFresh = &byPoint[1][i]
		}
	}
	if rFresh == nil || *rFresh.Value != 25 {
		t.Errorf("TEMP_F 归一 = %v, want 25", rFresh)
	}
	if rFresh.Quality != 0 {
		t.Errorf("TEMP_F fresh quality = %d (bit5? %v), want 0（sent_at 距 testNow 5s，不 skew）", rFresh.Quality, rFresh.Quality&quality.TsSkew)
	}
	// PUMP_STATUS backfill bit8。
	if q := byPoint[2][0].Quality; q&quality.Backfill == 0 {
		t.Errorf("PUMP_STATUS 应带 backfill 位: %d", q)
	}
	// NO_UNIT 空单位直通。
	if v := byPoint[3][0].Value; v == nil || *v != 42 {
		t.Errorf("NO_UNIT 直通 = %v, want 42", v)
	}
	// CFM_FLOW 未收录：原值 100 + bit4。
	r4 := byPoint[4][0]
	if r4.Value == nil || *r4.Value != 100 || r4.Quality&quality.UnitUnconverted == 0 {
		t.Errorf("CFM_FLOW 应原值入库 + bit4: %+v", r4)
	}
	// RANGED 超上限：bit0。
	if q := byPoint[5][0].Quality; q&quality.OutOfRange == 0 {
		t.Errorf("RANGED 应带 out_of_range 位: %d", q)
	}
	// TEMP_F null + bad：bit1|bit2（+bit8 旧 ts）。
	if rNull == nil || rNull.Quality&quality.DeviceBad == 0 || rNull.Quality&quality.NullValue == 0 {
		t.Errorf("TEMP_F null+bad 应 bit1|bit2: %+v", rNull)
	}

	// Kafka raw 契约：6 行 6 记录，key=gateway_id，headers tenant/trace/seq。
	raws := fk.byTopic(kafkaproducer.TopicTelemetryRaw)
	if len(raws) != 6 {
		t.Fatalf("raw 记录 = %d, want 6", len(raws))
	}
	var rp rawPayload
	if err := json.Unmarshal(raws[0].Value, &rp); err != nil {
		t.Fatalf("raw 值非法 JSON: %v", err)
	}
	if rp.GatewayID != "gid-a" || rp.TenantID != "tid-1" || rp.PointID == 0 || rp.TS == "" {
		t.Errorf("raw 值契约字段缺失: %+v", rp)
	}
	if string(raws[0].Key) != "gid-a" {
		t.Errorf("raw key = %s, want gid-a（同网关同分区）", raws[0].Key)
	}
	hdrs := map[string]string{}
	for _, h := range raws[0].Headers {
		hdrs[h.Key] = string(h.Value)
	}
	if hdrs["tenant_id"] != "tid-1" || hdrs["trace_id"] == "" || hdrs["gw_seq"] == "" {
		t.Errorf("raw headers 缺失: %v", hdrs)
	}

	// 质量事件：unit_unconverted 首次出现 1 条。
	evs := fk.byTopic(kafkaproducer.TopicTelemetryQuality)
	if len(evs) != 1 {
		t.Fatalf("质量事件 = %d, want 1（unit_unconverted 首次）: %+v", len(evs), evs)
	}
	var qp qualityPayload
	if err := json.Unmarshal(evs[0].Value, &qp); err != nil || qp.Event != "unit_unconverted" {
		t.Errorf("质量事件值错误: %v %v", err, qp)
	}

	mu.Lock()
	defer mu.Unlock()
	if acks != 2 {
		t.Errorf("ack 数 = %d, want 2", acks)
	}
}

// 信封级失败整消息 DLQ 后 ack：未知 msg_type / GW_MISMATCH / UNKNOWN_GATEWAY。
func TestPipelineEnvelopeDLQ(t *testing.T) {
	p, _, fw, fk, _ := newTestPipeline(t, Config{})
	runAndWait(t, p, func(intake chan<- mqtt.Inbound) {
		intake <- mqtt.Inbound{Topic: "thermio/gw/GW-A/up/data", Payload: []byte(`{"msg_type":"other","ver":1}`), Ack: func() {}}
		// payload gw 指向别家 serial（未注册 serial）→ GW_MISMATCH。
		intake <- mqtt.Inbound{Topic: "thermio/gw/GW-A/up/data", Payload: []byte(
			`{"msg_type":"telemetry_batch","ver":1,"gw":"SER-OTHER","seq":1,"sent_at":"2026-09-26T14:03:05+08:00","points":[{"name":"TEMP_F","value":1,"ts":"2026-09-26T14:03:04+08:00"}]}`), Ack: func() {}}
		// topic clientid 未注册 → UNKNOWN_GATEWAY。
		intake <- mqtt.Inbound{Topic: "thermio/gw/GHOST/up/data", Payload: []byte(
			`{"msg_type":"telemetry_batch","ver":1,"gw":"SER-A","seq":1,"sent_at":"2026-09-26T14:03:05+08:00","points":[{"name":"TEMP_F","value":1,"ts":"2026-09-26T14:03:04+08:00"}]}`), Ack: func() {}}
	})
	if got := len(fw.rowsAll()); got != 0 {
		t.Errorf("三条死信消息不应有行入库: %d", got)
	}
	dlqs := fk.byTopic(kafkaproducer.TopicDLQ)
	reasons := map[string]int{}
	for _, r := range dlqs {
		var m dlq.Message
		if err := json.Unmarshal(r.Value, &m); err != nil {
			t.Fatalf("dlq 值非法: %v", err)
		}
		reasons[m.Reason]++
		if m.PayloadB64 == "" || m.Topic == "" {
			t.Errorf("dlq 契约字段缺失: %+v", m)
		}
	}
	for _, want := range []string{dlq.UnknownMsgType, dlq.GWMismatch, dlq.UnknownGateway} {
		if reasons[want] == 0 {
			t.Errorf("缺少死信 reason %s（got %v）", want, reasons)
		}
	}
}

// 点级失败混合路径：UNREGISTERED_POINT / POINT_INACTIVE / TS_INVALID 与合法点共存。
func TestPipelinePointLevelDLQ(t *testing.T) {
	p, _, fw, fk, _ := newTestPipeline(t, Config{})
	runAndWait(t, p, func(intake chan<- mqtt.Inbound) {
		intake <- msg("GW-A", 1, `
			{"name":"GHOST_POINT","value":1,"ts":"2026-09-26T14:03:04+08:00"},
			{"name":"DISABLED","value":1,"ts":"2026-09-26T14:03:04+08:00"},
			{"name":"TEMP_F","value":32,"ts":"2026-09-26T14:03:04+08:00"},
			{"name":"NO_UNIT","value":5}
		`)
	})
	rows := fw.rowsAll()
	if len(rows) != 1 || rows[0].PointID != 1 {
		t.Fatalf("只有 TEMP_F 应入库: %+v", rows)
	}
	dlqs := fk.byTopic(kafkaproducer.TopicDLQ)
	reasons := map[string]bool{}
	for _, r := range dlqs {
		var m dlq.Message
		_ = json.Unmarshal(r.Value, &m)
		reasons[m.Reason] = true
		if m.PointName == "" {
			t.Errorf("点级死信应带 point_name: %+v", m)
		}
	}
	// NO_UNIT 的 ts 缺失在 decode 层是 TS_INVALID；GHOST→UNREGISTERED；DISABLED→POINT_INACTIVE。
	for _, want := range []string{dlq.UnregisteredPoint, dlq.PointInactive, dlq.TSInvalid} {
		if !reasons[want] {
			t.Errorf("缺少 %s（got %v）", want, reasons)
		}
	}
}

// TS_BEYOND_RETENTION：ts < now − 2 年不写库只死信（§7.1）。
func TestPipelineBeyondRetention(t *testing.T) {
	p, _, fw, fk, _ := newTestPipeline(t, Config{})
	runAndWait(t, p, func(intake chan<- mqtt.Inbound) {
		intake <- msg("GW-A", 1, fmt.Sprintf(
			`{"name":"TEMP_F","value":1,"ts":"%s"}`, tsFrom(-3*365*24*time.Hour)))
	})
	if got := len(fw.rowsAll()); got != 0 {
		t.Errorf("超期 ts 不应写库: %d", got)
	}
	dlqs := fk.byTopic(kafkaproducer.TopicDLQ)
	if len(dlqs) != 1 {
		t.Fatalf("死信 = %d, want 1", len(dlqs))
	}
	var m dlq.Message
	_ = json.Unmarshal(dlqs[0].Value, &m)
	if m.Reason != dlq.TSBeyondRetention {
		t.Errorf("reason = %s, want TS_BEYOND_RETENTION", m.Reason)
	}
}

// TSDB 失败路径：整批 DLQ TSDB_WRITE_FAILED、失败计数、仍 PUBACK、raw 不发。
func TestPipelineTSDBFailure(t *testing.T) {
	p, _, fw, fk, _ := newTestPipeline(t, Config{})
	fw.failNext = 99 // writer 是指针共享：注入持续失败
	var acked int32
	var mu sync.Mutex
	m := msg("GW-A", 1, `{"name":"TEMP_F","value":77,"ts":"2026-09-26T14:03:04+08:00"}`)
	m.Ack = func() { mu.Lock(); acked++; mu.Unlock() }
	runAndWait(t, p, func(intake chan<- mqtt.Inbound) { intake <- m })

	if got := len(fk.byTopic(kafkaproducer.TopicTelemetryRaw)); got != 0 {
		t.Errorf("TSDB 失败不应发 raw 流: %d", got)
	}
	dlqs := fk.byTopic(kafkaproducer.TopicDLQ)
	if len(dlqs) != 1 {
		t.Fatalf("死信 = %d, want 1（TSDB_WRITE_FAILED）", len(dlqs))
	}
	var dm dlq.Message
	if err := json.Unmarshal(dlqs[0].Value, &dm); err != nil || dm.Reason != dlq.TSDBWriteFailed {
		t.Errorf("死信 reason 错误: %v %+v", err, dm)
	}
	mu.Lock()
	defer mu.Unlock()
	if acked != 1 {
		t.Errorf("失败批仍应 PUBACK（防重投风暴）: %d", acked)
	}
}

// 批内排序：(point_id, ts) 升序进 TSDB（§5.1 阶段 6）。
func TestPipelineBatchSorted(t *testing.T) {
	p, _, fw, _, _ := newTestPipeline(t, Config{BatchMaxRows: 50})
	now := tsFrom(0)
	var pts []string
	for _, name := range []string{"TEMP_F", "PUMP_STATUS", "NO_UNIT", "RANGED", "CFM_FLOW"} {
		pts = append(pts, fmt.Sprintf(`{"name":%q,"value":1,"ts":"%s"}`, name, now))
	}
	runAndWait(t, p, func(intake chan<- mqtt.Inbound) { intake <- msg("GW-A", 1, strings.Join(pts, ",")) })
	rows := fw.rowsAll()
	for i := 1; i < len(rows); i++ {
		if rows[i].PointID < rows[i-1].PointID {
			t.Errorf("批内未按 point_id 排序: %v", rows)
			break
		}
	}
}

// ── §7.3 背压 ────────────────────────────────────────────────────────────

// 背压演练（单元级）：BUFFER_MAX_ROWS 打满后 intake 停止消化（worker 阻塞在
// rowGate），TSDB 恢复后自动排水、一行不丢。
func TestPipelineBackpressureBoundedNoLoss(t *testing.T) {
	const bufMax = 50
	p, _, fw, _, _ := newTestPipeline(t, Config{BufferMaxRows: bufMax, BatchMaxRows: 20})
	fw.gate = make(chan struct{}) // 阻塞 TSDB 写

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()

	// 灌满：单点消息逐条占满 rowGate（50 行上限）。
	now := tsFrom(0)
	intake := p.Intake()
	intake <- mqtt.Inbound{Topic: "thermio/gw/GW-A/up/data", Payload: []byte(
		`{"msg_type":"telemetry_batch","ver":1,"gw":"SER-A","seq":1,"sent_at":"2026-09-26T14:03:05+08:00","points":[{"name":"TEMP_F","value":1,"ts":"` + now + `"}]}`), Ack: func() {}}

	// 灌 60 条单点消息（每条 1 行 + gate 满 → 只进 intake 不消化）。
	sent := 0
	for i := 0; i < 60; i++ {
		m := mqtt.Inbound{Topic: "thermio/gw/GW-A/up/data", Payload: []byte(
			`{"msg_type":"telemetry_batch","ver":1,"gw":"SER-A","seq":` + fmt.Sprint(100+i) + `,"sent_at":"2026-09-26T14:03:05+08:00","points":[{"name":"NO_UNIT","value":` + fmt.Sprint(i) + `,"ts":"` + now + `"}]}`), Ack: func() {}}
		select {
		case intake <- m:
			sent++
		case <-time.After(500 * time.Millisecond):
			// intake（10k）不会满；worker 阻塞在 rowGate 是预期——intake 仍可接收。
		}
	}
	// 等待 worker 把 rowGate 占满（背压生效）。
	deadline := time.Now().Add(5 * time.Second)
	for len(p.rowGate) < bufMax && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(p.rowGate) < bufMax {
		t.Fatalf("rowGate 未打满: %d/%d", len(p.rowGate), bufMax)
	}
	// 背压期间：总“在制”行数受 bufMax 上界（内存有界 = 不 OOM 的结构性保证）。
	// 恢复：打开闸门，全部排水。
	close(fw.gate)
	waitRows(t, fw, 61)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("排水后未收尾")
	}
	all := fw.rowsAll()
	if len(all) != 61 {
		t.Errorf("排水后总行数 = %d, want 61（不丢数）", len(all))
	}
}

// ── §12 优雅退出 ─────────────────────────────────────────────────────────

// 优雅退出：关停时缓冲中的消息全部落库并 ack（冲缓冲语义）。
func TestPipelineGracefulDrain(t *testing.T) {
	p, _, fw, _, _ := newTestPipeline(t, Config{BatchFlushInterval: 10 * time.Second}) // 不靠 ticker
	var acks int32
	var mu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()

	intake := p.Intake()
	now := tsFrom(0)
	for i := 0; i < 5; i++ {
		m := mqtt.Inbound{Topic: "thermio/gw/GW-A/up/data", Payload: []byte(
			`{"msg_type":"telemetry_batch","ver":1,"gw":"SER-A","seq":` + fmt.Sprint(i) + `,"sent_at":"2026-09-26T14:03:05+08:00","points":[{"name":"TEMP_F","value":` + fmt.Sprint(i) + `,"ts":"` + now + `"}]}`),
			Ack: func() { mu.Lock(); acks++; mu.Unlock() }}
		intake <- m
	}
	// 等 workers 消化进 decoded/writer 缓冲（flush interval 10s → 停留在 pending）。
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("优雅退出超时")
	}
	if got := len(fw.rowsAll()); got != 5 {
		t.Errorf("final flush 后行数 = %d, want 5", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if acks != 5 {
		t.Errorf("ack = %d, want 5", acks)
	}
}

// §10 RED-R 入口速率与 §3.2 seq 缺口计数的防回归断言（评审阻塞项修复配套）：
// 每条进入 processMessage 的消息计 1（含死信路径）；seq 前进方向缺口计 1。
// DecodeWorkers=1 保证处理顺序确定（多 worker 下 seq 观测序不定）。
func TestMetricsMQTTMessagesAndSeqGaps(t *testing.T) {
	p, _, fw, _, _ := newTestPipeline(t, Config{DecodeWorkers: 1})
	valid := func(seq int64) mqtt.Inbound {
		return msg("GW-A", seq, `{"name":"TEMP_F","value":77,"ts":"2026-09-26T14:03:04+08:00"}`)
	}
	bad := mqtt.Inbound{ // 信封级死信（observeSeq 之前返回，不计缺口）
		Topic: "thermio/gw/GW-A/up/data", Payload: []byte(`{"msg_type":"what","ver":1}`), Ack: func() {},
	}
	runAndWait(t, p, func(intake chan<- mqtt.Inbound) {
		intake <- valid(1)
		intake <- valid(7) // 缺口 2..6 → SeqGaps +1
		intake <- bad
		intake <- valid(9) // 缺口 8 → +1
	})
	if got := testutil.ToFloat64(p.met.MQTTMessages); got != 4 {
		t.Errorf("ingest_mqtt_messages_total = %v, want 4（每条进入处理的消息计 1，含死信路径）", got)
	}
	if got := testutil.ToFloat64(p.met.SeqGaps); got != 2 {
		t.Errorf("ingest_gw_seq_gaps_total = %v, want 2（缺口 2..6 与 8）", got)
	}
	if got := len(fw.rowsAll()); got != 3 {
		t.Errorf("合法行 = %d, want 3", got)
	}
}

// ── DAT-121 健壮性跟进 ───────────────────────────────────────────────────

// syncBuf 并发安全的日志缓冲（writer/scanner 多 goroutine 写）。
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// DAT-121-2：TSDB 批写挂起（无响应、非快速失败）受写超时上界约束——超时走
// 既有 TSDB_WRITE_FAILED 死信路径，PUBACK 照发（防重投风暴），raw 不产
// （真相源未落）。无超时的旧实现会让 writer 永久阻塞、本测试 10s 收尾超时。
func TestPipelineTSDBWriteTimeout(t *testing.T) {
	p, _, fw, fk, _ := newTestPipeline(t, Config{TsdbWriteTimeout: 100 * time.Millisecond})
	fw.hang = true
	var acked int
	var mu sync.Mutex
	m := msg("GW-A", 1, `{"name":"TEMP_F","value":77,"ts":"2026-09-26T14:03:04+08:00"}`)
	m.Ack = func() { mu.Lock(); acked++; mu.Unlock() }
	runAndWait(t, p, func(intake chan<- mqtt.Inbound) { intake <- m })

	if got := testutil.ToFloat64(p.met.TSDBFailures); got != 1 {
		t.Errorf("ingest_tsdb_write_failures_total = %v, want 1", got)
	}
	if got := len(fk.byTopic(kafkaproducer.TopicTelemetryRaw)); got != 0 {
		t.Errorf("超时批不应发 raw 流: %d", got)
	}
	dlqs := fk.byTopic(kafkaproducer.TopicDLQ)
	if len(dlqs) != 1 {
		t.Fatalf("死信 = %d, want 1（TSDB_WRITE_FAILED 兜底）", len(dlqs))
	}
	var dm dlq.Message
	if err := json.Unmarshal(dlqs[0].Value, &dm); err != nil || dm.Reason != dlq.TSDBWriteFailed {
		t.Errorf("死信 reason 错误: %v %+v", err, dm)
	}
	mu.Lock()
	defer mu.Unlock()
	if acked != 1 {
		t.Errorf("超时批仍应 PUBACK: %d", acked)
	}
}

// runStaleSequence 演练 stale 全周期：老样本（ts 距 testNow 6min > 300s 超时）
// 等扫描置位发 stale_set；新鲜样本解除发 stale_cleared。返回收尾后的管线。
func runStaleSequence(t *testing.T, p *Pipeline, fw *fakeWriter) *Pipeline {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	inject := p.Intake()

	// 老样本 → 扫描置位（首轮 warmup，第二轮判定）。
	inject <- mqtt.Inbound{Topic: "thermio/gw/GW-A/up/data", Payload: []byte(
		`{"msg_type":"telemetry_batch","ver":1,"gw":"SER-A","seq":1,"sent_at":"2026-09-26T14:03:05+08:00","points":[{"name":"TEMP_F","value":1,"ts":"` + tsFrom(-6*time.Minute) + `"}]}`), Ack: func() {}}
	deadline := time.Now().Add(5 * time.Second)
	for !p.stale.IsStale(1) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !p.stale.IsStale(1) {
		t.Fatal("等待 stale 置位超时")
	}

	// 新鲜样本 → flush 落库后解除（stale_cleared）。
	inject <- msg("GW-A", 2, `{"name":"TEMP_F","value":2,"ts":"`+tsFrom(0)+`"}`)
	waitRows(t, fw, 2)

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("stale 演练未在 10s 内收尾")
	}
	return p
}

// DAT-121-3 正路径：stale_set / stale_cleared 事件确实直发 quality topic。
func TestStaleEventsProduced(t *testing.T) {
	p, _, fw, fk, _ := newTestPipeline(t, Config{})
	runStaleSequence(t, p, fw)
	evs := fk.byTopic(kafkaproducer.TopicTelemetryQuality)
	got := map[string]int{}
	for _, r := range evs {
		var qp qualityPayload
		if err := json.Unmarshal(r.Value, &qp); err != nil {
			t.Fatalf("质量事件值非法 JSON: %v", err)
		}
		got[qp.Event]++
	}
	if got[quality.EventStaleSet] != 1 || got[quality.EventStaleCleared] != 1 {
		t.Errorf("stale 事件 = %v, want stale_set=1 stale_cleared=1", got)
	}
	if c := testutil.ToFloat64(p.met.QualityProduceFailures); c != 0 {
		t.Errorf("正常路径失败计数 = %v, want 0", c)
	}
}

// DAT-121-3：stale 质量事件 produce 失败不吞——WARN 日志 + 失败指标
// （CODE-ST-01）；数据面（raw/落库）不受影响。
func TestStaleEventProduceFailureLoggedAndCounted(t *testing.T) {
	p, _, fw, fk, _ := newTestPipeline(t, Config{})
	buf := &syncBuf{}
	p.log = slog.New(slog.NewTextHandler(buf, nil)) // WARN 断言用
	fk.failDirectQuality = true
	runStaleSequence(t, p, fw)

	if c := testutil.ToFloat64(p.met.QualityProduceFailures); c != 2 {
		t.Errorf("ingest_quality_produce_failures_total = %v, want 2（stale_set + stale_cleared）", c)
	}
	logs := buf.String()
	if !strings.Contains(logs, "quality event produce failed") {
		t.Errorf("WARN 日志缺失，got: %s", logs)
	}
	// 数据面不受牵连：两行照落、raw 照发。
	if got := len(fw.rowsAll()); got != 2 {
		t.Errorf("落库行 = %d, want 2", got)
	}
	if got := len(fk.byTopic(kafkaproducer.TopicTelemetryRaw)); got != 2 {
		t.Errorf("raw 记录 = %d, want 2", got)
	}
}
