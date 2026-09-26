//go:build integration

// 集成测试（IMPL-5 验收）：自起 deploy/docker-compose.dev.yml 独立栈
// （EMQX+Kafka+TSDB+PG，环境隔离纪律：禁止复用宿主机/其他项目既有服务）。
// 栈由 scripts/integration.sh 拉起并注入环境变量；直接 `go test`（无环境）跳过。
//
// 覆盖：
//   - 端到端正路径：MQTT → TSDB telemetry upsert + Kafka raw/quality/DLQ 可消费；
//   - §3.2 负路径端到端（decode 单测之外的全链路死信出口验证）；
//   - 断网补传乱序：旧 ts 入库 + backfill 位 + 同网关同分区保序（§8）；
//   - 背压演练：打满 BUFFER_MAX_ROWS 不丢数不 OOM，恢复自动排水（§7.3）；
//   - stale 扫描质量事件（§6.3）。
package it

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/huang1234321/thermio-ingest/internal/dlq"
	"github.com/huang1234321/thermio-ingest/internal/kafkaproducer"
	"github.com/huang1234321/thermio-ingest/internal/metrics"
	"github.com/huang1234321/thermio-ingest/internal/mqtt"
	"github.com/huang1234321/thermio-ingest/internal/pipeline"
	"github.com/huang1234321/thermio-ingest/internal/points"
	"github.com/huang1234321/thermio-ingest/internal/quality"
	"github.com/huang1234321/thermio-ingest/internal/tsdb"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ── 环境与工具 ───────────────────────────────────────────────────────────

var itEnv = &env{}

type env struct {
	emqxURL  string
	kafka    []string
	pgDSN    string
	tsdbDSN  string
	pg       *pgxpool.Pool
	tsdbPool *pgxpool.Pool
	runID    string
}

func TestMain(m *testing.M) {
	e := &env{
		emqxURL: os.Getenv("IT_EMQX_URL"),
		pgDSN:   os.Getenv("IT_PG_DSN"),
		tsdbDSN: os.Getenv("IT_TSDB_DSN"),
		runID:   fmt.Sprintf("IT%d%04d", time.Now().UnixNano()%1e7, rand.Intn(9999)),
	}
	if b := os.Getenv("IT_KAFKA_BROKERS"); b != "" {
		e.kafka = strings.Split(b, ",")
	}
	if e.emqxURL == "" || e.pgDSN == "" || e.tsdbDSN == "" || len(e.kafka) == 0 {
		fmt.Println("SKIP: 集成测试环境变量不全（IT_EMQX_URL/IT_KAFKA_BROKERS/IT_PG_DSN/IT_TSDB_DSN），由 scripts/integration.sh 注入")
		os.Exit(0)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var err error
	if e.pg, err = pgxpool.New(ctx, e.pgDSN); err != nil {
		fmt.Println("SKIP: pg 连接失败:", err)
		os.Exit(0)
	}
	if e.tsdbPool, err = pgxpool.New(ctx, e.tsdbDSN); err != nil {
		e.pg.Close()
		fmt.Println("SKIP: tsdb 连接失败:", err)
		os.Exit(0)
	}
	*itEnv = *e
	code := m.Run()
	e.pg.Close()
	e.tsdbPool.Close()
	os.Exit(code)
}

// gwClient 假网关（paho QoS1 发布器）。
type gwClient struct {
	cl paho.Client
	id string
}

func newGWClient(t *testing.T, clientID string) *gwClient {
	t.Helper()
	opts := paho.NewClientOptions().
		AddBroker(itEnv.emqxURL).
		SetClientID(clientID).
		SetAutoReconnect(true)
	cl := paho.NewClient(opts)
	if tok := cl.Connect(); tok.WaitTimeout(10*time.Second) && tok.Error() != nil {
		t.Fatalf("gw client %s connect: %v", clientID, tok.Error())
	}
	t.Cleanup(func() { cl.Disconnect(200) })
	return &gwClient{cl: cl, id: clientID}
}

// publish 发布即返回（不等 PUBACK——背压演练时 EMQX 侧排队，等待会拖慢灌数）。
func (g *gwClient) publish(t *testing.T, body string) {
	t.Helper()
	g.cl.Publish(fmt.Sprintf(mqtt.TopicUpDataFmt, g.id), 1, false, []byte(body))
}

func envJSON(points, gwSerial string, seq int64, sentAt time.Time) string {
	return fmt.Sprintf(`{"msg_type":"telemetry_batch","ver":1,"gw":%q,"seq":%d,"sent_at":%q,"points":[%s]}`,
		gwSerial, seq, sentAt.Format(time.RFC3339Nano), points)
}

// seedPoint 测试点位种子（测试专用 PG schema，见 testdata/pg_schema.sql）。
type seedPoint struct {
	name         string
	unitR, unitS string
	min, max     *float64
	staleS       int32
	status       string
}

func (e *env) seed(t *testing.T, clientID, serial string, pts []seedPoint) (gwID string, ids map[string]int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var tenantID, buildingID string
	if err := e.pg.QueryRow(ctx, `INSERT INTO tenant (name) VALUES ($1) RETURNING id::text`,
		e.runID+"-tenant").Scan(&tenantID); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if err := e.pg.QueryRow(ctx, `INSERT INTO building (tenant_id, name) VALUES ($1,$2) RETURNING id::text`,
		tenantID, e.runID+"-bldg").Scan(&buildingID); err != nil {
		t.Fatalf("seed building: %v", err)
	}
	if err := e.pg.QueryRow(ctx,
		`INSERT INTO gateway (tenant_id, building_id, name, serial, mqtt_client_id) VALUES ($1,$2,$3,$4,$5) RETURNING id::text`,
		tenantID, buildingID, e.runID+"-gw", serial, clientID).Scan(&gwID); err != nil {
		t.Fatalf("seed gateway: %v", err)
	}
	ids = map[string]int64{}
	for _, p := range pts {
		st := p.status
		if st == "" {
			st = "active"
		}
		var id int64
		if err := e.pg.QueryRow(ctx, `
			INSERT INTO point (tenant_id, building_id, source_type, gateway_id, raw_name,
			                   unit_raw, unit_std, valid_range_min, valid_range_max, stale_timeout_s, status)
			VALUES ($1,$2,'mqtt_gateway',$3,$4,NULLIF($5,''),NULLIF($6,''),$7,$8,COALESCE($9,300),$10)
			RETURNING id`,
			tenantID, buildingID, gwID, p.name, p.unitR, p.unitS, p.min, p.max,
			nilIfZero(p.staleS), st).Scan(&id); err != nil {
			t.Fatalf("seed point %s: %v", p.name, err)
		}
		ids[p.name] = id
	}
	return gwID, ids
}

func nilIfZero(v int32) any {
	if v == 0 {
		return nil
	}
	return v
}

func (e *env) gwIDOf(t *testing.T, clientID string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var id string
	if err := e.pg.QueryRow(ctx, `SELECT id::text FROM gateway WHERE mqtt_client_id=$1`, clientID).Scan(&id); err != nil {
		t.Fatalf("gw id: %v", err)
	}
	return id
}

// kafkaConsumer 从 earliest 消费指定 topic（franz-go，无 group 直取分区）。
func newConsumer(t *testing.T, topics ...string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(itEnv.kafka...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// poll 拉取记录直到 cond 为真或超时；返回累计记录。
func poll(t *testing.T, cl *kgo.Client, timeout time.Duration, cond func([]*kgo.Record) bool) []*kgo.Record {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var all []*kgo.Record
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		fetches := cl.PollRecords(ctx, 500)
		cancel()
		fetches.EachError(func(_ string, _ int32, err error) { t.Logf("consume err: %v", err) })
		iter := fetches.RecordIter()
		for !iter.Done() {
			all = append(all, iter.Next())
		}
		if cond(all) {
			return all
		}
	}
	return all
}

// ── 测试装置：in-process pipeline ─────────────────────────────────────────

func itLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// startPipeline 起 in-process 管线 + MQTT 源；t.Cleanup 按 §12 序收尾
// （网关 client 断开 → 管线排水 → producer 关闭）。
func startPipeline(t *testing.T, mutators ...func(*pipeline.Config)) (*pipeline.Pipeline, *pipeline.Stats) {
	t.Helper()
	cache := points.NewCache(points.NewPGLoader(itEnv.pg), itLogger())
	ctx0, c0 := context.WithTimeout(context.Background(), 15*time.Second)
	defer c0()
	if err := cache.Initial(ctx0); err != nil {
		t.Fatalf("cache initial: %v", err)
	}
	writer := tsdb.NewWriter(itEnv.tsdbPool)
	kp, err := kafkaproducer.NewBrokers(itEnv.kafka)
	if err != nil {
		t.Fatalf("kafka: %v", err)
	}
	cfg := pipeline.Config{
		BatchMaxRows:       200,
		BatchFlushInterval: 150 * time.Millisecond,
		BufferMaxRows:      10000,
		TsSkewThreshold:    10 * time.Minute,
		BackfillThreshold:  10 * time.Minute,
		RetentionCutoff:    2 * 365 * 24 * time.Hour,
		StaleScanInterval:  500 * time.Millisecond,
		ProduceTimeout:     10 * time.Second,
	}
	for _, m := range mutators {
		m(&cfg)
	}
	stats := pipeline.NewStats()
	g1, g2, g3, g4 := stats.Gauge()
	met := metrics.New(g1, g2, g3, g4)
	ppl := pipeline.NewWithStats(cfg, cache, writer, kp, met, stats, itLogger())

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { ppl.Run(runCtx); close(done) }()

	src := mqtt.NewSource(ppl.IntakeChan())
	if err := src.Start(runCtx, itEnv.emqxURL, "svc-ingest-"+itEnv.runID, "", "", "ingest"); err != nil {
		cancel()
		t.Fatalf("mqtt start: %v", err)
	}
	ppl.SetSource(src)

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(60 * time.Second):
			t.Error("pipeline 收尾超时")
		}
		kp.Close()
	})
	return ppl, stats
}

type tsdbRow struct {
	TS      time.Time
	Value   *float64
	Text    *string
	Quality int16
}

func (e *env) tsdbRows(t *testing.T, pointID int64, since time.Time) []tsdbRow {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := e.tsdbPool.Query(ctx,
		`SELECT ts, value, value_text, quality FROM telemetry WHERE point_id=$1 AND ts >= $2 ORDER BY ts`,
		pointID, since)
	if err != nil {
		t.Fatalf("query telemetry: %v", err)
	}
	defer rows.Close()
	var out []tsdbRow
	for rows.Next() {
		var r tsdbRow
		if err := rows.Scan(&r.TS, &r.Value, &r.Text, &r.Quality); err != nil {
			t.Fatalf("scan telemetry: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// ── 用例 1：端到端正路径 + §3.2 负路径全链路死信 ───────────────────────────

func TestEndToEndHappyAndFieldRules(t *testing.T) {
	e := itEnv
	gwClientID := e.runID + "-GW1"
	serial := e.runID + "-SER1"
	gwID, ids := e.seed(t, gwClientID, serial, []seedPoint{
		{name: "TEMP_F", unitR: "degF", unitS: "degC"},
		{name: "PRESS_PSI", unitR: "psi", unitS: "kPa"},
		{name: "FLOW_M3H", unitR: "m³/h", unitS: "L/s"},
		{name: "PUMP_TEXT"},
		{name: "CFM_X", unitR: "CFM", unitS: "L/s"},
		{name: "DEAD_PT", status: "disabled"},
	})
	startPipeline(t)
	gw := newGWClient(t, gwClientID)

	since := time.Now().UTC().Add(-time.Minute)
	now := time.Now().UTC()
	ts := now.Format(time.RFC3339Nano)

	// 正路径：三类单位换算 + 枚态 + 未收录单位。
	gw.publish(t, envJSON(fmt.Sprintf(
		`{"name":"TEMP_F","value":77,"ts":%q},`+
			`{"name":"PRESS_PSI","value":14.5,"ts":%q},`+
			`{"name":"FLOW_M3H","value":3.6,"ts":%q},`+
			`{"name":"PUMP_TEXT","value_text":"running","ts":%q},`+
			`{"name":"CFM_X","value":100,"ts":%q}`,
		ts, ts, ts, ts, ts), serial, 1, now))

	// §3.2 负路径全家桶。
	gw.publish(t, `{"msg_type":"what","ver":1}`)                                             // UNKNOWN_MSG_TYPE
	gw.publish(t, `{"msg_type":"telemetry_batch","ver":9}`)                                  // UNSUPPORTED_VER
	gw.publish(t, `not-json`)                                                                // MALFORMED_JSON
	gw.publish(t, envJSON(`{"name":"TEMP_F","value":1,"ts":"`+ts+`"}`, "SER-OTHER", 5, now)) // GW_MISMATCH
	gw.publish(t, envJSON(`{"name":"NOT_REG","value":1,"ts":"`+ts+`"}`, serial, 6, now))     // UNREGISTERED_POINT
	gw.publish(t, envJSON(`{"name":"DEAD_PT","value":1,"ts":"`+ts+`"}`, serial, 7, now))     // POINT_INACTIVE
	gw.publish(t, envJSON(`{"name":"TEMP_F","value":1,"ts":"garbage"}`, serial, 8, now))     // TS_INVALID
	var sb strings.Builder                                                                   // 501 点 → PAYLOAD_TOO_LARGE
	for i := 0; i < 501; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf(`{"name":"TEMP_F","value":1,"ts":%q}`, ts))
	}
	gw.publish(t, envJSON(sb.String(), serial, 9, now))
	ghost := newGWClient(t, e.runID+"-GHOST") // 未知 clientid → UNKNOWN_GATEWAY
	ghost.publish(t, envJSON(`{"name":"TEMP_F","value":1,"ts":"`+ts+`"}`, serial, 1, now))

	// TSDB：单位归一数值抽样（§13 口径：degF→degC / psi→kPa / m³/h→L/s）。
	waitFor(t, 10*time.Second, func() bool { return len(e.tsdbRows(t, ids["TEMP_F"], since)) >= 1 })
	checkVal(t, e, ids["TEMP_F"], since, 25.0, "degF→degC")
	checkVal(t, e, ids["PRESS_PSI"], since, 14.5*6.894757293168, "psi→kPa")
	checkVal(t, e, ids["FLOW_M3H"], since, 1.0, "m³/h→L/s")
	rows := e.tsdbRows(t, ids["CFM_X"], since) // 未收录：原值 + bit4
	if len(rows) != 1 || rows[0].Value == nil || *rows[0].Value != 100 || rows[0].Quality&16 == 0 {
		t.Errorf("CFM_X 应原值入库 + bit4: %+v", rows)
	}
	rows = e.tsdbRows(t, ids["PUMP_TEXT"], since) // 枚态直通
	if len(rows) != 1 || rows[0].Text == nil || *rows[0].Text != "running" {
		t.Errorf("PUMP_TEXT 直通失败: %+v", rows)
	}

	// Kafka raw：归一后的行可消费，key=gateway_id。
	rc := newConsumer(t, kafkaproducer.TopicTelemetryRaw)
	recs := poll(t, rc, 10*time.Second, func(rs []*kgo.Record) bool { return countKey(rs, gwID) >= 5 })
	sawNormalized := false
	for _, r := range recs {
		if string(r.Key) != gwID {
			continue
		}
		var rp struct {
			PointID   int64    `json:"point_id"`
			GatewayID string   `json:"gateway_id"`
			TenantID  string   `json:"tenant_id"`
			Value     *float64 `json:"value"`
		}
		if json.Unmarshal(r.Value, &rp) == nil && rp.PointID == ids["TEMP_F"] &&
			rp.GatewayID == gwID && rp.TenantID != "" && rp.Value != nil && *rp.Value == 25 {
			sawNormalized = true
		}
	}
	if !sawNormalized {
		t.Error("raw topic 未见到归一后的 TEMP_F=25（值契约/key/tenant）")
	}

	// quality 事件：unit_unconverted 首次出现。
	qc := newConsumer(t, kafkaproducer.TopicTelemetryQuality)
	qrecs := poll(t, qc, 10*time.Second, func(rs []*kgo.Record) bool {
		return countEvent(rs, quality.EventUnitUnconverted, ids["CFM_X"]) >= 1
	})
	if countEvent(qrecs, quality.EventUnitUnconverted, ids["CFM_X"]) < 1 {
		t.Error("缺 unit_unconverted 质量事件")
	}

	// DLQ：全部 reason 到齐（按本网关 topic 过滤，隔离其他用例）。
	dc := newConsumer(t, kafkaproducer.TopicDLQ)
	want := []string{
		dlq.UnknownMsgType, dlq.UnsupportedVer, dlq.MalformedJSON, dlq.GWMismatch,
		dlq.UnregisteredPoint, dlq.PointInactive, dlq.TSInvalid, dlq.PayloadTooLarge,
		dlq.UnknownGateway, dlq.UnitUnconverted,
	}
	filterClients := []string{gwClientID, e.runID + "-GHOST"}
	drecs := poll(t, dc, 15*time.Second, func(rs []*kgo.Record) bool { return hasAllReasons(rs, want, filterClients...) })
	got := reasonSet(drecs, filterClients...)
	for _, w := range want {
		if !got[w] {
			t.Errorf("DLQ 缺 reason %s（got %v）", w, got)
		}
	}
}

// ── 用例 2：断网补传乱序 + 同网关同分区保序（§8） ─────────────────────────

func TestBackfillOutOfOrderAndPartitionOrdering(t *testing.T) {
	e := itEnv
	gwClientID := e.runID + "-GW2"
	serial := e.runID + "-SER2"
	_, ids := e.seed(t, gwClientID, serial, []seedPoint{{name: "SEQ_TEMP", unitR: "degC", unitS: "degC"}})
	startPipeline(t)
	gw := newGWClient(t, gwClientID)
	gwID := e.gwIDOf(t, gwClientID)
	since := time.Now().UTC().Add(-3 * time.Hour)

	// 断网补传：2 小时前起的 10 个样本倒序发布（乱序 + seq 缺口），再补一条实时。
	base := time.Now().UTC().Add(-2 * time.Hour)
	for i := 9; i >= 0; i-- {
		seq := int64(100 + i*2) // 缺口：0,2,4…
		ts := base.Add(time.Duration(i) * time.Minute)
		val := float64(20 + i)
		gw.publish(t, envJSON(fmt.Sprintf(`{"name":"SEQ_TEMP","value":%v,"ts":%q}`,
			val, ts.Format(time.RFC3339Nano)), serial, seq, time.Now().UTC()))
	}
	fresh := time.Now().UTC()
	gw.publish(t, envJSON(fmt.Sprintf(`{"name":"SEQ_TEMP","value":99,"ts":%q}`,
		fresh.Format(time.RFC3339Nano)), serial, 500, fresh))

	// TSDB：10 补传 + 1 实时；补传行带 bit8；乱序后值与 ts 一一对应。
	waitFor(t, 10*time.Second, func() bool { return len(e.tsdbRows(t, ids["SEQ_TEMP"], since)) >= 11 })
	rows := e.tsdbRows(t, ids["SEQ_TEMP"], since)
	if len(rows) != 11 {
		t.Fatalf("行数 = %d, want 11", len(rows))
	}
	backfill := 0
	for _, r := range rows {
		if r.TS.Before(time.Now().UTC().Add(-time.Hour)) {
			if r.Quality&256 == 0 {
				t.Errorf("补传行缺 backfill 位: %+v", r)
			}
			backfill++
		}
	}
	if backfill != 10 {
		t.Errorf("补传行 = %d, want 10", backfill)
	}
	// 乱序入库正确性：ts_i 的值 = 20+i。
	for i, r := range rows[:10] {
		wantTS := base.Add(time.Duration(i) * time.Minute)
		if !r.TS.Equal(wantTS) || r.Value == nil || *r.Value != float64(20+i) {
			t.Errorf("补传行 %d 错位: ts=%v want %v, val=%v want %v",
				i, r.TS, wantTS, r.Value, float64(20+i))
		}
	}

	// Kafka 同网关同分区保序：同 key 全部落同一分区（murmur2 hash）。
	rc := newConsumer(t, kafkaproducer.TopicTelemetryRaw)
	recs := poll(t, rc, 10*time.Second, func(rs []*kgo.Record) bool { return countKey(rs, gwID) >= 11 })
	partitions := map[int32]int{}
	for _, r := range recs {
		if string(r.Key) == gwID {
			partitions[r.Partition]++
		}
	}
	if got := partitions[gwIDPartition(recs, gwID)]; got < 11 {
		t.Fatalf("raw 记录不足: %v", partitions)
	}
	if len(partitions) != 1 {
		t.Errorf("同网关记录跨 %d 个分区（key=%s），want 1（§7.4 同分区保序）: %v", len(partitions), gwID, partitions)
	}
}

// ── 用例 3：背压演练——打满 BUFFER_MAX_ROWS 不丢数不 OOM（§7.3） ─────────────

func TestBackpressureNoLossNoOOM(t *testing.T) {
	e := itEnv
	gwClientID := e.runID + "-GW3"
	serial := e.runID + "-SER3"
	_, ids := e.seed(t, gwClientID, serial, []seedPoint{{name: "BP_POINT", unitR: "kW", unitS: "kW"}})

	const bufMax = 200 // 小阈值便于打满
	const msgs = 300   // 300 行 > 200 上限 → 背压

	gated := &gatedWriter{inner: tsdb.NewWriter(e.tsdbPool)}
	gated.hold()
	cache := points.NewCache(points.NewPGLoader(e.pg), itLogger())
	ctx0, c0 := context.WithTimeout(context.Background(), 15*time.Second)
	defer c0()
	if err := cache.Initial(ctx0); err != nil {
		t.Fatalf("cache initial: %v", err)
	}
	kp, err := kafkaproducer.NewBrokers(e.kafka)
	if err != nil {
		t.Fatalf("kafka: %v", err)
	}
	stats := pipeline.NewStats()
	g1, g2, g3, g4 := stats.Gauge()
	met := metrics.New(g1, g2, g3, g4)
	ppl := pipeline.NewWithStats(pipeline.Config{
		BatchMaxRows:       100,
		BatchFlushInterval: 100 * time.Millisecond,
		BufferMaxRows:      bufMax,
		TsSkewThreshold:    10 * time.Minute,
		BackfillThreshold:  10 * time.Minute,
		RetentionCutoff:    2 * 365 * 24 * time.Hour,
		StaleScanInterval:  500 * time.Millisecond,
	}, cache, gated, kp, met, stats, itLogger())
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { ppl.Run(runCtx); close(done) }()
	src := mqtt.NewSource(ppl.IntakeChan())
	if err := src.Start(runCtx, e.emqxURL, "svc-bp-"+e.runID, "", "", "ingest"); err != nil {
		cancel()
		t.Fatalf("mqtt: %v", err)
	}
	ppl.SetSource(src)

	gw := newGWClient(t, gwClientID)
	now := time.Now().UTC()
	for i := 1; i <= msgs; i++ { // 发布不等 PUBACK：EMQX 会话排队，背压不阻断网关
		gw.publish(t, envJSON(fmt.Sprintf(`{"name":"BP_POINT","value":%d,"ts":%q}`,
			i, now.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano)), serial, int64(i), now))
	}

	// 背压生效：缓冲打满（Stats 采样周期 5s）。
	waitFor(t, 60*time.Second, func() bool { return stats.BackpressureActiveNow() })

	// 不 OOM：堆在界内（结构性上界 = 有界队列 + 有界行门）。
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	if heapMB := ms.HeapInuse >> 20; heapMB > 1024 {
		t.Errorf("背压期间堆 %dMB 超 1GB 预算", heapMB)
	}

	// 恢复 → 自动排水 → 一行不丢。
	gated.release()
	since := now.Add(-time.Minute)
	waitFor(t, 90*time.Second, func() bool { return len(e.tsdbRows(t, ids["BP_POINT"], since)) >= msgs })
	rows := e.tsdbRows(t, ids["BP_POINT"], since)
	if len(rows) < msgs {
		t.Fatalf("排水后行数 = %d, want ≥%d（不丢数）", len(rows), msgs)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("背压演练后收尾超时")
	}
	kp.Close()
}

// ── 用例 4：stale 扫描质量事件（§6.3） ────────────────────────────────────

func TestStaleScanQualityEvents(t *testing.T) {
	e := itEnv
	gwClientID := e.runID + "-GW4"
	serial := e.runID + "-SER4"
	_, ids := e.seed(t, gwClientID, serial, []seedPoint{{name: "STALE_PT", staleS: 1}})
	startPipeline(t) // StaleScanInterval=500ms
	gw := newGWClient(t, gwClientID)

	qc := newConsumer(t, kafkaproducer.TopicTelemetryQuality)

	now := time.Now().UTC()
	gw.publish(t, envJSON(fmt.Sprintf(`{"name":"STALE_PT","value":1,"ts":%q}`,
		now.Format(time.RFC3339Nano)), serial, 1, now))

	// stale_set（首轮扫描不判 = §6.3 基线；1s 超时 + 500ms 扫描）。
	qrecs := poll(t, qc, 20*time.Second, func(rs []*kgo.Record) bool {
		return countEvent(rs, quality.EventStaleSet, ids["STALE_PT"]) >= 1
	})
	if countEvent(qrecs, quality.EventStaleSet, ids["STALE_PT"]) < 1 {
		t.Fatal("缺 stale_set 事件")
	}

	// 恢复写入：fresh 样本 → 行携带 bit3 + stale_cleared 事件。
	fresh := time.Now().UTC()
	gw.publish(t, envJSON(fmt.Sprintf(`{"name":"STALE_PT","value":2,"ts":%q}`,
		fresh.Format(time.RFC3339Nano)), serial, 2, fresh))
	qrecs = poll(t, qc, 20*time.Second, func(rs []*kgo.Record) bool {
		return countEvent(rs, quality.EventStaleCleared, ids["STALE_PT"]) >= 1
	})
	if countEvent(qrecs, quality.EventStaleCleared, ids["STALE_PT"]) < 1 {
		t.Fatal("缺 stale_cleared 事件")
	}
	since := fresh.Add(-time.Minute)
	waitFor(t, 10*time.Second, func() bool { return len(e.tsdbRows(t, ids["STALE_PT"], since)) >= 1 })
	rows := e.tsdbRows(t, ids["STALE_PT"], since)
	if len(rows) == 0 || rows[len(rows)-1].Quality&8 == 0 {
		t.Errorf("恢复写入应携带 bit3（stale 期间的写入，§6.3）: %+v", rows)
	}
}

// ── 小工具 ────────────────────────────────────────────────────────────────

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func checkVal(t *testing.T, e *env, pointID int64, since time.Time, want float64, label string) {
	t.Helper()
	rows := e.tsdbRows(t, pointID, since)
	if len(rows) == 0 {
		t.Fatalf("%s: 无行", label)
	}
	if v := rows[0].Value; v == nil || diff(*v, want) > 1e-9 {
		t.Errorf("%s = %v, want %v", label, v, want)
	}
}

func diff(a, b float64) float64 {
	if d := a - b; d < 0 {
		return -d
	} else {
		return d
	}
}

func countKey(rs []*kgo.Record, key string) int {
	n := 0
	for _, r := range rs {
		if string(r.Key) == key {
			n++
		}
	}
	return n
}

func gwIDPartition(rs []*kgo.Record, gwID string) int32 {
	for _, r := range rs {
		if string(r.Key) == gwID {
			return r.Partition
		}
	}
	return -1
}

func countEvent(rs []*kgo.Record, event string, pointID int64) int {
	n := 0
	for _, r := range rs {
		var p struct {
			PointID int64  `json:"point_id"`
			Event   string `json:"event"`
		}
		if json.Unmarshal(r.Value, &p) == nil && p.Event == event && p.PointID == pointID {
			n++
		}
	}
	return n
}

// reasonSet 按本用例相关 clientid（GW1 与 GHOST）过滤 topic，隔离同 run 其他用例。
func reasonSet(rs []*kgo.Record, clients ...string) map[string]bool {
	out := map[string]bool{}
	for _, r := range rs {
		var m dlq.Message
		if json.Unmarshal(r.Value, &m) != nil {
			continue
		}
		for _, c := range clients {
			if strings.Contains(m.Topic, c) {
				out[m.Reason] = true
				break
			}
		}
	}
	return out
}

func hasAllReasons(rs []*kgo.Record, want []string, clients ...string) bool {
	got := reasonSet(rs, clients...)
	for _, w := range want {
		if !got[w] {
			return false
		}
	}
	return true
}

// gatedWriter 可阻塞的 TSDB writer 包装（背压演练：卡住批量写触发全链路背压）。
type gatedWriter struct {
	inner *tsdb.Writer
	ch    chan struct{}
}

func (g *gatedWriter) hold()    { g.ch = make(chan struct{}) }
func (g *gatedWriter) release() { close(g.ch) }

func (g *gatedWriter) WriteBatch(ctx context.Context, rows []tsdb.Row) error {
	if g.ch != nil {
		<-g.ch
	}
	return g.inner.WriteBatch(ctx, rows)
}
func (g *gatedWriter) Close() { g.inner.Close() }
