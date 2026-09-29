package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// gatewaySim 单网关模拟：点位集采样 → 信封组装 → 故障注入 → QoS1 发布
// （或入 outbox）。断网窗口与发布失败统一走 outbox：按原始 ts/seq FIFO 重放
// （ADR-003 硬指标 3：补传不改采集时间戳；ingest.md §8）。
type gatewaySim struct {
	idx       int
	profile   *Profile
	brokerURL string
	clientID  string
	serial    string
	username  string
	password  string

	io     *mqttIO
	inj    *Injector
	down   *DownlinkState
	logger *slog.Logger
	stats  *runStats

	numPoints []pointDef
	spPoints  []pointDef // 设定值点位（跟随寄存器采样）
	rng       *rand.Rand

	seq        int64
	start      time.Time
	sampleTime time.Time // 模拟采样时刻（首轮 = start）

	ob *outbox

	// offline 窗口状态机（按 after_s 排序消费）。
	windows   []OfflineWindow
	winCursor int
	forcedOff bool
}

// pointDef 点位定义与数值生成参数。
type pointDef struct {
	name       string
	kind       string // "numeric" | "enum"
	unit       string
	base       float64
	amplitude  float64
	noise      float64
	phase      float64
	enumValues []string
}

// outboxEntry 缓存的一条待发消息（信封已组装、故障已注入、字节已定格——
// 重放不改任何字段，ts/seq 即组装时的原始值）。
type outboxEntry struct {
	topic   string
	payload []byte
	rows    int
}

// outbox 有界 FIFO 缓存：超限丢最旧（网关环形缓存语义），计数进摘要。
type outbox struct {
	mu      sync.Mutex
	entries []outboxEntry
	rows    int
	maxRows int
	dropped int64
	notify  chan struct{}
}

func newOutbox(maxRows int) *outbox {
	return &outbox{maxRows: maxRows, notify: make(chan struct{}, 1)}
}

func (b *outbox) add(e outboxEntry) {
	b.mu.Lock()
	b.entries = append(b.entries, e)
	b.rows += e.rows
	for b.rows > b.maxRows && len(b.entries) > 1 {
		b.rows -= b.entries[0].rows
		b.entries = b.entries[1:]
		b.dropped++
	}
	b.mu.Unlock()
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

func (b *outbox) peek() (outboxEntry, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.entries) == 0 {
		return outboxEntry{}, false
	}
	return b.entries[0], true
}

func (b *outbox) pop() {
	b.mu.Lock()
	if len(b.entries) > 0 {
		b.rows -= b.entries[0].rows
		b.entries = b.entries[1:]
	}
	b.mu.Unlock()
}

func (b *outbox) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.entries)
}

// runStats 进程级运行统计（多网关并发累加，退出时落摘要）。
type runStats struct {
	mu                sync.Mutex
	messages          int64
	points            int64
	bytes             int64
	publishErrors     int64
	bufferDroppedRows int64
	offlineWindows    int64
	backfillMessages  int64
	writeCmds         int64
	readCmds          int64
	faults            map[string]int64
	startedAt         time.Time
	endedAt           time.Time
}

func newRunStats() *runStats {
	return &runStats{faults: map[string]int64{}, startedAt: time.Now()}
}

func (s *runStats) addMessages(n int64) { s.mu.Lock(); s.messages += n; s.mu.Unlock() }
func (s *runStats) addPoints(n int64)   { s.mu.Lock(); s.points += n; s.mu.Unlock() }
func (s *runStats) addBytes(n int64)    { s.mu.Lock(); s.bytes += n; s.mu.Unlock() }
func (s *runStats) addPublishError()    { s.mu.Lock(); s.publishErrors++; s.mu.Unlock() }
func (s *runStats) addOfflineWindow()   { s.mu.Lock(); s.offlineWindows++; s.mu.Unlock() }
func (s *runStats) addBackfill(n int64) { s.mu.Lock(); s.backfillMessages += n; s.mu.Unlock() }
func (s *runStats) addWriteCmd()        { s.mu.Lock(); s.writeCmds++; s.mu.Unlock() }
func (s *runStats) addReadCmd()         { s.mu.Lock(); s.readCmds++; s.mu.Unlock() }

func (s *runStats) mergeFaults(m map[string]int64) {
	s.mu.Lock()
	for k, v := range m {
		s.faults[k] += v
	}
	s.mu.Unlock()
}

// finish 盖结束时间戳（摘要 duration 由此计算）。
func (s *runStats) finish() {
	s.mu.Lock()
	s.endedAt = time.Now()
	s.mu.Unlock()
}

func (s *runStats) addDroppedRows(n int64) { s.mu.Lock(); s.bufferDroppedRows += n; s.mu.Unlock() }

// snapshot 返回摘要（含 faults 深拷贝）。
func (s *runStats) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := make(map[string]int64, len(s.faults))
	for k, v := range s.faults {
		f[k] = v
	}
	return map[string]any{
		"started_at":          s.startedAt.Format(time.RFC3339),
		"ended_at":            s.endedAt.Format(time.RFC3339),
		"duration_s":          s.endedAt.Sub(s.startedAt).Truncate(time.Second).Seconds(),
		"messages_published":  s.messages,
		"points_published":    s.points,
		"bytes_published":     s.bytes,
		"publish_errors":      s.publishErrors,
		"buffer_dropped_rows": s.bufferDroppedRows,
		"offline_windows":     s.offlineWindows,
		"backfill_messages":   s.backfillMessages,
		"write_cmds":          s.writeCmds,
		"read_cmds":           s.readCmds,
		"faults_injected":     f,
	}
}

// newGatewaySim 构造第 idx 个网关（种子隔离，故障可复现）。
func newGatewaySim(p *Profile, idx int, brokerURL, password string, stats *runStats, logger *slog.Logger) *gatewaySim {
	serial := fmt.Sprintf("%s%03d", p.Gateways.SerialPrefix, idx+1)
	clientID := fmt.Sprintf("%s%03d", p.Gateways.ClientIDPrefix, idx+1)
	username := p.Gateways.UsernameTemplate
	username = replaceAll(username, "{serial}", serial)
	username = replaceAll(username, "{tenant}", p.Gateways.Tenant)

	g := &gatewaySim{
		idx:       idx,
		profile:   p,
		brokerURL: brokerURL,
		clientID:  clientID,
		serial:    serial,
		username:  username,
		password:  password,
		inj:       NewInjector(p.Faults, p.Seed+int64(idx)*1000),
		down: NewDownlinkState(p.Downlink, p.Points.WriteMin, p.Points.WriteMax,
			p.Seed+int64(idx)*1000+7, logger.With("gw", serial)),
		logger: logger.With("gw", serial, "clientid", clientID),
		stats:  stats,
		rng:    rand.New(rand.NewSource(p.Seed + int64(idx)*1000 + 3)),
		ob:     newOutbox(p.Offline.BufferMaxRows),
	}
	g.buildPoints()
	g.windows = append([]OfflineWindow(nil), p.Offline.Windows...)
	sort.Slice(g.windows, func(i, j int) bool { return g.windows[i].AfterS < g.windows[j].AfterS })
	return g
}

// replaceAll 最小模板替换（避免为两个占位符引入依赖）。
func replaceAll(s, old, new string) string {
	out := ""
	for {
		i := indexOf(s, old)
		if i < 0 {
			return out + s
		}
		out += s[:i] + new
		s = s[i+len(old):]
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// buildPoints 生成点位集：数值量（单位轮转）+ 枚态量 + 设定值点位。
// Offset 起始序号与 Overrides 点名级覆盖在此时套用（profile.go DAT-165 增量）。
func (g *gatewaySim) buildPoints() {
	p := g.profile.Points
	numeric := int(math.Round(float64(p.Count) * p.NumericRatio))
	for i := 0; i < p.Count; i++ {
		idx := p.Offset + i
		name := fmt.Sprintf("%s%04d", telemetryPrefix(g.idx), idx)
		if i < numeric {
			def := pointDef{
				name: name, kind: "numeric",
				unit:      p.Units[i%len(p.Units)],
				base:      p.Base + float64(idx%7),
				amplitude: p.Amplitude,
				noise:     p.Noise,
				phase:     float64(idx) * 0.37,
			}
			g.applyOverride(name, &def, false)
			g.numPoints = append(g.numPoints, def)
			continue
		}
		def := pointDef{ // 枚态也进遥测集
			name: name, kind: "enum", enumValues: append([]string(nil), p.EnumValues...),
		}
		g.applyOverride(name, &def, true)
		g.numPoints = append(g.numPoints, def)
	}
	for i := 0; i < p.Setpoints; i++ {
		name := fmt.Sprintf("SIM_SP_%04d", i)
		def := pointDef{
			name: name, kind: "numeric", unit: "degC",
			base: p.SetpointBase, noise: p.Noise, phase: float64(i) * 0.11,
		}
		g.applyOverride(name, &def, false)
		g.spPoints = append(g.spPoints, def)
		g.down.InitSetpoint(name, p.SetpointBase)
	}
}

// applyOverride 将 points.overrides[name] 套到点位定义上（未指出的字段沿用全局值；
// kind 切换允许把轮转出的数值位改枚态或反向——冷源场景的机组状态差异需要）。
func (g *gatewaySim) applyOverride(name string, def *pointDef, isEnum bool) {
	o, ok := g.profile.Points.Overrides[name]
	if !ok {
		return
	}
	switch o.Kind {
	case "numeric":
		def.kind = "numeric"
	case "enum":
		def.kind = "enum"
	}
	if def.kind == "enum" {
		if len(o.EnumValues) > 0 {
			def.enumValues = append([]string(nil), o.EnumValues...)
		}
		return
	}
	if o.Unit != "" {
		def.unit = o.Unit
	}
	if o.Base != nil {
		def.base = *o.Base
	}
	if o.Amplitude != nil {
		def.amplitude = *o.Amplitude
	}
	if o.Noise != nil {
		def.noise = *o.Noise
	}
}

// telemetryPrefix 网关序号编进点位名前缀：多网关场景下点表天然隔离，
// UNREGISTERED_POINT 语义不被网关间串扰。
func telemetryPrefix(idx int) string {
	if idx == 0 {
		return "SIM_"
	}
	return fmt.Sprintf("SIM%d_", idx+1)
}

// Run 驱动单网关：连接 → （可选）历史回放 → 采样循环 → 断网窗口状态机。
func (g *gatewaySim) Run(ctx context.Context) error {
	g.start = time.Now()
	g.sampleTime = g.start

	io := newMQTTIO(g.brokerURL, g.clientID, g.username, g.password, g.logger)
	io.onState = func(online bool) { g.ob.notifyDrain() }
	g.io = io

	if err := g.connect(ctx); err != nil {
		return err
	}
	go g.drainLoop(ctx)

	if g.profile.Backfill.Enabled {
		g.replayHistory(ctx)
	}

	// 首轮立即采样（短时运行场景不等第一个完整周期），此后按周期推进。
	g.stepOfflineWindows()
	g.sampleCycle(ctx, g.sampleTime, false)

	interval := time.Duration(g.profile.SampleIntervalS) * time.Second
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			g.flushBestEffort()
			return nil
		case <-tick.C:
			g.sampleTime = g.sampleTime.Add(interval)
			g.stepOfflineWindows()
			g.sampleCycle(ctx, g.sampleTime, false)
		}
	}
}

// connect（重）连 broker。
func (g *gatewaySim) connect(ctx context.Context) error {
	return g.io.connect(ctx, g.onDownMessage)
}

// onDownMessage 下行消息入口：write_cmd/read_cmd → up/event 应答。
func (g *gatewaySim) onDownMessage(topic string, payload []byte) {
	if !g.profile.Downlink.Enabled {
		g.logger.Warn("downlink disabled; message ignored", "topic", topic)
		return
	}
	var probe struct {
		MsgType string `json:"msg_type"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		g.logger.Warn("downlink: unparseable", "topic", topic)
		return
	}
	switch probe.MsgType {
	case msgTypeWriteCmd:
		g.stats.addWriteCmd()
	case msgTypeReadCmd:
		g.stats.addReadCmd()
	}
	now := time.Now()
	ack := g.down.HandleMessage(payload, g.clientID, now) // gw 字段=clientid（§4.3 与 topic 一致）
	if ack == nil {
		return
	}
	if err := g.io.publish(TopicUpEvent(g.clientID), ack); err != nil {
		// 应答不走 outbox（指令时效性优先，失败即丢并计数——与遥测语义不同）。
		g.stats.addPublishError()
		g.logger.Warn("downlink ack publish failed", "err", err.Error())
	}
}

// stepOfflineWindows 断网窗口状态机：进入窗口主动断开；窗口结束重连。
// 断网期间采样继续（进 outbox），恢复后按原始 ts/seq 顺序重放。
func (g *gatewaySim) stepOfflineWindows() {
	now := time.Since(g.start)
	if g.forcedOff {
		w := g.windows[g.winCursor-1]
		if int(now.Seconds()) >= w.AfterS+w.DurationS {
			g.forcedOff = false
			g.logger.Info("offline window ended; reconnecting",
				"buffered_messages", g.ob.len())
			if err := g.connect(context.Background()); err != nil {
				g.logger.Error("reconnect failed; will keep buffering", "err", err.Error())
			}
		}
		return
	}
	if g.winCursor < len(g.windows) {
		w := g.windows[g.winCursor]
		if int(now.Seconds()) >= w.AfterS {
			g.winCursor++
			g.forcedOff = true
			g.stats.addOfflineWindow()
			g.logger.Warn("simulated network outage; buffering samples",
				"duration_s", w.DurationS)
			g.io.disconnect()
		}
	}
}

// sampleCycle 一轮采样：全点位取值 → 分批组装 → 注入 → 发布/缓存。
// backfill=true 时样本时间用历史时刻（补传回放）。
func (g *gatewaySim) sampleCycle(ctx context.Context, sampleAt time.Time, backfill bool) {
	now := time.Now()
	pts := make([]Point, 0, len(g.numPoints)+len(g.spPoints))
	for _, pd := range g.numPoints {
		pts = append(pts, g.samplePoint(pd, sampleAt))
	}
	for _, pd := range g.spPoints {
		if v, ok := g.down.RegisterValue(pd.name); ok {
			pd.base = v // 设定值遥测跟随设备寄存器（写生效后在 up/data 可见）
		}
		pts = append(pts, g.samplePoint(pd, sampleAt))
	}

	batch := g.profile.BatchPoints
	for start := 0; start < len(pts); start += batch {
		end := start + batch
		if end > len(pts) {
			end = len(pts)
		}
		g.publishBatch(pts[start:end], now, backfill)
	}
}

// samplePoint 单点采样（数值：基线（或寄存器）+正弦+噪声；枚态：轮转取值）。
func (g *gatewaySim) samplePoint(pd pointDef, at time.Time) Point {
	p := Point{Name: pd.name, TS: FormatTS(at), Quality: "good"}
	switch pd.kind {
	case "enum":
		v := pd.enumValues[int(at.Unix())%len(pd.enumValues)]
		p.ValueText = &v
	default:
		v := pd.base + pd.amplitude*math.Sin(float64(at.Unix())*0.001+pd.phase) + g.noise(g.rng, pd.noise)
		p.Value = &v
		p.Unit = pd.unit
	}
	return p
}

// noise 单次均匀噪声，幅度 ±amp（模拟用途足够，无高斯必要）。
func (g *gatewaySim) noise(r *rand.Rand, amp float64) float64 {
	if amp <= 0 {
		return 0
	}
	return (r.Float64() - 0.5) * 2 * amp
}

// publishBatch 组装一条信封并发布（故障注入后 payload 定格；发布失败入 outbox）。
func (g *gatewaySim) publishBatch(pts []Point, now time.Time, backfill bool) {
	g.seq = g.inj.NextSeq(g.seq)
	env := newEnvelope(g.serial, g.seq, now, pts)
	if !g.injectedProfile() { // 正常路径发端自检契约红线
		if err := env.validateContract(); err != nil {
			g.logger.Error("contract self-check failed; dropping batch", "err", err.Error())
			return
		}
	}
	payload, _, err := g.inj.Apply(&env, now)
	if err != nil {
		g.logger.Error("fault injection failed; dropping batch", "err", err.Error())
		return
	}
	if backfill {
		g.stats.addBackfill(1)
	}
	g.deliver(TopicUpData(g.clientID), payload, len(pts))
}

// injectedProfile 报告该网关是否开启任何会破坏契约自检的故障旋钮。
func (g *gatewaySim) injectedProfile() bool {
	f := g.profile.Faults
	return f.OversizeEvery > 0 || f.MalformedEvery > 0 || f.UnknownPointsEvery > 0 ||
		f.UnknownMsgTypeEvery > 0 || f.UnsupportedVerEvery > 0 || f.GWMismatchEvery > 0 ||
		f.NullValueRate > 0 || f.InvalidTSEvery > 0 || f.TSBeyondRetentionEvery > 0
}

// deliver 发布一条消息。直发条件：在线且缓存为空——有积压时新样本一律入缓存，
// 由 drain FIFO 重放：断网补传与实时不乱序（ingest.md §7.4 同网关保序的网关侧
// 配合），避免重连后实时消息越过待重放的缓存消息。
func (g *gatewaySim) deliver(topic string, payload []byte, rows int) {
	if !g.forcedOff && g.online() && g.ob.len() == 0 {
		err := g.io.publish(topic, payload)
		if err == nil {
			g.stats.addMessages(1)
			g.stats.addPoints(int64(rows))
			g.stats.addBytes(int64(len(payload)))
			return
		}
		g.stats.addPublishError()
		g.logger.Warn("publish failed; buffering", "err", err.Error())
	}
	g.ob.add(outboxEntry{topic: topic, payload: payload, rows: rows})
}

// online 报告 MQTT 链路可用（未建连 = 离线，样本入缓存）。
func (g *gatewaySim) online() bool { return g.io != nil && g.io.online() }

// notifyDrain 唤醒 drain 循环（连接恢复回调复用此信号）。
func (b *outbox) notifyDrain() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

// drainLoop 缓存重放：FIFO、在线才发、失败退避重试同一条（保序）。
func (g *gatewaySim) drainLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-g.ob.notify:
		}
		for {
			if ctx.Err() != nil {
				return
			}
			e, ok := g.ob.peek()
			if !ok {
				break
			}
			if g.forcedOff || !g.online() {
				break // 等下一次连接恢复信号
			}
			if err := g.io.publish(e.topic, e.payload); err != nil {
				g.stats.addPublishError()
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
				}
				continue
			}
			g.ob.pop()
			g.stats.addMessages(1)
			g.stats.addPoints(int64(e.rows))
			g.stats.addBytes(int64(len(e.payload)))
		}
	}
}

// replayHistory 断网补传回放：从 now-days 到 now 按 step 生成历史样本，
// ts=历史采集时刻、sent_at=当前发送时刻（补传语义；days>7 覆盖已压缩 chunk）。
func (g *gatewaySim) replayHistory(ctx context.Context) {
	bf := g.profile.Backfill
	step := time.Duration(bf.StepS) * time.Second
	pace := time.Duration(bf.PublishIntervalMs) * time.Millisecond
	end := g.start
	start := end.Add(-time.Duration(bf.Days) * 24 * time.Hour)
	g.logger.Info("backfill replay started",
		"days", bf.Days, "step_s", bf.StepS, "from", FormatTS(start), "to", FormatTS(end))

	sent := 0
	for t := start; t.Before(end); t = t.Add(step) {
		if ctx.Err() != nil {
			return
		}
		g.sampleCycle(ctx, t, true)
		sent++
		select {
		case <-ctx.Done():
			return
		case <-time.After(pace):
		}
	}
	g.logger.Info("backfill replay done", "messages", sent)
}

// flushBestEffort 退出前尽力清空缓存（10s 上限，超时放弃并留摘要计数）。
func (g *gatewaySim) flushBestEffort() {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		e, ok := g.ob.peek()
		if !ok {
			break
		}
		if !g.online() {
			break
		}
		if err := g.io.publish(e.topic, e.payload); err != nil {
			break
		}
		g.ob.pop()
		g.stats.addMessages(1)
		g.stats.addPoints(int64(e.rows))
		g.stats.addBytes(int64(len(e.payload)))
	}
	if n := g.ob.len(); n > 0 {
		g.logger.Warn("exit with buffered messages (not delivered)", "count", n)
	}
	if g.io != nil {
		g.io.disconnect()
	}
}

// mergeInjectorStats 把本网关注入计数并进进程级摘要（Run 结束时调用方触发）。
func (g *gatewaySim) mergeInjectorStats() {
	g.stats.mergeFaults(g.inj.Stats())
	dropped := g.ob.droppedLoad()
	if dropped > 0 {
		g.stats.addDroppedRows(dropped)
	}
}

func (b *outbox) droppedLoad() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped
}
