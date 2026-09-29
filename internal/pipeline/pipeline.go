// Package pipeline 编排 ingest.md §5 处理管线：
//
//	MQTT handler → 有界 intake（10k）→ decode worker 池 → 单 writer（聚合批量）
//	  → TSDB 批量 upsert → Kafka produce（raw/quality/DLQ）→ PUBACK
//
// 写入顺序纪律（§7.1）：TSDB 先、Kafka 后、PUBACK 最后——崩溃窗口最坏丢流不丢
// 真相源（ADR-002）。背压（§7.3）：行占用 rowGate 信号量（容量 BUFFER_MAX_ROWS），
// 打满后 decode worker 阻塞 → intake 填满 → handler 阻塞 → TCP 背压 → EMQX
// inflight 暂停投递；全链路无人为丢弃，唯一丢弃口是 DLQ 显式原因。
// 优雅退出（§12）：停订阅 → 排空 intake → 冲缓冲 → 冲 producer；关连接归 main。
package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/huang1234321/thermio-ingest/internal/metrics"
	"github.com/huang1234321/thermio-ingest/internal/mqtt"
	"github.com/huang1234321/thermio-ingest/internal/points"
	"github.com/huang1234321/thermio-ingest/internal/quality"
	"github.com/huang1234321/thermio-ingest/internal/tsdb"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Config pipeline 视角的运行参数（config.Config 的子集投影，测试可独立构造）。
type Config struct {
	BatchMaxRows       int
	BatchFlushInterval time.Duration
	BufferMaxRows      int
	MaxQueueMessages   int

	TsSkewThreshold   time.Duration // bit5（10min）
	BackfillThreshold time.Duration // bit8（10min）
	RetentionCutoff   time.Duration // §7.1 拒写线（2 年）
	StaleScanInterval time.Duration // §6.3（30s）
	ProduceTimeout    time.Duration // Kafka produce 等待上界（默认 10s）
	TsdbWriteTimeout  time.Duration // TSDB 批写等待上界（默认 30s；挂起走 TSDB_WRITE_FAILED 死信兜底）

	DecodeWorkers int // 0 = 4
}

// KafkaSink Kafka 生产出口（kafkaproducer.Producer 的最小投影，GO-06）。
type KafkaSink interface {
	Produce(ctx context.Context, records ...*kgo.Record) error
	Flush(ctx context.Context) error
	Close()
}

// Pipeline 全链路编排。Run 阻塞运行；ctx 取消后走 §12 优雅退出序列再返回。
type Pipeline struct {
	cfg   Config
	cache *points.Cache
	tsdb  tsdb.BatchWriter
	kafka KafkaSink
	met   *metrics.Metrics
	log   *slog.Logger

	stats *Stats // gauge 数据源（bufferedRows / backpressure / cache 健康）
	stale *quality.StaleTracker

	nowFunc func() time.Time

	intake  chan mqtt.Inbound
	decoded chan *processed
	rowGate chan struct{} // 容量 BufferMaxRows：一行一占位，flush 落定后释放

	source *mqtt.Source // 可为 nil（测试直接喂 intake）

	flushing atomic.Bool // writer 是否在 flush 内（D-38 心跳会计面）

	mu       sync.Mutex
	lastSeq  map[string]int64 // seq 缺口检测（§3.2：只记指标与 WARN）
	unitSeen map[int64]bool   // unit_unconverted 首次出现去抖（§5.3）

	wg       sync.WaitGroup // decode workers
	writerWG sync.WaitGroup
	staleWG  sync.WaitGroup
}

// New 构建管线（未启动）。gauge 闭包经 p.Stats() 接进 metrics.New。
func New(cfg Config, cache *points.Cache, w tsdb.BatchWriter, k KafkaSink,
	met *metrics.Metrics, log *slog.Logger) *Pipeline {
	if cfg.DecodeWorkers <= 0 {
		cfg.DecodeWorkers = 4
	}
	if cfg.ProduceTimeout <= 0 {
		cfg.ProduceTimeout = 10 * time.Second
	}
	if cfg.TsdbWriteTimeout <= 0 {
		// 30s 上界：覆盖内部 3 次重试（100/200/400ms 退避）+ 批量 upsert 余量，
		// 只兜「无响应挂起」（真慢可等）；超时后整批死信 + PUBACK 释放背压，
		// 约一个 BUFFER_MAX_ROWS（100k 行）的积压窗口（DAT-121）。
		cfg.TsdbWriteTimeout = 30 * time.Second
	}
	if cfg.MaxQueueMessages <= 0 {
		cfg.MaxQueueMessages = 10000
	}
	if log == nil {
		log = slog.Default()
	}
	p := &Pipeline{
		cfg:      cfg,
		cache:    cache,
		tsdb:     w,
		kafka:    k,
		met:      met,
		log:      log,
		stats:    NewStats(),
		nowFunc:  time.Now,
		lastSeq:  map[string]int64{},
		unitSeen: map[int64]bool{},
		rowGate:  make(chan struct{}, cfg.BufferMaxRows),
		intake:   make(chan mqtt.Inbound, cfg.MaxQueueMessages),
		decoded:  make(chan *processed, 64),
	}
	p.stale = quality.NewStaleTracker(func(pointID int64) (time.Duration, bool) {
		pc, ok := cache.PointByID(pointID)
		if !ok {
			return 0, false
		}
		return time.Duration(pc.StaleTimeoutS) * time.Second, true
	})
	return p
}

// NewWithStats 与 New 的区别：gauge 数据源外部注入（main 需要先建 Stats 才能
// 建 metrics，再把两者交回管线——闭环解耦）。
func NewWithStats(cfg Config, cache *points.Cache, w tsdb.BatchWriter, k KafkaSink,
	met *metrics.Metrics, stats *Stats, log *slog.Logger) *Pipeline {
	p := New(cfg, cache, w, k, met, log)
	if stats != nil {
		p.stats = stats
	}
	return p
}

// IntakeChan 双向暴露 intake（Source 需要 chan<- 写入侧）。
func (p *Pipeline) IntakeChan() chan mqtt.Inbound { return p.intake }

// Stats 返回 gauge 数据源（main 在 metrics.New 时接线）。
func (p *Pipeline) Stats() *Stats { return p.stats }

// SetSource 挂接 MQTT 源（main 用；测试直接喂 intake 时不挂）。
func (p *Pipeline) SetSource(s *mqtt.Source) { p.source = s }

// Intake 测试注入通道（正常流量来自 mqtt.Source 的 handler）。
func (p *Pipeline) Intake() chan<- mqtt.Inbound { return p.intake }

// Run 启动全部阶段并阻塞，直到 ctx 取消；随后按 §12 顺序收尾：
// 停订阅 → 排空 intake（decode workers 消化完退出）→ 关 decoded → writer
// 最后一批（final flush）→ 冲 producer。连接关闭（Kafka/TSDB/MQTT）归 main。
func (p *Pipeline) Run(ctx context.Context) {
	p.wg.Add(p.cfg.DecodeWorkers)
	for i := 0; i < p.cfg.DecodeWorkers; i++ {
		go p.runDecodeWorker()
	}
	p.writerWG.Add(1)
	go p.runWriter()
	p.staleWG.Add(1)
	go p.runStaleScanner(ctx)
	p.stats.sampleLoop(ctx, p.cache, p.rowGate)

	<-ctx.Done()

	// 1. 停订阅：在途 handler 不 ACK 直接让路（EMQX 会话保持，重启重投）。
	if p.source != nil {
		p.source.Shutdown()
	}
	// 2. 排空 intake：handler 已全数退出，close 安全；workers range 到关闭即退出。
	close(p.intake)
	p.wg.Wait()
	// 3. 冲缓冲：workers 退出后 decoded 不再有生产者，writer 排空并做 final flush。
	close(p.decoded)
	p.writerWG.Wait()
	// 4. 冲 producer（在途 promise 全部落定）。
	flushCtx, cancel := context.WithTimeout(context.Background(), p.cfg.ProduceTimeout)
	defer cancel()
	_ = p.kafka.Flush(flushCtx) // 退出路径无恢复手段；错误以日志口径见 main
	p.staleWG.Wait()
}

// acquireRows 占用 n 个行占位（背压点：满则阻塞——§7.3）。
func (p *Pipeline) acquireRows(n int) {
	for i := 0; i < n; i++ {
		p.rowGate <- struct{}{}
	}
}

// decodedSlotEst decoded 通道内消息的占位行数估计（消息数 × 批行上界 500 的
// 收敛估计——D-38 会计面：持有量归因的主盲区即此通道的行乘数）。
func (p *Pipeline) decodedSlotEst() int {
	n := len(p.decoded)
	if n == 0 {
		return 0
	}
	// 不精确但可对账：按已入通道消息数 × 平均批行规模（配置的批上界与 500 契约
	// 上限取小）作量级估计；精确拆解需消息级记账，不值得为此加锁。
	per := p.cfg.BatchMaxRows
	if per > 500 {
		per = 500
	}
	return n * per
}

// releaseRows 释放行占位（flush 落定后）。
func (p *Pipeline) releaseRows(n int) {
	for i := 0; i < n; i++ {
		<-p.rowGate
	}
}
