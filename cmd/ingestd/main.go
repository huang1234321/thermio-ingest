// Command ingestd 是 thermio 遥测管道守护进程（MQTT in → TimescaleDB / Kafka out，
// ADR-016：Go 不越过 Kafka 缝进入应用族）。
//
// 组装顺序（ingest.md §11/§12）：env 配置 → PG/TSDB/Kafka/MQTT 连接 →
// 配置缓存首版（阻塞直至成功，防空缓存死信风暴）→ 指标 → pipeline.Run。
// 优雅退出：SIGINT/SIGTERM → 取消 ctx（pipeline 内部走 §12 序列）→ 关连接。
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/huang1234321/thermio-ingest/internal/config"
	"github.com/huang1234321/thermio-ingest/internal/kafkaproducer"
	"github.com/huang1234321/thermio-ingest/internal/metrics"
	"github.com/huang1234321/thermio-ingest/internal/mqtt"
	"github.com/huang1234321/thermio-ingest/internal/pipeline"
	"github.com/huang1234321/thermio-ingest/internal/points"
	"github.com/huang1234321/thermio-ingest/internal/tsdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

// version 由构建时注入：-ldflags "-X main.version=v1.2.3"。
var version = "dev"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)) // 结构化 JSON（§10）
	slog.SetDefault(logger)

	cfg := config.MustLoad()
	logger.Info("ingestd starting", "version", version,
		"mqtt", cfg.MQTTBrokerURL, "kafka_brokers", cfg.KafkaBrokers,
		"batch_max_rows", cfg.BatchMaxRows, "buffer_max_rows", cfg.BufferMaxRows)

	if err := run(context.Background(), cfg, logger); err != nil {
		logger.Error("ingestd exited with error", "err", err.Error())
		os.Exit(1)
	}
	logger.Info("ingestd stopped")
}

func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	rootCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 连接期重试：dev compose 各件启动有先后（deploy.md），失败每 5s 重试
	// 直到成功或退出信号（CODE-LOG-05：每分钟一条 WARN 防日志风暴）。
	var pgPool *pgxpool.Pool
	if _, err := retryUntil(rootCtx, logger, "pg connect", func() error {
		p, err := points.Connect(rootCtx, cfg.PGDSN)
		if err == nil {
			pgPool = p
		}
		return err
	}); err != nil {
		return err
	}
	defer pgPool.Close()

	var tsdbPool *pgxpool.Pool
	if _, err := retryUntil(rootCtx, logger, "tsdb connect", func() error {
		p, err := tsdb.Connect(rootCtx, cfg.TSDBDSN)
		if err == nil {
			tsdbPool = p
		}
		return err
	}); err != nil {
		return err
	}
	writer := tsdb.NewWriter(tsdbPool)
	defer writer.Close()

	kp, err := kafkaproducer.NewBrokers(cfg.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}

	// 配置缓存首版必须成功（空缓存会把全部点位打成 UNREGISTERED_POINT 死信）。
	cache := points.NewCache(points.NewPGLoader(pgPool), logger)
	if _, err := retryUntil(rootCtx, logger, "config cache initial", func() error {
		return cache.Initial(rootCtx)
	}); err != nil {
		return err
	}

	// 指标（§10 全套；gauge 闭包绑 pipeline.Stats）。
	stats := pipeline.NewStats()
	g1, g2, g3, g4 := stats.Gauge()
	met := metrics.New(g1, g2, g3, g4)

	ppl := pipeline.NewWithStats(pipeline.Config{
		BatchMaxRows:       cfg.BatchMaxRows,
		BatchFlushInterval: cfg.BatchFlushInterval,
		BufferMaxRows:      cfg.BufferMaxRows,
		MaxQueueMessages:   cfg.MaxQueueMessages,
		TsSkewThreshold:    cfg.TsSkewThreshold,
		BackfillThreshold:  cfg.BackfillThreshold,
		RetentionCutoff:    cfg.RetentionCutoff,
		StaleScanInterval:  cfg.StaleScanInterval,
	}, cache, writer, kp, met, stats, logger)

	// /metrics HTTP（deploy.md：宿主进程 :9091，Prometheus job 预留位）。
	mux := http.NewServeMux()
	mux.Handle("/metrics", met.Handler())
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server error", "err", err.Error())
		}
	}()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shutCtx)
	}()

	// MQTT 源（intake 由 pipeline 持有；Source 把 handler 指进去）。
	intake := ppl.IntakeChan()
	source := mqtt.NewSource(intake)
	ppl.SetSource(source)
	if _, err := retryUntil(rootCtx, logger, "mqtt start", func() error {
		return source.Start(rootCtx, cfg.MQTTBrokerURL, cfg.MQTTClientID, cfg.MQTTUsername,
			cfg.MQTTPassword, cfg.MQTTGroup)
	}); err != nil {
		return err
	}

	// 配置缓存刷新循环（§6.2：增量 30s + 全量 1h，失败保旧）。
	refreshCtx, refreshCancel := context.WithCancel(rootCtx)
	defer refreshCancel()
	go cache.RunRefreshLoop(refreshCtx, cfg.CacheRefreshInterval, cfg.CacheFullResyncInterval)

	// 管线主体。Run 返回 = §12 序列完成（停订阅→冲缓冲→冲 producer）。
	pipelineDone := make(chan struct{})
	go func() { ppl.Run(rootCtx); close(pipelineDone) }()

	<-rootCtx.Done()
	logger.Info("shutdown signal received")
	<-pipelineDone

	// 关连接（§12 最后一步）：Kafka → MQTT → TSDB（PG 池由 defer 关）。
	kp.Close()
	source.Disconnect()
	return nil
}

// retryUntil 每 5s 重试 fn 直至成功或 ctx 结束；每 12 次打一条 WARN。
func retryUntil(ctx context.Context, logger *slog.Logger, what string, fn func() error) (bool, error) {
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			return true, nil
		}
		if attempt%12 == 1 {
			logger.Warn(what+" retrying", "attempt", attempt, "err", err.Error())
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("%s: %w", what, ctx.Err())
		case <-time.After(5 * time.Second):
		}
	}
}
