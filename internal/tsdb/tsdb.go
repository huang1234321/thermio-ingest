// Package tsdb 负责 telemetry 批量 upsert 写入（ingest.md §7.1：pgx v5 batch，
// ON CONFLICT (point_id, ts) DO UPDATE，last-write-wins 幂等——ddl.md §8 用例 8
// 已验证）。写顺序纪律（TSDB 先、Kafka 后、PUBACK 最后）在 pipeline 层编排，
// 本包只管把一批行可靠落库（可重试错误指数退避重试）。
package tsdb

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Row 一行待写遥测（pipeline 侧共享结构的最小投影——消费侧小接口，GO-06）。
type Row struct {
	PointID   int64
	TS        time.Time
	Value     *float64
	ValueText *string
	Quality   uint16
}

// BatchWriter 消费侧接口：一批行原子落库（pipeline 与测试共用）。
type BatchWriter interface {
	WriteBatch(ctx context.Context, rows []Row) error
	Close()
}

// Connect 按 TSDB_DSN（tsdb_ingest 角色，ingest.md §11）创建连接池并 Ping 验通。
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create tsdb pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping tsdb: %w", err)
	}
	return pool, nil
}

// Writer pgx 实现。
type Writer struct {
	pool    *pgxpool.Pool
	retries int
	backoff time.Duration
}

// NewWriter retries/backoff 为重试策略（默认 3 次 / 100ms 起步指数退避）。
func NewWriter(pool *pgxpool.Pool) *Writer {
	return &Writer{pool: pool, retries: 3, backoff: 100 * time.Millisecond}
}

// upsertSQL 单行 upsert（ddl.md §11.1 表结构；幂等语义 §8 用例 8）。
const upsertSQL = `INSERT INTO telemetry (point_id, ts, value, value_text, quality)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (point_id, ts) DO UPDATE SET
  value = EXCLUDED.value,
  value_text = EXCLUDED.value_text,
  quality = EXCLUDED.quality`

// WriteBatch 单事务批量 upsert；重试只针对整批（事务原子，无部分写）。
// 调用方需先按 (point_id, ts) 排序（§5.1 阶段 6 索引局部性）。
func (w *Writer) WriteBatch(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	var lastErr error
	for attempt := 0; attempt <= w.retries; attempt++ {
		if attempt > 0 {
			// 指数退避：100ms、200ms、400ms；ctx 取消可打断（GO-03）。
			select {
			case <-ctx.Done():
				return fmt.Errorf("tsdb write canceled after %d attempts: %w", attempt, ctx.Err())
			case <-time.After(w.backoff << (attempt - 1)):
			}
		}
		err := w.writeOnce(ctx, rows)
		if err == nil {
			return nil
		}
		lastErr = err
		// ctx 取消不该重试（进程在退出）。
		if ctx.Err() != nil {
			return fmt.Errorf("tsdb write canceled: %w", ctx.Err())
		}
	}
	return fmt.Errorf("tsdb write exhausted %d retries: %w", w.retries, lastErr)
}

func (w *Writer) writeOnce(ctx context.Context, rows []Row) error {
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(upsertSQL, r.PointID, r.TS, r.Value, r.ValueText, int16(r.Quality))
	}
	br := w.pool.SendBatch(ctx, batch)
	// SendBatch 后必须 Close（pgx 契约）；批量语句错误以逐条 Exec 返回为准，
	// Close 只回收连接状态——错误不丢（GO-01：显式处理）。
	defer func() { _ = br.Close() }()
	for i := 0; i < len(rows); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("exec upsert #%d (batch %d rows): %w", i, len(rows), err)
		}
	}
	return nil
}

// Close 关闭连接池。
func (w *Writer) Close() { w.pool.Close() }
