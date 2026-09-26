// Package tsdb 负责 telemetry 批量 upsert 写入（ingest.md §7.1：pgx v5 batch，
// ON CONFLICT (point_id, ts) DO UPDATE，last-write-wins 幂等）。批量缓冲与写入
// 顺序（TSDB 先、Kafka 后、PUBACK 最后）随 IMPL-5 落地；本文件提供连接池基线。
package tsdb

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
