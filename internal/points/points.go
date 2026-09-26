// Package points 负责 point/gateway 配置缓存（ingest.md §6）：PG 只读 point、
// gateway 两表（thermio_ingest 旁路只读角色，ddl.md §5.1）；增量刷新 30s
// （updated_at 游标）+ 全量重对齐 1h（防 cursor 漂移）+ 刷新失败保旧缓存继续
// 服务（配置新鲜度指标暴露年龄，不用缓存年龄硬拒绝数据）。
// 取数在锁外且带 loadTimeout 上界（DAT-121）：DSN 无 statement_timeout，PG
// 挂起（非快速失败）时不得阻塞热路径读，也不得让刷新/首版加载无限悬挂。
package points

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GatewayConfig gatewayCache 值（§6.1）。
type GatewayConfig struct {
	GatewayID string // uuid（Kafka key、遥测行归属）
	TenantID  string
	Serial    string // 出厂序列号，与 payload.gw 对应
	Status    string // online/offline（EMQX 事件维护；ingest 不据此拒数）
}

// PointConfig pointCache 值（§6.1）。
type PointConfig struct {
	PointID       int64
	GatewayID     string
	TenantID      string
	RawName       string
	UnitRaw       string
	UnitStd       string
	ValidRangeMin *float64
	ValidRangeMax *float64
	StaleTimeoutS int32
	Status        string // active/disabled；disabled → POINT_INACTIVE DLQ
}

// pointKey (gateway_id, raw_name)——point 物理身份（ddl.md 唯一索引）。
type pointKey struct {
	gatewayID, rawName string
}

// Cache 线程安全配置缓存。读多写少：RWMutex + 双 map（id 主档 + 复合键索引）。
type Cache struct {
	mu sync.RWMutex

	gwByClientID map[string]GatewayConfig // mqtt_client_id → gateway
	gwBySerial   map[string]string        // serial → gateway_id

	ptsByID    map[int64]PointConfig
	ptsByPhyID map[pointKey]int64 // (gateway_id, raw_name) → point_id

	cursor        time.Time // 增量游标 = 见过的最大 updated_at
	lastRefreshAt time.Time // 成功刷新时刻（配置新鲜度指标源）

	loadTimeout time.Duration // 单轮取数上界（默认 defaultLoadTimeout）
	loader      Loader
	log         *slog.Logger
}

// defaultLoadTimeout 单轮取数超时：增量刷间隔 30s，上界取其半——挂起时每轮
// 必失败保旧、下一轮再试，同时不把刷新循环长期钉死在一次挂起上。
const defaultLoadTimeout = 15 * time.Second

// SetLoadTimeout 覆盖取数超时（测试用；非正值忽略）。
func (c *Cache) SetLoadTimeout(d time.Duration) {
	if d > 0 {
		c.loadTimeout = d
	}
}

// Loader 一次增量/全量数据拉取（消费侧小接口，GO-06；PG 实现与测试假实现共用）。
type Loader interface {
	// LoadGateways 全量网关（量级：百~千行，不值得增量）。
	LoadGateways(ctx context.Context) (map[string]GatewayConfig, error)
	// LoadPointsSince 增量点位；since 为零值时全量。
	LoadPointsSince(ctx context.Context, since time.Time) (map[int64]PointConfig, error)
	// MaxUpdatedAt 当前库里 point.updated_at 上界（刷新后校准游标，防时钟回拨漏读）。
	MaxUpdatedAt(ctx context.Context) (time.Time, error)
}

// NewCache 构建缓存；Initial 阻塞加载首版（空缓存上线会把全部点位打成
// UNREGISTERED_POINT 死信风暴，首版必须成功，由调用方重试）。
func NewCache(loader Loader, log *slog.Logger) *Cache {
	return &Cache{
		gwByClientID: map[string]GatewayConfig{},
		gwBySerial:   map[string]string{},
		ptsByID:      map[int64]PointConfig{},
		ptsByPhyID:   map[pointKey]int64{},
		loadTimeout:  defaultLoadTimeout,
		loader:       loader,
		log:          log,
	}
}

// Initial 首版全量加载并校准游标。PG 不可用（含挂起至超时）时调用方应重试
// 而非带空缓存服务。
func (c *Cache) Initial(ctx context.Context) error {
	return c.fullResync(ctx)
}

// GatewayByClientID topic clientid → 网关（未命中即 UNKNOWN_GATEWAY 判据）。
func (c *Cache) GatewayByClientID(clientID string) (GatewayConfig, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	g, ok := c.gwByClientID[clientID]
	return g, ok
}

// GatewayIDBySerial payload.gw（serial）→ gateway_id（GW_MISMATCH 判据）。
func (c *Cache) GatewayIDBySerial(serial string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	id, ok := c.gwBySerial[serial]
	return id, ok
}

// Point (gateway_id, raw_name) → 点配置（未命中即 UNREGISTERED_POINT 判据）。
func (c *Cache) Point(gatewayID, rawName string) (PointConfig, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	id, ok := c.ptsByPhyID[pointKey{gatewayID, rawName}]
	if !ok {
		return PointConfig{}, false
	}
	p, ok := c.ptsByID[id]
	return p, ok
}

// PointByID stale 扫描与观测回查用。
func (c *Cache) PointByID(id int64) (PointConfig, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.ptsByID[id]
	return p, ok
}

// PointsSnapshot 全量点配置快照（stale 扫描器每轮一次，量级 1e4~1e5 可承受）。
func (c *Cache) PointsSnapshot() map[int64]PointConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[int64]PointConfig, len(c.ptsByID))
	for id, p := range c.ptsByID {
		out[id] = p
	}
	return out
}

// RefreshAge 配置新鲜度秒（§6.2：失败保旧继续服务，年龄进指标告警）。
func (c *Cache) RefreshAge(now time.Time) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lastRefreshAt.IsZero() {
		return 0
	}
	return now.Sub(c.lastRefreshAt).Seconds()
}

// Size 点位数（指标 ingest_config_cache_points）。
func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.ptsByID)
}

// Refresh 一轮刷新：incremental=true 走游标增量，否则全量重对齐。
// 取数在锁外（DAT-121：持写锁跨 PG 查询会让 PG 挂起阻塞全部热路径读），
// 仅套用阶段短暂持写锁；失败保留旧缓存（§6.2），只报错——调用方（刷新循环）
// 记指标与 WARN。刷新循环单 goroutine 串行调用，锁外取数不存在并发套用乱序；
// 与 Initial 也不会并发（首版加载完成后循环才启动）。
func (c *Cache) Refresh(ctx context.Context, incremental bool) error {
	if !incremental {
		return c.fullResync(ctx)
	}

	c.mu.RLock()
	cursor := c.cursor
	c.mu.RUnlock()

	changed, err := c.loadPoints(ctx, cursor)
	if err != nil {
		return fmt.Errorf("incremental load: %w", err)
	}
	maxUpd, maxErr := c.maxUpdatedAt(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.applyPointsLocked(changed)
	if maxErr == nil && maxUpd.After(c.cursor) {
		c.cursor = maxUpd
	}
	c.lastRefreshAt = time.Now()
	return nil
}

// fullResync 全量重对齐：锁外取数（网关 + 整表点位 + 游标上界），持锁整表
// 替换 + 游标校准。
func (c *Cache) fullResync(ctx context.Context) error {
	gws, err := c.loadGateways(ctx)
	if err != nil {
		return fmt.Errorf("load gateways: %w", err)
	}
	pts, err := c.loadPoints(ctx, time.Time{})
	if err != nil {
		return fmt.Errorf("full load points: %w", err)
	}
	maxUpd, maxErr := c.maxUpdatedAt(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.gwByClientID = gws
	c.gwBySerial = make(map[string]string, len(gws))
	for _, g := range gws {
		c.gwBySerial[g.Serial] = g.GatewayID
	}
	// 整表替换：捕获删除与网关迁移（增量只看得见还存在的行）。
	c.ptsByID = make(map[int64]PointConfig, len(pts))
	c.ptsByPhyID = make(map[pointKey]int64, len(pts))
	for id, p := range pts {
		c.applyPointLocked(id, p)
	}
	if maxErr == nil {
		c.cursor = maxUpd
	} else {
		c.cursor = time.Now()
	}
	c.lastRefreshAt = time.Now()
	return nil
}

// loadGateways / loadPoints / maxUpdatedAt 统一带取数超时的 Loader 调用
// （DSN 无 statement_timeout，挂起只能靠 ctx 兜底——DAT-121）。
func (c *Cache) loadGateways(ctx context.Context) (map[string]GatewayConfig, error) {
	ctx, cancel := context.WithTimeout(ctx, c.loadTimeout)
	defer cancel()
	return c.loader.LoadGateways(ctx)
}

func (c *Cache) loadPoints(ctx context.Context, since time.Time) (map[int64]PointConfig, error) {
	ctx, cancel := context.WithTimeout(ctx, c.loadTimeout)
	defer cancel()
	return c.loader.LoadPointsSince(ctx, since)
}

func (c *Cache) maxUpdatedAt(ctx context.Context) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, c.loadTimeout)
	defer cancel()
	return c.loader.MaxUpdatedAt(ctx)
}

// applyPointsLocked 增量套用变更行（含网关迁移/改名时的旧键驱逐）。
func (c *Cache) applyPointsLocked(changed map[int64]PointConfig) {
	for id, p := range changed {
		if old, ok := c.ptsByID[id]; ok && (old.GatewayID != p.GatewayID || old.RawName != p.RawName) {
			delete(c.ptsByPhyID, pointKey{old.GatewayID, old.RawName})
		}
		c.applyPointLocked(id, p)
	}
}

func (c *Cache) applyPointLocked(id int64, p PointConfig) {
	c.ptsByID[id] = p
	c.ptsByPhyID[pointKey{p.GatewayID, p.RawName}] = id
}

// RunRefreshLoop 刷新循环：增量每 refreshInterval、全量每 fullInterval；
// 失败保旧缓存 + WARN（§6.2）。ctx 取消即返回（GO-04）。
func (c *Cache) RunRefreshLoop(ctx context.Context, refreshInterval, fullInterval time.Duration) {
	tick := time.NewTicker(refreshInterval)
	full := time.NewTicker(fullInterval)
	defer tick.Stop()
	defer full.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := c.Refresh(ctx, true); err != nil {
				c.log.Warn("config incremental refresh failed, serving stale cache", "err", err.Error())
			}
		case <-full.C:
			if err := c.Refresh(ctx, false); err != nil {
				c.log.Warn("config full resync failed, serving stale cache", "err", err.Error())
			}
		}
	}
}

// Connect 按 PG_DSN（thermio_ingest 旁路只读角色，ingest.md §11）创建连接池并
// Ping 验通。
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create pg pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping pg: %w", err)
	}
	return pool, nil
}

// PGLoader 从 PG 业务库只读 point/gateway 两表（thermio_ingest 角色，ddl.md §5.1；
// 列名与 ddl.md §4 建表一致——表结构以仓库当前 PG 迁移链为准）。
type PGLoader struct {
	pool *pgxpool.Pool
}

// NewPGLoader 按已建连池构建。
func NewPGLoader(pool *pgxpool.Pool) *PGLoader { return &PGLoader{pool: pool} }

// LoadGateways 实现 Loader。
func (l *PGLoader) LoadGateways(ctx context.Context) (map[string]GatewayConfig, error) {
	rows, err := l.pool.Query(ctx, `
		SELECT id::text, tenant_id::text, serial, mqtt_client_id, status
		FROM gateway`)
	if err != nil {
		return nil, fmt.Errorf("query gateway: %w", err)
	}
	defer rows.Close()
	out := map[string]GatewayConfig{}
	for rows.Next() {
		var clientID string
		var g GatewayConfig
		if err := rows.Scan(&g.GatewayID, &g.TenantID, &g.Serial, &clientID, &g.Status); err != nil {
			return nil, fmt.Errorf("scan gateway: %w", err)
		}
		out[clientID] = g
	}
	return out, rows.Err()
}

// LoadPointsSince 实现 Loader；since 零值 = 全量。只取挂网关的点
// （gateway_id IS NULL 的点不经 MQTT 上报，§6.1）。
func (l *PGLoader) LoadPointsSince(ctx context.Context, since time.Time) (map[int64]PointConfig, error) {
	q := `
		SELECT id, gateway_id::text, tenant_id::text, raw_name,
		       unit_raw, unit_std, valid_range_min, valid_range_max,
		       stale_timeout_s, status
		FROM point
		WHERE gateway_id IS NOT NULL`
	args := []any{}
	if !since.IsZero() {
		q += ` AND updated_at > $1`
		args = append(args, since)
	}
	rows, err := l.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query point: %w", err)
	}
	defer rows.Close()
	out := map[int64]PointConfig{}
	for rows.Next() {
		var p PointConfig
		// unit 两列可空：经 **string 扫描（NULL → nil），再落到 string 字段。
		var unitRaw, unitStd *string
		if err := rows.Scan(&p.PointID, &p.GatewayID, &p.TenantID, &p.RawName,
			&unitRaw, &unitStd, &p.ValidRangeMin, &p.ValidRangeMax,
			&p.StaleTimeoutS, &p.Status); err != nil {
			return nil, fmt.Errorf("scan point: %w", err)
		}
		if unitRaw != nil {
			p.UnitRaw = *unitRaw
		}
		if unitStd != nil {
			p.UnitStd = *unitStd
		}
		out[p.PointID] = p
	}
	return out, rows.Err()
}

// MaxUpdatedAt 实现 Loader；空表返回零值。
func (l *PGLoader) MaxUpdatedAt(ctx context.Context) (time.Time, error) {
	var t *time.Time
	if err := l.pool.QueryRow(ctx, `SELECT max(updated_at) FROM point`).Scan(&t); err != nil {
		return time.Time{}, fmt.Errorf("query max(updated_at): %w", err)
	}
	if t == nil {
		return time.Time{}, nil
	}
	return t.UTC(), nil
}
