package points

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

// fakeLoader 内存假实现：可编程变更与失败注入。
type fakeLoader struct {
	gws      map[string]GatewayConfig
	pts      map[int64]PointConfig
	maxUpd   time.Time
	failNext error
}

func (f *fakeLoader) LoadGateways(ctx context.Context) (map[string]GatewayConfig, error) {
	if f.failNext != nil {
		return nil, f.failNext
	}
	out := map[string]GatewayConfig{}
	for k, v := range f.gws {
		out[k] = v
	}
	return out, nil
}

func (f *fakeLoader) LoadPointsSince(ctx context.Context, since time.Time) (map[int64]PointConfig, error) {
	if f.failNext != nil {
		return nil, f.failNext
	}
	out := map[int64]PointConfig{}
	for id, p := range f.pts {
		out[id] = p
	}
	return out, nil
}

func (f *fakeLoader) MaxUpdatedAt(ctx context.Context) (time.Time, error) {
	if f.failNext != nil {
		return time.Time{}, f.failNext
	}
	return f.maxUpd, nil
}

func testCache(t *testing.T, l Loader) *Cache {
	t.Helper()
	c := NewCache(l, slog.New(slog.DiscardHandler))
	if err := c.Initial(context.Background()); err != nil {
		t.Fatalf("Initial: %v", err)
	}
	return c
}

// §6.1 缓存内容：clientid/serial 双索引与 (gateway_id, raw_name) 点位索引。
func TestCacheLookup(t *testing.T) {
	l := &fakeLoader{
		gws: map[string]GatewayConfig{
			"GW-BLDG-A-01": {GatewayID: "g1", TenantID: "t1", Serial: "GW2026001", Status: "online"},
		},
		pts: map[int64]PointConfig{
			101: {PointID: 101, GatewayID: "g1", TenantID: "t1", RawName: "CHW_SUPPLY_TEMP_1",
				UnitRaw: "degF", UnitStd: "degC", StaleTimeoutS: 300, Status: "active"},
		},
	}
	c := testCache(t, l)

	g, ok := c.GatewayByClientID("GW-BLDG-A-01")
	if !ok || g.GatewayID != "g1" || g.Serial != "GW2026001" {
		t.Errorf("GatewayByClientID = %+v, %v", g, ok)
	}
	if id, ok := c.GatewayIDBySerial("GW2026001"); !ok || id != "g1" {
		t.Errorf("GatewayIDBySerial = %q, %v", id, ok)
	}
	if _, ok := c.GatewayByClientID("nope"); ok {
		t.Error("未知 clientid 应未命中（UNKNOWN_GATEWAY 判据）")
	}
	p, ok := c.Point("g1", "CHW_SUPPLY_TEMP_1")
	if !ok || p.PointID != 101 || p.UnitRaw != "degF" || p.UnitStd != "degC" {
		t.Errorf("Point = %+v, %v", p, ok)
	}
	if _, ok := c.Point("g1", "NOT_REGISTERED"); ok {
		t.Error("未注册点应未命中（UNREGISTERED_POINT 判据）")
	}
	if _, ok := c.Point("other-gw", "CHW_SUPPLY_TEMP_1"); ok {
		t.Error("跨网关同名点不应命中（物理身份 = (gateway_id, raw_name)）")
	}
}

// §6.2 刷新失败保旧缓存：Loader 报错后缓存内容与查询能力不变。
func TestRefreshFailureKeepsOld(t *testing.T) {
	l := &fakeLoader{
		gws: map[string]GatewayConfig{"c1": {GatewayID: "g1", Serial: "s1"}},
		pts: map[int64]PointConfig{1: {PointID: 1, GatewayID: "g1", RawName: "P1", Status: "active"}},
	}
	c := testCache(t, l)

	l.failNext = errors.New("pg down")
	if err := c.Refresh(context.Background(), true); err == nil {
		t.Fatal("增量失败应返回错误")
	}
	if err := c.Refresh(context.Background(), false); err == nil {
		t.Fatal("全量失败应返回错误")
	}
	if _, ok := c.Point("g1", "P1"); !ok {
		t.Error("刷新失败后旧缓存应继续服务")
	}
	if c.Size() != 1 {
		t.Errorf("Size = %d, want 1", c.Size())
	}
	// 新鲜度继续增长（年龄指标源），不再更新 lastRefreshAt。
	age := c.RefreshAge(time.Now())
	time.Sleep(10 * time.Millisecond)
	if c.RefreshAge(time.Now()) <= age {
		t.Error("刷新失败后年龄应持续增长")
	}
}

// §6.2 增量可见性：变更行（改名/换网关/停用）套用后旧键被驱逐。
func TestIncrementalApply(t *testing.T) {
	l := &fakeLoader{
		gws: map[string]GatewayConfig{"c1": {GatewayID: "g1", Serial: "s1"}},
		pts: map[int64]PointConfig{
			1: {PointID: 1, GatewayID: "g1", RawName: "OLD_NAME", Status: "active"},
		},
	}
	c := testCache(t, l)

	// 点 1 改名 OLD_NAME → NEW_NAME；点 2 新增。
	l.pts = map[int64]PointConfig{
		1: {PointID: 1, GatewayID: "g1", RawName: "NEW_NAME", Status: "active"},
		2: {PointID: 2, GatewayID: "g1", RawName: "P2", Status: "disabled"},
	}
	if err := c.Refresh(context.Background(), true); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if _, ok := c.Point("g1", "OLD_NAME"); ok {
		t.Error("改名后旧键应被驱逐")
	}
	if p, ok := c.Point("g1", "NEW_NAME"); !ok || p.PointID != 1 {
		t.Errorf("新键未命中: %+v %v", p, ok)
	}
	if p, ok := c.Point("g1", "P2"); !ok || p.Status != "disabled" {
		t.Errorf("新增点未命中或状态错误: %+v %v", p, ok)
	}

	// 全量重对齐捕获删除：点 2 从库里消失。
	delete(l.pts, 2)
	if err := c.Refresh(context.Background(), false); err != nil {
		t.Fatalf("full Refresh: %v", err)
	}
	if _, ok := c.Point("g1", "P2"); ok {
		t.Error("全量重对齐应捕获删除（增量看不见的行）")
	}
}

// §6.2 配置新鲜度：成功刷新后归零计时。
func TestRefreshAge(t *testing.T) {
	l := &fakeLoader{gws: map[string]GatewayConfig{}, pts: map[int64]PointConfig{}}
	c := NewCache(l, slog.New(slog.DiscardHandler))
	if age := c.RefreshAge(time.Now()); age != 0 {
		t.Errorf("未初始化年龄 = %v, want 0", age)
	}
	testCache(t, l)
	if age := c.RefreshAge(time.Now()); age < 0 || age > 5 {
		t.Errorf("刷新后年龄 = %v, want [0,5]", age)
	}
}
