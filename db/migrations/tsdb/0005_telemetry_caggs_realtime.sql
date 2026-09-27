-- +goose Up
-- 存量库 real-time 收口（DAT-158）：0002 曾依赖「real-time 默认开」，但 TS ≥2.7
-- 默认已翻转为 materialized_only=true（钉版 2.17.2 实测）——已建 cagg 近 1h
-- （刷新策略 end_offset）数据在视图不可见，FDD 5min 节奏被迫手动 CALL refresh
-- 代偿（DAT-154 验收遗留 #4）。0002 已改显式 materialized_only=false（新建路径），
-- 本迁移对已过 0002 的存量库补 ALTER：目录级开关翻转，不动物化数据与策略 job。
ALTER MATERIALIZED VIEW telemetry_5min SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW telemetry_1h  SET (timescaledb.materialized_only = false);

-- +goose Down
-- 回退 = 恢复 0005 前行为（近窗不可见，需手动 refresh / 等 end_offset 过后物化）
ALTER MATERIALIZED VIEW telemetry_5min SET (timescaledb.materialized_only = true);
ALTER MATERIALIZED VIEW telemetry_1h  SET (timescaledb.materialized_only = true);
