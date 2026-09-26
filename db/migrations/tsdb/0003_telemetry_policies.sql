-- +goose Up
-- 压缩：7 天后列存压缩（ADR-005：10–20x）。
-- segmentby=point_id：同点数据列内连续，点位点查与时段范围查都受益；
-- 补传落已压缩 chunk 自动解压再写（§8.1 用例 11 实测）。
ALTER TABLE telemetry SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'point_id',
  timescaledb.compress_orderby   = 'ts DESC'
);
SELECT add_compression_policy('telemetry', INTERVAL '7 days');

-- 保留：原始遥测 2 年（ADR-005；ingest 侧另有「ts < now()-2 年拒写 + DLQ」防线，ingest.md §7.1）
SELECT add_retention_policy('telemetry', INTERVAL '2 years');

-- cagg 刷新：start_offset 5 天 ≥ 网关断网缓存 3 天（ADR-003 硬指标 3）+ 裕量（ingest.md §8）；
-- 补传超过 5 天的物化覆盖走 runbook 手动 CALL refresh_continuous_aggregate（部署交付物）。
SELECT add_continuous_aggregate_policy('telemetry_5min',
  start_offset      => INTERVAL '5 days',
  end_offset        => INTERVAL '1 hour',
  schedule_interval => INTERVAL '5 minutes');

SELECT add_continuous_aggregate_policy('telemetry_1h',
  start_offset      => INTERVAL '5 days',
  end_offset        => INTERVAL '1 hour',
  schedule_interval => INTERVAL '30 minutes');

-- cagg 自身压缩 + 保留（原始层删除后聚合仍在：5min 5 年、1h 永久，ADR-005 阶梯）
ALTER MATERIALIZED VIEW telemetry_5min SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'point_id',
  timescaledb.compress_orderby   = 'bucket DESC'
);
SELECT add_compression_policy('telemetry_5min', INTERVAL '30 days');
SELECT add_retention_policy('telemetry_5min', INTERVAL '5 years');

ALTER MATERIALIZED VIEW telemetry_1h SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'point_id',
  timescaledb.compress_orderby   = 'bucket DESC'
);
SELECT add_compression_policy('telemetry_1h', INTERVAL '90 days');
-- telemetry_1h 永久保留（对标报告/KPI 层），不设保留策略

-- +goose Down
-- 逆序摘除策略（先 cagg 后原始），对象本体由 0002/0001 Down 删除
SELECT remove_compression_policy('telemetry_1h');
SELECT remove_retention_policy('telemetry_5min');
SELECT remove_compression_policy('telemetry_5min');
SELECT remove_continuous_aggregate_policy('telemetry_1h');
SELECT remove_continuous_aggregate_policy('telemetry_5min');
SELECT remove_retention_policy('telemetry');
SELECT remove_compression_policy('telemetry');
