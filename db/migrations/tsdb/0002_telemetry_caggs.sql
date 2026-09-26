-- +goose Up
-- 双 continuous aggregate（ADR-005：5min 保留 5 年 / 1h 永久）。
-- 均自原始 telemetry 直接物化（不做 cagg 嵌套 rollup）：刷新语义单一，
-- 嵌套优化（1h 由 5min rollup）量级到区域中心再评估。
-- 列名对齐 DATA-MODEL §4；sample_count/bad_count 为本文增量（§7.1 #15）。
-- 默认 real-time 聚合开：近窗（刷新策略 end_offset 之内）由原始表现场合并。
CREATE MATERIALIZED VIEW telemetry_5min
WITH (timescaledb.continuous) AS
SELECT point_id,
       time_bucket(INTERVAL '5 minutes', ts) AS bucket,
       avg(value)          AS avg,
       min(value)          AS min,
       max(value)          AS max,
       last(value, ts)     AS last,
       stddev_samp(value)  AS stddev,
       count(*)            AS sample_count,
       count(*) FILTER (WHERE quality <> 0) AS bad_count,
       bit_or(quality)     AS quality_mask
FROM telemetry
GROUP BY point_id, bucket
WITH NO DATA;

CREATE MATERIALIZED VIEW telemetry_1h
WITH (timescaledb.continuous) AS
SELECT point_id,
       time_bucket(INTERVAL '1 hour', ts) AS bucket,
       avg(value)          AS avg,
       min(value)          AS min,
       max(value)          AS max,
       last(value, ts)     AS last,
       stddev_samp(value)  AS stddev,
       count(*)            AS sample_count,
       count(*) FILTER (WHERE quality <> 0) AS bad_count,
       bit_or(quality)     AS quality_mask
FROM telemetry
GROUP BY point_id, bucket
WITH NO DATA;

-- ingest 零授权（管道服务无读聚合场景，§8.1 用例 10）
GRANT SELECT ON public.telemetry_5min, public.telemetry_1h TO tsdb_api, tsdb_algo;

-- +goose Down
DROP MATERIALIZED VIEW IF EXISTS telemetry_1h;
DROP MATERIALIZED VIEW IF EXISTS telemetry_5min;
