-- +goose Up
-- 天气域（DATA-MODEL §5）：M&V 天气归一化 + 负荷预测特征；写入方 = thermio-algo 调度 job（P2-4）。
-- 结构与遥测不同（forecast 是「发布时刻 × 预测时刻」二维），独立建表，不塞 telemetry。
CREATE TABLE weather_actual (
  station_id    text NOT NULL,                      -- 供应商站点标识
  obs_ts        timestamptz NOT NULL,               -- 观测时刻
  temp_c        double precision,
  rh_pct        double precision,
  wind_speed_ms double precision,
  pressure_hpa  double precision,
  precip_mm     double precision,
  condition_code text,                              -- 供应商天气现象码原样存（枚举治理在应用层）
  source        text NOT NULL,                      -- 供应商标识（溯源）
  PRIMARY KEY (station_id, obs_ts)
);
SELECT create_hypertable('weather_actual', 'obs_ts', chunk_time_interval => INTERVAL '1 year');

-- forecast 保留 90 天（DATA-MODEL §5「最近 N 次发布」的时间窗近似，§7.1 #18）；
-- chunk 7 天对齐保留粒度，避免「整 chunk 不满不删」的删除滞后
CREATE TABLE weather_forecast (
  station_id    text NOT NULL,
  issued_at     timestamptz NOT NULL,               -- 预报发布时刻（同站点多次发布并存）
  target_ts     timestamptz NOT NULL,               -- 预报目标时刻（取「最新发布 × 目标窗」）
  temp_c        double precision,
  rh_pct        double precision,
  wind_speed_ms double precision,
  pressure_hpa  double precision,
  precip_mm     double precision,
  condition_code text,
  source        text NOT NULL,
  PRIMARY KEY (station_id, issued_at, target_ts)
);
SELECT create_hypertable('weather_forecast', 'target_ts', chunk_time_interval => INTERVAL '7 days');

-- 保留：实况 10 年（M&V 长基线归一化需要跨季跨年历史，§7.1 #19）
SELECT add_retention_policy('weather_actual', INTERVAL '10 years');
SELECT add_retention_policy('weather_forecast', INTERVAL '90 days');

-- 量级：站点 × 小时级 ≈ 每站万行/年，压缩收益趋零——不设压缩策略（与 telemetry 不同，蓝本决策）

-- 权限：algo 读写（采集 job）、api 只读、ingest 零授权（§8.1 用例 10）
GRANT SELECT, INSERT, UPDATE ON public.weather_actual, public.weather_forecast TO tsdb_algo;
GRANT SELECT ON public.weather_actual, public.weather_forecast TO tsdb_api;

-- +goose Down
SELECT remove_retention_policy('weather_forecast');
SELECT remove_retention_policy('weather_actual');
DROP TABLE IF EXISTS weather_forecast;
DROP TABLE IF EXISTS weather_actual;
