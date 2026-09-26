-- +goose Up
-- 纪律：与 PG 侧同——初始建表索引随主键内联；ts 索引由 create_hypertable 默认创建
--       （本文 §7 差异 #10），不重复建。
-- 跨库完整性（P1-2）：point_id 不设跨库 FK，注册校验在 ingest 应用层（ingest.md §6）。
CREATE TABLE telemetry (
  point_id   bigint NOT NULL,                       -- 语义身份 = PG point.id（ADR-005）
  ts         timestamptz NOT NULL,                  -- 采集真实时刻，断网补传不改（ADR-003）
  value      double precision,                      -- 数值量（归一后 std 单位）
  value_text text,                                  -- 枚态量（run_status 等，直通不归一）
  quality    smallint NOT NULL DEFAULT 0,           -- 位掩码 0=good，位表 v1 冻结（ingest.md §4）
  PRIMARY KEY (point_id, ts)
);
SELECT create_hypertable('telemetry', 'ts', chunk_time_interval => INTERVAL '7 days');

-- upsert 最小权限：ON CONFLICT DO UPDATE 需读冲突行，SELECT 是该语法的硬前置
-- （§8.1 用例 8 实测：仅 INSERT+UPDATE 时 upsert 报 permission denied）；
-- 无 DELETE（保留策略是唯一删除通道），cagg/天气零授权（§8.1 用例 10）。
GRANT INSERT, SELECT, UPDATE ON public.telemetry TO tsdb_ingest;
GRANT SELECT ON public.telemetry TO tsdb_api, tsdb_algo;

-- +goose Down
DROP TABLE IF EXISTS telemetry;
