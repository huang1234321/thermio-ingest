# db/ —— TimescaleDB 迁移链（遥测真相源）

目录契约（伞仓 ddl.md §1）：遥测真相源的迁移链归本仓——thermio-ingest 是 TSDB 的
唯一写入方。PG 侧迁移链在 thermio-platform 仓（`db/migrations/pg/`），两链版本号
各自独立。

## 执行顺序契约（ddl.md §1/§10/§5.4）

```
先 bootstrap 角色（db/bootstrap/tsdb-roles.sql）
  → 后迁移（goose，管理员执行）
    → 后起服务
```

- **bootstrap 不入版本链**：与 PG 侧同形，部署管线一次性执行；角色与口令只来自
  环境变量（SEC-KEY-01），幂等性由部署侧保证。
- **迁移执行角色 = 数据库管理员（postgres）**：0003 的 cagg 刷新/压缩/保留策略会
  注册 TimescaleDB 后台 job，job 以提交角色权限运行；「NOLOGIN owner 经 SET ROLE」
  形态对后台 job 归属未验收，v1 以管理员直跑收窄未验收面（ddl.md §5.4）。角色仅
  承担数据面权限（tsdb_ingest 窄写 / tsdb_api 只读 / tsdb_algo 读 + 天气写）。
- 对象权限 GRANT 随迁移文件内联（与 PG 侧 §4 同风格），未来新对象同理。
- **TSDB 侧不设 RLS**：遥测无 tenant_id 列，租户归属经 point_id → PG point 推导；
  角色边界是服务职能隔离，不是租户隔离（ddl.md §5.4）。

## 命令

```bash
# 1) bootstrap（口令来自环境变量）
psql -U postgres -d thermio_ts \
  -v ingest_password="$THERMIO_TSDB_INGEST_PASSWORD" \
  -v api_password="$THERMIO_TSDB_API_PASSWORD" \
  -v algo_password="$THERMIO_TSDB_ALGO_PASSWORD" \
  -f db/bootstrap/tsdb-roles.sql

# 2) 迁移（管理员）
goose -dir db/migrations/tsdb postgres "$ADMIN_DSN" up
```

迁移文件（蓝本 = 伞仓 ddl.md v1.2 §11，DAT-94 [IMPL-0]，逐字落盘）：

| 文件 | 内容 |
|---|---|
| `0001_telemetry.sql` | telemetry hypertable（7 天 chunk）+ tsdb_ingest upsert 最小权限（INSERT+SELECT+UPDATE） |
| `0002_telemetry_caggs.sql` | 双 continuous aggregate（5min/1h，直接物化，sample_count/bad_count 增列） |
| `0003_telemetry_policies.sql` | 压缩（7 天）/ 保留（2 年）/ cagg 刷新（start_offset 5 天）+ cagg 自身压缩保留 |
| `0004_weather.sql` | 天气域两表（actual 10 年 / forecast 90 天）+ algo 读写授权 |

## 验证

```bash
# 干净一次性容器上的完整冒烟（bootstrap → up → §8 用例 8–11 → down-to 0 → up）
scripts/tsdb-migration-smoke.sh   # 需 TSDB_HOST/TSDB_PORT/TSDB_SUPER_PASSWORD

# 只复跑 ddl.md §8 用例 8–11（幂等 upsert / cagg 聚合 / 压缩后补传 / 角色矩阵）
scripts/verify-tsdb.sh            # 另需三角色口令环境变量
```

CI 在一次性 `timescale/timescaledb:2.17.2-pg16` 容器上跑同一脚本
（`tsdb-migration-smoke` job）。goose 用 v3.24.0（与蓝本复验同版）；四个迁移均无
CONCURRENTLY 场景，全部默认事务（ddl.md §11 纪律），此后 TSDB 侧新增索引与 PG 同
走 CONCURRENTLY 单文件模板（ddl.md §6.2）。
