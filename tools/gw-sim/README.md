# gw-sim — thermio 网关模拟器（IMPL-6）

场景 profile 驱动的网关模拟器：MQTT 上行遥测（ingest.md §2–§3 契约）+ 下行通道
（`down/write` 写应答与可编程回读值）。供 ingest 管线（IMPL-5）联调与链路 E2E
（IMPL-9 场景套件）驱动使用。

- 上行契约常量复用 `internal/decode`（单一真源）；本工具不引入新依赖
  （paho.mqtt.golang，ADR-016 指定库）。
- **凭证走真实认证链路，无白名单后门**：username 按 `{serial}@{tenant}` 约定
  （emqx.md §3.2），密码只经环境变量注入（SEC-KEY-01），不进 profile、不进
  命令行、不落日志。broker 是否核验由部署侧决定（dev compose 内置认证 /
  IMPL-8 钩子模式），gw-sim 永远以带凭证的 CONNECT 接入。

## 快速开始

```bash
cd tools/gw-sim
export GWSIM_BROKER_URL=tcp://localhost:1883     # 伞仓 deploy/docker-compose.dev.yml 的 thermio-emqx
export GWSIM_MQTT_PASSWORD='<设备密码>'           # dev 内置认证：预置用户；IMPL-7/8 钩子模式：平台注册的 device_credential
go run . -profile profiles/normal-10k.json -summary-file /tmp/run.json
```

- 运行摘要（发布消息/点位/字节数、注入故障分类计数、断网窗口数、发布失败数）
  退出时打印，并可经 `-summary-file` 落盘（E2E 执行留痕）。
- `-duration 0` 且 profile `duration_s: 0` = 运行到 SIGINT（长稳场景）；退出前
  尽力冲刷缓存（10s 上限）。
- TLS：broker URL 用 `ssl://`/`tls://`，自签链经 `GWSIM_TLS_CA_FILE` 显式信任
  （SEC-NET-04；不提供跳过校验开关）。

## 前置：dev 栈接入

环境隔离纪律：一律用伞仓 `deploy/docker-compose.dev.yml` 独立栈（thermio- 前缀），
禁止连宿主机既有 MQTT/Kafka/PG/EMQX。

**dev compose 当前为 EMQX 内置认证/匿名模式**（IMPL-8 钩子模式开关未落），
为 gw-sim 预置与真实链路同形的账号（以 dashboard 或管理 API 建用户）：

```bash
docker exec -it thermio-emqx emqx ctl users add \
  "GWSIM001@tenant-sim" "$GWSIM_MQTT_PASSWORD"
```

IMPL-7/8 落地后：凭证改由平台 `POST /internal/mqtt/authenticate` 链路注册
（device_credential + gateway 绑定），gw-sim 侧零改动——这正是「不做白名单后门」
的验收口径：模拟器行为 = 真实网关行为。

## 场景矩阵（每场景一条命令）

### A. IMPL-9 E2E 场景清单

| # | 场景 | 命令（`cd tools/gw-sim` 后执行） | 期望观测 |
|---|---|---|---|
| 1 | 正常采集万点级 | `go run . -profile profiles/normal-10k.json` | telemetry 全量落库、quality=0、raw topic 可消费、DLQ 零增长 |
| 2 | 坏点 / null | `go run . -profile profiles/bad-null.json` | 数据不丢；bit1 device_bad / bit2 null_value 置位；quality 事件 |
| 3 | 未注册点 | `go run . -profile profiles/unregistered-points.json` | 点级 DLQ `UNREGISTERED_POINT`；`ingest_unregistered_points_total` 递增 |
| 4a | 断网 3 天补传 | `go run . -profile profiles/backfill-3d.json` | 原始 ts 乱序入库、bit8 backfill 置位、同网关 Kafka 同分区保序 |
| 4b | 补传落入已压缩 chunk | `go run . -profile profiles/backfill-8d-compressed.json` | 8 天数据写入自动解压（ddl.md §8 用例 11），无 DLQ |
| 5 | 背压 | `go run . -profile profiles/backpressure.json` | ingest 缓冲打满 → TCP 背压；gw-sim 发布超时进缓存不丢数；恢复排水 |
| 6 | EMQX 重启窗口（会话 ≥1h） | `go run . -profile profiles/emqx-restart-window.json -duration 70m &` 然后 `docker restart thermio-emqx` | clean_session=false 会话保持；QoS1 重投 + 缓存重试；窗口数据不丢 |
| 7 | ingest 滚动重启 | `go run . -profile profiles/ingest-rolling-restart.json &` 然后重启 ingest | 重启窗口前后 TSDB 行数差 = 采样行数（真相源无丢失） |

### B. IMPL-6 故障注入（DLQ 负路径，网关侧可触达的全集）

| 场景 | 命令 | 预期 DLQ reason / 观测 |
|---|---|---|
| 坏点率 / null 值 | `go run . -profile profiles/bad-null.json` | 质量位（见 A-2） |
| 时钟偏移 | `go run . -profile profiles/ts-skew.json` | bit5 ts_skew + 质量事件（偏移 15min > 10min 阈值） |
| seq 缺口 | `go run . -profile profiles/seq-gap.json` | 不拒收；缺口指标 + WARN |
| 断网补传（实时窗口） | `go run . -profile profiles/offline-buffer-retransmit.json` | 30s 后断链 120s：缓存重发、原始 ts/seq、无丢失 |
| 超长 payload（点数） | `go run . -profile profiles/oversize-points.json` | `PAYLOAD_TOO_LARGE`（>500 点），PUBACK 照常 |
| 超长 payload（字节） | `go run . -profile profiles/oversize-bytes.json` | `PAYLOAD_TOO_LARGE`（>256KB） |
| 错 gw 字段 | `go run . -profile profiles/gw-mismatch.json` | `GW_MISMATCH`（安全关注项） |
| 非法 JSON | `go run . -profile profiles/malformed-json.json` | `MALFORMED_JSON` |
| 未知 msg_type | `go run . -profile profiles/unknown-msg-type.json` | `UNKNOWN_MSG_TYPE` |
| 越级 ver | `go run . -profile profiles/unsupported-ver.json` | `UNSUPPORTED_VER` |
| 不可解析 ts | `go run . -profile profiles/invalid-ts.json` | `TS_INVALID`（点级） |
| ts 超保留期 | `go run . -profile profiles/ts-beyond-retention.json` | `TS_BEYOND_RETENTION`（不写库，计数留档） |

> 网关侧不可触达的 DLQ reason（服务端/配置态）：`UNKNOWN_GATEWAY`（clientid 未
> 注册）、`POINT_INACTIVE`（点位停用）、`UNIT_UNCONVERTED`（无转换对——由
> 点表 unit 配置与上报单位组合决定，可用任意 profile + 改 points.units 成
> 生僻单位触发）、`TSDB_WRITE_FAILED`（TSDB 故障注入）。

### C. 下行通道（write 应答 + 可编程回读）

先起模拟器，再向 `thermio/gw/GW-SIM-001/down/write`（或 `/down/read`）发指令：

```bash
# 写设定值 → up/event 收 write_ack
mosquitto_pub -h localhost -p 1883 -u '<内部账号>' \
  -t thermio/gw/GW-SIM-001/down/write -q 1 -m '{
    "msg_type":"write_cmd","ver":1,"cmd_id":"c1",
    "sent_at":"2026-09-26T15:00:00+08:00",
    "writes":[{"name":"SIM_SP_0001","value":7.5}]}'

# 写后回读 → up/event 收 read_ack
mosquitto_pub -h localhost -p 1883 -u '<内部账号>' \
  -t thermio/gw/GW-SIM-001/down/read -q 1 -m '{
    "msg_type":"read_cmd","ver":1,"cmd_id":"c2",
    "sent_at":"2026-09-26T15:00:05+08:00",
    "names":["SIM_SP_0001"]}'

# 观察应答
mosquitto_sub -h localhost -p 1883 -u '<内部账号>' \
  -t 'thermio/gw/GW-SIM-001/up/event' -q 1 -C 2 -v
```

| 场景 | 命令 | 行为 |
|---|---|---|
| 下行正常链路 | `go run . -profile profiles/downlink-echo.json` | write_ack(ok)；read_ack 回读=写入值；遥测跟随新值 |
| 回读不一致（IMPL-18 回滚演练） | `go run . -profile profiles/downlink-readback-mismatch.json` | write_ack 仍 ok，但 read_ack 恒为 20 → 写后回读验证必失败，驱动重试/回写原值路径 |
| 写失败注入 | 复制 downlink-echo.json 改 `downlink.write_fail_rate: 1.0` 后运行 | write_ack(status=failed) |
| 越界写 | 运行任一下行 profile，向 SIM_SP_* 写 [-∞,0)∪(100,+∞) 值 | write_ack(status=rejected, detail=值域) |

**下行 v0 契约**（本工具单方面提案，待 IMPL-18 control-safety 落地时对齐；
flows.md §2 已定义语义，JSON 信封未钉死——分歧在 DAT-109 跟踪）：

- `down/write`：`{"msg_type":"write_cmd","ver":1,"cmd_id","sent_at","writes":[{"name","value"}]}`
- `down/read`：`{"msg_type":"read_cmd","ver":1,"cmd_id","sent_at","names":[...]}`
- 应答（`up/event`，ingest.md §2 预留的自报事件通道）：
  `{"msg_type":"write_ack","ver":1,"cmd_id","gw","sent_at","results":[{"name","status","written?","detail?"}]}`
  / `{"msg_type":"read_ack","ver":1,"cmd_id","gw","sent_at","values":[{"name","value","ts"}]}`
- `status ∈ ok | unknown_point | rejected | failed`

## Profile 结构速览

```jsonc
{
  "gateways": { "count": 20, "clientid_prefix": "GW-SIM-", "serial_prefix": "GWSIM",
                "tenant": "tenant-sim", "username_template": "{serial}@{tenant}",
                "password_env": "GWSIM_MQTT_PASSWORD" },
  "points":   { "count": 500, "numeric_ratio": 0.95, "units": ["degC","degF"],
                "base": 20, "amplitude": 5, "noise": 0.2,
                "enum_values": ["stopped","running"],
                "setpoints": 20, "setpoint_base": 21, "write_min": 0, "write_max": 100 },
  "sample_interval_s": 1, "batch_points": 500, "duration_s": 120, "seed": 1,
  "faults":   { "bad_quality_rate": 0.1, "null_value_rate": 0.2, "ts_skew_s": 900,
                "seq_gap_every": 3, "gw_mismatch_every": 0, "unknown_points_every": 0,
                "malformed_every": 0, "unknown_msg_type_every": 0, "unsupported_ver_every": 0,
                "invalid_ts_every": 0, "ts_beyond_retention_every": 0,
                "oversize_every": 0, "oversize_mode": "points" },
  "offline":  { "windows": [ { "after_s": 30, "duration_s": 120 } ], "buffer_max_rows": 1000000 },
  "backfill": { "enabled": true, "days": 3, "step_s": 300, "publish_interval_ms": 20 },
  "downlink": { "enabled": true, "readback": "echo|pinned|stale|offset",
                "readback_pinned_value": 20, "readback_offset": 0.5, "write_fail_rate": 0 }
}
```

回读模式：`echo`（写生效，回读=写入值）/ `pinned`（写不动设备，回读恒为定值）/
`stale`（写不动设备，回读=旧值）/ `offset`（写生效，回读=值+偏差）——后三者即
「回读不一致」的三种注入形态。多网关时点位名带网关序号前缀（`SIM3_0001`），
天然隔离点表注册域。

## 已知边界

- **sent_at 语义**：断网缓存消息保留组装时刻的 `sent_at`（重发不重写）；`ts`
  恒为采集原始时刻（契约硬指标）。bit8 backfill 判定只依赖 `ts`，不受影响。
- 下行应答不走缓存重试（指令时效性优先，失败即丢 + 计数）——与遥测「数据
  永不丢」语义有意不同。
- `points.units` 只影响上报 payload 的 `unit` 字段（信息性）；`UNIT_UNCONVERTED`
  由 ingest 侧点表配置裁决。
- 单进程多网关共享一条进程内日志流；每网关独立连接/seq/故障计数（种子
  `seed + 1000×idx` 可复现）。
