// Package main 实现 gw-sim：thermio 网关模拟器（implementation-plan.md IMPL-6）。
//
// 以可配置场景 profile 驱动 MQTT 上行模拟（ingest.md §3 契约）与下行通道模拟
// （down/write 应答 + 可编程回读值）。上行信封常量复用 internal/decode 冻结值，
// 单一契约真源；凭证一律经真实认证链路（emqx.md §3：username/password + clientid
// 绑定），密码只从环境变量读取（SEC-KEY-01），不落 profile、不落日志。
package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// Profile 是一份场景描述：网关池、点位集、采样节奏、故障注入、断网/补传与下行
// 回读策略。全部非秘密配置；秘密（密码）只经 PasswordEnv 指名的环境变量进入。
type Profile struct {
	Name        string `json:"name"`
	Description string `json:"description"`

	// DurationS 运行时长（秒）；0 = 一直运行到 SIGINT（长稳/重启窗口场景）。
	DurationS int `json:"duration_s,omitempty"`

	Gateways GatewaysCfg `json:"gateways"`
	Points   PointsCfg   `json:"points"`

	// SampleIntervalS 采样周期（秒）：每周期对全部点位采样一轮。
	SampleIntervalS int `json:"sample_interval_s"`
	// BatchPoints 单消息点位数上限（契约硬顶 500，ingest.md §3.2）。
	BatchPoints int `json:"batch_points"`
	// Seed 故障注入与数值噪声的随机种子（可复现）。
	Seed int64 `json:"seed,omitempty"`

	Faults  FaultsCfg  `json:"faults"`
	Offline OfflineCfg `json:"offline"`
	// Backfill 启动即按原始 ts 回放历史数据（断网补传场景：ts=采集时刻，
	// sent_at=补传发送时刻，ingest.md §8）。
	Backfill BackfillCfg `json:"backfill"`

	Downlink DownlinkCfg `json:"downlink"`
}

// GatewaysCfg 描述模拟网关池：同进程内 N 条独立 MQTT 连接（各自 clientid/serial/
// username，共用密码环境变量）。clientid = gateway.mqtt_client_id（emqx.md §1
// 身份锚点）；serial 为 payload `gw` 字段；username 按 emqx.md §3.2 约定
// {serial}@{tenant}。
type GatewaysCfg struct {
	Count          int    `json:"count"`
	ClientIDPrefix string `json:"clientid_prefix"`
	SerialPrefix   string `json:"serial_prefix"`
	Tenant         string `json:"tenant"`
	// UsernameTemplate 默认 "{serial}@{tenant}"。
	UsernameTemplate string `json:"username_template,omitempty"`
	// PasswordEnv 密码环境变量名（默认 GWSIM_MQTT_PASSWORD）。
	PasswordEnv string `json:"password_env,omitempty"`
}

// PointsCfg 描述点位集：数值量（正弦+噪声）、枚态量（value_text）、设定值点位
// （下行写/回读对象，遥测值跟随寄存器值）。
type PointsCfg struct {
	Count        int      `json:"count"` // 遥测点位总数（数值+枚态按比例拆分）
	NumericRatio float64  `json:"numeric_ratio"`
	Units        []string `json:"units"`         // 数值量单位轮转（degC/degF/psi/m³/h…触发单位归一路径）
	Base         float64  `json:"base"`          // 数值基线
	Amplitude    float64  `json:"amplitude"`     // 正弦幅度
	Noise        float64  `json:"noise"`         // 噪声幅度
	EnumValues   []string `json:"enum_values"`   // 枚态取值域
	Setpoints    int      `json:"setpoints"`     // 额外设定值点位数（可写）
	SetpointBase float64  `json:"setpoint_base"` // 设定值初始值
	// WriteMin/WriteMax 设定值合法写入域（越界写应答 rejected）。
	WriteMin float64 `json:"write_min"`
	WriteMax float64 `json:"write_max"`
}

// FaultsCfg 全部故障注入旋钮（默认零值 = 全关）。rate 类按点位独立投掷；
// every 类按消息序号触发（第 N 的倍数条消息注入一次）。
type FaultsCfg struct {
	BadQualityRate         float64 `json:"bad_quality_rate,omitempty"`       // quality="bad"（bit1 device_bad 源头）
	UncertainQualityRate   float64 `json:"uncertain_quality_rate,omitempty"` // quality="uncertain"
	NullValueRate          float64 `json:"null_value_rate,omitempty"`        // value=null（采不到）
	TSkewS                 int     `json:"ts_skew_s,omitempty"`              // 网关时钟偏移秒数（>600 触发 bit5）
	SeqGapEvery            int     `json:"seq_gap_every,omitempty"`          // 每 N 条消息跳号一次
	SeqGapSize             int     `json:"seq_gap_size,omitempty"`
	GWMismatchEvery        int     `json:"gw_mismatch_every,omitempty"`    // 每 N 条消息 gw 字段错配（GW_MISMATCH）
	UnknownPointsEvery     int     `json:"unknown_points_every,omitempty"` // 每 N 条消息夹带未注册点
	UnknownPointsCount     int     `json:"unknown_points_count,omitempty"`
	InvalidTSEvery         int     `json:"invalid_ts_every,omitempty"`          // 点 ts 不可解析（TS_INVALID）
	TSBeyondRetentionEvery int     `json:"ts_beyond_retention_every,omitempty"` // 点 ts = now-3 年（TS_BEYOND_RETENTION）
	MalformedEvery         int     `json:"malformed_every,omitempty"`           // 发布非法 JSON 字节（MALFORMED_JSON）
	UnknownMsgTypeEvery    int     `json:"unknown_msg_type_every,omitempty"`    // msg_type 未知（UNKNOWN_MSG_TYPE）
	UnsupportedVerEvery    int     `json:"unsupported_ver_every,omitempty"`     // ver 越级（UNSUPPORTED_VER）
	OversizeEvery          int     `json:"oversize_every,omitempty"`            // 超限（PAYLOAD_TOO_LARGE）
	OversizeMode           string  `json:"oversize_mode,omitempty"`             // "points"（>500 点）| "bytes"（>256KB）
}

// OfflineCfg 模拟网关侧断网：窗口内断开 MQTT、采样继续进本地缓存；恢复后按
// 原始 ts/seq 顺序重发（ADR-003 硬指标 3：断网补传不改采集时间戳）。
type OfflineCfg struct {
	Windows []OfflineWindow `json:"windows,omitempty"`
	// BufferMaxRows 缓存行数上限（行=点位样本）；超限丢最旧（网关环形缓存语义）。
	BufferMaxRows int `json:"buffer_max_rows,omitempty"`
}

// OfflineWindow 相对启动时刻的断网窗口。
type OfflineWindow struct {
	AfterS    int `json:"after_s"`
	DurationS int `json:"duration_s"`
}

// BackfillCfg 启动即回放历史：从 now-days 到 now，按 StepS 步进生成历史样本，
// ts=历史采集时刻、sent_at=当前（补传语义）。days>7 时样本落入已压缩 chunk
// （TSDB 压缩策略 7 天，db/migrations/tsdb/0003）。
type BackfillCfg struct {
	Enabled bool `json:"enabled"`
	Days    int  `json:"days,omitempty"`
	StepS   int  `json:"step_s,omitempty"`
	// PublishIntervalMs 回放限速（避免瞬时打爆 EMQX inflight）。
	PublishIntervalMs int `json:"publish_interval_ms,omitempty"`
}

// DownlinkCfg 下行通道行为：write_cmd 应答策略与回读值策略。
// Readback 模式：
//   - echo   写成功即更新寄存器，回读=写入值（默认，回读一致）
//   - pinned 写不动寄存器，回读恒为 PinnedValue（回读不一致，IMPL-18 回滚演练）
//   - stale  写不动寄存器，回读=旧值（写入未生效的另一种形态）
//   - offset 写更新寄存器，回读=寄存器值+Offset（偏差型不一致）
type DownlinkCfg struct {
	Enabled       bool    `json:"enabled"`
	Readback      string  `json:"readback,omitempty"`
	PinnedValue   float64 `json:"readback_pinned_value,omitempty"`
	Offset        float64 `json:"readback_offset,omitempty"`
	WriteFailRate float64 `json:"write_fail_rate,omitempty"` // 模拟写寄存器通讯失败（status=failed）
}

// LoadProfile 读取并校验 profile 文件。
func LoadProfile(path string) (*Profile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read profile: %w", err)
	}
	var p Profile
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("parse profile %s: %w", path, err)
	}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("profile %s invalid: %w", path, err)
	}
	return &p, nil
}

// Validate 填默认值并校验约束（契约硬顶在此拦截，坏 profile 直接报错退出）。
func (p *Profile) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("name is required")
	}
	if p.SampleIntervalS <= 0 {
		return fmt.Errorf("sample_interval_s must be > 0")
	}
	if p.BatchPoints <= 0 {
		p.BatchPoints = 500
	}
	if p.BatchPoints > 500 {
		return fmt.Errorf("batch_points %d exceeds contract max 500 (ingest.md §3.2)", p.BatchPoints)
	}
	if p.Points.Count < 0 || p.Points.Setpoints < 0 {
		return fmt.Errorf("points.count/setpoints must be >= 0")
	}
	if p.Points.NumericRatio < 0 || p.Points.NumericRatio > 1 {
		return fmt.Errorf("points.numeric_ratio must be in [0,1]")
	}
	if p.Gateways.Count <= 0 {
		p.Gateways.Count = 1
	}
	if p.Gateways.ClientIDPrefix == "" {
		p.Gateways.ClientIDPrefix = "GW-SIM-"
	}
	if p.Gateways.SerialPrefix == "" {
		p.Gateways.SerialPrefix = "GWSIM"
	}
	if p.Gateways.Tenant == "" {
		p.Gateways.Tenant = "tenant-sim"
	}
	if p.Gateways.UsernameTemplate == "" {
		p.Gateways.UsernameTemplate = "{serial}@{tenant}"
	}
	if p.Gateways.PasswordEnv == "" {
		p.Gateways.PasswordEnv = "GWSIM_MQTT_PASSWORD"
	}
	if p.Points.Units == nil {
		p.Points.Units = []string{"degC"}
	}
	if p.Points.EnumValues == nil {
		p.Points.EnumValues = []string{"stopped", "running"}
	}
	if p.Points.WriteMin == 0 && p.Points.WriteMax == 0 {
		p.Points.WriteMin, p.Points.WriteMax = -1e9, 1e9
	}
	if p.Seed == 0 {
		p.Seed = 1
	}

	f := &p.Faults
	for _, r := range []struct {
		name string
		v    float64
	}{
		{"bad_quality_rate", f.BadQualityRate},
		{"uncertain_quality_rate", f.UncertainQualityRate},
		{"null_value_rate", f.NullValueRate},
		{"write_fail_rate", p.Downlink.WriteFailRate},
	} {
		if r.v < 0 || r.v > 1 {
			return fmt.Errorf("faults.%s must be in [0,1]", r.name)
		}
	}
	if f.OversizeMode == "" {
		f.OversizeMode = "points"
	}
	if f.OversizeMode != "points" && f.OversizeMode != "bytes" {
		return fmt.Errorf("faults.oversize_mode must be points|bytes")
	}
	if f.SeqGapEvery > 0 && f.SeqGapSize <= 0 {
		f.SeqGapSize = 1
	}
	if f.UnknownPointsEvery > 0 && f.UnknownPointsCount <= 0 {
		f.UnknownPointsCount = 1
	}

	if p.Offline.BufferMaxRows <= 0 {
		p.Offline.BufferMaxRows = 1_000_000
	}
	for i, w := range p.Offline.Windows {
		if w.DurationS <= 0 {
			return fmt.Errorf("offline.windows[%d].duration_s must be > 0", i)
		}
	}

	if p.Backfill.Enabled {
		if p.Backfill.Days <= 0 {
			return fmt.Errorf("backfill.days must be > 0 when enabled")
		}
		if p.Backfill.StepS <= 0 {
			p.Backfill.StepS = 300
		}
		if p.Backfill.PublishIntervalMs <= 0 {
			p.Backfill.PublishIntervalMs = 20
		}
	}

	if p.Downlink.Readback == "" {
		p.Downlink.Readback = "echo"
	}
	switch p.Downlink.Readback {
	case "echo", "pinned", "stale", "offset":
	default:
		return fmt.Errorf("downlink.readback must be echo|pinned|stale|offset")
	}
	return nil
}
