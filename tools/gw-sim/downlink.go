package main

import (
	"encoding/json"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// 下行通道契约（control-safety.md §4.2/§4.3 定稿；shared-types ControlWriteCommand /
// ControlUpEvent 单源对齐——DAT-109 契约分歧收口，G4 预演 DAT-202 实跑对齐）：
//
//	down/write（中台 svc-control → 网关，QoS1）：
//	  {"msg_type":"write_cmd","ver":1,"cmd_id":"<uuid>","point_ref":"SIM_SP_0001",
//	   "value":7.5,"unit":"degC","issued_at":"...","expires_in_s":30}
//	down/read（写后回读验证，同 topic，msg_type 区分）：
//	  {"msg_type":"read_cmd","ver":1,"cmd_id":"<uuid>","point_ref":"SIM_SP_0001",
//	   "issued_at":"...","expires_in_s":30}
//	应答统一发 thermio/gw/{clientid}/up/event：
//	  {"msg_type":"write_ack","ver":1,"cmd_id":"...","gw":"<clientid>",
//	   "result":"accepted","code":null,"at":"..."}
//	  {"msg_type":"read_result","ver":1,"cmd_id":"...","gw":"<clientid>",
//	   "value":7.5,"unit":null,"quality":"good","ts":"...","at":"..."}
//
// 语义映射（旧 v0 批量形态 → 定稿单点形态）：
//   - ok              → result=accepted, code=null
//   - unknown_point   → result=rejected, code=POINT_UNKNOWN
//   - 越界 / 缺 value → result=rejected, code=WRITE_REFUSED
//   - 指令过期        → result=rejected, code=CMD_EXPIRED（expires_in_s 窗口外）
//   - 写失败注入      → 不应答（§4.4 超时路径：null ack 交回读仲裁）
//
// gw 字段 = MQTT clientid（§4.3：payload.gw 必须与 topic clientid 一致，否则对端丢弃）。

const (
	msgTypeWriteCmd   = "write_cmd"
	msgTypeReadCmd    = "read_cmd"
	msgTypeWriteAck   = "write_ack"
	msgTypeReadResult = "read_result"

	ackAccepted = "accepted"
	ackRejected = "rejected"

	codePointUnknown = "POINT_UNKNOWN"
	codeWriteRefused = "WRITE_REFUSED"
	codeCmdExpired   = "CMD_EXPIRED"

	qualityGood = "good"
	qualityBad  = "bad"
)

// WriteCmd 下行写/读指令（定稿信封：单 point_ref；read_cmd 无 value/unit）。
type WriteCmd struct {
	MsgType    string   `json:"msg_type"`
	Ver        int      `json:"ver"`
	CmdID      string   `json:"cmd_id"`
	PointRef   string   `json:"point_ref"`
	Value      *float64 `json:"value"`
	Unit       *string  `json:"unit"`
	IssuedAt   string   `json:"issued_at"`
	ExpiresInS *int64   `json:"expires_in_s"`
}

// WriteAck 写应答（up/event，定稿单点形态）。
type WriteAck struct {
	MsgType string  `json:"msg_type"`
	Ver     int     `json:"ver"`
	CmdID   string  `json:"cmd_id"`
	GW      string  `json:"gw"`
	Result  string  `json:"result"`
	Code    *string `json:"code"`
	At      string  `json:"at"`
}

// ReadResult 回读应答（up/event，定稿单点形态；quality≠good 对端按读失败计）。
type ReadResult struct {
	MsgType string   `json:"msg_type"`
	Ver     int      `json:"ver"`
	CmdID   string   `json:"cmd_id"`
	GW      string   `json:"gw"`
	Value   *float64 `json:"value"`
	Unit    *string  `json:"unit"`
	Quality string   `json:"quality"`
	TS      *string  `json:"ts"`
	At      string   `json:"at"`
}

// DownlinkState 网关侧下行通道状态：寄存器值 + 写入域 + 回读策略。
// 并发模型：写/读指令在 paho 回调 goroutine 处理，遥测采样在引擎 goroutine 读
// 寄存器——mu 保护全部 map 与 rng。
type DownlinkState struct {
	mu       sync.Mutex
	cfg      DownlinkCfg
	writeMin float64
	writeMax float64
	// registers：设定值点位名 → 当前设备值（echo/offset 模式写入后更新）。
	registers map[string]float64
	// lastWritten：设定值点位名 → 最近一次下发值（stale/pinned 回读对照用）。
	lastWritten map[string]float64
	// lastWriteTS：点位名 → 最近写应答时刻（回读值 ts）。
	lastWriteTS map[string]time.Time
	rng         *rand.Rand
	logger      *slog.Logger
}

// NewDownlinkState 构造下行状态。
func NewDownlinkState(cfg DownlinkCfg, writeMin, writeMax float64, seed int64, logger *slog.Logger) *DownlinkState {
	return &DownlinkState{
		cfg:         cfg,
		writeMin:    writeMin,
		writeMax:    writeMax,
		registers:   make(map[string]float64),
		lastWritten: make(map[string]float64),
		lastWriteTS: make(map[string]time.Time),
		rng:         rand.New(rand.NewSource(seed)),
		logger:      logger,
	}
}

// InitSetpoint 注册设定值点位初始值（engine 建点时调用）。
func (d *DownlinkState) InitSetpoint(name string, v float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.registers[name] = v
}

// RegisterValue 返回点位当前设备值（遥测采样跟随；未注册点 ok=false）。
func (d *DownlinkState) RegisterValue(name string) (float64, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.registers[name]
	return v, ok
}

// HandleMessage 处理一条下行消息，返回应答字节（不识别/解析失败/写失败注入返回 nil）。
func (d *DownlinkState) HandleMessage(payload []byte, gwClientID string, now time.Time) []byte {
	var probe struct {
		MsgType string `json:"msg_type"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		d.logger.Warn("downlink: unparseable message dropped", "err", err.Error())
		return nil
	}
	switch probe.MsgType {
	case msgTypeWriteCmd:
		return d.handleWrite(payload, gwClientID, now)
	case msgTypeReadCmd:
		return d.handleRead(payload, gwClientID, now)
	default:
		d.logger.Warn("downlink: unknown msg_type ignored", "msg_type", probe.MsgType)
		return nil
	}
}

// writeAckOf 构造写应答字节（marshal 失败返回 nil）。
func writeAckOf(cmdID, gwClientID, result string, code *string, now time.Time) []byte {
	b, err := json.Marshal(WriteAck{
		MsgType: msgTypeWriteAck, Ver: 1, CmdID: cmdID,
		GW: gwClientID, Result: result, Code: code, At: now.Format(sentAtLayout),
	})
	if err != nil {
		return nil
	}
	return b
}

// expired 指令时效判定（issued_at + expires_in_s 窗口外 = 过期；不可解析字段宽容忽略）。
func (d *DownlinkState) expired(cmd WriteCmd, now time.Time) bool {
	if cmd.IssuedAt == "" || cmd.ExpiresInS == nil || *cmd.ExpiresInS <= 0 {
		return false
	}
	issued, err := time.Parse(time.RFC3339, cmd.IssuedAt)
	if err != nil {
		return false // 解析失败不拦执行（对端时钟格式问题不应吞指令）
	}
	return now.After(issued.Add(time.Duration(*cmd.ExpiresInS) * time.Second))
}

// handleWrite 处理写指令：时效/值域校验 → 寄存器更新（按回读策略）→ 应答。
// 写失败注入（write_fail_rate）= 不应答：§4.4 超时路径，null ack 交对端回读仲裁。
func (d *DownlinkState) handleWrite(payload []byte, gwClientID string, now time.Time) []byte {
	var cmd WriteCmd
	if err := json.Unmarshal(payload, &cmd); err != nil {
		d.logger.Warn("downlink: bad write_cmd dropped", "err", err.Error())
		return nil
	}
	switch {
	case d.expired(cmd, now):
		return writeAckOf(cmd.CmdID, gwClientID, ackRejected, strPtr(codeCmdExpired), now)
	case cmd.Value == nil:
		return writeAckOf(cmd.CmdID, gwClientID, ackRejected, strPtr(codeWriteRefused), now)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	switch {
	case !d.knownSetpoint(cmd.PointRef):
		return writeAckOf(cmd.CmdID, gwClientID, ackRejected, strPtr(codePointUnknown), now)
	case *cmd.Value < d.writeMin || *cmd.Value > d.writeMax:
		return writeAckOf(cmd.CmdID, gwClientID, ackRejected, strPtr(codeWriteRefused), now)
	case d.cfg.WriteFailRate > 0 && d.rng.Float64() < d.cfg.WriteFailRate:
		// 注入形态：寄存器写通讯失败——不应答（超时路径），遥测/回读维持旧值。
		d.logger.Warn("downlink: injected register-write comm failure", "cmd_id", cmd.CmdID)
		return nil
	default:
		d.applyWrite(cmd.PointRef, *cmd.Value, now)
	}
	return writeAckOf(cmd.CmdID, gwClientID, ackAccepted, nil, now)
}

// knownSetpoint 点位是否为已注册设定值（调用方持锁）。
func (d *DownlinkState) knownSetpoint(name string) bool {
	_, ok := d.registers[name]
	return ok
}

// applyWrite 按回读策略落寄存器（pinned/stale：写不动设备值——回读不一致注入）。
func (d *DownlinkState) applyWrite(name string, v float64, now time.Time) {
	d.lastWritten[name] = v
	d.lastWriteTS[name] = now
	switch d.cfg.Readback {
	case "pinned", "stale":
		// 设备值不变（写入未生效形态）。
	default: // echo | offset
		d.registers[name] = v
	}
}

// handleRead 处理回读指令：按策略组值（unknown point → value null + quality bad）。
// 过期 read_cmd 与 write 同判（control-safety §4 信封 expires_in_s 对 write_cmd/
// read_cmd 两形态同列生效——DAT-211 清理轮对齐）：不以新读数背书旧指令，回
// value null + quality bad（对端按读失败/超时节奏处理，§5.2）。
func (d *DownlinkState) handleRead(payload []byte, gwClientID string, now time.Time) []byte {
	var cmd WriteCmd // read_cmd 与 write_cmd 同信封（无 value/unit）
	if err := json.Unmarshal(payload, &cmd); err != nil {
		d.logger.Warn("downlink: bad read_cmd dropped", "err", err.Error())
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	res := ReadResult{
		MsgType: msgTypeReadResult, Ver: 1, CmdID: cmd.CmdID,
		GW: gwClientID, Value: nil, Unit: nil,
		Quality: qualityBad, At: now.Format(sentAtLayout),
	}
	if reg, ok := d.registers[cmd.PointRef]; ok && !d.expired(cmd, now) {
		v := d.readbackValue(cmd.PointRef, reg)
		ts := now
		if t, ok := d.lastWriteTS[cmd.PointRef]; ok {
			ts = t
		}
		formatted := FormatTS(ts)
		res.Value = &v
		res.Quality = qualityGood
		res.TS = &formatted
	}
	b, err := json.Marshal(res)
	if err != nil {
		d.logger.Error("downlink: marshal read_result", "err", err.Error())
		return nil
	}
	return b
}

// readbackValue 回读策略取值（写后回读验证的对端读到的值）。
func (d *DownlinkState) readbackValue(name string, reg float64) float64 {
	switch d.cfg.Readback {
	case "pinned":
		return d.cfg.PinnedValue
	case "offset":
		return reg + d.cfg.Offset
	default: // echo | stale
		return reg
	}
}

func strPtr(s string) *string { return &s }
