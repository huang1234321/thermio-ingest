package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// 下行通道 v0 契约（gw-sim 侧提案，待 IMPL-18 control-safety 落地时对齐——
// 设计文档 flows.md §2 序列已定义「写指令→写应答→回读指令→回读值」语义，
// JSON 信封未钉死；本文件给出可联调的最小实现，分歧在 DAT-109 评论跟踪）：
//
//	down/write（中台 → 网关）：
//	  {"msg_type":"write_cmd","ver":1,"cmd_id":"...","sent_at":"...",
//	   "writes":[{"name":"SIM_SP_0001","value":7.5}]}
//	down/read（中台 → 网关，写后回读验证）：
//	  {"msg_type":"read_cmd","ver":1,"cmd_id":"...","sent_at":"...","names":[...]}
//	应答统一发 thermio/gw/{clientid}/up/event（ingest.md §2 预留的自报事件通道）：
//	  {"msg_type":"write_ack","ver":1,"cmd_id":"...","gw":"...","sent_at":"...",
//	   "results":[{"name":"...","status":"ok","written":7.5}]}
//	  {"msg_type":"read_ack","ver":1,"cmd_id":"...","gw":"...","sent_at":"...",
//	   "values":[{"name":"...","value":7.5,"ts":"..."}]}
//
// status ∈ ok | unknown_point | rejected（越界）| failed（通讯失败注入）。

const (
	msgTypeWriteCmd = "write_cmd"
	msgTypeReadCmd  = "read_cmd"
	msgTypeWriteAck = "write_ack"
	msgTypeReadAck  = "read_ack"
)

// WriteCmd 下行写指令。
type WriteCmd struct {
	MsgType string      `json:"msg_type"`
	Ver     int         `json:"ver"`
	CmdID   string      `json:"cmd_id"`
	SentAt  string      `json:"sent_at"`
	Writes  []WriteItem `json:"writes"`
}

// WriteItem 单条写项。
type WriteItem struct {
	Name  string   `json:"name"`
	Value *float64 `json:"value"`
}

// ReadCmd 下行回读指令。
type ReadCmd struct {
	MsgType string   `json:"msg_type"`
	Ver     int      `json:"ver"`
	CmdID   string   `json:"cmd_id"`
	SentAt  string   `json:"sent_at"`
	Names   []string `json:"names"`
}

// WriteAck 写应答（up/event）。
type WriteAck struct {
	MsgType string        `json:"msg_type"`
	Ver     int           `json:"ver"`
	CmdID   string        `json:"cmd_id"`
	GW      string        `json:"gw"`
	SentAt  string        `json:"sent_at"`
	Results []WriteResult `json:"results"`
}

// WriteResult 单点写结果。
type WriteResult struct {
	Name    string   `json:"name"`
	Status  string   `json:"status"`
	Written *float64 `json:"written,omitempty"`
	Detail  string   `json:"detail,omitempty"`
}

// ReadAck 回读应答（up/event）。
type ReadAck struct {
	MsgType string      `json:"msg_type"`
	Ver     int         `json:"ver"`
	CmdID   string      `json:"cmd_id"`
	GW      string      `json:"gw"`
	SentAt  string      `json:"sent_at"`
	Values  []ReadValue `json:"values"`
}

// ReadValue 单点回读值（含设备侧采集时刻）。
type ReadValue struct {
	Name  string   `json:"name"`
	Value *float64 `json:"value"`
	TS    string   `json:"ts"`
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

// HandleMessage 处理一条下行消息，返回应答字节（不识别/解析失败返回 nil）。
func (d *DownlinkState) HandleMessage(payload []byte, gwSerial string, now time.Time) []byte {
	var probe struct {
		MsgType string `json:"msg_type"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		d.logger.Warn("downlink: unparseable message dropped", "err", err.Error())
		return nil
	}
	switch probe.MsgType {
	case msgTypeWriteCmd:
		return d.handleWrite(payload, gwSerial, now)
	case msgTypeReadCmd:
		return d.handleRead(payload, gwSerial, now)
	default:
		d.logger.Warn("downlink: unknown msg_type ignored", "msg_type", probe.MsgType)
		return nil
	}
}

// handleWrite 处理写指令：值域校验 → 寄存器更新（按回读策略）→ 应答。
func (d *DownlinkState) handleWrite(payload []byte, gwSerial string, now time.Time) []byte {
	var cmd WriteCmd
	if err := json.Unmarshal(payload, &cmd); err != nil {
		d.logger.Warn("downlink: bad write_cmd dropped", "err", err.Error())
		return nil
	}
	ack := WriteAck{
		MsgType: msgTypeWriteAck, Ver: 1, CmdID: cmd.CmdID,
		GW: gwSerial, SentAt: now.Format(sentAtLayout),
		Results: make([]WriteResult, 0, len(cmd.Writes)),
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, w := range cmd.Writes {
		res := WriteResult{Name: w.Name, Written: w.Value}
		switch {
		case w.Value == nil:
			res.Status = "rejected"
			res.Detail = "value required"
		case !d.knownSetpoint(w.Name):
			res.Status = "unknown_point"
			res.Detail = "not a simulated setpoint"
		case *w.Value < d.writeMin || *w.Value > d.writeMax:
			res.Status = "rejected"
			res.Detail = fmt.Sprintf("value %v out of [%v,%v]", *w.Value, d.writeMin, d.writeMax)
		case d.cfg.WriteFailRate > 0 && d.rng.Float64() < d.cfg.WriteFailRate:
			res.Status = "failed"
			res.Detail = "injected register-write comm failure"
			res.Written = nil
		default:
			d.applyWrite(w.Name, *w.Value, now)
			res.Status = "ok"
		}
		ack.Results = append(ack.Results, res)
	}
	b, err := json.Marshal(ack)
	if err != nil {
		d.logger.Error("downlink: marshal write_ack", "err", err.Error())
		return nil
	}
	return b
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

// handleRead 处理回读指令：按策略组值。
func (d *DownlinkState) handleRead(payload []byte, gwSerial string, now time.Time) []byte {
	var cmd ReadCmd
	if err := json.Unmarshal(payload, &cmd); err != nil {
		d.logger.Warn("downlink: bad read_cmd dropped", "err", err.Error())
		return nil
	}
	ack := ReadAck{
		MsgType: msgTypeReadAck, Ver: 1, CmdID: cmd.CmdID,
		GW: gwSerial, SentAt: now.Format(sentAtLayout),
		Values: make([]ReadValue, 0, len(cmd.Names)),
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, name := range cmd.Names {
		rv := ReadValue{Name: name, TS: FormatTS(now)}
		reg, ok := d.registers[name]
		switch {
		case !ok:
			rv.Value = nil // unknown point：value null（对端自行判定）
		default:
			v := d.readbackValue(name, reg)
			rv.Value = &v
			if ts, ok := d.lastWriteTS[name]; ok {
				rv.TS = FormatTS(ts)
			}
		}
		ack.Values = append(ack.Values, rv)
	}
	b, err := json.Marshal(ack)
	if err != nil {
		d.logger.Error("downlink: marshal read_ack", "err", err.Error())
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
