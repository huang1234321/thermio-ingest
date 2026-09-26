package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// mqttIO 是 paho 薄封装：MQTT 3.1.1 + QoS1 + 持久会话（clean_session=false，
// ingest.md §2「会话不清除」）；断线重连、下行订阅与尽力发布。
// 补传缓存不在此层——引擎级单一 outbox 保证原始 ts/seq 顺序（ADR-003 硬指标 3）。
type mqttIO struct {
	clientID  string
	brokerURL string
	username  string
	password  string
	logger    *slog.Logger

	client    mqtt.Client
	connected atomic.Bool
	// onState 连接状态翻转回调（通知 drain 循环）。
	onState func(online bool)
}

// TopicUpData 上行遥测 topic（契约 §2）。
func TopicUpData(clientID string) string { return "thermio/gw/" + clientID + "/up/data" }

// TopicUpEvent 上行自报事件 topic（契约 §2 预留通道，下行应答承载）。
func TopicUpEvent(clientID string) string { return "thermio/gw/" + clientID + "/up/event" }

// TopicDownFilter 下行订阅 filter（ACL 规则 2：网关只订自己的 down/#）。
func TopicDownFilter(clientID string) string { return "thermio/gw/" + clientID + "/down/#" }

// newMQTTIO 构造（不连接）。
func newMQTTIO(brokerURL, clientID, username, password string, logger *slog.Logger) *mqttIO {
	return &mqttIO{
		clientID: clientID, brokerURL: brokerURL,
		username: username, password: password, logger: logger,
	}
}

// connect 建立连接（每次新建 client：paho Disconnect 后 client 不可复用；
// clean_session=false 保证 broker 侧会话/订阅延续）。onMessage 收下行。
// ctx 取消时中止等待（凭证被拒/网络不通时不空转到超时）。
func (m *mqttIO) connect(ctx context.Context, onMessage func(topic string, payload []byte)) error {
	opts := mqtt.NewClientOptions().
		AddBroker(m.brokerURL).
		SetClientID(m.clientID).
		SetUsername(m.username).
		SetPassword(m.password).
		SetCleanSession(false). // 持久会话：重启窗口消息由 EMQX 会话保持
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		SetKeepAlive(30 * time.Second).
		SetPingTimeout(10 * time.Second).
		SetConnectTimeout(10 * time.Second).
		SetResumeSubs(true)

	if tlsCfg, err := loadTLSConfig(); err != nil {
		return err
	} else if tlsCfg != nil {
		opts.SetTLSConfig(tlsCfg)
	}

	opts.OnConnect = func(c mqtt.Client) {
		m.connected.Store(true)
		m.logger.Info("mqtt connected", "broker", redactBroker(m.brokerURL), "clientid", m.clientID)
		if onMessage != nil {
			tok := c.Subscribe(TopicDownFilter(m.clientID), 1, func(_ mqtt.Client, msg mqtt.Message) {
				onMessage(msg.Topic(), msg.Payload())
			})
			if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
				m.logger.Error("mqtt subscribe down/# failed", "err", tok.Error())
			}
		}
		if m.onState != nil {
			m.onState(true)
		}
	}
	opts.OnConnectionLost = func(_ mqtt.Client, err error) {
		m.connected.Store(false)
		m.logger.Warn("mqtt connection lost", "err", err.Error())
		if m.onState != nil {
			m.onState(false)
		}
	}

	m.client = mqtt.NewClient(opts)
	tok := m.client.Connect()
	select {
	case <-tok.Done():
		if err := tok.Error(); err != nil {
			return fmt.Errorf("mqtt connect (broker %s): %w", redactBroker(m.brokerURL), err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("mqtt connect abandoned (broker %s): %w", redactBroker(m.brokerURL), ctx.Err())
	}
}

// disconnect 主动断开（模拟断网窗口；broker 侧会话保持）。
func (m *mqttIO) disconnect() {
	if m.client != nil && m.client.IsConnected() {
		m.client.Disconnect(250)
	}
	m.connected.Store(false)
	if m.onState != nil {
		m.onState(false)
	}
}

// online 报告连接状态。
func (m *mqttIO) online() bool { return m.connected.Load() }

// publish QoS1 发布；返回错误由引擎决定入 outbox 重试。
func (m *mqttIO) publish(topic string, payload []byte) error {
	if m.client == nil || !m.client.IsConnectionOpen() {
		return fmt.Errorf("mqtt not connected")
	}
	tok := m.client.Publish(topic, 1, false, payload)
	if !tok.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("publish timeout (topic %s)", topic)
	}
	if err := tok.Error(); err != nil {
		return fmt.Errorf("publish (topic %s): %w", topic, err)
	}
	return nil
}

// loadTLSConfig 处理 tls:// broker：可选自签 CA（GWSIM_TLS_CA_FILE，
// SEC-NET-04 显式信任链；不提供全局跳过校验的后门）。
func loadTLSConfig() (*tls.Config, error) {
	caFile := os.Getenv("GWSIM_TLS_CA_FILE")
	if caFile == "" {
		return nil, nil // tcp:// 直连或 tls:// 走系统根
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read GWSIM_TLS_CA_FILE: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certs parsed from %s", caFile)
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}

// redactBroker 日志里的 broker 地址不含 userinfo 凭证成分（防御性，CODE-LOG-01 同思路）。
func redactBroker(raw string) string {
	if i := strings.IndexByte(raw, '@'); i >= 0 {
		return raw[:i+1] + "***"
	}
	return raw
}
