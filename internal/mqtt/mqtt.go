// Package mqtt 负责 EMQX 接入（ingest.md §2）：共享订阅
// `$share/ingest/thermio/gw/+/up/data`，MQTT 3.1.1 + QoS 1，会话不清除
// （重启窗口内消息由 EMQX 会话保持），手动 ACK（PUBACK 在 TSDB → Kafka 落定
// 之后由管线尾部触发，§7.1）。
//
// 背压机制（§7.3）：handler 在 paho 路由 goroutine 内同步执行（Order=true），
// 投递到有界 intake 队列时阻塞 → 客户端停读 socket → TCP 背压 → EMQX inflight
// 窗口暂停投递。全链路无人为丢弃。
package mqtt

import (
	"context"
	"fmt"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// Topic 契约（ingest.md §2）。下行 thermio/gw/{id}/down/write 由中台
// control-safety 发布（ADR-009），ingest 不订阅不发布，故不在此声明。
const (
	// TopicUpDataFmt 上行遥测批量上报 topic；{clientid} = gateway.mqtt_client_id。
	TopicUpDataFmt = "thermio/gw/%s/up/data"
	// TopicUpDataFilter 订阅 filter（共享订阅在此基础上加 $share/{group}/ 前缀）。
	TopicUpDataFilter = "thermio/gw/+/up/data"
)

// SharedTopic 返回共享订阅 filter：`$share/{group}/thermio/gw/+/up/data`（ingest.md §2，
// ingest 多实例水平扩由共享订阅组承载）。
func SharedTopic(group string) string {
	return fmt.Sprintf("$share/%s/%s", group, TopicUpDataFilter)
}

// NewClientOptions 构建 ingestd 自身服务账号的客户端选项基线（ingest.md §2/§11）。
// 会话不清除（session expiry ≥ 1h）：重启窗口内消息由 EMQX 会话保持；
// 自动 ACK 关闭：QoS 1 的 PUBACK 在 TSDB → Kafka 落定之后由管线尾部触发（§7.1）。
func NewClientOptions(brokerURL, clientID, username, password string) *paho.ClientOptions {
	o := paho.NewClientOptions()
	o.AddBroker(brokerURL)
	o.SetClientID(clientID)
	o.SetUsername(username)
	o.SetPassword(password)
	o.SetCleanSession(false)
	o.SetAutoAckDisabled(true)
	return o
}

// Inbound 一条上行消息的管线视图（payload 拷贝出 paho 缓冲，Ack 幂等安全）。
type Inbound struct {
	Topic      string
	Payload    []byte
	Ack        func() // 幂等；多次调用只发一次 PUBACK
	ReceivedAt time.Time
}

// Source 共享订阅源。Handler 在 paho 路由 goroutine 内被调用——只做入队，
// 阻塞即背压（§7.3）。
type Source struct {
	client paho.Client
	topic  string
	queue  chan<- Inbound

	stopCh  chan struct{} // 关闭后 handler 不再入队（消息留给 EMQX 会话重投）
	stopped sync.Once
	handMu  sync.Mutex // 串行化 handWG 的 Add/Wait（WaitGroup 契约：零计数后的
	//            Add 必须先于 Wait——否则 Wait 提前返回，pipeline 随后 close(intake)
	//            会与迟到 handler 的入队 select 竞争，选中已闭通道发送即 panic）
	handWG sync.WaitGroup
}

// NewSource queue 为有界 intake 队列（§5.1 10k 消息）。
func NewSource(queue chan<- Inbound) *Source {
	return &Source{queue: queue, stopCh: make(chan struct{})}
}

// Start 连接并订阅。断线由 paho AutoReconnect（默认开）自动重连，OnConnect
// 回调幂等重订阅。初始连接失败返回错误，由调用方重试（启动期 fail-fast）。
func (s *Source) Start(_ context.Context, brokerURL, clientID, username, password, group string) error {
	s.topic = SharedTopic(group)
	opts := NewClientOptions(brokerURL, clientID, username, password)
	opts.SetConnectTimeout(30 * time.Second)
	opts.SetOnConnectHandler(func(_ paho.Client) {
		// 每次连接建立（含重连）都重订阅：CleanSession=false 下重复 SUBSCRIBE
		// 幂等；漏订阅比重复订阅危险。
		tok := s.client.Subscribe(s.topic, 1, s.onMessage)
		_ = tok.WaitTimeout(30 * time.Second)
	})
	s.client = paho.NewClient(opts)
	tok := s.client.Connect()
	if tok.Wait() && tok.Error() != nil {
		return fmt.Errorf("mqtt connect %s: %w", brokerURL, tok.Error())
	}
	return nil
}

// onMessage paho handler：同步入队（满则阻塞 → TCP 背压）；停机窗口直接
// 返回（不 ACK，消息留在 EMQX 会话，重启后重投）。
func (s *Source) onMessage(_ paho.Client, msg paho.Message) {
	s.handMu.Lock()
	s.handWG.Add(1)
	s.handMu.Unlock()
	defer s.handWG.Done()
	select {
	case <-s.stopCh:
		return // 停机窗口：不 ACK，交给会话重投
	default:
	}
	payload := make([]byte, len(msg.Payload()))
	copy(payload, msg.Payload())
	var ackOnce sync.Once
	in := Inbound{
		Topic:      msg.Topic(),
		Payload:    payload,
		ReceivedAt: time.Now().UTC(),
		Ack: func() {
			ackOnce.Do(func() { msg.Ack() })
		},
	}
	// 阻塞入队 = 背压传导（§7.3）。停机信号优先于入队，防死锁。
	select {
	case s.queue <- in:
	case <-s.stopCh:
	}
}

// Shutdown 停止接收 → 等在途 handler 退出。**不发 UNSUBSCRIBE**（DAT-153 定因：
// 持久会话上先退订 = $share 组内再无持有订阅的会话，停机窗口内发布的消息全走
// no_subscribers 丢弃）——会话继续持有订阅，窗口消息由 EMQX 入离线会话 mqueue，
// 重连续投；未 ACK 的 QoS1 消息由会话保持，下次启动重投——at-least-once（§2）。
func (s *Source) Shutdown() {
	s.stopped.Do(func() { close(s.stopCh) })
	s.handMu.Lock()
	defer s.handMu.Unlock()
	s.handWG.Wait() // 持锁等待：与 onMessage 的 Add 互斥，见 handMu 注释
}

// Disconnect 关闭连接（Shutdown 之后调用；等待 500ms 让 PUBACK 落网）。
func (s *Source) Disconnect() {
	if s.client != nil && s.client.IsConnected() {
		s.client.Disconnect(500)
	}
}
