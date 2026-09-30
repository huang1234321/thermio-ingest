package mqtt

import (
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// 共享订阅 filter 形态：$share/{group}/thermio/gw/+/up/data（ingest.md §2）。
func TestSharedTopic(t *testing.T) {
	cases := []struct {
		group string
		want  string
	}{
		{"ingest", "$share/ingest/thermio/gw/+/up/data"},
		{"ingest-2", "$share/ingest-2/thermio/gw/+/up/data"},
	}
	for _, c := range cases {
		if got := SharedTopic(c.group); got != c.want {
			t.Errorf("SharedTopic(%q) = %q, want %q", c.group, got, c.want)
		}
	}
}

// 客户端选项基线：会话不清除 + 手动 ACK（ingest.md §2 / §7.1）。
func TestNewClientOptions(t *testing.T) {
	o := NewClientOptions("tcp://emqx:1883", "svc-ingest-1", "svc-ingest", "secret")
	if o.CleanSession {
		t.Error("CleanSession = true, want false（会话不清除，session expiry ≥ 1h）")
	}
	if !o.AutoAckDisabled {
		t.Error("AutoAckDisabled = false, want true（PUBACK 在 TSDB → Kafka 之后）")
	}
}

// guardClient 记录停机路径上的协议副作用。嵌入 nil 接口：除下列三个方法外
// 任何调用即 panic——Shutdown/Disconnect 面上不允许出现其他客户端动作。
type guardClient struct {
	paho.Client
	unsubTopics []string
	disconnects int
}

func (g *guardClient) IsConnected() bool { return true }
func (g *guardClient) Unsubscribe(topics ...string) paho.Token {
	g.unsubTopics = append(g.unsubTopics, topics...)
	return doneToken{}
}
func (g *guardClient) Disconnect(quiesce uint) { g.disconnects++ }

// doneToken 已完成的 Token（Unsubscribe 若被误调也能立刻返回，测试不因
// token 等待挂死）。
type doneToken struct{}

func (doneToken) Wait() bool                     { return true }
func (doneToken) WaitTimeout(time.Duration) bool { return true }
func (doneToken) Error() error                   { return nil }
func (doneToken) Done() <-chan struct{} {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return ch
}

// DAT-153 定因守护：Shutdown 不得发 UNSUBSCRIBE——持久会话上先退订 =
// $share 组内再无持有订阅的会话，停机窗口内发布的消息全走 no_subscribers
// 丢弃（s7 曾实测丢 294 条）。会话保订阅由 Disconnect（main 按 §12 序调用）
// 与 broker 会话保持兜底。
func TestShutdownSendsNoUnsubscribe(t *testing.T) {
	s := NewSource(make(chan Inbound, 1))
	s.topic = SharedTopic("ingest")
	gc := &guardClient{}
	s.client = gc

	done := make(chan struct{})
	go func() { s.Shutdown(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown 未返回（在途 handler 等待死锁？）")
	}

	if len(gc.unsubTopics) != 0 {
		t.Errorf("Shutdown 发送了 UNSUBSCRIBE：%v——会话保订阅被毁，停机窗口消息将走 no_subscribers 丢弃", gc.unsubTopics)
	}
	if gc.disconnects != 0 {
		t.Errorf("Shutdown 断开了连接（%d 次）——连接关闭归 Disconnect（main §12 序）", gc.disconnects)
	}

	// Disconnect 独立可用（Shutdown 之后由 main 调用）。
	s.Disconnect()
	if gc.disconnects != 1 {
		t.Errorf("Disconnect 调用后断开次数 = %d, want 1", gc.disconnects)
	}
}

// 停机后到达的消息：不入队、不 ACK（留在 EMQX 会话，重启重投——§7.1）。
// Shutdown 须在 handler 阻塞于满队列时也能退出（停机信号优先于入队）。
func TestShutdownDropsInflightWithoutEnqueue(t *testing.T) {
	queue := make(chan Inbound) // 无缓冲 = 入队必阻塞
	s := NewSource(queue)
	s.topic = SharedTopic("ingest")
	s.client = &guardClient{}

	acked := make(chan struct{}, 1)
	go s.onMessage(nil, &fakeMessage{ack: func() { acked <- struct{}{} }})

	done := make(chan struct{})
	go func() { s.Shutdown(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown 在 handler 阻塞满队列时未退出（防死锁语义回归）")
	}
	select {
	case in := <-queue:
		t.Errorf("停机窗口消息被入队：%v（应留给会话重投）", in.Topic)
	default:
	}
	select {
	case <-acked:
		t.Error("停机窗口消息被 ACK（应留给会话重投）")
	default:
	}
}

// fakeMessage 最小 paho.Message 实现（onMessage 只读 Topic/Payload 并拷贝）。
type fakeMessage struct {
	topic   string
	payload []byte
	ack     func()
}

func (m *fakeMessage) Duplicate() bool   { return false }
func (m *fakeMessage) Qos() byte         { return 1 }
func (m *fakeMessage) Retained() bool    { return false }
func (m *fakeMessage) Topic() string     { return m.topic }
func (m *fakeMessage) MessageID() uint16 { return 0 }
func (m *fakeMessage) Payload() []byte   { return m.payload }
func (m *fakeMessage) Ack()              { m.ack() }
