// Package mqtt 负责 EMQX 接入（ingest.md §2）：共享订阅
// `$share/ingest/thermio/gw/+/up/data`，MQTT 3.1.1 + QoS 1。
// 订阅循环与 handler 接线随 IMPL-5 落地；本文件冻结 topic 契约与客户端选项基线。
package mqtt

import (
	"fmt"

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
