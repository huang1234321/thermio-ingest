// Package kafkaproducer 封装 franz-go producer（ingest.md §7.4：topic
// thermio.telemetry.raw / thermio.telemetry.quality / thermio.dlq，key=gateway_id
// 同网关同分区保序，headers 带 tenant_id/trace_id/gw_seq）。at-least-once +
// 幂等 producer；linger 100ms 批效率。Produce 等待 broker 确认（PUBACK 前置，
// §7.1 写入顺序），失败由调用方记指标与日志——数据已在 TSDB（真相源不丢）。
package kafkaproducer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic 清单（ADR-004，未新增 topic；compose kafka-init 幂等建题）。
const (
	TopicTelemetryRaw     = "thermio.telemetry.raw"     // 保留 7 天
	TopicTelemetryQuality = "thermio.telemetry.quality" // 质量事件
	TopicDLQ              = "thermio.dlq"               // 保留 30 天
)

// Header 常量（§7.4 headers：tenant_id/trace_id/gw_seq）。
const (
	HeaderTenantID = "tenant_id"
	HeaderTraceID  = "trace_id"
	HeaderGwSeq    = "gw_seq"
)

// DefaultOpts 返回 producer 选项基线（ingest.md §7.2/§7.4）：
// at-least-once + 幂等 producer（franz-go 默认启用幂等生产与 AllISR 确认，
// 除非显式 DisableIdempotentWrite，此处不关）；linger 100ms 批效率。
func DefaultOpts(brokers []string) []kgo.Opt {
	return append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ProducerLinger(100 * time.Millisecond),
	}, requiredOpts()...)
}

// requiredOpts 与 broker 无关的硬性选项（测试 fake 不走这里，真实现共用）。
func requiredOpts() []kgo.Opt {
	return []kgo.Opt{
		// 记录携带 key 的分区走 murmur2(hi/lo)——与 JVM 生态默认一致，
		// 保证同 key（gateway_id）同分区（§7.4 保序纪律）。
		kgo.RecordRetries(5),
		kgo.RequestTimeoutOverhead(2 * time.Second),
	}
}

// Producer 消费侧接口（GO-06：消费侧定义、小接口）。Produce 阻塞到全部记录
// 落定（成功或失败——返回首个错误；已成功的不回滚，at-least-once）。
type Producer interface {
	Produce(ctx context.Context, records ...*kgo.Record) error
	Flush(ctx context.Context) error
	Close()
}

// Kafka franz-go 实现。
type Kafka struct {
	client *kgo.Client
}

// New 按 opts 建客户端（opts 至少含 SeedBrokers；DefaultOpts 为基线）。
func New(opts ...kgo.Opt) (*Kafka, error) {
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &Kafka{client: cl}, nil
}

// NewBrokers 便捷构造：DefaultOpts + 可选覆盖。
func NewBrokers(brokers []string, extra ...kgo.Opt) (*Kafka, error) {
	return New(append(DefaultOpts(brokers), extra...)...)
}

// Produce 同步语义的批量生产：等待全部 promise 落定。
func (k *Kafka) Produce(ctx context.Context, records ...*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	wg.Add(len(records))
	for _, rec := range records {
		r := rec
		k.client.Produce(ctx, r, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("produce topic=%s key=%s: %w", r.Topic, string(r.Key), err)
				}
				mu.Unlock()
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return firstErr
	case <-ctx.Done():
		return fmt.Errorf("produce canceled: %w", ctx.Err())
	}
}

// Flush 排空在途记录（优雅退出 §12：冲 producer）。
func (k *Kafka) Flush(ctx context.Context) error {
	return k.client.Flush(ctx)
}

// Close 关闭客户端。
func (k *Kafka) Close() { k.client.Close() }
