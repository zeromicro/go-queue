package kq

import (
	"context"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-queue/kq/internal"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
	"go.opentelemetry.io/otel"
)

type (
	PushOption func(options *pushOptions)

	Pusher struct {
		producer *kafka.Writer
		topic    string
		executor *executors.ChunkExecutor
	}

	pushOptions struct {
		// kafka.Writer options
		allowAutoTopicCreation bool

		// executors.ChunkExecutor options
		chunkSize     int
		flushInterval time.Duration

		// syncPush is used to enable sync push
		syncPush bool
	}
)

// NewPusher returns a Pusher with the given Kafka addresses and topic.
func NewPusher(addrs []string, topic string, opts ...PushOption) *Pusher {
	producer := &kafka.Writer{
		Addr:        kafka.TCP(addrs...),
		Topic:       topic,
		Balancer:    &kafka.LeastBytes{},
		Compression: kafka.Snappy,
	}

	var options pushOptions
	for _, opt := range opts {
		opt(&options)
	}

	// apply kafka.Writer options
	producer.AllowAutoTopicCreation = options.allowAutoTopicCreation

	pusher := &Pusher{
		producer: producer,
		topic:    topic,
	}

	// if syncPush is true, return the pusher directly
	if options.syncPush {
		return pusher
	}

	// apply ChunkExecutor options
	var chunkOpts []executors.ChunkOption
	if options.chunkSize > 0 {
		chunkOpts = append(chunkOpts, executors.WithChunkBytes(options.chunkSize))
	}
	if options.flushInterval > 0 {
		chunkOpts = append(chunkOpts, executors.WithFlushInterval(options.flushInterval))
	}

	pusher.executor = executors.NewChunkExecutor(func(tasks []interface{}) {
		chunk := make([]kafka.Message, len(tasks))
		for i := range tasks {
			chunk[i] = tasks[i].(kafka.Message)
		}
		if err := pusher.producer.WriteMessages(context.Background(), chunk...); err != nil {
			logx.Error(err)
		}
	}, chunkOpts...)

	return pusher
}

// Close closes the Pusher and releases any resources used by it.
func (p *Pusher) Close() error {
	if p.executor != nil {
		p.executor.Flush()
	}

	return p.producer.Close()
}

// Name returns the name of the Kafka topic that the Pusher is sending messages to.
func (p *Pusher) Name() string {
	return p.topic
}

// Push sends a message to the Kafka topic.
func (p *Pusher) Push(ctx context.Context, v string) error {
	msg := kafka.Message{
		Key:   []byte(strconv.FormatInt(time.Now().UnixNano(), 10)), // current timestamp
		Value: []byte(v),
	}

	// wrap message into message carrier
	mc := internal.NewMessageCarrier(internal.NewMessage(&msg))
	// inject trace context into message
	otel.GetTextMapPropagator().Inject(ctx, mc)

	if p.executor != nil {
		return p.executor.Add(msg, len(v))
	} else {
		return p.producer.WriteMessages(ctx, msg)
	}
}

// WithChunkSize customizes the Pusher with the given chunk size.
func WithChunkSize(chunkSize int) PushOption {
	return func(options *pushOptions) {
		options.chunkSize = chunkSize
	}
}

// WithFlushInterval customizes the Pusher with the given flush interval.
func WithFlushInterval(interval time.Duration) PushOption {
	return func(options *pushOptions) {
		options.flushInterval = interval
	}
}

// WithAllowAutoTopicCreation allows the Pusher to create the given topic if it does not exist.
func WithAllowAutoTopicCreation() PushOption {
	return func(options *pushOptions) {
		options.allowAutoTopicCreation = true
	}
}

// WithSyncPush enables the Pusher to push messages synchronously.
func WithSyncPush() PushOption {
	return func(options *pushOptions) {
		options.syncPush = true
	}
}
