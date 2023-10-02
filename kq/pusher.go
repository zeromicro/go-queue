package kq

import (
	"context"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
)

type (
	PushOption func(options *chunkOptions)

	Pusher struct {
		producer *kafka.Writer
		topic    string
		executor *executors.ChunkExecutor
	}

	chunkOptions struct {
		chunkSize     int
		flushInterval time.Duration
	}
)

func NewPusher(addrs []string, topic string, opts ...PushOption) *Pusher {
	producer := &kafka.Writer{
		Addr:  kafka.TCP(addrs...),
		Topic: topic,
		//todo move the follwoing to config kpusherConfig?
		Balancer:    &kafka.LeastBytes{},
		Compression: kafka.Snappy,
		//if this is not set, the writer will not create a nonexistent topic
		AllowAutoTopicCreation: true,
	}
	pusher := &Pusher{
		producer: producer,
		topic:    topic,
	}
	pusher.executor = executors.NewChunkExecutor(func(tasks []interface{}) {
		chunk := make([]kafka.Message, len(tasks))
		for i := range tasks {
			chunk[i] = tasks[i].(kafka.Message)
		}
		if err := pusher.producer.WriteMessages(context.Background(), chunk...); err != nil {
			logx.Error(err)
		}
	}, newOptions(opts)...)

	return pusher
}

func (p *Pusher) Close() error {
	if p.executor != nil {
		p.executor.Flush()
	}

	return p.producer.Close()
}

func (p *Pusher) Name() string {
	return p.topic
}

func (p *Pusher) Push(v string) error {
	msg := kafka.Message{
		Key:   []byte(strconv.FormatInt(time.Now().UnixNano(), 10)),
		Value: []byte(v),
	}
	if p.executor != nil {
		return p.executor.Add(msg, len(v))
	} else {
		return p.producer.WriteMessages(context.Background(), msg)
	}
}

func WithChunkSize(chunkSize int) PushOption {
	return func(options *chunkOptions) {
		options.chunkSize = chunkSize
	}
}

func WithFlushInterval(interval time.Duration) PushOption {
	return func(options *chunkOptions) {
		options.flushInterval = interval
	}
}

func newOptions(opts []PushOption) []executors.ChunkOption {
	var options chunkOptions
	for _, opt := range opts {
		opt(&options)
	}

	var chunkOpts []executors.ChunkOption
	if options.chunkSize > 0 {
		chunkOpts = append(chunkOpts, executors.WithChunkBytes(options.chunkSize))
	}
	if options.flushInterval > 0 {
		chunkOpts = append(chunkOpts, executors.WithFlushInterval(options.flushInterval))
	}
	return chunkOpts
}
