package pq

import (
	"context"
	"github.com/tal-tech/go-zero/core/logx"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/tal-tech/go-zero/core/executors"
)

type (
	PushOption func(options *chunkOptions)

	Pusher struct {
		producer pulsar.Producer
		topic    string
		executor *executors.ChunkExecutor
	}

	chunkOptions struct {
		chunkSize     int
		flushInterval time.Duration
	}
)

func NewPusher(addrs []string, topic string, opts ...PushOption) *Pusher {
	url := strings.Join(addrs, ",")
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://" + url,
		ConnectionTimeout: 5 * time.Second,
		OperationTimeout:  5 * time.Second,
	})
	if err != nil {
		logx.Errorf("Could not instantiate Pulsar client: %v", err)
	}
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		logx.Error(err)
	}

	pusher := &Pusher{
		producer: producer,
		topic:    topic,
	}

	pusher.executor = executors.NewChunkExecutor(func(tasks []interface{}) {
		for i := range tasks {
			message := tasks[i].(pulsar.ProducerMessage)
			_, err = pusher.producer.Send(context.Background(), &message)
		}

	}, newOptions(opts)...)

	return pusher
}

func (p *Pusher) Close() {
	p.producer.Close()
}

func (p *Pusher) Name() string {
	return p.topic
}

func (p *Pusher) Push(key string, val []byte, properties map[string]string) error {
	msg := pulsar.ProducerMessage{
		Key:        key,
		Payload:    val,
		Properties: properties,
	}
	if p.executor != nil {
		// TODO
		return p.executor.Add(msg, len(val))
	} else {
		_, err := p.producer.Send(context.Background(), &msg)
		return err
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
