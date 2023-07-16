package kq

import (
	"context"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
)

type (
	PushOption func(options *chunkOptions)

	Pusher struct {
		produer  *kafka.Writer
		topic    string
		executor *executors.ChunkExecutor
	}

	chunkOptions struct {
		chunkSize     int
		flushInterval time.Duration
	}
)

func NewPusher(config KqConf, opts ...PushOption) *Pusher {
	var producer *kafka.Writer
	if config.Username != "" && config.Password != "" {
		if config.Mechanism == "" {
			// 使用SASL 明文认证
			mechanism := plain.Mechanism{
				Username: config.Username,
				Password: config.Password,
			}

			sharedTransport := &kafka.Transport{
				SASL: mechanism,
			}

			producer = &kafka.Writer{
				Addr:      kafka.TCP(config.Brokers...),
				Topic:     config.Topic,
				Balancer:  &kafka.Hash{},
				Transport: sharedTransport,
			}
		} else {
			// 使用SASL SCRAM-SHA认证
			var mechanism sasl.Mechanism
			var err error
			if config.Mechanism == "SCRAM-SHA-512" {
				mechanism, err = scram.Mechanism(scram.SHA512, config.Username, config.Password)
				if err != nil {
					panic(err)
				}
			} else if config.Mechanism == "SCRAM-SHA-256" {
				mechanism, err = scram.Mechanism(scram.SHA256, config.Username, config.Password)
				if err != nil {
					panic(err)
				}
			}
			// Transports are responsible for managing connection pools and other resources,
			// it's generally best to create a few of these and share them across your
			// application.
			sharedTransport := &kafka.Transport{
				SASL: mechanism,
			}

			producer = &kafka.Writer{
				Addr:      kafka.TCP(config.Brokers...),
				Topic:     config.Topic,
				Balancer:  &kafka.Hash{},
				Transport: sharedTransport,
			}
		}
	} else {
		// 不使用认证
		producer = &kafka.Writer{
			Addr:        kafka.TCP(config.Brokers...),
			Topic:       config.Topic,
			Balancer:    &kafka.LeastBytes{},
			Compression: kafka.Snappy,
		}
	}

	pusher := &Pusher{
		produer: producer,
		topic:   config.Topic,
	}

	pusher.executor = executors.NewChunkExecutor(func(tasks []interface{}) {
		chunk := make([]kafka.Message, len(tasks))
		for i := range tasks {
			chunk[i] = tasks[i].(kafka.Message)
		}
		if err := pusher.produer.WriteMessages(context.Background(), chunk...); err != nil {
			logx.Error(err)
		}
	}, newOptions(opts)...)

	return pusher
}

func (p *Pusher) Close() error {
	if p.executor != nil {
		p.executor.Flush()
	}

	return p.produer.Close()
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
		return p.produer.WriteMessages(context.Background(), msg)
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
