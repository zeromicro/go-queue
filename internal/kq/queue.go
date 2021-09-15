package kq

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip"
	_ "github.com/segmentio/kafka-go/lz4"
	_ "github.com/segmentio/kafka-go/snappy"
	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/queue"
	"github.com/tal-tech/go-zero/core/service"
	"github.com/tal-tech/go-zero/core/stat"
	"github.com/tal-tech/go-zero/core/threading"
	"github.com/tal-tech/go-zero/core/timex"
)

const (
	defaultCommitInterval = time.Second
	defaultMaxWait        = time.Second
	defaultQueueCapacity  = 1000
)

type (
	ConsumeHandle func(key, value string) error

	ConsumeHandler interface {
		Consume(key, value string) error
	}

	queueOptions struct {
		commitInterval time.Duration
		queueCapacity  int
		maxWait        time.Duration
		metrics        *stat.Metrics
	}

	QueueOption func(*queueOptions)

	kafkaQueue struct {
		c                Conf
		consumer         *kafka.Reader
		handler          ConsumeHandler
		channel          chan kafka.Message
		producerRoutines *threading.RoutineGroup
		consumerRoutines *threading.RoutineGroup
		metrics          *stat.Metrics
	}

	kafkaQueues struct {
		queues []queue.MessageQueue
		group  *service.ServiceGroup
	}
)

func MustNewQueue(c Conf, handler ConsumeHandler, opts ...QueueOption) queue.MessageQueue {
	q, err := NewQueue(c, handler, opts...)
	if err != nil {
		log.Fatal(err)
	}

	return q
}

func NewQueue(c Conf, handler ConsumeHandler, opts ...QueueOption) (queue.MessageQueue, error) {
	if err := c.SetUp(); err != nil {
		return nil, err
	}

	var options queueOptions
	for _, opt := range opts {
		opt(&options)
	}
	ensureQueueOptions(c, &options)

	if c.Conns < 1 {
		c.Conns = 1
	}
	q := kafkaQueues{
		group: service.NewServiceGroup(),
	}
	for i := 0; i < c.Conns; i++ {
		q.queues = append(q.queues, newKafkaQueue(c, handler, options))
	}

	return q, nil
}

func newKafkaQueue(c Conf, handler ConsumeHandler, options queueOptions) queue.MessageQueue {
	var offset int64
	if c.Offset == firstOffset {
		offset = kafka.FirstOffset
	} else {
		offset = kafka.LastOffset
	}
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.Brokers,
		GroupID:        c.Group,
		Topic:          c.Topic,
		StartOffset:    offset,
		MinBytes:       c.MinBytes, // 10KB
		MaxBytes:       c.MaxBytes, // 10MB
		MaxWait:        options.maxWait,
		CommitInterval: options.commitInterval,
		QueueCapacity:  options.queueCapacity,
	})

	return &kafkaQueue{
		c:                c,
		consumer:         consumer,
		handler:          handler,
		channel:          make(chan kafka.Message),
		producerRoutines: threading.NewRoutineGroup(),
		consumerRoutines: threading.NewRoutineGroup(),
		metrics:          options.metrics,
	}
}

func (q *kafkaQueue) Start() {
	q.startConsumers()
	q.startProducers()

	q.producerRoutines.Wait()
	close(q.channel)
	q.consumerRoutines.Wait()
}

func (q *kafkaQueue) Stop() {
	q.consumer.Close()
	logx.Close()
}

func (q *kafkaQueue) consumeOne(key, val string) error {
	startTime := timex.Now()
	err := q.handler.Consume(key, val)
	q.metrics.Add(stat.Task{
		Duration: timex.Since(startTime),
	})
	return err
}

func (q *kafkaQueue) startConsumers() {
	for i := 0; i < q.c.Processors; i++ {
		q.consumerRoutines.Run(func() {
			for msg := range q.channel {
				if err := q.consumeOne(string(msg.Key), string(msg.Value)); err != nil {
					logx.Errorf("Error on consuming: %s, error: %v", string(msg.Value), err)
				}
				q.consumer.CommitMessages(context.Background(), msg)
			}
		})
	}
}

func (q *kafkaQueue) startProducers() {
	for i := 0; i < q.c.Consumers; i++ {
		q.producerRoutines.Run(func() {
			for {
				msg, err := q.consumer.FetchMessage(context.Background())
				// io.EOF means consumer closed
				// io.ErrClosedPipe means committing messages on the consumer,
				// kafka will refire the messages on uncommitted messages, ignore
				if err == io.EOF || err == io.ErrClosedPipe {
					return
				}
				if err != nil {
					logx.Errorf("Error on reading message, %q", err.Error())
					continue
				}
				q.channel <- msg
			}
		})
	}
}

func (q kafkaQueues) Start() {
	for _, each := range q.queues {
		q.group.Add(each)
	}
	q.group.Start()
}

func (q kafkaQueues) Stop() {
	q.group.Stop()
}

func WithCommitInterval(interval time.Duration) QueueOption {
	return func(options *queueOptions) {
		options.commitInterval = interval
	}
}

func WithQueueCapacity(queueCapacity int) QueueOption {
	return func(options *queueOptions) {
		options.queueCapacity = queueCapacity
	}
}

func WithHandle(handle ConsumeHandle) ConsumeHandler {
	return innerConsumeHandler{
		handle: handle,
	}
}

func WithMaxWait(wait time.Duration) QueueOption {
	return func(options *queueOptions) {
		options.maxWait = wait
	}
}

func WithMetrics(metrics *stat.Metrics) QueueOption {
	return func(options *queueOptions) {
		options.metrics = metrics
	}
}

type innerConsumeHandler struct {
	handle ConsumeHandle
}

func (ch innerConsumeHandler) Consume(k, v string) error {
	return ch.handle(k, v)
}

func ensureQueueOptions(c Conf, options *queueOptions) {
	if options.commitInterval == 0 {
		options.commitInterval = defaultCommitInterval
	}
	if options.queueCapacity == 0 {
		options.queueCapacity = defaultQueueCapacity
	}
	if options.maxWait == 0 {
		options.maxWait = defaultMaxWait
	}
	if options.metrics == nil {
		options.metrics = stat.NewMetrics(c.Name)
	}
}
