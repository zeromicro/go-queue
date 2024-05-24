package kq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip"
	_ "github.com/segmentio/kafka-go/lz4"
	"github.com/segmentio/kafka-go/sasl/plain"
	_ "github.com/segmentio/kafka-go/snappy"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/queue"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/threading"
	"github.com/zeromicro/go-zero/core/timex"
)

const (
	defaultCommitInterval = time.Second
	defaultMaxWait        = time.Second
	defaultQueueCapacity  = 1000
)

type (
	ConsumeHandle func(key, value string) error

	ConsumeErrorHandler func(msg kafka.Message, err error)

	ConsumeHandler interface {
		Consume(key, value string) error
	}

	queueOptions struct {
		commitInterval time.Duration
		queueCapacity  int
		maxWait        time.Duration
		metrics        *stat.Metrics
		errorHandler   ConsumeErrorHandler
	}

	QueueOption func(*queueOptions)

	kafkaQueue struct {
		c                KqConf
		consumer         *kafka.Reader
		handler          ConsumeHandler
		channel          chan kafka.Message
		producerRoutines *threading.RoutineGroup
		consumerRoutines *threading.RoutineGroup
		metrics          *stat.Metrics
		errorHandler     ConsumeErrorHandler
	}

	kafkaQueues struct {
		queues []queue.MessageQueue
		group  *service.ServiceGroup
	}
)

func MustNewQueue(c KqConf, handler ConsumeHandler, opts ...QueueOption) queue.MessageQueue {
	q, err := NewQueue(c, handler, opts...)
	if err != nil {
		log.Fatal(err)
	}

	return q
}

func NewQueue(c KqConf, handler ConsumeHandler, opts ...QueueOption) (queue.MessageQueue, error) {
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

func newKafkaQueue(c KqConf, handler ConsumeHandler, options queueOptions) queue.MessageQueue {
	var offset int64
	if c.Offset == firstOffset {
		offset = kafka.FirstOffset
	} else {
		offset = kafka.LastOffset
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:        c.Brokers,
		GroupID:        c.Group,
		Topic:          c.Topic,
		StartOffset:    offset,
		MinBytes:       c.MinBytes, // 10KB
		MaxBytes:       c.MaxBytes, // 10MB
		MaxWait:        options.maxWait,
		CommitInterval: options.commitInterval,
		QueueCapacity:  options.queueCapacity,
	}
	if len(c.Username) > 0 && len(c.Password) > 0 {
		readerConfig.Dialer = &kafka.Dialer{
			SASLMechanism: plain.Mechanism{
				Username: c.Username,
				Password: c.Password,
			},
		}
	}
	if len(c.CaFile) > 0 {
		caCert, err := os.ReadFile(c.CaFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		ok := caCertPool.AppendCertsFromPEM(caCert)
		if !ok {
			log.Fatal(err)
		}

		readerConfig.Dialer.TLS = &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: true,
		}
	}
	consumer := kafka.NewReader(readerConfig)

	return &kafkaQueue{
		c:                c,
		consumer:         consumer,
		handler:          handler,
		channel:          make(chan kafka.Message),
		producerRoutines: threading.NewRoutineGroup(),
		consumerRoutines: threading.NewRoutineGroup(),
		metrics:          options.metrics,
		errorHandler:     options.errorHandler,
	}
}

func (q *kafkaQueue) Start() {
	q.startConsumers()
	q.startProducers()

	q.producerRoutines.Wait()
	close(q.channel)
	q.consumerRoutines.Wait()

	logx.Infof("Consumer %s is closed", q.c.Name)
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
					if q.errorHandler != nil {
						q.errorHandler(msg, err)
					}

					if !q.c.ForceCommit {
						continue
					}
				}

				if err := q.consumer.CommitMessages(context.Background(), msg); err != nil {
					logx.Errorf("commit failed, error: %v", err)
				}
			}
		})
	}
}

func (q *kafkaQueue) startProducers() {
	for i := 0; i < q.c.Consumers; i++ {
		i := i
		q.producerRoutines.Run(func() {
			for {
				msg, err := q.consumer.FetchMessage(context.Background())
				// io.EOF means consumer closed
				// io.ErrClosedPipe means committing messages on the consumer,
				// kafka will refire the messages on uncommitted messages, ignore
				if err == io.EOF || err == io.ErrClosedPipe {
					logx.Infof("Consumer %s-%d is closed, error: %q", q.c.Name, i, err.Error())
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

func WithErrorHandler(errorHandler ConsumeErrorHandler) QueueOption {
	return func(options *queueOptions) {
		options.errorHandler = errorHandler
	}
}

type innerConsumeHandler struct {
	handle ConsumeHandle
}

func (ch innerConsumeHandler) Consume(k, v string) error {
	return ch.handle(k, v)
}

func ensureQueueOptions(c KqConf, options *queueOptions) {
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
	if options.errorHandler == nil {
		options.errorHandler = func(msg kafka.Message, err error) {
			logx.Errorf("consume: %s, error: %v", string(msg.Value), err)
		}
	}
}
