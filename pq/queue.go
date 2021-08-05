package pq

import (
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/queue"
	"github.com/tal-tech/go-zero/core/service"
	"github.com/tal-tech/go-zero/core/stat"
	"github.com/tal-tech/go-zero/core/threading"
	"github.com/tal-tech/go-zero/core/timex"
)

const (
	defaultQueueCapacity = 1000
)

type (
	ConsumeHandle  func(key string, value []byte, properties map[string]string) error
	ConsumeHandler interface {
		Consume(key string, value []byte, properties map[string]string) error
	}

	queueOptions struct {
		queueCapacity int
		metrics       *stat.Metrics
	}

	QueueOption func(options *queueOptions)

	pulsarQueue struct {
		c                PqConf
		consumer         pulsar.Consumer
		handler          ConsumeHandler
		channel          chan pulsar.ConsumerMessage
		stopChan         chan struct{}
		consumerRoutines *threading.RoutineGroup
		metrics          *stat.Metrics
	}

	pulsarQueues struct {
		queues []queue.MessageQueue
		client pulsar.Client
		group  *service.ServiceGroup
	}
)

func MustNewQueue(c PqConf, handler ConsumeHandler, opts ...QueueOption) queue.MessageQueue {
	q, err := NewQueue(c, handler, opts...)
	if err != nil {
		log.Fatal(err)
	}
	return q
}

func NewQueue(c PqConf, handler ConsumeHandler, opts ...QueueOption) (queue.MessageQueue, error) {
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
	// create a client
	url := strings.Join(c.Brokers, ",")
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://" + url,
		ConnectionTimeout: 5 * time.Second,
		OperationTimeout:  5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	q := pulsarQueues{
		group:  service.NewServiceGroup(),
		client: client,
	}
	
	// shutdown
	shutdown := make(chan os.Signal)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	stopChan := make(chan struct{})
	go func() {
		<-shutdown
		close(stopChan)
	}()

	for i := 0; i < c.Conns; i++ {
		q.queues = append(q.queues, newPulsarQueue(c, q.client, handler, stopChan, options))
	}
	return q, nil
}

func newPulsarQueue(c PqConf, client pulsar.Client, handler ConsumeHandler, stopChan chan struct{}, options queueOptions) queue.MessageQueue {
	// use client create more consumers, one consumer has one channel message
	channel := make(chan pulsar.ConsumerMessage, options.queueCapacity)
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            c.Topic,
		Type:             pulsar.Shared,
		SubscriptionName: c.SubscriptionName,
		MessageChannel:   channel,
	})
	if err != nil {
		log.Fatal(err)
	}

	return &pulsarQueue{
		c:                c,
		consumer:         consumer,
		handler:          handler,
		channel:          channel,
		consumerRoutines: threading.NewRoutineGroup(),
		metrics:          options.metrics,
		stopChan:         stopChan,
	}
}

func (q *pulsarQueue) Start() {
	q.startConsumers()
	q.consumerRoutines.Wait()
}

func (q *pulsarQueue) Stop() {
	// close consumer
	q.consumer.Close()
	logx.Close()
}

func (q *pulsarQueue) consumeOne(key string, value []byte, properties map[string]string) error {
	startTime := timex.Now()
	err := q.handler.Consume(key, value, properties)
	q.metrics.Add(stat.Task{
		Duration: timex.Since(startTime),
	})

	return err
}

func (q *pulsarQueue) startConsumers() {
	// deal one consumer messages
	for i := 0; i < q.c.Processors; i++ {
		q.consumerRoutines.Run(func() {
			for {
				select {
				case msg := <-q.channel:
					if err := q.consumeOne(msg.Key(), msg.Payload(), msg.Properties()); err != nil {
						logx.Errorf("error on consuming: %s, error: %v", msg.Payload(), err)
						//
						continue
					}
					q.consumer.Ack(msg)
				case <-q.stopChan:
					logx.Info("Receive stop signal")
					return
				}
			}
		})
	}
}

func (q pulsarQueues) Start() {
	for _, item := range q.queues {
		q.group.Add(item)
	}
	q.group.Start()
}

func (q pulsarQueues) Stop() {
	q.group.Stop()
	// close pulsar client
	q.client.Close()
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

func WithMetrics(metrics *stat.Metrics) QueueOption {
	return func(options *queueOptions) {
		options.metrics = metrics
	}
}

type innerConsumeHandler struct {
	handle ConsumeHandle
}

func (ch innerConsumeHandler) Consume(k string, v []byte, properties map[string]string) error {
	return ch.handle(k, v, properties)
}
func ensureQueueOptions(c PqConf, options *queueOptions) {

	if options.queueCapacity == 0 {
		options.queueCapacity = defaultQueueCapacity
	}

	if options.metrics == nil {
		options.metrics = stat.NewMetrics(c.Name)
	}
}
