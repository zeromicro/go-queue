package natsq

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/queue"
)

const (
	NatDefaultMode = iota
	NatJetMode
)

type (
	Msg struct {
		Subject string
		Data    []byte
	}

	ConsumeHandle func(m *Msg) error

	// ConsumeHandler Consumer interface, used to define the methods required by the consumer
	ConsumeHandler interface {
		HandleMessage(m *Msg) error
	}

	// ConsumerQueue Consumer queue, used to maintain the relationship between a consumer queue
	ConsumerQueue struct {
		StreamName string                     // stream name
		QueueName  string                     // queue name
		Subjects   []string                   // Subscribe subject
		Consumer   ConsumeHandler             // consumer object
		JetOption  []jetstream.PullConsumeOpt // Jetstream configuration
	}

	// ConsumerManager Consumer manager for managing multiple consumer queues
	ConsumerManager struct {
		mutex    sync.RWMutex    // read-write lock
		conn     *nats.Conn      // nats connect
		mode     uint            // nats mode
		queues   []ConsumerQueue // consumer queue list
		options  []nats.Option   // Connection configuration items
		doneChan chan struct{}   // close channel
	}
)

// MustNewConsumerManager creates a new ConsumerManager instance.
// It connects to NATS server, registers the provided consumer queues, and returns the ConsumerManager.
// If any error occurs during the process, it logs the error and continues.
func MustNewConsumerManager(cfg *NatsConfig, cq []*ConsumerQueue, mode uint) queue.MessageQueue {
	sc, err := nats.Connect(cfg.ServerUri, cfg.Options...)
	if err != nil {
		logx.Errorf("failed to connect nats, error: %v", err)
	}
	cm := &ConsumerManager{
		conn:     sc,
		options:  cfg.Options,
		mode:     mode,
		doneChan: make(chan struct{}),
	}
	if len(cq) == 0 {
		logx.Errorf("failed consumerQueue register to  nats, error: cq len is 0")
	}
	for _, item := range cq {
		err = cm.registerQueue(item)
		if err != nil {
			logx.Errorf("failed to register nats, error: %v", err)
		}
	}

	return cm
}

// Start starts consuming messages from all the registered consumer queues.
// It launches a goroutine for each consumer queue to subscribe and process messages.
// The method blocks until the doneChan is closed.
func (cm *ConsumerManager) Start() {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	if len(cm.queues) == 0 {
		logx.Errorf("no consumer queues found")
	}
	for _, consumerQueue := range cm.queues {
		go cm.subscribe(consumerQueue)
	}
	<-cm.doneChan
}

// Stop closes the NATS connection and stops the ConsumerManager.
func (cm *ConsumerManager) Stop() {
	if cm.conn != nil {
		cm.conn.Close()
	}
}

// registerQueue registers a new consumer queue with the ConsumerManager.
// It validates the required fields of the ConsumerQueue and adds it to the list of queues.
// If any required field is missing, it returns an error.
func (cm *ConsumerManager) registerQueue(queue *ConsumerQueue) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if cm.mode == NatJetMode && queue.StreamName == "" {
		return errors.New("stream name is required")
	}

	if queue.QueueName == "" {
		return errors.New("queue name is required")
	}
	if len(queue.Subjects) == 0 {
		return errors.New("subject is required")
	}
	if queue.Consumer == nil {
		return errors.New("consumer is required")
	}

	cm.queues = append(cm.queues, *queue)
	return nil
}

// subscribe subscribes to the specified consumer queue and starts processing messages.
// If the NATS mode is NatJetMode, it creates a JetStream consumer and consumes messages using the provided options.
// If the NATS mode is NatDefaultMode, it subscribes to the specified subjects using the queue name.
// The method blocks until the doneChan is closed.
func (cm *ConsumerManager) subscribe(queue ConsumerQueue) {
	ctx := context.Background()
	if cm.mode == NatJetMode {
		js, _ := jetstream.New(cm.conn)
		stream, err := js.Stream(ctx, "ccc")
		if err != nil {
			log.Fatalf("Error creating stream: %v", err)
			return
		}
		consumer, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Name:           queue.QueueName,
			AckPolicy:      jetstream.AckExplicitPolicy,
			FilterSubjects: queue.Subjects,
		})
		consContext, subErr := consumer.Consume(func(msg jetstream.Msg) {
			err := queue.Consumer.HandleMessage(&Msg{Subject: msg.Subject(), Data: msg.Data()})
			if err != nil {
				logx.Errorf("error handling message: %v", err.Error())
			} else {
				msg.Ack()
			}
		}, queue.JetOption...)
		if subErr != nil {
			logx.Errorf("error subscribing to queue %s: %v", queue.QueueName, subErr.Error())
			return
		}
		defer consContext.Stop()
	}
	if cm.mode == NatDefaultMode {
		for _, subject := range queue.Subjects {
			cm.conn.QueueSubscribe(subject, queue.QueueName, func(m *nats.Msg) {
				err := queue.Consumer.HandleMessage(&Msg{Subject: m.Subject, Data: m.Data})
				if err != nil {
					logx.Errorf("error handling message: %v", err.Error())
				} else {
					m.Ack()
				}
			})
		}
	}

	<-cm.doneChan
}
