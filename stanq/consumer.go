package stanq

import (
	"errors"
	"fmt"
	stan "github.com/nats-io/stan.go"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/queue"
	"reflect"
	"sync"
	"time"
)

type (
	ConsumeHandle func(m *stan.Msg) error

	// ConsumeHandler Consumer interface, used to define the methods required by the consumer
	ConsumeHandler interface {
		HandleMessage(m *stan.Msg) error
	}

	// ConsumerQueue Consumer queue, used to maintain the relationship between a consumer group and queue
	ConsumerQueue struct {
		GroupName     string                    // consumer group name
		QueueName     string                    // queue name
		Subject       string                    // Subscribe subject
		Consumer      ConsumeHandler            // consumer object
		AckWaitTime   int                       // Waiting time for Ack
		MaxInflight   int                       // Maximum number of unacked messages
		ManualAckMode bool                      //Whether to manually ack
		Options       []stan.SubscriptionOption // Subscription configuration item
	}

	// ConsumerManager Consumer manager for managing multiple consumer queues
	ConsumerManager struct {
		mutex    sync.RWMutex    // read-write lock
		conn     stan.Conn       // nats-streaming connect
		queues   []ConsumerQueue // consumer queue list
		options  []stan.Option   // Connection configuration items
		doneChan chan struct{}   // close channel
	}
)

// MustNewConsumerManager
func MustNewConsumerManager(cfg *StanqConfig, cq []*ConsumerQueue) queue.MessageQueue {
	sc, err := stan.Connect(cfg.ClusterID, cfg.ClientID, cfg.Options...)
	if err != nil {
		logx.Errorf("failed to connect stan, error: %v", err)
	}
	cm := &ConsumerManager{
		conn:     sc,
		options:  cfg.Options,
		doneChan: make(chan struct{}),
	}
	if len(cq) == 0 {
		logx.Errorf("failed consumerQueue register to  stan, error: cq len is 0")
	}
	for _, item := range cq {
		err = cm.registerQueue(item)
		if err != nil {
			logx.Errorf("failed to register stan, error: %v", err)
		}
	}

	return cm
}

// Start consuming messages in the queue
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

// Stop close connect
func (cm *ConsumerManager) Stop() {
	if cm.conn != nil {
		_ = cm.conn.Close()
	}
}

// RegisterQueue Register a consumer queue
func (cm *ConsumerManager) registerQueue(queue *ConsumerQueue) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if queue.GroupName == "" {
		return errors.New("group name is required")
	}
	if queue.QueueName == "" {
		return errors.New("queue name is required")
	}
	if queue.Subject == "" {
		return errors.New("subject is required")
	}
	if queue.Consumer == nil {
		return errors.New("consumer is required")
	}

	cm.queues = append(cm.queues, *queue)
	return nil
}

// subscribe news
func (cm *ConsumerManager) subscribe(queue ConsumerQueue) {
	var opts []stan.SubscriptionOption
	if queue.AckWaitTime > 0 {
		opts = append(opts, stan.AckWait(time.Duration(queue.AckWaitTime)*time.Second))
	}
	if queue.MaxInflight > 0 {
		opts = append(opts, stan.MaxInflight(queue.MaxInflight))
	}
	if len(queue.Options) > 0 {
		opts = append(opts, queue.Options...)
	}
	durableName := fmt.Sprintf("%s-%s", queue.GroupName, queue.QueueName)
	opts = append(opts, stan.DurableName(durableName))
	if queue.ManualAckMode {
		opts = append(opts, stan.SetManualAckMode())
	}

	sub, err := cm.conn.QueueSubscribe(queue.Subject, queue.QueueName, func(m *stan.Msg) {
		err := queue.Consumer.HandleMessage(m)
		if err != nil {
			logx.Errorf("error handling message: %v", err)
		} else {
			if queue.ManualAckMode {
				err := m.Ack()
				if err != nil {
					logx.Errorf("error acking message: %v", err)
				}
			}
		}
	}, opts...)
	if err != nil {
		logx.Errorf("error subscribing to queue %s: %v", queue.QueueName, err)
		return
	}
	<-cm.doneChan

	err = sub.Unsubscribe()
	if err != nil {
		logx.Errorf("error unsubscribing from queue %s: %v", queue.QueueName, err)
	}

	// delete consumer queue
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	for i, q := range cm.queues {
		if reflect.DeepEqual(q, queue) {
			cm.queues = append(cm.queues[:i], cm.queues[i+1:]...)
			break
		}
	}
}
