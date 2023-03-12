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

	// ConsumeHandler 消费者接口，用于定义消费者所需的方法
	ConsumeHandler interface {
		HandleMessage(m *stan.Msg) error
	}

	// ConsumerQueue 消费者队列，用于维护一个消费者组和队列的关系
	ConsumerQueue struct {
		GroupName     string                    // 消费者组名称
		QueueName     string                    // 队列名称
		Subject       string                    // 订阅的subject
		Consumer      ConsumeHandler            // 消费者对象
		AckWaitTime   int                       // 等待Ack的时间
		MaxInflight   int                       // 最大未ack的消息数
		ManualAckMode bool                      //是否手动ack
		Options       []stan.SubscriptionOption // 订阅配置项
	}

	// ConsumerManager 消费者管理器，用于管理多个消费者队列
	ConsumerManager struct {
		mutex    sync.RWMutex    // 读写锁
		conn     stan.Conn       // nats-streaming连接
		queues   []ConsumerQueue // 消费者队列列表
		options  []stan.Option   // 连接配置项
		doneChan chan struct{}   // 关闭通道
	}
)

// MustNewConsumerManager  创建一个新的消费者管理器
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

// Start 开始消费者队列中的消息
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

// Close 关闭连接
func (cm *ConsumerManager) Stop() {
	if cm.conn != nil {
		_ = cm.conn.Close()
	}
}

// RegisterQueue 注册一个消费者队列
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

// 订阅消息
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
	// 删除消费者队列
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	for i, q := range cm.queues {
		if reflect.DeepEqual(q, queue) {
			cm.queues = append(cm.queues[:i], cm.queues[i+1:]...)
			break
		}
	}
}
