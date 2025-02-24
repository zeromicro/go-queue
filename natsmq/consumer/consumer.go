package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/zeromicro/go-queue/natsmq/common"
	"github.com/zeromicro/go-zero/core/queue"
	"log"
)

// ConsumerManager manages consumer operations including NATS connection, JetStream stream initialization,
// and consumer queue creation and subscription.
type ConsumerManager struct {
	// NATS connection configuration and instance.
	nc       *nats.Conn
	natsConf *common.NatsConfig

	// List of global JetStream configurations parsed from a config file.
	JetStreamConfigs []*common.JetStreamConfig

	// List of consumer queue configurations.
	queues []*ConsumerQueueConfig

	// Channel to signal stop of the consumer manager.
	stopCh chan struct{}

	// List of push-based subscription contexts.
	subscribers []jetstream.ConsumeContext

	// List of cancel functions for managing pull-based consumption contexts.
	cancelFuncs []context.CancelFunc
}

// NewConsumerManager creates a new ConsumerManager instance.
// Parameters:
//
//	natsConf         - pointer to NatsConfig defining the NATS connection settings
//	jetStreamConfigs - list of JetStreamConfig, used to initialize global streams
//	cq               - list of ConsumerQueueConfig defining individual consumer queue settings
//
// Returns:
//
//	queue.MessageQueue - the created consumer manager (implements MessageQueue)
//	error              - error if no consumer queues provided or if connection fails
func NewConsumerManager(natsConf *common.NatsConfig, jetStreamConfigs []*common.JetStreamConfig, cq []*ConsumerQueueConfig) (queue.MessageQueue, error) {
	if len(cq) == 0 {
		return nil, errors.New("no consumer queues provided")
	}
	cm := &ConsumerManager{
		natsConf:         natsConf,
		JetStreamConfigs: jetStreamConfigs,
		queues:           cq,
		stopCh:           make(chan struct{}),
		subscribers:      []jetstream.ConsumeContext{},
		cancelFuncs:      []context.CancelFunc{},
	}
	if err := cm.connectToNATS(); err != nil {
		return nil, err
	}
	return cm, nil
}

// connectToNATS establishes a connection to the NATS server.
// Returns:
//
//	error - non-nil error if connection fails
func (cm *ConsumerManager) connectToNATS() error {
	nc, err := nats.Connect(cm.natsConf.URL, cm.natsConf.Options...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	cm.nc = nc
	return nil
}

// Start initializes stream instances and creates consumers according to the provided consumer queue configurations.
// This function blocks until Stop() is invoked.
func (cm *ConsumerManager) Start() {
	// Initialize stream instances (register or update streams) based on the provided JetStream configurations.
	common.RegisterStreamInstances(cm.nc, cm.JetStreamConfigs)

	// Iterate over each consumer queue configuration to create the consumer on the corresponding stream.
	for _, cfg := range cm.queues {
		var stream jetstream.Stream
		if cfg.StreamName != "" {
			s, ok := common.GetStream(cfg.StreamName)
			if !ok {
				log.Printf("stream %s not found, skipping consumer: %s", cfg.StreamName, cfg.ConsumerConfig.Name)
				continue
			} else {
				stream = s
			}
		} else {
			s, ok := common.GetStream(common.DefaultStream)
			if !ok {
				log.Printf("default stream not found, skipping consumer: %s", cfg.ConsumerConfig.Name)
				continue
			}
			stream = s
		}

		ctx := context.Background()
		if err := cm.createConsumer(ctx, cfg, stream); err != nil {
			log.Printf("failed to create consumer %s: %v", cfg.ConsumerConfig.Name, err)
			continue
		}
	}
	<-cm.stopCh
}

// createConsumer creates a consumer for a given queue configuration and attaches it to the provided JetStream stream.
// Parameters:
//
//	ctx    - context to manage cancellation and timeout during consumer creation
//	cfg    - pointer to ConsumerQueueConfig containing consumer settings and delivery options
//	stream - JetStream stream instance to be used
//
// Returns:
//
//	error - non-nil error if creating the consumer or subscribing fails
func (cm *ConsumerManager) createConsumer(ctx context.Context, cfg *ConsumerQueueConfig, stream jetstream.Stream) error {
	var consumer jetstream.Consumer
	var err error

	// Create an ordered consumer or a standard consumer based on the configuration.
	if cfg.ConsumerConfig.Ordered {
		opts := jetstream.OrderedConsumerConfig{
			FilterSubjects: cfg.ConsumerConfig.FilterSubjects,
			DeliverPolicy:  jetstream.DeliverPolicy(cfg.ConsumerConfig.OrderedConsumerOptions.DeliverPolicy),
			OptStartSeq:    cfg.ConsumerConfig.OrderedConsumerOptions.OptStartSeq,
			OptStartTime:   cfg.ConsumerConfig.OrderedConsumerOptions.OptStartTime,
			ReplayPolicy:   jetstream.ReplayPolicy(cfg.ConsumerConfig.OrderedConsumerOptions.ReplayPolicy),
		}
		consumer, err = stream.OrderedConsumer(ctx, opts)
		if err != nil {
			return fmt.Errorf("failed to create ordered consumer: %w", err)
		}
	} else {
		consumer, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Name:           cfg.ConsumerConfig.Name,
			Durable:        cfg.ConsumerConfig.Durable,
			Description:    cfg.ConsumerConfig.Description,
			FilterSubjects: cfg.ConsumerConfig.FilterSubjects,
			AckPolicy:      jetstream.AckPolicy(cfg.ConsumerConfig.AckPolicy),
		})
		if err != nil {
			return fmt.Errorf("failed to create standard consumer: %w", err)
		}
	}

	// Create consumer instances based on the specified number for the consumer queue. Each instance
	// uses either push-based (subscription) or pull-based consumption.
	for i := 0; i < cfg.QueueConsumerCount; i++ {
		log.Printf("Consumer [%s] instance [%d] with filterSubjects %v created successfully", cfg.ConsumerConfig.Name, i, cfg.ConsumerConfig.FilterSubjects)
		switch cfg.Delivery.ConsumptionMethod {
		case Consumer:
			consumerCtx, err := cm.consumerSubscription(consumer, cfg)
			if err != nil {
				return fmt.Errorf("failed to subscribe to push messages: %w", err)
			}
			cm.subscribers = append(cm.subscribers, consumerCtx)
		case Pull, PullNoWait:
			pullFn := func(num int) (jetstream.MessageBatch, error) {
				if cfg.Delivery.ConsumptionMethod == Pull {
					return consumer.Fetch(num)
				}
				return consumer.FetchNoWait(num)
			}
			pullCtx, cancel := context.WithCancel(ctx)
			cm.cancelFuncs = append(cm.cancelFuncs, cancel)
			go cm.runPullMessages(pullCtx, cfg, pullFn)
		default:
			return fmt.Errorf("unsupported consumption method: %v", cfg.Delivery.ConsumptionMethod)
		}
	}
	return nil
}

// consumerSubscription sets up a push-based subscription using the provided JetStream consumer.
// Parameters:
//
//	consumer - JetStream consumer instance to be used for subscription
//	cfg      - pointer to ConsumerQueueConfig containing consumer settings and the message handler
//
// Returns:
//
//	jetstream.ConsumeContext - context to manage the subscription lifecycle
//	error                  - non-nil error if subscription fails
func (cm *ConsumerManager) consumerSubscription(consumer jetstream.Consumer, cfg *ConsumerQueueConfig) (jetstream.ConsumeContext, error) {
	consumerCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		cm.ackMessage(cfg, msg)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to messages: %w", err)
	}
	return consumerCtx, nil
}

// ackMessage processes a message using the user-provided handler and acknowledges the message if required.
// Parameters:
//
//	cfg - pointer to ConsumerQueueConfig containing the message handler and acknowledgement settings
//	msg - the JetStream message to process
func (cm *ConsumerManager) ackMessage(cfg *ConsumerQueueConfig, msg jetstream.Msg) {
	if err := cfg.Handler.Consume(msg); err != nil {
		log.Printf("message processing error: %v", err)
		return
	}

	// Acknowledge the message unless using AckNonePolicy or in an ordered consumer scenario.
	if jetstream.AckPolicy(cfg.ConsumerConfig.AckPolicy) != jetstream.AckNonePolicy && !cfg.ConsumerConfig.Ordered {
		if err := msg.Ack(); err != nil {
			log.Printf("failed to acknowledge message: %v", err)
		}
	}
}

// runPullMessages continuously pulls messages in batches using the provided fetch function.
// Parameters:
//
//	ctx     - context to control the pull loop (supports cancellation)
//	cfg     - pointer to ConsumerQueueConfig with pull configuration, including the fetch count
//	fetchFn - function that fetches a batch of messages; takes an integer defining the number of messages
func (cm *ConsumerManager) runPullMessages(ctx context.Context, cfg *ConsumerQueueConfig, fetchFn func(num int) (jetstream.MessageBatch, error)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgs, err := fetchFn(cfg.Delivery.FetchCount)
		if err != nil {
			log.Printf("error fetching messages: %v", err)
			continue
		}
		for msg := range msgs.Messages() {
			cm.ackMessage(cfg, msg)
		}
		if fetchErr := msgs.Error(); fetchErr != nil {
			log.Printf("error after fetching messages: %v", fetchErr)
		}
	}
}

// Stop terminates all active subscriptions and pull routines,
// closes the underlying NATS connection, and signals exit via stopCh.
func (cm *ConsumerManager) Stop() {
	for _, consumerCtx := range cm.subscribers {
		consumerCtx.Stop()
	}
	for _, cancel := range cm.cancelFuncs {
		cancel()
	}
	if cm.nc != nil {
		cm.nc.Close()
	}
	close(cm.stopCh)
}
