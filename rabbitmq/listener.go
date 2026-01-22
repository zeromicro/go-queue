package rabbitmq

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zeromicro/go-zero/core/logc"
	"github.com/zeromicro/go-zero/core/queue"
)

type (
	ConsumeHandle func(message string) error

	ConsumeHandler interface {
		Consume(message string) error
	}

	ConsumeErrorHandler func(ctx context.Context, msg amqp.Delivery, err error)

	listenerOptions struct {
		errorHandler ConsumeErrorHandler
	}

	ListenerOption func(*listenerOptions)

	RabbitListener struct {
		conn         *amqp.Connection
		channel      *amqp.Channel
		forever      chan bool
		handler      ConsumeHandler
		queues       RabbitListenerConf
		errorHandler ConsumeErrorHandler
	}
)

func MustNewListener(listenerConf RabbitListenerConf, handler ConsumeHandler, opts ...ListenerOption) queue.MessageQueue {
	var options listenerOptions
	for _, opt := range opts {
		opt(&options)
	}
	ensureListenerOptions(&options)

	listener := RabbitListener{
		queues:       listenerConf,
		handler:      handler,
		forever:      make(chan bool),
		errorHandler: options.errorHandler,
	}
	conn, err := amqp.Dial(getRabbitURL(listenerConf.RabbitConf))
	if err != nil {
		log.Fatalf("failed to connect rabbitmq, error: %v", err)
	}

	listener.conn = conn
	channel, err := listener.conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}

	listener.channel = channel
	return listener
}

func (q RabbitListener) Start() {
	for _, que := range q.queues.ListenerQueues {
		que := que
		msg, err := q.channel.Consume(
			que.Name,
			"",
			que.AutoAck,
			que.Exclusive,
			que.NoLocal,
			que.NoWait,
			nil,
		)
		if err != nil {
			log.Fatalf("failed to listener, error: %v", err)
		}

		go func() {
			for d := range msg {
				ctx := context.Background()
				if err := q.handler.Consume(string(d.Body)); err != nil {
					q.errorHandler(ctx, d, err)
					if !que.AutoAck {
						if e := d.Nack(false, true); e != nil {
							logc.Errorf(ctx, "nack failed, error: %v", e)
						}
					}
				}
			}
		}()
	}

	<-q.forever
}

func (q RabbitListener) Stop() {
	q.channel.Close()
	q.conn.Close()
	close(q.forever)
}

func WithErrorHandler(errorHandler ConsumeErrorHandler) ListenerOption {
	return func(options *listenerOptions) {
		options.errorHandler = errorHandler
	}
}

func ensureListenerOptions(options *listenerOptions) {
	if options.errorHandler == nil {
		options.errorHandler = func(ctx context.Context, msg amqp.Delivery, err error) {
			logc.Errorf(ctx, "consume: %s, error: %v", string(msg.Body), err)
		}
	}
}
