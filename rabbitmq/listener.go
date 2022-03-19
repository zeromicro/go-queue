package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/queue"
	"log"
)

type (
	ConsumeHandle func(message string) error

	ConsumeHandler interface {
		Consume(message string) error
	}

	RabbitListener struct {
		conn    *amqp.Connection
		channel *amqp.Channel
		forever chan bool
		handler ConsumeHandler
		queues  RabbitMqListenerConf
	}
)

func MustNewRabbitMqListener(listenerConf RabbitMqListenerConf, handler ConsumeHandler) queue.MessageQueue {
	q, err := newRabbitMq(listenerConf, handler)
	if err != nil {
		log.Fatal(err)
	}

	return q
}

func newRabbitMq(listenerConf RabbitMqListenerConf, handler ConsumeHandler) (queue.MessageQueue, error) {
	listener := RabbitListener{queues: listenerConf, handler: handler, forever: make(chan bool)}
	conn, err := amqp.Dial(getRabbitMqURL(listenerConf.RabbitMqConf))
	listener.ErrorHandler(err, "failed to connect rabbitmq!")
	listener.conn = conn

	channel, err := listener.conn.Channel()
	listener.ErrorHandler(err, "failed to open a channel")
	listener.channel = channel
	return listener, nil
}

func (q RabbitListener) ErrorHandler(err error, message string) {
	if err != nil {
		logx.Errorf("%s:%s", message, err)
		panic(fmt.Sprintf("%s:%s", message, err))
	}
}

func (q RabbitListener) Start() {

	for _, que := range q.queues.ListenerQueues {
		msg, err := q.channel.Consume(
			que.Name,
			"",
			que.AutoAck,
			que.Exclusive,
			que.NoLocal,
			que.NoWait,
			nil,
		)
		q.ErrorHandler(err, "failed to listener")
		go func() {
			for d := range msg {
				if err := q.handler.Consume(string(d.Body)); err != nil {
					logx.Errorf("Error on consuming: %s, error: %v", string(d.Body), err)
				}
			}
		}()
	}
	<-q.forever
}

func (q RabbitListener) Stop() {
	q.channel.Close()
	q.channel.Close()
	close(q.forever)
}
