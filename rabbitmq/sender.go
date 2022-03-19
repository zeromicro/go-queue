package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"github.com/zeromicro/go-zero/core/logx"
)

type (
	Sender interface {
		Send(string, string, string) error
	}

	RabbitMqSender struct {
		conn    *amqp.Connection
		channel *amqp.Channel
	}
)

func NewRabbitMqSender(rabbitMqConf RabbitMqSenderConf) Sender {
	sender := &RabbitMqSender{}
	conn, err := amqp.Dial(getRabbitMqURL(rabbitMqConf.RabbitMqConf))
	sender.ErrorHandler(err, "failed to connect rabbitmq!")
	sender.conn = conn

	channel, err := sender.conn.Channel()
	sender.ErrorHandler(err, "failed to open a channel")
	sender.channel = channel
	return sender
}
func (q *RabbitMqSender) ErrorHandler(err error, message string) {
	if err != nil {
		logx.Errorf("%s:%s", message, err)
		panic(fmt.Sprintf("%s:%s", message, err))
	}
}
func (q *RabbitMqSender) Send(exchange string, routeKey string, message string) error {

	err := q.channel.Publish(
		exchange,
		routeKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
	return err
}
