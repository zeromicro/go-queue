package rabbitmq

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type (
	Sender interface {
		Send(exchange string, routeKey string, msg []byte) error
	}

	RabbitMqSender struct {
		conn        *amqp.Connection
		channel     *amqp.Channel
		ContentType string
	}
)

func MustNewSender(rabbitMqConf RabbitSenderConf) Sender {
	sender := &RabbitMqSender{ContentType: rabbitMqConf.ContentType}
	conn, err := amqp.Dial(getRabbitURL(rabbitMqConf.RabbitConf))
	if err != nil {
		log.Fatalf("failed to connect rabbitmq, error: %v", err)
	}

	sender.conn = conn
	channel, err := sender.conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel, error: %v", err)
	}

	sender.channel = channel
	return sender
}

func (q *RabbitMqSender) Send(exchange string, routeKey string, msg []byte) error {
	return q.channel.PublishWithContext(
		context.Background(),
		exchange,
		routeKey,
		false,
		false,
		amqp.Publishing{
			ContentType: q.ContentType,
			Body:        msg,
		},
	)
}
