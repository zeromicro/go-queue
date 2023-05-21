package rabbitmq

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Admin struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func MustNewAdmin(rabbitMqConf RabbitConf) *Admin {
	var admin Admin
	conn, err := amqp.Dial(getRabbitURL(rabbitMqConf))
	if err != nil {
		log.Fatalf("failed to connect rabbitmq, error: %v", err)
	}

	admin.conn = conn
	channel, err := admin.conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel, error: %v", err)
	}

	admin.channel = channel
	return &admin
}

func (q *Admin) DeclareExchange(conf ExchangeConf, args amqp.Table) error {
	return q.channel.ExchangeDeclare(
		conf.ExchangeName,
		conf.Type,
		conf.Durable,
		conf.AutoDelete,
		conf.Internal,
		conf.NoWait,
		args,
	)
}

func (q *Admin) DeclareQueue(conf QueueConf, args amqp.Table) error {
	_, err := q.channel.QueueDeclare(
		conf.Name,
		conf.Durable,
		conf.AutoDelete,
		conf.Exclusive,
		conf.NoWait,
		args,
	)

	return err
}

func (q *Admin) Bind(queueName string, routekey string, exchange string, notWait bool, args amqp.Table) error {
	return q.channel.QueueBind(
		queueName,
		routekey,
		exchange,
		notWait,
		args,
	)
}
