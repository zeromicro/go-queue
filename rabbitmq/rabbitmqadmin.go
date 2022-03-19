package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"github.com/zeromicro/go-zero/core/logx"
)

type (
	RabbitMqAdmin struct {
		conn    *amqp.Connection
		channel *amqp.Channel
	}
)

func NewRabbitMqAdmin(rabbitMqConf RabbitMqConf) *RabbitMqAdmin {
	admin := &RabbitMqAdmin{}
	conn, err := amqp.Dial(getRabbitMqURL(rabbitMqConf))
	admin.ErrorHandler(err, "failed to connect rabbitmq!")
	admin.conn = conn

	channel, err := admin.conn.Channel()
	admin.ErrorHandler(err, "failed to open a channel")
	admin.channel = channel
	return admin
}

func (q *RabbitMqAdmin) ErrorHandler(err error, message string) {
	if err != nil {
		logx.Errorf("%s:%s", message, err)
		panic(fmt.Sprintf("%s:%s", message, err))
	}
}

func (q *RabbitMqAdmin) DeclareExchange(conf ExchangeConf, args amqp.Table) error {
	err := q.channel.ExchangeDeclare(
		conf.ExchangeName,
		conf.Type,
		conf.Durable,
		conf.AutoDelete,
		conf.Internal,
		conf.NoWait,
		args,
	)

	return err
}

func (q *RabbitMqAdmin) DeclareQueue(conf QueueConf, args amqp.Table) error {
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

func (q *RabbitMqAdmin) Bind(queueName string, routekey string, exchange string, notWait bool, args amqp.Table) error {
	err := q.channel.QueueBind(
		queueName,
		routekey,
		exchange,
		notWait,
		args,
	)
	return err
}
