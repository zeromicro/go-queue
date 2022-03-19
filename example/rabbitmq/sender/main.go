package main

import (
	"github.com/zeromicro/go-queue/rabbitmq"
	"log"
)

func main() {

	conf := rabbitmq.RabbitMqSenderConf{RabbitMqConf: rabbitmq.RabbitMqConf{
		Host:     "192.168.253.100",
		Port:     5672,
		Username: "guest",
		Password: "guest",
	}}
	sender := rabbitmq.NewRabbitMqSender(conf)
	err := sender.Send("exchange.direct", "gozero", "test")
	if err != nil {
		log.Fatal(err)
	}
}
