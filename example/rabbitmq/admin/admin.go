package main

import (
	"github.com/zeromicro/go-queue/rabbitmq"
	"log"
)

func main() {

	conf := rabbitmq.RabbitMqConf{
		Host:     "192.168.253.100",
		Port:     5672,
		Username: "guest",
		Password: "guest",
	}
	admin := rabbitmq.MustNewRabbitMqAdmin(conf)
	exchangeConf := rabbitmq.ExchangeConf{
		ExchangeName: "jiang",
		Type:         "direct",
		Durable:      true,
		AutoDelete:   false,
		Internal:     false,
		NoWait:       false,
	}

	err := admin.DeclareExchange(exchangeConf, nil)
	if err != nil {
		log.Fatal(err)
	}
	queueConf := rabbitmq.QueueConf{
		Name:       "jxj",
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
	}
	err = admin.DeclareQueue(queueConf, nil)
	if err != nil {
		log.Fatal(err)
	}
	err = admin.Bind("jxj", "jxj", "jiang", false, nil)
	if err != nil {
		log.Fatal(err)
	}
}
