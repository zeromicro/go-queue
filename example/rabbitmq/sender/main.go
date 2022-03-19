package main

import (
	"encoding/json"
	"github.com/zeromicro/go-queue/rabbitmq"
	"log"
)

func main() {

	conf := rabbitmq.RabbitMqSenderConf{RabbitMqConf: rabbitmq.RabbitMqConf{
		Host:     "192.168.253.100",
		Port:     5672,
		Username: "guest",
		Password: "guest",
	}, ContentType: "application/json"}
	sender := rabbitmq.MustNewRabbitMqSender(conf)
	data := map[string]interface{}{
		"msg": "json test 111",
	}
	msg, _ := json.Marshal(data)
	err := sender.Send("exchange.direct", "gozero", msg)
	if err != nil {
		log.Fatal(err)
	}
	conf.ContentType = "text/plain"
	sender = rabbitmq.MustNewRabbitMqSender(conf)
	message := "test message"
	err = sender.Send("exchange.direct", "gozero", []byte(message))
	if err != nil {
		log.Fatal(err)
	}
}
