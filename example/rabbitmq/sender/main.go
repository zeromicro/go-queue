package main

import (
	"encoding/json"
	"log"

	"github.com/zeromicro/go-queue/rabbitmq"
)

func main() {
	conf := rabbitmq.RabbitSenderConf{RabbitConf: rabbitmq.RabbitConf{
		Host:     "192.168.253.100",
		Port:     5672,
		Username: "guest",
		Password: "guest",
	}, ContentType: "application/json"}
	sender := rabbitmq.MustNewSender(conf)
	data := map[string]interface{}{
		"msg": "json test 111",
	}
	msg, _ := json.Marshal(data)
	err := sender.Send("exchange.direct", "gozero", msg)
	if err != nil {
		log.Fatal(err)
	}

	conf.ContentType = "text/plain"
	sender = rabbitmq.MustNewSender(conf)
	message := "test message"
	err = sender.Send("exchange.direct", "gozero", []byte(message))
	if err != nil {
		log.Fatal(err)
	}
}
