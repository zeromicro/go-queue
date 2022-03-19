package rabbitmq

import (
	"testing"
)

func TestRabbitMqSender(t *testing.T) {
	conf := RabbitMqSenderConf{RabbitMqConf: RabbitMqConf{
		Host:     "192.168.253.100",
		Port:     5672,
		Username: "guest",
		Password: "guest",
	}}
	sender := NewRabbitMqSender(conf)
	err := sender.Send("exchange.direct", "atguigu", "test")
	if err != nil {
		t.Error(err)
	}
}
