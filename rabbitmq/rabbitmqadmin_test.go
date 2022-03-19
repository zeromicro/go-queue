package rabbitmq

import "testing"

func TestDeclareExchange(t *testing.T) {
	conf := RabbitMqConf{
		Host:     "192.168.253.100",
		Port:     5672,
		Username: "guest",
		Password: "guest",
	}
	admin := NewRabbitMqAdmin(conf)
	exchangeConf := ExchangeConf{
		ExchangeName: "jiang",
		Type:         "direct",
		Durable:      true,
		AutoDelete:   false,
		Internal:     false,
		NoWait:       false,
	}

	err := admin.DeclareExchange(exchangeConf, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestDeclareQueue(t *testing.T) {
	conf := RabbitMqConf{
		Host:     "192.168.253.100",
		Port:     5672,
		Username: "guest",
		Password: "guest",
	}
	admin := NewRabbitMqAdmin(conf)
	queueConf := QueueConf{
		Name:       "jxj",
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
	}
	err := admin.DeclareQueue(queueConf, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestBind(t *testing.T) {
	conf := RabbitMqConf{
		Host:     "192.168.253.100",
		Port:     5672,
		Username: "guest",
		Password: "guest",
	}
	admin := NewRabbitMqAdmin(conf)
	err := admin.Bind("jxj", "jxj", "jiang", false, nil)
	if err != nil {
		t.Error(err)
	}
}
