package config

import "github.com/zeromicro/go-queue/rabbitmq"

type Config struct {
	ListenerConf rabbitmq.RabbitListenerConf
}
