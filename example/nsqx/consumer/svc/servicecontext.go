package svc

import "github.com/zeromicro/go-queue/example/nsqx/consumer/config"

type ServiceContext struct {
	Config config.Config
}

func NewServiceContext(config config.Config) *ServiceContext {
	return &ServiceContext{
		Config: config,
	}
}
