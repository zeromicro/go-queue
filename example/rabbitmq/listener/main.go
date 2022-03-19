package main

import (
	"flag"
	"fmt"
	"github.com/zeromicro/go-queue/example/rabbitmq/listener/config"
	"github.com/zeromicro/go-queue/rabbitmq"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
)

var configFile = flag.String("f", "listener.yaml", "Specify the config file")

func main() {
	flag.Parse()
	var c config.Config
	conf.MustLoad(*configFile, &c)

	listener := rabbitmq.MustNewRabbitMqListener(c.ListenerConf, Handler{})

	serviceGroup := service.NewServiceGroup()
	serviceGroup.Add(listener)
	defer serviceGroup.Stop()
	serviceGroup.Start()

}

type Handler struct {
}

func (h Handler) Consume(message string) error {
	fmt.Printf("listener %s\n", message)
	return nil
}
