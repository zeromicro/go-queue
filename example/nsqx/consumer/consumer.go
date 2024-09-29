package main

import (
	"fmt"
	"github.com/zeromicro/go-queue/example/nsqx/consumer/config"
	"github.com/zeromicro/go-queue/example/nsqx/consumer/handler"
	"github.com/zeromicro/go-queue/example/nsqx/consumer/svc"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	var c config.Config
	conf.MustLoad("dev.yaml", &c)

	ctx := svc.NewServiceContext(c)
	group := service.NewServiceGroup()
	handler.RegisterConsumer(group, ctx)
	group.Start()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-signals
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			fmt.Println("stop queue")
			group.Stop()
			time.Sleep(time.Second)
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}
