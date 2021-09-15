package main

import (
	"fmt"

	"github.com/tal-tech/go-zero/core/conf"

	queue "github.com/tal-tech/go-queue"
)

func main() {
	var c queue.KqConf
	conf.MustLoad("config.yaml", &c)

	q := queue.NewKafka()
	q.SetUp(c.Brokers, c.Topic)
	q.RegConsumer("test", func(key, value string) error {
		fmt.Printf("=> %s:%s\n", key, value)
		return nil
	})

	defer q.Stop()
	q.Start()

	select {}
}
