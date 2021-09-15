package main

import (
	"fmt"

	queue "github.com/tal-tech/go-queue"

	"github.com/tal-tech/go-zero/core/stores/redis"
)

func main() {
	consumer := queue.NewDelayMq()
	consumer.SetUp([]string{"localhost:11300", "localhost:11301"}, "tube", redis.RedisConf{
		Host: "localhost:6379",
		Type: redis.NodeType,
	})

	consumer.RegConsumer(1, func(body []byte) {
		fmt.Println(string(body))
	})

	consumer.Start()
	defer consumer.Stop()

	select {}
}
