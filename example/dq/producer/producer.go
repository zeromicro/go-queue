package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/tal-tech/go-zero/core/stores/redis"

	queue "github.com/tal-tech/go-queue"
)

func main() {
	producer := queue.NewDelayMq()
	producer.SetUp([]string{"localhost:11300", "localhost:11301"}, "tube", redis.RedisConf{
		Host: "localhost:6379",
		Type: redis.NodeType,
	})

	for i := 1000; i < 1005; i++ {
		taskId, err := producer.Delay(1, []byte(strconv.Itoa(i)), time.Second*5)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(taskId)
		}
	}
}
