package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/zeromicro/go-queue/dq"
)

func main() {
	producer := dq.NewProducerNode("localhost:11300", "tube")

	for i := 1000; i < 1005; i++ {
		_, err := producer.Delay([]byte(strconv.Itoa(i)), time.Second*5)
		if err != nil {
			fmt.Println(err)
		}
	}
}
