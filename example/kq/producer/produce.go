package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/tal-tech/go-zero/core/cmdline"
	"github.com/tal-tech/go-zero/core/conf"

	queue "github.com/tal-tech/go-queue"
)

type message struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Payload string `json:"message"`
}

func main() {
	var c queue.KqConf
	conf.MustLoad("config.yaml", &c)

	pusher := queue.NewKafka()
	pusher.SetUp(c.Brokers, c.Topic)

	ticker := time.NewTicker(time.Millisecond)
	for round := 0; round < 3; round++ {
		select {
		case <-ticker.C:
			count := rand.Intn(100)
			m := message{
				Key:     strconv.FormatInt(time.Now().UnixNano(), 10),
				Value:   fmt.Sprintf("%d,%d", round, count),
				Payload: fmt.Sprintf("%d,%d", round, count),
			}

			if err := pusher.SendMsg(m); err != nil {
				log.Fatal(err)
			}
		}
	}

	cmdline.EnterToContinue()
}
