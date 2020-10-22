package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/tal-tech/go-zero/core/cmdline"

	"github.com/tal-tech/go-queue/kq"
)

type message struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Payload string `json:"message"`
}

func main() {
	pusher := kq.NewPusher([]string{
		"127.0.0.1:19092",
		"127.0.0.1:19092",
		"127.0.0.1:19092",
	}, "kq")

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
			body, err := json.Marshal(m)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println(string(body))
			if err := pusher.Push(string(body)); err != nil {
				log.Fatal(err)
			}
		}
	}

	cmdline.EnterToContinue()
}
