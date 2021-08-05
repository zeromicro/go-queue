package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/tal-tech/go-queue/pq"
	"github.com/tal-tech/go-zero/core/cmdline"
)

type message struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Payload string `json:"message"`
}

func main() {
	pusher := pq.NewPusher([]string{
        "127.0.0.1:6650",
	}, "pq")

	ticker := time.NewTicker(time.Millisecond)

	for round := 0; round < 30; round++ {
		select {
		case <-ticker.C:
			key := strconv.FormatInt(time.Now().UnixNano(), 10)
			count := rand.Intn(100)
			m := message{
				Key:     key,
				Value:   fmt.Sprintf("%d,%d", round, count),
				Payload: fmt.Sprintf("%d,%d", round, count),
			}
			body, err := json.Marshal(m)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println(string(body))
			if err := pusher.Push(key, body, nil); err != nil {
				log.Fatal(err)
			}
		}
	}

	cmdline.EnterToContinue()
}
