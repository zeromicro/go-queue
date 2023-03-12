package main

import (
	"fmt"
	"github.com/nats-io/stan.go"
	"github.com/zeromicro/go-queue/stanq"
	"math/rand"
	"time"
)

func main() {
	c := &stanq.StanqConfig{
		ClusterID: "nats-streaming",
		ClientID:  "publish123",
		Options:   []stan.Option{stan.NatsURL("nats://127.0.0.1:14222,nats://127.0.0.1:24222,nats://127.0.0.1:34222")},
	}
	p, err := stanq.NewProducer(c)
	if err != nil {
		fmt.Println(err.Error())
	}

	publish, err := p.AsyncPublish(randSub(), randBody(), func(guid string, err error) {
		if err != nil {
			fmt.Printf("failed to publish message with guid %s: %v\n", guid, err)
		} else {
			fmt.Printf("message with guid %s published\n", guid)
		}
	})
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println(publish)
	}

	for {
		time.Sleep(300 * time.Millisecond)
		sub := randSub()
		err = p.Publish(sub, []byte(fmt.Sprintf("%s-%s", sub, randBody())))
		if err != nil {
			fmt.Println(err.Error())
		}
	}

}

func randSub() string {
	rand.Seed(time.Now().UnixNano())
	charSet := "abc"
	length := 1
	result := make([]byte, length)
	for i := range result {
		result[i] = charSet[rand.Intn(len(charSet))]
	}
	return string(result)
}

func randBody() []byte {
	charSet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	length := 10
	result := make([]byte, length)
	for i := range result {
		result[i] = charSet[rand.Intn(len(charSet))]
	}
	return result
}
