package main

import (
	"log"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/zeromicro/go-queue/natsq"
)

func main() {

	c := natsq.NatsConfig{
		ServerUri: "127.0.0.1:4222",
	}

	// Default Mode
	p, _ := natsq.NewDefaultProducer(&c)
	for i := 0; i < 3; i++ {
		payload := randBody()
		err := p.Publish(randSub(), payload)
		if err != nil {
			log.Fatalf("Error publishing message: %v", err)
		} else {
			log.Printf("Published message: %s", string(payload))
		}
	}
	p.Close()

	// JetMode
	j, _ := natsq.NewJetProducer(&c)
	j.CreateOrUpdateStream(jetstream.StreamConfig{
		Name:     "ccc",
		Subjects: []string{"ccc", "ddd", "eee"},
		Storage:  jetstream.FileStorage,
		NoAck:    false,
	})
	for i := 0; i < 3; i++ {
		payload := randBody()
		err := j.Publish(randSub(), payload)
		if err != nil {
			log.Fatalf("Error publishing message: %v", err)
		} else {
			log.Printf("Published message: %s", string(payload))
		}
	}
	j.Close()
}

func randSub() string {
	rand.Seed(time.Now().UnixNano())
	strings := []string{"ccc", "ddd", "eee"}
	randomIndex := rand.Intn(len(strings))
	return strings[randomIndex]
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
