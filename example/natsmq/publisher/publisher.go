package main

import (
	"context"
	"flag"
	"github.com/nats-io/nats.go"
	"github.com/zeromicro/go-queue/natsmq/common"
	"github.com/zeromicro/go-queue/natsmq/publisher"
	"github.com/zeromicro/go-zero/core/conf"

	"log"
	"time"
)

var configFile = flag.String("f", "config.yaml", "Specify the config file")

type PublisherExampleConfig struct {
	Nats NatsConf `json:"nats"`
}

type NatsConf struct {
	URL string `json:"url"`
}

func main() {
	flag.Parse()
	var c PublisherExampleConfig
	conf.MustLoad(*configFile, &c)

	natsConf := &common.NatsConfig{
		URL:     c.Nats.URL,
		Options: []nats.Option{},
	}

	jSPublisher, err := publisher.NewJetStreamPublisher(natsConf)

	if err != nil {
		log.Fatalf("failed to NewJetStreamPublisher message: %v", err)
	}

	subjects := []string{
		"user.register",
		"user.recharge",
		"subject.activity.example",
	}
	messages := []string{
		"Test message: user.register message",
		"Test message: user.recharge message",
		"Test message: subject.activity message",
	}
	ctx := context.Background()
	for i, subj := range subjects {
		msg := []byte(messages[i])
		for j := 0; j < 3; j++ {
			go func(s string, m []byte) {
				ack, err := jSPublisher.Publish(ctx, s, m)
				if err != nil {
					log.Fatalf("failed to publish message: %v", err)
				}
				log.Printf("published message to %s, ack: %+v", s, ack.Stream)
			}(subj, msg)
		}
	}

	time.Sleep(2 * time.Second)
}
