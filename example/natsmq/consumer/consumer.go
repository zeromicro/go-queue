package main

import (
	"flag"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/zeromicro/go-queue/natsmq/common"
	"github.com/zeromicro/go-queue/natsmq/consumer"
	"github.com/zeromicro/go-zero/core/conf"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var configFile = flag.String("f", "config.yaml", "Specify the config file")

type ConsumerExampleConfig struct {
	Streams        []*common.JetStreamConfig      `json:"streams"`
	Nats           NatsConf                       `json:"nats"`
	ConsumerQueues []consumer.ConsumerQueueConfig `json:"consumerQueues"`
}

type NatsConf struct {
	URL string `json:"url"`
}

type MyConsumeHandler struct{}

func (h *MyConsumeHandler) Consume(msg jetstream.Msg) error {
	log.Printf("subject [%s] Received message: %s", msg.Subject(), string(msg.Data()))
	return nil
}

func main() {
	flag.Parse()
	var c ConsumerExampleConfig
	conf.MustLoad(*configFile, &c)

	var queueConfigs []*consumer.ConsumerQueueConfig
	for i := range c.ConsumerQueues {
		c.ConsumerQueues[i].Handler = &MyConsumeHandler{}
		queueConfigs = append(queueConfigs, &c.ConsumerQueues[i])
	}

	natsConf := &common.NatsConfig{
		URL:     c.Nats.URL,
		Options: []nats.Option{},
	}

	cm, err := consumer.NewConsumerManager(natsConf, c.Streams, queueConfigs)
	if err != nil {
		log.Fatalf("failed to create consumer manager: %v", err)
	}

	go cm.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	log.Printf("Received signal %s, shutting down...", sig)
	cm.Stop()
	time.Sleep(time.Second)
}
