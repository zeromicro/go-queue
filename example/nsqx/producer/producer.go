package main

import (
	"fmt"
	"github.com/zeromicro/go-queue/example/nsqx/msg"
	"github.com/zeromicro/go-queue/example/nsqx/topic"
	"github.com/zeromicro/go-queue/nsqx"
	"time"
)

type Sender struct {
	TestSender      nsqx.Sender
	TestMultiSender nsqx.MultiSender
	TestDelaySender nsqx.DelaySender
}

func main() {
	producerConfig := nsqx.NewProducerConfig("local.dev:4150")
	producer, _ := nsqx.NewProducer(producerConfig)
	sender := &Sender{
		TestSender:      producer.Sender(topic.Test),
		TestMultiSender: producer.MultiSender(topic.Test),
		TestDelaySender: producer.DelaySender(topic.Test),
	}

	for i := 0; i < 1; i++ {
		_ = sender.TestSender(&msg.TestMsg{A: fmt.Sprintf("TestSender:[%d]%v", i, time.Now())})
		_ = sender.TestMultiSender([]nsqx.Msg{
			&msg.TestMsg{A: fmt.Sprintf("TestMultiSender:[%d]%v", i, time.Now())},
			&msg.TestMsg{A: fmt.Sprintf("TestMultiSender:[%d]%v", i, time.Now())},
		})
		_ = sender.TestDelaySender(&msg.TestMsg{A: fmt.Sprintf("TestDelaySender[%d]%v", i, time.Now())}, 10*time.Second)
	}
}
