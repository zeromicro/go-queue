package main

import (
	"fmt"
	"github.com/nats-io/stan.go"
	"github.com/zeromicro/go-queue/stanq"
)

type MyConsumer struct {
	Channel string
}

func (c *MyConsumer) HandleMessage(m *stan.Msg) error {
	fmt.Printf("%s Received a message: %s\n", c.Channel, string(m.Data))
	return nil
}

func main() {

	mc1 := &MyConsumer{Channel: "vip"}
	mc2 := &MyConsumer{Channel: "recharge"}
	mc3 := &MyConsumer{Channel: "levelUp"}

	c := &stanq.StanqConfig{
		ClusterID: "nats-streaming",
		ClientID:  "vip-consumer",
		Options: []stan.Option{
			stan.NatsURL("nats://127.0.0.1:14222,nats://127.0.0.1:24222,nats://127.0.0.1:34222"),
		},
	}
	cq := []*stanq.ConsumerQueue{
		{
			Consumer:      mc1,
			GroupName:     "vip",
			QueueName:     "vip1",
			Subject:       "a",
			ManualAckMode: false,
		},
		{
			Consumer:      mc2,
			GroupName:     "recharge",
			QueueName:     "recharge1",
			Subject:       "b",
			ManualAckMode: false,
		},
		{
			Consumer:      mc3,
			GroupName:     "levelUp",
			QueueName:     "levelUp1",
			Subject:       "c",
			ManualAckMode: false,
		},
	}

	q := stanq.MustNewConsumerManager(c, cq)
	q.Start()
	defer q.Stop()
}
