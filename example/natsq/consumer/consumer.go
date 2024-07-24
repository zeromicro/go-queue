package main

import (
	"fmt"

	"github.com/zeromicro/go-queue/natsq"
)

type MyConsumer struct {
	Channel string
}

func (c *MyConsumer) HandleMessage(m *natsq.Msg) error {
	fmt.Printf("%s Received %s's a message: %s\n", c.Channel, m.Subject, string(m.Data))
	return nil
}

func main() {

	mc1 := &MyConsumer{Channel: "vipUpgrade"}
	mc2 := &MyConsumer{Channel: "taskFinish"}

	c := &natsq.NatsConfig{
		ServerUri: "nats://127.0.0.1:4222",
	}

	//JetMode
	// cq := []*natsq.ConsumerQueue{
	// 	{
	// 		Consumer:   mc1,
	// 		QueueName:  "vipUpgrade",
	// 		StreamName: "ccc",
	// 		Subjects:   []string{"ddd", "eee"},
	// 	},
	// 	{
	// 		Consumer:   mc2,
	// 		QueueName:  "taskFinish",
	// 		StreamName: "ccc",
	// 		Subjects:   []string{"ccc", "eee"},
	// 	},
	// }
	//q := natsq.MustNewConsumerManager(c, cq, natsq.NatJetMode)

	//DefaultMode
	cq := []*natsq.ConsumerQueue{
		{
			Consumer:  mc1,
			QueueName: "vipUpgrade",
			Subjects:  []string{"ddd", "eee"},
		},
		{
			Consumer:  mc2,
			QueueName: "taskFinish",
			Subjects:  []string{"ccc", "eee"},
		},
	}
	q := natsq.MustNewConsumerManager(c, cq, natsq.NatDefaultMode)
	q.Start()
	defer q.Stop()
}
