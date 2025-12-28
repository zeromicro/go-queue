package main

import (
	"flag"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zeromicro/go-queue/example/rabbitmq/listener/config"
	"github.com/zeromicro/go-queue/rabbitmq"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
)

var configFile = flag.String("f", "listener.yaml", "Specify the config file")

func main() {
	flag.Parse()
	var c config.Config
	conf.MustLoad(*configFile, &c)

	// Use ManualAckHandler for manual acknowledgment
	listener := rabbitmq.MustNewListener(c.ListenerConf, &ManualAckHandler{})
	serviceGroup := service.NewServiceGroup()
	serviceGroup.Add(listener)
	defer serviceGroup.Stop()
	serviceGroup.Start()
}

// ManualAckHandler demonstrates manual acknowledgment
type ManualAckHandler struct {
}

func (h *ManualAckHandler) Consume(message string) error {
	fmt.Printf("Auto-ack handler received: %s\n", message)
	return nil
}

// ConsumeWithAck allows manual control over message acknowledgment
func (h *ManualAckHandler) ConsumeWithAck(message string, delivery amqp.Delivery) error {
	fmt.Printf("Manual-ack handler received: %s\n", message)

	// Process the message
	if message == "error" {
		// On error, reject the message (don't requeue)
		if err := delivery.Reject(false); err != nil {
			fmt.Printf("Failed to reject message: %v\n", err)
			return err
		}
		fmt.Println("Message rejected")
		return fmt.Errorf("simulated error")
	} else if message == "retry" {
		// On temporary error, nack and requeue
		if err := delivery.Nack(false, true); err != nil {
			fmt.Printf("Failed to nack message: %v\n", err)
			return err
		}
		fmt.Println("Message nacked and requeued")
		return fmt.Errorf("simulated temporary error")
	} else {
		// On success, acknowledge the message
		if err := delivery.Ack(false); err != nil {
			fmt.Printf("Failed to ack message: %v\n", err)
			return err
		}
		fmt.Println("Message acknowledged")
		return nil
	}
}
