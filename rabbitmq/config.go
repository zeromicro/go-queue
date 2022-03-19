package rabbitmq

import "fmt"

type RabbitMqConf struct {
	Username string
	Password string
	Host     string
	Port     int
	VHost    string `json:",optional"`
}

type RabbitMqListenerConf struct {
	RabbitMqConf
	ListenerQueues []ConsumerConf
}

type ConsumerConf struct {
	Name      string
	AutoAck   bool `json:",default=true"`
	Exclusive bool `json:",default=false"`
	NoLocal   bool `json:",default=false"` // Set to true, which means that messages sent by producers in the same connection cannot be delivered to consumers in this connection
	NoWait    bool `json:",default=false"` // Whether to block processing
}

type RabbitMqSenderConf struct {
	RabbitMqConf
	ContentType string // MIME content type
}

type QueueConf struct {
	Name       string
	Durable    bool `json:",default=true"`
	AutoDelete bool `json:",default=false"`
	Exclusive  bool `json:",default=false"`
	NoWait     bool `json:",default=false"`
}

type ExchangeConf struct {
	ExchangeName string
	Type         string `json:",options=direct|fanout|topic|headers"` // exchange type
	Durable      bool   `json:",default=true"`
	AutoDelete   bool   `json:",default=false"`
	Internal     bool   `json:",default=false"`
	NoWait       bool   `json:",default=false"`
	Queues       []QueueConf
}

func getRabbitMqURL(rabbitConf RabbitMqConf) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", rabbitConf.Username, rabbitConf.Password, rabbitConf.Host, rabbitConf.Port, rabbitConf.VHost)
}
