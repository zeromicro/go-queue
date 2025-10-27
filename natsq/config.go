package natsq

import "github.com/nats-io/nats.go"

type NatsConfig struct {
	ServerUri  string
	ClientName string
	Options    []nats.Option
}
