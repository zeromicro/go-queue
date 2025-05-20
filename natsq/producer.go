package natsq

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type DefaultProducer struct {
	conn *nats.Conn
}

// NewDefaultProducer creates a new default NATS producer.
// It takes a NatsConfig as input and returns a pointer to a DefaultProducer and an error.
// It connects to the NATS server using the provided configuration.
func NewDefaultProducer(c *NatsConfig) (*DefaultProducer, error) {
	sc, err := nats.Connect(c.ServerUri, c.Options...)
	if err != nil {
		return nil, err
	}

	return &DefaultProducer{
		conn: sc,
	}, nil
}

// Publish publishes a message with the specified subject and data using the default NATS producer.
// It takes a subject string and data byte slice as input and returns an error if the publish fails.
func (p *DefaultProducer) Publish(subject string, data []byte) error {
	return p.conn.Publish(subject, data)
}

// Close closes the NATS connection of the default producer.
func (p *DefaultProducer) Close() {
	if p.conn != nil {
		p.conn.Close()
	}
}

type JetProducer struct {
	conn *nats.Conn
	js   jetstream.JetStream
	ctx  context.Context
}

// NewJetProducer creates a new JetStream producer.
// It takes a NatsConfig as input and returns a pointer to a JetProducer and an error.
// It connects to the NATS server using the provided configuration and creates a new JetStream context.
func NewJetProducer(c *NatsConfig, ctx context.Context) (*JetProducer, error) {
	sc, err := nats.Connect(c.ServerUri, c.Options...)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(sc)
	if err != nil {
		return nil, err
	}
	return &JetProducer{
		conn: sc,
		js:   js,
		ctx:  ctx,
	}, nil
}

// CreateOrUpdateStream creates or updates a JetStream stream with the specified configuration.
// It takes a jetstream.StreamConfig as input and returns an error if the operation fails.
func (j *JetProducer) CreateOrUpdateStream(config jetstream.StreamConfig) error {
	_, err := j.js.CreateOrUpdateStream(j.ctx, config)
	if err != nil {
		return err
	}
	return nil
}

// Publish publishes a message with the specified subject and data using the JetStream producer.
// It takes a subject string and data byte slice as input and returns an error if the publish fails.
func (j *JetProducer) Publish(subject string, data []byte) error {
	_, err := j.js.Publish(j.ctx, subject, data)
	if err != nil {
		return err
	}
	return nil
}

// Close closes the NATS connection of the JetStream producer.
func (j *JetProducer) Close() {
	j.conn.Close()
}
