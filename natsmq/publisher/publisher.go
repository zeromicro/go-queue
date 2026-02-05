package publisher

import (
	"context"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/zeromicro/go-queue/natsmq/common"
	"github.com/zeromicro/go-queue/natsmq/internal"
	"go.opentelemetry.io/otel"
)

// JetStreamPublisher implements the Publisher interface by utilizing an internal JetStream context for message publishing.
// Note: It is recommended to rename the package from "publiser" to "publisher" for better clarity.
type JetStreamPublisher struct {
	natsConf *common.NatsConfig
	conn     *nats.Conn
	js       jetstream.JetStream
}

// NewJetStreamPublisher creates a new JetStreamPublisher instance based on the provided NATS configuration.
func NewJetStreamPublisher(natsConf *common.NatsConfig) (*JetStreamPublisher, error) {
	p := &JetStreamPublisher{
		natsConf: natsConf,
	}

	// Establish a connection to the NATS server using the URL and options from the configuration.
	if err := p.connectToNATS(); err != nil {
		return nil, fmt.Errorf("connect to NATS failed: %w", err)
	}

	// Initialize the JetStream context.
	if err := p.initJetStream(); err != nil {
		return nil, fmt.Errorf("initialize JetStream failed: %w", err)
	}

	return p, nil
}

// connectToNATS establishes a connection to the NATS server using the URL and Options specified in the configuration.
func (p *JetStreamPublisher) connectToNATS() error {
	conn, err := nats.Connect(p.natsConf.URL, p.natsConf.Options...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS at %s: %w", p.natsConf.URL, err)
	}
	p.conn = conn
	log.Printf("Successfully connected to NATS: %s", p.natsConf.URL)
	return nil
}

// initJetStream initializes the JetStream context.
// For newer versions of the NATS library, consider using p.conn.JetStream() instead.
func (p *JetStreamPublisher) initJetStream() error {
	// Using the jetstream.New method (legacy API); if there are no special requirements,
	// you can switch to the newer API:
	// js, err := p.conn.JetStream()
	// if err != nil {
	//     return fmt.Errorf("failed to create JetStream context: %w", err)
	// }
	// p.js = js
	js, err := jetstream.New(p.conn)
	if err != nil {
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}
	p.js = js
	log.Printf("JetStream context initialized")
	return nil
}

// Publish synchronously publishes a message to the specified subject and waits for a server acknowledgment.
func (p *JetStreamPublisher) Publish(ctx context.Context, subject string, payload []byte) (*jetstream.PubAck, error) {
	ack, err := p.PublishWithHeaders(ctx, subject, payload, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to publish message on subject %s: %w", subject, err)
	}
	return ack, nil
}

// PublishWithHeaders publishes a message with optional headers and waits for a server acknowledgment.
func (p *JetStreamPublisher) PublishWithHeaders(ctx context.Context, subject string, payload []byte, headers map[string]string) (*jetstream.PubAck, error) {
	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
	}
	if len(headers) > 0 {
		msg.Header = make(nats.Header, len(headers))
		for key, value := range headers {
			msg.Header.Set(key, value)
		}
	}

	return p.PublishMsg(ctx, msg)
}

// PublishMsg publishes a full NATS message and waits for a server acknowledgment.
func (p *JetStreamPublisher) PublishMsg(ctx context.Context, msg *nats.Msg) (*jetstream.PubAck, error) {
	if msg == nil {
		return nil, fmt.Errorf("message is nil")
	}

	// inject trace context into message headers
	mc := internal.NewHeaderCarrier(&msg.Header)
	otel.GetTextMapPropagator().Inject(ctx, mc)

	ack, err := p.js.PublishMsg(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish message on subject %s: %w", msg.Subject, err)
	}
	return ack, nil
}

// Close terminates the NATS connection used by the JetStreamPublisher and releases all associated resources.
func (p *JetStreamPublisher) Close() {
	if p.conn != nil {
		p.conn.Close()
		log.Printf("NATS connection closed")
	}
}
