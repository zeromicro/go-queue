package common

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"time"
)

// NatsConfig holds the configuration for connecting to a NATS server.
type NatsConfig struct {
	URL     string        // NATS server URL
	Options []nats.Option // Additional NATS connection options
}

// JetStreamManager manages JetStream operations and holds the stream configuration.
type JetStreamManager struct {
	JS         jetstream.JetStream    // JetStream context
	streamConf jetstream.StreamConfig // Configuration for the JetStream stream
}

// JetStreamConfig defines the configuration parameters for a JetStream stream.
type JetStreamConfig struct {
	// Basic configuration
	Name        string   `json:"name"`                              // Stream name (required)
	Description string   `json:"description,optional,default=desc"` // Stream description, default "desc"
	Subjects    []string `json:"subjects"`                          // Subjects associated with the stream (required)

	// Storage and retention policies
	Retention            int   `json:"retention,options=0|1|2,default=0"`
	MaxConsumers         int   `json:"maxConsumers,optional,default=-1"`
	MaxMsgs              int64 `json:"maxMsgs,optional,default=-1"`
	MaxBytes             int64 `json:"maxBytes,optional,default=-1"`
	Discard              int   `json:"discard,options=0|1,default=0"`
	DiscardNewPerSubject bool  `json:"discardNewPerSubject,optional,default=false"`

	// Message lifecycle settings
	MaxAge            time.Duration `json:"maxAge,optional,default=0"`
	MaxMsgsPerSubject int64         `json:"maxMsgsPerSubject,optional,default=10000"`
	MaxMsgSize        int32         `json:"maxMsgSize,optional,default=10000"`

	// Message acknowledgment policy
	NoAck bool `json:"noAck,optional,default=false"`
}

// NewJetStream creates a new instance of JetStreamManager based on the provided configuration.
// Parameters:
//
//	cfg - pointer to a JetStreamConfig containing stream settings
//
// Returns:
//
//	*JetStreamManager - the new JetStreamManager instance
func NewJetStream(cfg *JetStreamConfig) *JetStreamManager {
	// Map custom configuration to jetstream.StreamConfig
	streamConf := jetstream.StreamConfig{
		Name:                 cfg.Name,
		Subjects:             cfg.Subjects,
		Retention:            jetstream.RetentionPolicy(cfg.Retention),
		MaxConsumers:         cfg.MaxConsumers,
		MaxMsgs:              cfg.MaxMsgs,
		MaxBytes:             cfg.MaxBytes,
		Discard:              jetstream.DiscardPolicy(cfg.Discard),
		DiscardNewPerSubject: cfg.DiscardNewPerSubject,
		MaxAge:               cfg.MaxAge,
		MaxMsgsPerSubject:    cfg.MaxMsgsPerSubject,
		MaxMsgSize:           cfg.MaxMsgSize,
		NoAck:                cfg.NoAck,
	}

	return &JetStreamManager{
		streamConf: streamConf,
	}
}
