package common

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log"
	"sync"
	"time"
)

const (
	DefaultStream = "defaultStream"
)

var (
	streamRegistry  = make(map[string]*JetStreamManager)
	registryLock    sync.RWMutex
	streamInstances = make(map[string]jetstream.Stream)
	streamInstLock  sync.RWMutex
)

// RegisterManager registers a JetStreamManager with the specified streamID.
func RegisterManager(streamID string, mgr *JetStreamManager) {
	registryLock.Lock()
	defer registryLock.Unlock()
	streamRegistry[streamID] = mgr
}

// GetManager retrieves the JetStreamManager for the given streamID.
// Returns the manager and true if found; otherwise returns nil and false.
func GetManager(streamID string) (*JetStreamManager, bool) {
	registryLock.RLock()
	defer registryLock.RUnlock()
	mgr, ok := streamRegistry[streamID]
	return mgr, ok
}

// RegisterStreamInstances initializes the JetStream contexts (if needed),
// creates or updates streams based on the provided JetStream configurations,
// and stores the stream instances in a global map for later usage.
// Parameters:
//
//	nc   - pointer to the NATS connection
//	cfgs - list of JetStreamConfig configurations to register
func RegisterStreamInstances(nc *nats.Conn, cfgs []*JetStreamConfig) {
	// Register managers for each provided configuration if not already registered.
	if len(cfgs) > 0 {
		for _, cfg := range cfgs {
			if _, ok := GetManager(cfg.Name); !ok {
				mgr := NewJetStream(cfg)
				RegisterManager(cfg.Name, mgr)
			} else {
				log.Printf("manager for stream %q already registered", cfg.Name)
			}
		}
	}

	// Iterate through all registered stream managers to initialize JetStream and create stream instances.
	for streamName, streamMgr := range streamRegistry {
		streamInstLock.RLock()
		_, exists := streamInstances[streamName]
		streamInstLock.RUnlock()
		if exists {
			log.Printf("streamInstance %q already created", streamName)
			continue
		}
		// Initialize JetStream context
		if err := streamMgr.InitJetStream(nc); err != nil {
			log.Printf("failed to initialize jetstream for stream %q: %v", streamName, err)
			continue
		}
		ctx := context.Background()
		stream, err := streamMgr.CreateStream(ctx)
		if err != nil {
			log.Printf("failed to create stream %q: %v", streamName, err)
			continue
		}
		streamInstLock.Lock()
		streamInstances[streamName] = stream
		streamInstLock.Unlock()
		log.Printf("streamInstance %q created", streamName)
	}
}

// GetStream retrieves a JetStream stream instance by streamID.
// Returns the stream instance and true if found.
func GetStream(streamID string) (jetstream.Stream, bool) {
	streamInstLock.RLock()
	defer streamInstLock.RUnlock()
	stream, ok := streamInstances[streamID]
	return stream, ok
}

func init() {
	// Registers a default stream manager if one hasn't been registered.
	if _, ok := GetManager(DefaultStream); !ok {
		defaultCfg := &JetStreamConfig{
			Name:                 DefaultStream,
			Subjects:             []string{"subject.*.*"},
			Description:          DefaultStream,
			Retention:            0,
			MaxConsumers:         30,
			MaxMsgs:              -1,
			MaxBytes:             -1,
			Discard:              0,
			DiscardNewPerSubject: false,
			MaxAge:               0,
			MaxMsgsPerSubject:    10000,
			MaxMsgSize:           10000,
			NoAck:                false,
		}
		defaultManager := NewJetStream(defaultCfg)
		RegisterManager(DefaultStream, defaultManager)
		log.Printf("default stream %q registered", DefaultStream)
	}
}

// InitJetStream initializes the JetStream context for the manager using the given NATS connection.
// Parameters:
//
//	nc - pointer to the NATS connection
//
// Returns:
//
//	error - non-nil error if JetStream context creation fails
func (jsm *JetStreamManager) InitJetStream(nc *nats.Conn) error {
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}
	jsm.JS = js
	return nil
}

// CreateStream creates or updates a JetStream stream using the manager's configuration.
// Parameters:
//
//	ctx - context to control request timeout
//
// Returns:
//
//	jetstream.Stream - the created/updated stream instance
//	error            - non-nil error if stream creation fails
func (jsm *JetStreamManager) CreateStream(ctx context.Context) (jetstream.Stream, error) {
	streamCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stream, err := jsm.JS.CreateOrUpdateStream(streamCtx, jsm.streamConf)
	if err != nil {
		return nil, err
	}
	return stream, nil
}
