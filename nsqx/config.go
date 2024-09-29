package nsqx

import "github.com/nsqio/go-nsq"

// Define the consumer configuration structure
// It includes the topic name, channel name, list of NSQ Lookupd addresses, and an NSQ configuration object.
type (
	ConsumerConfig struct {
		Topic          string      // Topic name
		Channel        string      // Channel name for the consumer
		NsqLookupdAddr []string    // List of NSQ Lookupd addresses
		NsqConfig      *nsq.Config // NSQ configuration object
	}

	// Define the producer configuration structure
	// It includes the NSQD address and an NSQ configuration object.
	ProducerConfig struct {
		NsqConfig *nsq.Config // NSQ configuration object
		NsqdAddr  string      // NSQD address
	}
)

// NewConsumerConfig creates a new consumer configuration instance
// Parameters:
// - topic: Topic name
// - channel: Channel name for the consumer
// - nsqLookupdAddr: List of NSQ Lookupd addresses
// Returns:
// - Pointer to a ConsumerConfig instance
func NewConsumerConfig(topic string, channel string, nsqLookupdAddr []string) *ConsumerConfig {
	return &ConsumerConfig{
		Topic:          topic,
		Channel:        channel,
		NsqLookupdAddr: nsqLookupdAddr,
		NsqConfig:      nsq.NewConfig(), // Initialize the NSQ configuration object
	}
}

// NewProducerConfig creates a new producer configuration instance
// Parameters:
// - nsqdAddr: NSQD address
// Returns:
// - Pointer to a ProducerConfig instance
func NewProducerConfig(nsqdAddr string) *ProducerConfig {
	return &ProducerConfig{
		NsqdAddr:  nsqdAddr,        // Set the NSQD address
		NsqConfig: nsq.NewConfig(), // Initialize the NSQ configuration object
	}
}
