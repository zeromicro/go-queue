package consumer

import (
	"github.com/nats-io/nats.go/jetstream"
	"time"
)

// ConsumerConfig combines core consumer settings with advanced parameters.
// Fields marked as optional are not strictly required, but you may want to provide
// default values in your configuration if consistency is needed.
type ConsumerConfig struct {
	// Core parameters
	Name           string   `json:"name,optional"`        // Consumer name (optional)
	Durable        string   `json:"durable,optional"`     // Durable name for persistent consumer (optional)
	FilterSubjects []string `json:"filterSubjects"`       // Subjects filtered by this consumer (required)
	Description    string   `json:"description,optional"` // Consumer description (optional)

	// Advanced parameters
	AckPolicy              int                    `json:"ackPolicy,options=0|1|2,default=0"` // Acknowledgment policy (e.g., 0 for auto-ack, others for manual ack)
	Ordered                bool                   `json:"ordered,optional,default=false"`    // Whether the consumer is ordered
	OrderedConsumerOptions OrderedConsumerOptions `json:"orderedConsumerOptions,optional"`   // Extra options for ordered consumers
}

// OrderedConsumerOptions defines additional options used when the consumer is configured to be ordered.
type OrderedConsumerOptions struct {
	DeliverPolicy int        `json:"deliverPolicy,options=0|1|2|3|4|5,default=0"` // Delivery policy option for ordered consumers
	OptStartSeq   uint64     `json:"optStartSeq,optional"`                        // Optional starting sequence for message consumption
	OptStartTime  *time.Time `json:"optStartTime,optional"`                       // Optional starting time for message consumption
	ReplayPolicy  int        `json:"replayPolicy,options=0|1,default=0"`          // Replay policy for ordered consumers
}

// DeliveryConfig groups the consumption method and pull-related settings.
// ConsumptionMethod can be push-based ("consumer") or pull-based ("fetch"/"fetchNoWait").
// FetchCount defines how many messages to pull in one batch.
type DeliveryConfig struct {
	ConsumptionMethod ConsumerType `json:"consumptionMethod,options=consumer|fetch|fetchNoWait,default=consumer"`
	FetchCount        int          `json:"fetchCount,optional,default=10"`
}

// ConsumerQueueConfig defines the full configuration for building a consumer queue.
// If StreamName is empty, the default stream (DefaultStream) will be used.
type ConsumerQueueConfig struct {
	StreamName         string         `json:"streamName,optional"`                   // Name of the stream to associate with; if empty, uses default stream
	ConsumerConfig     ConsumerConfig `json:"consumerConfig"`                        // Consumer core and advanced configuration
	QueueConsumerCount int            `json:"queueConsumerCount,optional,default=1"` // Number of consumer instances to create for this queue
	Delivery           DeliveryConfig `json:"delivery"`                              // Delivery settings including consumption method and pull batch size
	Handler            ConsumeHandler `json:"-"`                                     // Message handler to process incoming messages
}

// ConsumerType defines the mode of consumption: push-based or pull-based.
type ConsumerType string

const (
	Consumer   ConsumerType = "consumer"    // Push-based consumption (server pushes messages)
	Pull       ConsumerType = "fetch"       // Pull-based consumption (client actively pulls messages)
	PullNoWait ConsumerType = "fetchNoWait" // Pull-based consumption without waiting for messages
)

// ConsumeHandler defines an interface for message processing.
// Users need to implement the Consume method to handle individual messages.
type ConsumeHandler interface {
	Consume(msg jetstream.Msg) error
}
