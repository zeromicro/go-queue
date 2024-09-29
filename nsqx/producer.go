package nsqx

import (
	"errors"
	"github.com/nsqio/go-nsq"
	"time"
)

// ErrNsqdClose indicates that nsqd is closed.
// ErrEmptyMsg indicates an empty message error.
var (
	ErrNsqdClose = errors.New("nsqd is closed")
	ErrEmptyMsg  = errors.New("empty msg")
)

// Sender is a function type for sending a single message.
// DelaySender is a function type for sending a delayed message.
// MultiSender is a function type for sending multiple messages.
type (
	Sender      func(msg Msg) error
	DelaySender func(msg Msg, delay time.Duration) error
	MultiSender func(msgS []Msg) error

	// Producer represents a structure for sending messages to NSQ.
	Producer struct {
		config   *ProducerConfig
		producer *nsq.Producer
	}
)

// Sender returns a Sender function for sending a message to the specified topic.
func (s *Producer) Sender(topic Topic) Sender {
	return func(msg Msg) error {
		// Check if the message is nil
		if msg == nil {
			return ErrEmptyMsg
		}
		// Check if the producer is closed
		if s.producer == nil {
			return ErrNsqdClose
		}

		// Encode the message
		data, err := msg.Encode(msg)
		if err != nil {
			return err
		}
		// Publish the message to the specified topic
		return s.producer.Publish(topic.ToString(), data)
	}
}

// DelaySender returns a DelaySender function for sending a delayed message to the specified topic.
func (s *Producer) DelaySender(topic Topic) DelaySender {
	return func(msg Msg, delay time.Duration) error {
		// Check if the message is nil
		if msg == nil {
			return ErrEmptyMsg
		}
		// Check if the producer is closed
		if s.producer == nil {
			return ErrNsqdClose
		}

		// Encode the message
		data, err := msg.Encode(msg)
		if err != nil {
			return err
		}
		// Publish the delayed message to the specified topic
		return s.producer.DeferredPublish(topic.ToString(), delay, data)
	}
}

// MultiSender returns a MultiSender function for sending multiple messages to the specified topic.
func (s *Producer) MultiSender(topic Topic) MultiSender {
	return func(msgS []Msg) error {
		// Check if the producer is closed
		if s.producer == nil {
			return ErrNsqdClose
		}

		// Collect all message byte slices
		var dataMsg [][]byte
		if len(msgS) > 0 {
			for _, msg := range msgS {
				data, err := msg.Encode(msg)
				if err != nil {
					return err
				}
				dataMsg = append(dataMsg, data)
			}
			// Batch publish messages to the specified topic
			if len(dataMsg) > 0 {
				return s.producer.MultiPublish(topic.ToString(), dataMsg)
			}
		}
		// Return an error if there are no messages to send
		return ErrEmptyMsg
	}
}

// Producer returns the internal nsq.Producer instance.
func (s *Producer) Producer() *nsq.Producer {
	return s.producer
}

// NewProducer creates and returns a new NSQ producer instance.
func NewProducer(config *ProducerConfig) (*Producer, error) {
	// Create a new NSQ producer
	producer, err := nsq.NewProducer(config.NsqdAddr, config.NsqConfig)
	if err != nil {
		return nil, err
	}
	return &Producer{
		config:   config,
		producer: producer,
	}, nil
}
