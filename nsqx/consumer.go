package nsqx

import (
	"github.com/go-logr/stdr"
	"github.com/nsqio/go-nsq"
	"github.com/zeromicro/go-zero/core/logx"
)

// The Consumer structure encapsulates the configuration and status of an NSQ consumer.
// It enables users to create, configure, and control an NSQ consumer including setting handlers, loggers, and managing its lifecycle.
type Consumer struct {
	config      *ConsumerConfig // Points to the configuration details for the consumer such as topic, channel, etc.
	nsqConsumer *nsq.Consumer   // Represents the underlying NSQ consumer instance.
}

// NewConsumer creates and returns a new instance of Consumer.
// Parameters:
//
//	config *ConsumerConfig - Pointer to the consumer's configuration.
//
// Returns:
//
//	*Consumer - An instance of the Consumer.
//	error - If consumer creation fails, returns an error.
func NewConsumer(config *ConsumerConfig) (*Consumer, error) {
	// Creates a new NSQ consumer instance using the provided configuration.
	nsqConsumer, err := nsq.NewConsumer(config.Topic, config.Channel, config.NsqConfig)
	if err != nil {
		// Returns an error if creation fails.
		return nil, err
	}

	// Constructs and returns the Consumer instance.
	return &Consumer{
		config:      config,
		nsqConsumer: nsqConsumer,
	}, err
}

// AddHandler adds a handler to the consumer with a specified concurrency level.
// Parameters:
//
//	hd nsq.Handler - The NSQ message handler.
//	concurrent int - The concurrency level for the handler.
func (c *Consumer) AddHandler(hd nsq.Handler, concurrent int) {
	// Adds the handler to the underlying NSQ consumer with the given concurrency.
	c.nsqConsumer.AddConcurrentHandlers(hd, concurrent)
}

// SetLogger sets the logger for the consumer.
// Parameters:
//
//	logger stdr.StdLogger - The logger instance.
//	lv nsq.LogLevel - The logging level.
func (c *Consumer) SetLogger(logger stdr.StdLogger, lv nsq.LogLevel) {
	// Sets the logger and log level for the underlying NSQ consumer.
	c.nsqConsumer.SetLogger(logger, lv)
}

// Start initiates the consumer, connecting it to NSQ Lookupd to begin consuming messages.
func (c *Consumer) Start() {
	// Attempts to connect to NSQ Lookupd.
	err := c.nsqConsumer.ConnectToNSQLookupds(c.config.NsqLookupdAddr)
	if err != nil {
		// Logs an error if connection fails.
		logx.Errorf("connect to nsqlookupd failed:%v", err)
	}
}

// Stop discontinues the consumer, disconnecting from NSQ.
func (c *Consumer) Stop() {
	// Stops the underlying NSQ consumer and disconnects all connections.
	c.nsqConsumer.Stop()
}
