package config
// ConsumerCnf defines the configuration structure for consumers, including topic and channel information, as well as concurrency and number settings.
type (
	ConsumerCnf struct {
		Topic             string // Topic name for messages.
		Channel           string // Channel name for messages.
		ConcurrentHandler int    // Number of concurrent handlers.
		Num               int64  // Number setting for consumers.
	}
)

// Config defines the configuration structure for NSQ consumers, including NSQ lookupd address information and consumer configuration.
type Config struct {
	NsqLookupdAddr []string // Address list of NSQ lookupd nodes.

	Consumer struct {
		Test ConsumerCnf // Test configuration under the consumer, used for setting related parameters of the test topic.
	}
}
