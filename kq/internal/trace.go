package internal

import "go.opentelemetry.io/otel/propagation"

var _ propagation.TextMapCarrier = (*MessageCarrier)(nil)

// MessageCarrier injects and extracts traces from a types.Message.
type MessageCarrier struct {
	msg *Message
}

// NewMessageCarrier returns a new MessageCarrier.
func NewMessageCarrier(msg *Message) MessageCarrier {
	return MessageCarrier{msg: msg}
}

// Get returns the value associated with the passed key.
func (m MessageCarrier) Get(key string) string {
	return m.msg.GetHeader(key)
}

// Set stores the key-value pair.
func (m MessageCarrier) Set(key string, value string) {
	m.msg.SetHeader(key, value)
}

// Keys lists the keys stored in this carrier.
func (m MessageCarrier) Keys() []string {
	out := make([]string, len(m.msg.Headers))
	for i, h := range m.msg.Headers {
		out[i] = h.Key
	}

	return out
}
