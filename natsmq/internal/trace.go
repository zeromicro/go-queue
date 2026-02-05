package internal

import (
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
)

var _ propagation.TextMapCarrier = (*HeaderCarrier)(nil)

// HeaderCarrier injects and extracts traces from NATS headers.
type HeaderCarrier struct {
	headers *nats.Header
}

// NewHeaderCarrier returns a new HeaderCarrier.
func NewHeaderCarrier(headers *nats.Header) HeaderCarrier {
	return HeaderCarrier{headers: headers}
}

// Get returns the value associated with the passed key.
func (h HeaderCarrier) Get(key string) string {
	if h.headers == nil || *h.headers == nil {
		return ""
	}
	return (*h.headers).Get(key)
}

// Set stores the key-value pair.
func (h HeaderCarrier) Set(key string, value string) {
	if h.headers == nil {
		return
	}
	if *h.headers == nil {
		*h.headers = nats.Header{}
	}
	(*h.headers).Set(key, value)
}

// Keys lists the keys stored in this carrier.
func (h HeaderCarrier) Keys() []string {
	if h.headers == nil || *h.headers == nil {
		return []string{}
	}
	out := make([]string, 0, len(*h.headers))
	for key := range *h.headers {
		out = append(out, key)
	}
	return out
}
