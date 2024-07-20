package internal

import "github.com/segmentio/kafka-go"

type Message struct {
	*kafka.Message
}

func NewMessage(msg *kafka.Message) *Message {
	return &Message{Message: msg}
}

func (m *Message) GetHeader(key string) string {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (m *Message) SetHeader(key, val string) {
	// Ensure uniqueness of keys
	for i := 0; i < len(m.Headers); i++ {
		if m.Headers[i].Key == key {
			m.Headers = append(m.Headers[:i], m.Headers[i+1:]...)
			i--
		}
	}
	m.Headers = append(m.Headers, kafka.Header{
		Key:   key,
		Value: []byte(val),
	})
}
