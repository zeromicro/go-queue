package internal

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestMessageGetHeader(t *testing.T) {
	testCases := []struct {
		name     string
		msg      *Message
		key      string
		expected string
	}{
		{
			name: "exists",
			msg: &Message{
				Message: &kafka.Message{Headers: []kafka.Header{
					{Key: "foo", Value: []byte("bar")},
				}}},
			key:      "foo",
			expected: "bar",
		},
		{
			name:     "not exists",
			msg:      &Message{Message: &kafka.Message{Headers: []kafka.Header{}}},
			key:      "foo",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.msg.GetHeader(tc.key)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestMessageSetHeader(t *testing.T) {
	msg := &Message{Message: &kafka.Message{Headers: []kafka.Header{
		{Key: "foo", Value: []byte("bar")}},
	}}

	msg.SetHeader("foo", "bar2")
	msg.SetHeader("foo2", "bar2")
	msg.SetHeader("foo2", "bar3")
	msg.SetHeader("foo3", "bar4")

	assert.ElementsMatch(t, msg.Headers, []kafka.Header{
		{Key: "foo", Value: []byte("bar2")},
		{Key: "foo2", Value: []byte("bar3")},
		{Key: "foo3", Value: []byte("bar4")},
	})
}
