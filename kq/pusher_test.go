package kq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// mockKafkaWriter is a mock for kafka.Writer
type mockKafkaWriter struct {
	mock.Mock
}

func (m *mockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	args := m.Called(ctx, msgs)
	return args.Error(0)
}

func (m *mockKafkaWriter) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestNewPusher(t *testing.T) {
	addrs := []string{"localhost:9092"}
	topic := "test-topic"

	t.Run("DefaultOptions", func(t *testing.T) {
		pusher := NewPusher(addrs, topic)
		assert.NotNil(t, pusher)
		assert.NotNil(t, pusher.producer)
		assert.Equal(t, topic, pusher.topic)
		assert.NotNil(t, pusher.executor)
	})

	t.Run("WithSyncPush", func(t *testing.T) {
		pusher := NewPusher(addrs, topic, WithSyncPush())
		assert.NotNil(t, pusher)
		assert.NotNil(t, pusher.producer)
		assert.Equal(t, topic, pusher.topic)
		assert.Nil(t, pusher.executor)
	})

	t.Run("WithChunkSize", func(t *testing.T) {
		pusher := NewPusher(addrs, topic, WithChunkSize(100))
		assert.NotNil(t, pusher)
		assert.NotNil(t, pusher.executor)
	})

	t.Run("WithFlushInterval", func(t *testing.T) {
		pusher := NewPusher(addrs, topic, WithFlushInterval(time.Second))
		assert.NotNil(t, pusher)
		assert.NotNil(t, pusher.executor)
	})

	t.Run("WithAllowAutoTopicCreation", func(t *testing.T) {
		pusher := NewPusher(addrs, topic, WithAllowAutoTopicCreation())
		assert.NotNil(t, pusher)
		assert.True(t, pusher.producer.(*kafka.Writer).AllowAutoTopicCreation)
	})
}

func TestPusher_Close(t *testing.T) {
	mockWriter := new(mockKafkaWriter)
	pusher := &Pusher{
		producer: mockWriter,
	}

	mockWriter.On("Close").Return(nil)

	err := pusher.Close()
	assert.NoError(t, err)
	mockWriter.AssertExpectations(t)
}

func TestPusher_Name(t *testing.T) {
	topic := "test-topic"
	pusher := &Pusher{topic: topic}

	assert.Equal(t, topic, pusher.Name())
}

func TestPusher_Push(t *testing.T) {
	mockWriter := new(mockKafkaWriter)
	pusher := &Pusher{
		producer: mockWriter,
		topic:    "test-topic",
	}

	ctx := context.Background()
	value := "test-value"

	mockWriter.On("WriteMessages", mock.Anything, mock.AnythingOfType("[]kafka.Message")).Return(nil)

	err := pusher.Push(ctx, value)
	assert.NoError(t, err)
	mockWriter.AssertExpectations(t)
}

func TestPusher_PushWithKey(t *testing.T) {
	mockWriter := new(mockKafkaWriter)
	pusher := &Pusher{
		producer: mockWriter,
		topic:    "test-topic",
	}

	ctx := context.Background()
	key := "test-key"
	value := "test-value"

	mockWriter.On("WriteMessages", mock.Anything, mock.AnythingOfType("[]kafka.Message")).Return(nil)

	err := pusher.PushWithKey(ctx, key, value)
	assert.NoError(t, err)
	mockWriter.AssertExpectations(t)
}

func TestPusher_PushWithKey_Error(t *testing.T) {
	mockWriter := new(mockKafkaWriter)
	pusher := &Pusher{
		producer: mockWriter,
		topic:    "test-topic",
	}

	ctx := context.Background()
	key := "test-key"
	value := "test-value"

	expectedError := errors.New("write error")
	mockWriter.On("WriteMessages", mock.Anything, mock.AnythingOfType("[]kafka.Message")).Return(expectedError)

	err := pusher.PushWithKey(ctx, key, value)
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	mockWriter.AssertExpectations(t)
}
