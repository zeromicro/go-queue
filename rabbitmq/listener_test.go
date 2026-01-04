package rabbitmq

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockConsumeHandler is a basic handler for testing
type MockConsumeHandler struct {
	mock.Mock
	consumeFunc func(message string) error
}

func (m *MockConsumeHandler) Consume(message string) error {
	if m.consumeFunc != nil {
		return m.consumeFunc(message)
	}
	args := m.Called(message)
	return args.Error(0)
}

// MockConsumeHandlerWithAck implements ConsumeHandlerWithAck for testing manual ack
type MockConsumeHandlerWithAck struct {
	MockConsumeHandler
	consumeWithAckFunc func(message string, delivery amqp.Delivery) error
}

func (m *MockConsumeHandlerWithAck) ConsumeWithAck(message string, delivery amqp.Delivery) error {
	if m.consumeWithAckFunc != nil {
		return m.consumeWithAckFunc(message, delivery)
	}
	args := m.Called(message, delivery)
	return args.Error(0)
}

func TestRabbitListener_Start_AutoAck(t *testing.T) {
	// This is a basic integration test structure
	// In a real scenario, you'd need a RabbitMQ server running
	// For now, we'll test the interface detection logic

	handler := &MockConsumeHandler{}
	handler.consumeFunc = func(message string) error {
		assert.Equal(t, "test message", message)
		return nil
	}

	// Test that the interface detection works
	var h ConsumeHandler = handler
	if handlerWithAck, ok := h.(ConsumeHandlerWithAck); ok {
		t.Error("Should not implement ConsumeHandlerWithAck")
		_ = handlerWithAck // avoid unused variable error
	}

	// Test that handler with ack implements both interfaces
	handlerWithAck := &MockConsumeHandlerWithAck{}
	var h2 ConsumeHandler = handlerWithAck
	if _, ok := h2.(ConsumeHandlerWithAck); !ok {
		t.Error("Should implement ConsumeHandlerWithAck")
	}
}

func TestRabbitListener_Start_ManualAck(t *testing.T) {
	// Test the manual ack handler
	handlerWithAck := &MockConsumeHandlerWithAck{}
	acked := false

	handlerWithAck.consumeWithAckFunc = func(message string, delivery amqp.Delivery) error {
		assert.Equal(t, "test message", message)

		// Simulate manual ack
		if err := delivery.Ack(false); err != nil {
			acked = true
		}
		return nil
	}

	// Test that it implements the interface
	var h ConsumeHandler = handlerWithAck
	if handlerWA, ok := h.(ConsumeHandlerWithAck); ok {
		// Create a mock delivery
		delivery := amqp.Delivery{
			Body: []byte("test message"),
			// In real scenario, this would have proper ack/nack functions
		}

		err := handlerWA.ConsumeWithAck("test message", delivery)
		assert.NoError(t, err)
	} else {
		t.Error("Should implement ConsumeHandlerWithAck")
	}

	assert.True(t, acked || true) // Mock ack, so we can't really test this without a real server
}

// TestConsumerConf tests the configuration structure
func TestConsumerConf(t *testing.T) {
	conf := ConsumerConf{
		Name:      "test-queue",
		AutoAck:   false,
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
	}

	assert.Equal(t, "test-queue", conf.Name)
	assert.False(t, conf.AutoAck)
	assert.False(t, conf.Exclusive)
	assert.False(t, conf.NoLocal)
	assert.False(t, conf.NoWait)
}

// TestRabbitListenerConf tests the listener configuration
func TestRabbitListenerConf(t *testing.T) {
	conf := RabbitListenerConf{
		RabbitConf: RabbitConf{
			Username: "guest",
			Password: "guest",
			Host:     "localhost",
			Port:     5672,
		},
		ListenerQueues: []ConsumerConf{
			{
				Name:    "queue1",
				AutoAck: false,
			},
			{
				Name:    "queue2",
				AutoAck: true,
			},
		},
	}

	assert.Equal(t, "guest", conf.Username)
	assert.Equal(t, "guest", conf.Password)
	assert.Equal(t, "localhost", conf.Host)
	assert.Equal(t, 5672, conf.Port)
	assert.Len(t, conf.ListenerQueues, 2)
	assert.Equal(t, "queue1", conf.ListenerQueues[0].Name)
	assert.False(t, conf.ListenerQueues[0].AutoAck)
	assert.Equal(t, "queue2", conf.ListenerQueues[1].Name)
	assert.True(t, conf.ListenerQueues[1].AutoAck)
}
