package rabbitmq

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestEnsureListenerOptions(t *testing.T) {
	t.Run("default error handler", func(t *testing.T) {
		var opts listenerOptions
		ensureListenerOptions(&opts)
		assert.NotNil(t, opts.errorHandler)
	})

	t.Run("custom error handler is preserved", func(t *testing.T) {
		var opts listenerOptions
		called := false
		customHandler := func(ctx context.Context, msg amqp.Delivery, err error) {
			called = true
		}
		opts.errorHandler = customHandler
		ensureListenerOptions(&opts)
		opts.errorHandler(context.Background(), amqp.Delivery{}, nil)
		assert.True(t, called, "custom handler should be preserved")
	})
}

func TestWithErrorHandler(t *testing.T) {
	t.Run("set error handler", func(t *testing.T) {
		var opts listenerOptions
		called := false
		customHandler := func(ctx context.Context, msg amqp.Delivery, err error) {
			called = true
		}
		opt := WithErrorHandler(customHandler)
		opt(&opts)
		assert.NotNil(t, opts.errorHandler)
		opts.errorHandler(context.Background(), amqp.Delivery{}, nil)
		assert.True(t, called, "custom handler should be called")
	})
}

type mockConsumeHandler struct {
	shouldFail bool
}

func (m mockConsumeHandler) Consume(message string) error {
	if m.shouldFail {
		return errors.New("consume failed")
	}
	return nil
}

func TestConsumeErrorHandler(t *testing.T) {
	t.Run("error handler is called on failure", func(t *testing.T) {
		errorCalled := false
		var capturedMsg amqp.Delivery
		var capturedErr error

		customHandler := func(ctx context.Context, msg amqp.Delivery, err error) {
			errorCalled = true
			capturedMsg = msg
			capturedErr = err
		}

		testMsg := amqp.Delivery{
			Body: []byte("test message"),
		}

		opts := listenerOptions{errorHandler: customHandler}
		handler := mockConsumeHandler{shouldFail: true}

		ctx := context.Background()
		err := handler.Consume(string(testMsg.Body))
		if err != nil {
			opts.errorHandler(ctx, testMsg, err)
		}

		assert.True(t, errorCalled)
		assert.Equal(t, "test message", string(capturedMsg.Body))
		assert.Equal(t, "consume failed", capturedErr.Error())
	})
}
