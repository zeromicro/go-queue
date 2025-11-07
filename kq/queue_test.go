package kq

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/threading"
)

// mockKafkaReader is a mock for kafka.Reader
type mockKafkaReader struct {
	mock.Mock
}

func (m *mockKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	args := m.Called(ctx)
	return args.Get(0).(kafka.Message), args.Error(1)
}

func (m *mockKafkaReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	args := m.Called(ctx, msgs)
	return args.Error(0)
}

func (m *mockKafkaReader) Close() error {
	args := m.Called()
	return args.Error(0)
}

// mockConsumeHandler is a mock for ConsumeHandler
type mockConsumeHandler struct {
	mock.Mock
}

func (m *mockConsumeHandler) Consume(ctx context.Context, key, value string) error {
	args := m.Called(ctx, key, value)
	return args.Error(0)
}

func TestNewQueue(t *testing.T) {
	c := KqConf{
		ServiceConf: service.ServiceConf{
			Name: "test-queue",
		},
		Brokers: []string{"localhost:9092"},
		Group:   "test-group",
		Topic:   "test-topic",
		Offset:  "first",
		Conns:   1,
	}
	handler := &mockConsumeHandler{}

	q, err := NewQueue(c, handler)
	assert.NoError(t, err)
	assert.NotNil(t, q)
}

func TestKafkaQueue_consumeOne(t *testing.T) {
	handler := &mockConsumeHandler{}
	q := &kafkaQueue{
		handler: handler,
		metrics: stat.NewMetrics("test"),
	}

	ctx := context.Background()
	key := "test-key"
	value := "test-value"

	handler.On("Consume", ctx, key, value).Return(nil)

	err := q.consumeOne(ctx, key, value)
	assert.NoError(t, err)
	handler.AssertExpectations(t)
}

func TestKafkaQueue_consume(t *testing.T) {
	mockReader := &mockKafkaReader{}
	q := &kafkaQueue{
		consumer: mockReader,
	}

	msg := kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	mockReader.On("FetchMessage", mock.Anything).Return(msg, nil).Once()
	mockReader.On("FetchMessage", mock.Anything).Return(kafka.Message{}, io.EOF).Once()

	called := false
	err := q.consume(func(msg kafka.Message) {
		called = true
		assert.Equal(t, "test-key", string(msg.Key))
		assert.Equal(t, "test-value", string(msg.Value))
	})

	assert.Error(t, err)
	assert.True(t, called)
	mockReader.AssertExpectations(t)
}

func TestKafkaQueue_Start(t *testing.T) {
	mockReader := &mockKafkaReader{}
	handler := &mockConsumeHandler{}
	q := &kafkaQueue{
		c: KqConf{
			ServiceConf: service.ServiceConf{
				Name: "test-queue",
			},
			Processors: 1,
			Consumers:  1,
		},
		consumer:         mockReader,
		handler:          handler,
		consumerRoutines: threading.NewRoutineGroup(),
		producerRoutines: threading.NewRoutineGroup(),
		channel:          make(chan kafka.Message, 1),
		metrics:          stat.NewMetrics("test"),
	}

	msg := kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	mockReader.On("FetchMessage", mock.Anything).Return(msg, nil).Once()
	mockReader.On("FetchMessage", mock.Anything).Return(kafka.Message{}, io.EOF).Once()
	handler.On("Consume", mock.Anything, "test-key", "test-value").Return(nil)
	mockReader.On("CommitMessages", mock.Anything, []kafka.Message{msg}).Return(nil)
	mockReader.On("Close").Return(nil)

	group := threading.NewRoutineGroup()
	group.Run(func() {
		time.Sleep(100 * time.Millisecond)
		q.Stop()
	})

	q.Start()
	group.Wait()

	mockReader.AssertExpectations(t)
	handler.AssertExpectations(t)
}

func TestKafkaQueue_Stop(t *testing.T) {
	mockReader := &mockKafkaReader{}
	q := &kafkaQueue{
		consumer: mockReader,
	}

	mockReader.On("Close").Return(nil)

	q.Stop()

	mockReader.AssertExpectations(t)
}

func TestWithCommitInterval(t *testing.T) {
	options := &queueOptions{}
	interval := time.Second * 5
	WithCommitInterval(interval)(options)
	assert.Equal(t, interval, options.commitInterval)
}

func TestWithQueueCapacity(t *testing.T) {
	options := &queueOptions{}
	capacity := 100
	WithQueueCapacity(capacity)(options)
	assert.Equal(t, capacity, options.queueCapacity)
}

func TestWithMaxWait(t *testing.T) {
	options := &queueOptions{}
	wait := time.Second * 2
	WithMaxWait(wait)(options)
	assert.Equal(t, wait, options.maxWait)
}

func TestWithMetrics(t *testing.T) {
	options := &queueOptions{}
	metrics := stat.NewMetrics("test")
	WithMetrics(metrics)(options)
	assert.Equal(t, metrics, options.metrics)
}

func TestWithErrorHandler(t *testing.T) {
	options := &queueOptions{}
	handler := func(ctx context.Context, msg kafka.Message, err error) {}
	WithErrorHandler(handler)(options)
	assert.NotNil(t, options.errorHandler)
}

func TestWithBatchTimeout(t *testing.T) {
	options := &pushOptions{}
	timeout := time.Second * 5
	WithBatchTimeout(timeout)(options)
	assert.Equal(t, timeout, options.batchTimeout)
}

func TestWithBatchSize(t *testing.T) {
	options := &pushOptions{}
	size := 100
	WithBatchSize(size)(options)
	assert.Equal(t, size, options.batchSize)
}

func TestWithBatchBytes(t *testing.T) {
	options := &pushOptions{}
	bytes := int64(1024)
	WithBatchBytes(bytes)(options)
	assert.Equal(t, bytes, options.batchBytes)
}
