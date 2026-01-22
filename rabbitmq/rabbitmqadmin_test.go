package rabbitmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildQueueArgs(t *testing.T) {
	t.Run("classic queue without extra args", func(t *testing.T) {
		conf := QueueConf{
			Name:    "test-queue",
			Durable: true,
		}
		args := buildQueueArgs(conf)

		assert.Empty(t, args)
	})

	t.Run("quorum queue type", func(t *testing.T) {
		conf := QueueConf{
			Name:      "test-quorum",
			QueueType: "quorum",
		}
		args := buildQueueArgs(conf)

		assert.Equal(t, "quorum", args["x-queue-type"])
	})

	t.Run("classic queue type explicit", func(t *testing.T) {
		conf := QueueConf{
			Name:      "test-classic",
			QueueType: "classic",
		}
		args := buildQueueArgs(conf)

		assert.Empty(t, args)
	})

	t.Run("with delivery limit", func(t *testing.T) {
		conf := QueueConf{
			Name:          "test-delivery-limit",
			DeliveryLimit: 20,
		}
		args := buildQueueArgs(conf)

		assert.Equal(t, int64(20), args["x-delivery-limit"])
	})

	t.Run("with delivery limit zero", func(t *testing.T) {
		conf := QueueConf{
			Name:          "test-no-delivery-limit",
			DeliveryLimit: 0,
		}
		args := buildQueueArgs(conf)

		assert.Nil(t, args["x-delivery-limit"])
	})

	t.Run("with dead letter exchange", func(t *testing.T) {
		conf := QueueConf{
			Name:               "test-dlx",
			DeadLetterExchange: "dlx.exchange",
		}
		args := buildQueueArgs(conf)

		assert.Equal(t, "dlx.exchange", args["x-dead-letter-exchange"])
	})

	t.Run("with dead letter routing key", func(t *testing.T) {
		conf := QueueConf{
			Name:                 "test-dlx-rk",
			DeadLetterRoutingKey: "failed",
		}
		args := buildQueueArgs(conf)

		assert.Equal(t, "failed", args["x-dead-letter-routing-key"])
	})

	t.Run("quorum queue with all options", func(t *testing.T) {
		conf := QueueConf{
			Name:                 "orders.quorum",
			QueueType:            "quorum",
			DeliveryLimit:        20,
			DeadLetterExchange:   "orders.dlx",
			DeadLetterRoutingKey: "failed",
		}
		args := buildQueueArgs(conf)

		assert.Equal(t, "quorum", args["x-queue-type"])
		assert.Equal(t, int64(20), args["x-delivery-limit"])
		assert.Equal(t, "orders.dlx", args["x-dead-letter-exchange"])
		assert.Equal(t, "failed", args["x-dead-letter-routing-key"])
	})

	t.Run("delivery limit -1 means no limit", func(t *testing.T) {
		conf := QueueConf{
			Name:          "test-unlimited",
			DeliveryLimit: -1,
		}
		args := buildQueueArgs(conf)

		assert.Equal(t, int64(-1), args["x-delivery-limit"])
	})
}

func TestAdmin_DeclareQueueConf_Integration(t *testing.T) {
	t.Skip("integration test skipped, requires RabbitMQ server")
}
