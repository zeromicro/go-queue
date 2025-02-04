package dq

import (
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/hash"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

const (
	expiration = 3600 // seconds
	tolerance  = time.Minute * 30
)

var maxCheckBytes = getMaxTimeLen()

type (
	Consume func(body []byte)

	Consumer interface {
		Consume(consume Consume)
	}

	consumerCluster struct {
		nodes []*consumerNode
		red   *redis.Redis
	}
)

func NewConsumer(c DqConf) Consumer {
	var nodes []*consumerNode
	for _, node := range c.Beanstalks {
		nodes = append(nodes, newConsumerNode(node.Endpoint, node.Tube))
	}
	return &consumerCluster{
		nodes: nodes,
		red:   c.Redis.NewRedis(),
	}
}

func (c *consumerCluster) Consume(consume Consume) {
	guardedConsume := func(body []byte) {
		key := hash.Md5Hex(body)
		taskBody, ok := c.unwrap(body)
		if !ok {
			logx.Errorf("discarded: %q", string(body))
			return
		}

		redisLock := redis.NewRedisLock(c.red, key)
		redisLock.SetExpire(expiration)
		ok, err := redisLock.Acquire()
		if err != nil {
			logx.Error(err)
		} else if ok {
			consume(taskBody)
		}
	}

	group := service.NewServiceGroup()
	for _, node := range c.nodes {
		group.Add(consumeService{
			c:       node,
			consume: guardedConsume,
		})
	}
	group.Start()
}

func (c *consumerCluster) unwrap(body []byte) ([]byte, bool) {
	var pos = -1
	for i := 0; i < maxCheckBytes && i < len(body); i++ {
		if body[i] == timeSep {
			pos = i
			break
		}
	}
	if pos < 0 {
		return nil, false
	}

	val, err := strconv.ParseInt(string(body[:pos]), 10, 64)
	if err != nil {
		logx.Error(err)
		return nil, false
	}

	t := time.Unix(0, val)
	if t.Add(tolerance).Before(time.Now()) {
		return nil, false
	}

	return body[pos+1:], true
}

func getMaxTimeLen() int {
	return len(strconv.FormatInt(time.Now().UnixNano(), 10)) + 2
}
