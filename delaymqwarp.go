/**
 * @Author: 陈建君
 * @Date: 2021/5/17 2:59 下午
 * @Description: 延迟消息队列的包装
 */

package go_queue

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/service"
	"github.com/tal-tech/go-zero/core/stores/redis"
	"github.com/tal-tech/go-zero/core/threading"

	"github.com/tal-tech/go-queue/internal/dq"
)

type (
	DelayMqConsumeFn = dq.Consume

	DelayMqWarp interface {
		SetUp(adds []string, group string, redisConfig redis.RedisConf)
		RegConsumer(key int, consume DelayMqConsumeFn)
		At(key int, body []byte, at time.Time) (string, error)
		Delay(key int, body []byte, delay time.Duration) (string, error)
		Revoke(ids string) error
		Stop()
		Start()
	}

	defaultDelayMq struct {
		cfg        DqConf
		producer   dq.Producer
		consumer   dq.Consumer
		consumes   map[int]DelayMqConsumeFn
		consumeSrv *service.ServiceGroup
	}

	SendMsg struct {
		Key  int    `json:"key"`
		Body []byte `json:"body"`
	}
)

func NewDelayMq() DelayMqWarp {
	return &defaultDelayMq{
		consumes: map[int]DelayMqConsumeFn{},
	}
}

// 初始化设置
func (k *defaultDelayMq) SetUp(adds []string, group string, redisConfig redis.RedisConf) {
	// 生产端
	var beanstalks []Beanstalk
	for _, v := range adds {
		beanstalks = append(beanstalks, dq.Beanstalk{
			Endpoint: v,
			Tube:     group,
		})
	}
	k.producer = dq.NewProducer(beanstalks)

	// 消费端
	dqCfg := DqConf{}
	dqCfg.Beanstalks = beanstalks
	dqCfg.Redis.Host = redisConfig.Host
	dqCfg.Redis.Type = redisConfig.Type
	dqCfg.Redis.Pass = redisConfig.Pass
	k.consumer = dq.NewConsumer(dqCfg)
}

// 注册消费端
func (k *defaultDelayMq) RegConsumer(key int, consume DelayMqConsumeFn) {
	k.consumes[key] = consume
}

// 定时消费
func (k *defaultDelayMq) At(key int, body []byte, at time.Time) (string, error) {
	msg := &SendMsg{
		Key:  key,
		Body: body,
	}
	m, _ := json.Marshal(msg)

	return k.producer.At(m, at)
}

// 延迟消费
func (k *defaultDelayMq) Delay(key int, body []byte, delay time.Duration) (string, error) {
	msg := &SendMsg{
		Key:  key,
		Body: body,
	}
	m, _ := json.Marshal(msg)

	return k.producer.Delay(m, delay)
}

// 撤销
func (k *defaultDelayMq) Revoke(ids string) error {
	return k.producer.Revoke(ids)
}

// 开启
func (k *defaultDelayMq) Start() {
	k.consumeSrv = k.consumer.Consume(k.consumeFun)
	threading.GoSafe(func() {
		k.consumeSrv.Start()
	})
}

func (k *defaultDelayMq) Stop() {
	_ = k.producer.Close()
	if k.consumeSrv != nil {
		k.consumeSrv.Stop()
	}
}

func (k *defaultDelayMq) consumeFun(body []byte) {
	msg := &SendMsg{}
	err := json.Unmarshal(body, msg)
	if err != nil {
		logx.Error("消费消息失败", err)
	}

	if c, ok := k.consumes[msg.Key]; ok {
		logx.Info(fmt.Sprintf("beanstalkd收到延迟消息%s", msg.Body))
		c(msg.Body)
	} else {
		logx.Error("无效的消息", msg)
	}
}
