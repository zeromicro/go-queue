/**
 * @Author: 陈建君
 * @Date: 2021/5/17 2:44 下午
 * @Description: kafka消息队列的包装
 */

package go_queue

import (
	"encoding/json"

	"github.com/tal-tech/go-zero/core/queue"

	"github.com/tal-tech/go-queue/internal/kq"
)

type (
	KafkaConsumeFn = kq.ConsumeHandle

	KafkaWarp interface {
		SetUp(adds []string, topic string)
		RegConsumer(group string, consume KafkaConsumeFn)
		SendMsg(v interface{}) error
		Start()
		Stop()
	}

	defaultKafka struct {
		cfg       KqConf
		producer  *kq.Pusher
		consumers map[string]queue.MessageQueue
	}
)

func NewKafka() KafkaWarp {
	return &defaultKafka{}
}

func (k *defaultKafka) SetUp(adds []string, topic string) {
	k.cfg = KqConf{}
	k.cfg.Brokers = adds
	k.cfg.Topic = topic
	k.producer = kq.NewPusher(k.cfg.Brokers, k.cfg.Topic)
	k.consumers = map[string]queue.MessageQueue{}
}

func (k *defaultKafka) RegConsumer(group string, consume KafkaConsumeFn) {
	if len(k.cfg.Brokers) == 0 {
		panic("无可用的节点信息")
	}

	kcfg := kq.Conf{
		Brokers:    k.cfg.Brokers,
		Topic:      k.cfg.Topic,
		Group:      group,
		Offset:     "last",
		Conns:      1,
		Consumers:  8,
		Processors: 8,
		MinBytes:   10240,
		MaxBytes:   10485760,
	}
	kcfg.Log.Mode = "console"
	kcfg.Log.Level = "info"

	k.consumers[k.cfg.Topic] = kq.MustNewQueue(kcfg, kq.WithHandle(consume))
}

func (k *defaultKafka) SendMsg(v interface{}) error {
	if len(k.cfg.Brokers) == 0 {
		panic("无可用的节点信息")
	}

	body, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return k.producer.Push(string(body))
}

func (k *defaultKafka) Start() {
	for _, q := range k.consumers {
		go q.Start()
	}
}

func (k *defaultKafka) Stop() {
	for _, q := range k.consumers {
		q.Stop()
	}
}
