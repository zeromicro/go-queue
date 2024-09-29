package handler

import (
	"context"
	"fmt"
	"github.com/nsqio/go-nsq"
	"github.com/zeromicro/go-queue/example/nsqx/consumer/config"
	"github.com/zeromicro/go-queue/example/nsqx/consumer/svc"
	"github.com/zeromicro/go-queue/nsqx"
	"time"
)

type ConsumerTemplate struct {
	ctx      context.Context
	svcCtx   *svc.ServiceContext
	cnf      config.ConsumerCnf
	consumer *nsqx.Consumer
}

func Create(ctx context.Context, svcCtx *svc.ServiceContext, cnf config.ConsumerCnf) *ConsumerTemplate {
	consumerConfig := nsqx.NewConsumerConfig(cnf.Topic, cnf.Topic, svcCtx.Config.NsqLookupdAddr)
	consumer, err := nsqx.NewConsumer(consumerConfig)
	if err != nil {
		panic(err)
	}
	return &ConsumerTemplate{
		ctx:      ctx,
		svcCtx:   svcCtx,
		cnf:      cnf,
		consumer: consumer,
	}
}

func (c *ConsumerTemplate) AddHandler(hd nsq.Handler, concurrent int) {
	c.consumer.AddHandler(hd, concurrent)
}

func (c *ConsumerTemplate) Start() {
	fmt.Printf("%v [start]topic:%s channel:%s num:%d\n", time.Now(), c.cnf.Topic, c.cnf.Channel, c.cnf.Num)
	c.consumer.Start()

}

func (c *ConsumerTemplate) Stop() {
	fmt.Printf("%v [stop]topic:%s channel:%s child:%d\n", time.Now(), c.cnf.Topic, c.cnf.Channel, c.cnf.Num)
	c.consumer.Stop()
}
