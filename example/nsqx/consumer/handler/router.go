package handler

import (
	"context"
	"github.com/nsqio/go-nsq"
	"github.com/zeromicro/go-queue/example/nsqx/consumer/config"
	"github.com/zeromicro/go-queue/example/nsqx/consumer/logic"
	"github.com/zeromicro/go-queue/example/nsqx/consumer/svc"
	"github.com/zeromicro/go-zero/core/service"
)

func parallelRegisterConsumer(group *service.ServiceGroup, svcCtx *svc.ServiceContext, cnf config.ConsumerCnf, hd nsq.Handler) {
	for i := 0; i < int(cnf.Num); i++ {
		consumerIns := Create(context.Background(), svcCtx, cnf)
		consumerIns.AddHandler(hd, cnf.ConcurrentHandler)
		group.Add(consumerIns)
	}
}

func RegisterConsumer(group *service.ServiceGroup, svcCtx *svc.ServiceContext) {
	parallelRegisterConsumer(group, svcCtx, svcCtx.Config.Consumer.Test, logic.NewTestLogic(svcCtx))
}
