package logic

import (
	"github.com/nsqio/go-nsq"
	"github.com/zeromicro/go-queue/example/nsqx/consumer/svc"
	"github.com/zeromicro/go-queue/example/nsqx/msg"
	"github.com/zeromicro/go-zero/core/logx"
)

type TestLogic struct {
	svcCtx *svc.ServiceContext
}

func NewTestLogic(svcCtx *svc.ServiceContext) nsq.Handler {
	return &TestLogic{
		svcCtx: svcCtx,
	}
}

// HandleMessage implements the Handler interface.
func (h *TestLogic) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		// In this case, a message with an empty body is simply ignored/discarded.
		return nil
	}

	// do whatever actual message processing is desired

	var msgData msg.TestMsg
	err := msgData.Decode(m.Body, &msgData)
	logx.Infof("HandleMessage %s %v:%v\n", string(m.Body), msgData, err)

	// Returning a non-nil error will automatically send a REQ command to NSQ to re-queue the message.
	return nil
}
