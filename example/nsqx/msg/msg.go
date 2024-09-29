package msg

import (
	"github.com/zeromicro/go-queue/nsqx"
)

type TestMsg struct {
	nsqx.UniqueId
	nsqx.Encoder
	nsqx.Decoder
	A string `json:"a"`
}
