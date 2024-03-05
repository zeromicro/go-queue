package dq

import (
	"errors"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
)

type (
	consumerNode struct {
		conn *connection
		tube string
		on   *syncx.AtomicBool
	}

	consumeService struct {
		c       *consumerNode
		consume Consume
	}
)

func newConsumerNode(endpoint, tube string) *consumerNode {
	return &consumerNode{
		conn: newConnection(endpoint, tube),
		tube: tube,
		on:   syncx.ForAtomicBool(true),
	}
}

func (c *consumerNode) dispose() {
	c.on.Set(false)
}

func (c *consumerNode) consumeEvents(consume Consume) {
	for c.on.True() {
		conn, err := c.conn.get()
		if err != nil {
			logx.Error(err)
			time.Sleep(time.Second)
			continue
		}

		// because getting conn takes at most one second, reserve tasks at most 5 seconds,
		// if don't check on/off here, the conn might not be closed due to
		// graceful shutdon waits at most 5.5 seconds.
		if !c.on.True() {
			break
		}

		conn.Tube.Name = c.tube
		conn.TubeSet.Name[c.tube] = true
		id, body, err := conn.Reserve(reserveTimeout)
		if err == nil {
			conn.Delete(id)
			consume(body)
			continue
		}

		// the error can only be beanstalk.NameError or beanstalk.ConnError
		var cerr beanstalk.ConnError
		switch {
		case errors.As(err, &cerr):
			switch {
			case errors.Is(cerr.Err, beanstalk.ErrTimeout):
				// timeout error on timeout, just continue the loop
			case
				errors.Is(cerr.Err, beanstalk.ErrBadChar),
				errors.Is(cerr.Err, beanstalk.ErrBadFormat),
				errors.Is(cerr.Err, beanstalk.ErrBuried),
				errors.Is(cerr.Err, beanstalk.ErrDeadline),
				errors.Is(cerr.Err, beanstalk.ErrDraining),
				errors.Is(cerr.Err, beanstalk.ErrEmpty),
				errors.Is(cerr.Err, beanstalk.ErrInternal),
				errors.Is(cerr.Err, beanstalk.ErrJobTooBig),
				errors.Is(cerr.Err, beanstalk.ErrNoCRLF),
				errors.Is(cerr.Err, beanstalk.ErrNotFound),
				errors.Is(cerr.Err, beanstalk.ErrNotIgnored),
				errors.Is(cerr.Err, beanstalk.ErrTooLong):
				// won't reset
				logx.Error(err)
			default:
				// beanstalk.ErrOOM, beanstalk.ErrUnknown and other errors
				logx.Error(err)
				c.conn.reset()
				time.Sleep(time.Second)
			}
		default:
			logx.Error(err)
		}
	}

	if err := c.conn.Close(); err != nil {
		logx.Error(err)
	}
}

func (cs consumeService) Start() {
	cs.c.consumeEvents(cs.consume)
}

func (cs consumeService) Stop() {
	cs.c.dispose()
}
