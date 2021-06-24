package dq

import (
	"sync/atomic"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/syncx"
)

type (
	consumerNode struct {
		conn          *connection
		tube          string
		on            *syncx.AtomicBool
		processingNum int64
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
	defer func() {
		if err := recover(); err != nil {
			logx.Error(err)
			// prevent accidental crashes leading to inaccurate counting
			atomic.AddInt64(&c.processingNum, -1)
		}
	}()
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
			atomic.AddInt64(&c.processingNum, 1)
			consume(body)
			atomic.AddInt64(&c.processingNum, -1)
			continue
		}

		// the error can only be beanstalk.NameError or beanstalk.ConnError
		switch cerr := err.(type) {
		case beanstalk.ConnError:
			switch cerr.Err {
			case beanstalk.ErrTimeout:
				// timeout error on timeout, just continue the loop
			case beanstalk.ErrBadChar, beanstalk.ErrBadFormat, beanstalk.ErrBuried, beanstalk.ErrDeadline,
				beanstalk.ErrDraining, beanstalk.ErrEmpty, beanstalk.ErrInternal, beanstalk.ErrJobTooBig,
				beanstalk.ErrNoCRLF, beanstalk.ErrNotFound, beanstalk.ErrNotIgnored, beanstalk.ErrTooLong:
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
	for {
		if atomic.LoadInt64(&cs.c.processingNum) == 0 {
			// wait all service consumer func process complete
			break
		}
		// wait 100 millisecond check again
		time.Sleep(time.Millisecond * 100)
	}
}
