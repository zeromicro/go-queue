package dq

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/zeromicro/go-zero/core/logx"
)

var ErrTimeBeforeNow = errors.New("can't schedule task to past time")

type producerNode struct {
	endpoint string
	tube     string
	conn     *connection
}

func NewProducerNode(endpoint, tube string) Producer {
	return &producerNode{
		endpoint: endpoint,
		tube:     tube,
		conn:     newConnection(endpoint, tube),
	}
}

func (p *producerNode) At(body []byte, at time.Time) (string, error) {
	return p.at(wrap(body, at), at)
}

func (p *producerNode) Close() error {
	return p.conn.Close()
}

func (p *producerNode) Delay(body []byte, delay time.Duration) (string, error) {
	return p.delay(wrap(body, time.Now().Add(delay)), delay)
}

func (p *producerNode) Revoke(jointId string) error {
	ids := strings.Split(jointId, idSep)
	for _, id := range ids {
		fields := strings.Split(id, "/")
		if len(fields) < 3 {
			continue
		}
		if fields[0] != p.endpoint || fields[1] != p.tube {
			continue
		}

		conn, err := p.conn.get()
		if err != nil {
			return err
		}

		n, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return err
		}

		return conn.Delete(n)
	}

	// if not in this beanstalk, ignore
	return nil
}

func (p *producerNode) at(body []byte, at time.Time) (string, error) {
	now := time.Now()
	if at.Before(now) {
		return "", ErrTimeBeforeNow
	}

	duration := at.Sub(now)
	return p.delay(body, duration)
}

func (p *producerNode) delay(body []byte, delay time.Duration) (string, error) {
	conn, err := p.conn.get()
	if err != nil {
		return "", err
	}

	id, err := conn.Put(body, PriNormal, delay, defaultTimeToRun)
	if err == nil {
		return fmt.Sprintf("%s/%s/%d", p.endpoint, p.tube, id), nil
	}

	// the error can only be beanstalk.NameError or beanstalk.ConnError
	// just return when the error is beanstalk.NameError, don't reset
	var cerr beanstalk.ConnError
	switch {
	case errors.As(err, &cerr):
		switch {
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
		default:
			// beanstalk.ErrOOM, beanstalk.ErrTimeout, beanstalk.ErrUnknown and other errors
			p.conn.reset()
		}
	default:
		logx.Error(err)
	}

	return "", err
}
