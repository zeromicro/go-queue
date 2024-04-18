package dq

import (
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/errorx"
	"github.com/zeromicro/go-zero/core/fx"
	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	replicaNodes    = 3
	minWrittenNodes = 2
)

type (
	Producer interface {
		atWithWrapper(body []byte, at time.Time) (string, error)
		At(body []byte, at time.Time) (string, error)
		Close() error
		delayWithWrapper(body []byte, delay time.Duration) (string, error)
		Delay(body []byte, delay time.Duration) (string, error)
		Revoke(ids string) error
	}

	producerCluster struct {
		nodes []Producer
	}
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func NewProducer(beanstalks []Beanstalk) Producer {
	if len(beanstalks) < minWrittenNodes {
		log.Fatalf("nodes must be equal or greater than %d", minWrittenNodes)
	}

	var nodes []Producer
	producers := make(map[string]lang.PlaceholderType)
	for _, node := range beanstalks {
		if _, ok := producers[node.Endpoint]; ok {
			log.Fatal("all node endpoints must be different")
		}

		producers[node.Endpoint] = lang.Placeholder
		nodes = append(nodes, NewProducerNode(node.Endpoint, node.Tube))
	}

	return &producerCluster{nodes: nodes}
}

func (p *producerCluster) At(body []byte, at time.Time) (string, error) {
	wrapped := wrap(body, at)
	return p.insert(func(node Producer) (string, error) {
		return node.atWithWrapper(wrapped, at)
	})
}

func (p *producerCluster) Close() error {
	var be errorx.BatchError
	for _, node := range p.nodes {
		if err := node.Close(); err != nil {
			be.Add(err)
		}
	}
	return be.Err()
}

func (p *producerCluster) Delay(body []byte, delay time.Duration) (string, error) {
	wrapped := wrap(body, time.Now().Add(delay))
	return p.insert(func(node Producer) (string, error) {
		return node.delayWithWrapper(wrapped, delay)
	})
}

func (p *producerCluster) Revoke(ids string) error {
	var be errorx.BatchError

	fx.From(func(source chan<- interface{}) {
		for _, node := range p.nodes {
			source <- node
		}
	}).Map(func(item interface{}) interface{} {
		node := item.(Producer)
		return node.Revoke(ids)
	}).ForEach(func(item interface{}) {
		if item != nil {
			be.Add(item.(error))
		}
	})

	return be.Err()
}

func (p *producerCluster) cloneNodes() []Producer {
	return append([]Producer(nil), p.nodes...)
}

func (p *producerCluster) getWriteNodes() []Producer {
	if len(p.nodes) <= replicaNodes {
		return p.nodes
	}

	nodes := p.cloneNodes()
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	return nodes[:replicaNodes]
}

func (p *producerCluster) insert(fn func(node Producer) (string, error)) (string, error) {
	type idErr struct {
		id  string
		err error
	}
	var ret []idErr
	fx.From(func(source chan<- interface{}) {
		for _, node := range p.getWriteNodes() {
			source <- node
		}
	}).Map(func(item interface{}) interface{} {
		node := item.(Producer)
		id, err := fn(node)
		return idErr{
			id:  id,
			err: err,
		}
	}).ForEach(func(item interface{}) {
		ret = append(ret, item.(idErr))
	})

	var ids []string
	var be errorx.BatchError
	for _, val := range ret {
		if val.err != nil {
			be.Add(val.err)
		} else {
			ids = append(ids, val.id)
		}
	}

	jointId := strings.Join(ids, idSep)
	if len(ids) >= minWrittenNodes {
		return jointId, nil
	}

	if err := p.Revoke(jointId); err != nil {
		logx.Error(err)
	}

	return "", be.Err()
}

func (p *producerCluster) atWithWrapper(body []byte, at time.Time) (string, error) {
	return p.insert(func(node Producer) (string, error) {
		return node.atWithWrapper(body, at)
	})
}

func (p *producerCluster) delayWithWrapper(body []byte, delay time.Duration) (string, error) {
	return p.insert(func(node Producer) (string, error) {
		return node.delayWithWrapper(body, delay)
	})
}
