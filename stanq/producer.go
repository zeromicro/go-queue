package stanq

import "github.com/nats-io/stan.go"

type Producer struct {
	conn stan.Conn
}

func NewProducer(c *StanqConfig) (*Producer, error) {
	sc, err := stan.Connect(c.ClusterID, c.ClientID, c.Options...)
	if err != nil {
		return nil, err
	}

	return &Producer{
		conn: sc,
	}, nil
}

func (p *Producer) Publish(subject string, data []byte) error {
	return p.conn.Publish(subject, data)
}

func (p *Producer) Close() {
	if p.conn != nil {
		_ = p.conn.Close()
	}
}
