package stanq

import (
	stan "github.com/nats-io/stan.go"
)

type StanqConfig struct {
	ClusterID string        `json:"cluster_id"`
	ClientID  string        `json:"client_id"`
	Options   []stan.Option `json:"options"`
}
