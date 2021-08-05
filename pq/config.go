package pq

import "github.com/tal-tech/go-zero/core/service"

type PqConf struct {
	service.ServiceConf
	Brokers          []string
	Topic            string
	SubscriptionName string
	Conns            int `json:",default=1"`
	Processors       int `json:",default=8"`
}
