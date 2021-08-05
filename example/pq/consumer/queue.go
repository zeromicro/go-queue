package main

import (
	"fmt"
	"github.com/tal-tech/go-queue/pq"
	"github.com/tal-tech/go-zero/core/conf"
)

func main() {
	var c pq.PqConf
	conf.MustLoad("config.yaml", &c)

	q := pq.MustNewQueue(c, pq.WithHandle(func(k string, v []byte, properties map[string]string) error {
		fmt.Printf("%s => %s; %v\n", k, v, properties)
		return nil
	}))
	defer q.Stop()
	q.Start()
}
