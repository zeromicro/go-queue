package dq

import "github.com/tal-tech/go-zero/core/stores/redis"

type (
	Beanstalk struct {
		Endpoint string
		Tube     string
	}

	Conf struct {
		Beanstalks []Beanstalk
		Redis      redis.RedisConf
	}
)
