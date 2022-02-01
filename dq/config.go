package dq

import "github.com/zeromicro/go-zero/core/stores/redis"

type (
	Beanstalk struct {
		Endpoint string
		Tube     string
	}

	DqConf struct {
		Beanstalks []Beanstalk
		Redis      redis.RedisConf
	}
)
