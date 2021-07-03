/**
 * @Author: 陈建君
 * @Date: 2021/7/3 11:24 上午
 * @Description: 配置信息
 */

package go_queue

import (
	"github.com/tal-tech/go-queue/internal/dq"
	"github.com/tal-tech/go-queue/internal/kq"
)

type (
	Beanstalk = dq.Beanstalk
	DqConf    = dq.Conf
	KqConf    = kq.Conf
)
