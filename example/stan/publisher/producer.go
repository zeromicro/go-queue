package main

import (
	"fmt"
	"github.com/nats-io/stan.go"
	"github.com/zeromicro/go-queue/stanq"
	"math/rand"
	"time"
)

func main() {
	c := &stanq.StanqConfig{
		ClusterID: "nats-streaming",
		ClientID:  "publish123",
		Options:   []stan.Option{stan.NatsURL("nats://127.0.0.1:14222,nats://127.0.0.1:24222,nats://127.0.0.1:34222")},
	}
	p, err := stanq.NewProducer(c)
	if err != nil {
		fmt.Println(err.Error())
	}

	for {
		time.Sleep(300 * time.Millisecond)
		sub := randSub()
		err = p.Publish(sub, []byte(fmt.Sprintf("%s-%s", sub, randBody())))
		if err != nil {
			fmt.Println(err.Error())
		}
	}

}

func randSub() string {
	rand.Seed(time.Now().UnixNano()) // 设置随机数种子
	// 定义需要生成的字符集
	charSet := "abc"
	// 定义需要生成的字符长度
	length := 1
	// 随机生成字符
	result := make([]byte, length)
	for i := range result {
		result[i] = charSet[rand.Intn(len(charSet))]
	}
	return string(result)
}

func randBody() []byte {
	charSet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	length := 10
	result := make([]byte, length)
	for i := range result {
		result[i] = charSet[rand.Intn(len(charSet))]
	}
	return result
}
