# go-queue

## dq

High available beanstalkd.

### consumer example
```go
consumer := dq.NewConsumer(dq.DqConf{
	Beanstalks: []dq.Beanstalk{
		{
			Endpoint: "localhost:11300",
			Tube:     "tube",
		},
		{
			Endpoint: "localhost:11300",
			Tube:     "tube",
		},
	},
	Redis: redis.RedisConf{
		Host: "localhost:6379",
		Type: redis.NodeType,
	},
})
consumer.Consume(func(body []byte) {
	fmt.Println(string(body))
})
```
### producer example
```go
producer := dq.NewProducer([]dq.Beanstalk{
	{
		Endpoint: "localhost:11300",
		Tube:     "tube",
	},
	{
		Endpoint: "localhost:11300",
		Tube:     "tube",
	},
})	

for i := 1000; i < 1005; i++ {
	_, err := producer.Delay([]byte(strconv.Itoa(i)), time.Second*5)
	if err != nil {
		fmt.Println(err)
	}
}
```

## kq

Kafka Pub/Sub framework

### consumer example

config.json
```yaml
Name: kq
Brokers:
- 127.0.0.1:19092
- 127.0.0.1:19092
- 127.0.0.1:19092
Group: adhoc
Topic: kq
Offset: first
Consumers: 1
```

example code
```go
 var c kq.KqConf
    conf.MustLoad("config.json", &c)

    q := kq.MustNewQueue(c, kq.WithHandle(func(k, v string) error {
        fmt.Printf("=> %s\n", v)
        return nil
    }))
    defer q.Stop()
    q.Start()
```

### producer example

```go
type message struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Payload string `json:"message"`
}


pusher := kq.NewPusher([]string{
	"127.0.0.1:19092",
	"127.0.0.1:19092",
	"127.0.0.1:19092",
}, "kq")

ticker := time.NewTicker(time.Millisecond)
for round := 0; round < 3; round++ {
	select {
	case <-ticker.C:
		count := rand.Intn(100)
		m := message{
			Key:     strconv.FormatInt(time.Now().UnixNano(), 10),
			Value:   fmt.Sprintf("%d,%d", round, count),
			Payload: fmt.Sprintf("%d,%d", round, count),
		}
		body, err := json.Marshal(m)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(string(body))
		if err := pusher.Push(string(body)); err != nil {
			log.Fatal(err)
		}
	}
}
cmdline.EnterToContinue()
```

## pq

Pulsar Pub/Sub framework

### consumer example

config.json

```yaml
Name: pq
Brokers:
  - 127.0.0.1:6650
Topic: pq
Conns: 2
Processors: 2
SubscriptionName: pq
```

consumer  code

```go
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
```

producer code

```go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/tal-tech/go-queue/pq"
	"github.com/tal-tech/go-zero/core/cmdline"
)

type message struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Payload string `json:"message"`
}

func main() {
	pusher := pq.NewPusher([]string{
        "127.0.0.1:6650",
	}, "pq")

	ticker := time.NewTicker(time.Millisecond)

	for round := 0; round < 30; round++ {
		select {
		case <-ticker.C:
			key := strconv.FormatInt(time.Now().UnixNano(), 10)
			count := rand.Intn(100)
			m := message{
				Key:     key,
				Value:   fmt.Sprintf("%d,%d", round, count),
				Payload: fmt.Sprintf("%d,%d", round, count),
			}
			body, err := json.Marshal(m)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println(string(body))
			if err := pusher.Push(key, body, nil); err != nil {
				log.Fatal(err)
			}
		}
	}

	cmdline.EnterToContinue()
}
```

