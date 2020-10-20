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
```json
{
    "Name": "kq",
    "Brokers": [
        "127.0.0.1:19092",
        "127.0.0.1:19092",
        "127.0.0.1:19092"
    ],
    "Group": "adhoc",
    "Topic": "kq",
    "Offset": "first",
    "NumProducers": 1
}
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