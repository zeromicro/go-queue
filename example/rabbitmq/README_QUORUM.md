# RabbitMQ Quorum Queue 使用指南

## 概述

go-queue 的 RabbitMQ 组件支持声明 Quorum Queue 队列，并利用其原生的 Delivery Limit 机制避免消息处理失败导致的死循环问题。

## Quorum Queue vs Classic Queue

| 特性 | Classic Queue | Quorum Queue |
|------|---------------|---------------|
| 数据持久化 | 按消息配置 | 始终持久化 |
| 高可用性 | 不支持 | 支持（基于 Raft 共识算法） |
| 毒消息处理 | 不支持 | 支持（Delivery Limit） |
| 适用场景 | 临时队列、低延迟要求 | 关键业务数据、高可靠性要求 |

## 使用场景

**推荐使用 Quorum Queue 的场景：**
- 订单系统
- 投票系统
- 需要确保消息不丢失的关键业务
- 需要避免毒消息死循环的场景

**使用 Classic Queue 的场景：**
- 临时队列
- 对延迟极其敏感的场景
- 数据安全优先级不高的场景

## 配置说明

### QueueConf 配置参数

```go
type QueueConf struct {
    Name                string  // 队列名称
    QueueType           string  // 队列类型: "classic" 或 "quorum"，默认 "classic"
    Durable             bool    // 是否持久化
    AutoDelete          bool    // 自动删除
    Exclusive           bool    // 排他性
    NoWait              bool    // 是否阻塞等待
    DeliveryLimit       int64   // 投递限制（仅 Quorum Queue），0 表示使用 RabbitMQ 默认值
    DeadLetterExchange   string  // 死信交换机
    DeadLetterRoutingKey string  // 死信路由键
}
```

### 声明 Quorum Queue

```go
admin := rabbitmq.MustNewAdmin(conf)

// 使用新的 DeclareQueueConf 方法声明队列
queueConf := rabbitmq.QueueConf{
    Name:                 "orders.quorum",
    QueueType:            "quorum",
    Durable:              true,
    DeliveryLimit:         20,  // 超过 20 次投递后，消息将被丢弃或死信
    DeadLetterExchange:     "orders.dlx",
    DeadLetterRoutingKey:   "failed",
}

err := admin.DeclareQueueConf(queueConf)
if err != nil {
    log.Fatal(err)
}
```

## Delivery Limit 机制

### 工作原理

Quorum Queue 自动跟踪每条消息的投递次数，通过消息头 `x-delivery-count` 暴露给消费者：

1. 消息首次被投递，`x-delivery-count = 1`
2. 如果消费者处理失败并执行 `Nack(requeue=true)`，消息重新入队
3. 下次投递时，`x-delivery-count = 2`
4. 重复此过程，直到超过 `delivery-limit`
5. 超过限制后：
   - 如果配置了死信交换机：消息被发送到死信队列
   - 未配置死信交换机：消息被丢弃

### 避免队列阻塞

Delivery Limit 机制完美解决了以下问题：

**问题场景：**
```go
// 消费者代码中存在 Bug，导致某些消息始终处理失败
func (h Handler) Consume(message string) error {
    if strings.Contains(message, "buggy") {
        return errors.New("处理失败")
    }
    return nil
}
```

**如果不使用 Delivery Limit：**
1. 包含 "buggy" 的消息被消费
2. 处理失败，Nack + requeue=true
3. 消息立即重新投递
4. 无限循环，队列被阻塞

**使用 Delivery Limit：**
1. 包含 "buggy" 的消息被消费
2. 处理失败，Nack + requeue=true，投递次数 +1
3. 重复投递最多 20 次（默认）
4. 超过限制后，消息进入死信队列或被丢弃
5. 队列可以继续处理其他消息

## 完整示例

### 1. 声明基础设施

```go
package main

import (
    "log"

    "github.com/zeromicro/go-queue/rabbitmq"
)

func main() {
    conf := rabbitmq.RabbitConf{
        Host:     "localhost",
        Port:     5672,
        Username: "guest",
        Password: "guest",
    }
    admin := rabbitmq.MustNewAdmin(conf)

    // 声明死信交换机
    err := admin.DeclareExchange(rabbitmq.ExchangeConf{
        ExchangeName: "orders.dlx",
        Type:         "direct",
        Durable:      true,
    }, nil)
    if err != nil {
        log.Fatal(err)
    }

    // 声明死信队列
    err = admin.DeclareQueueConf(rabbitmq.QueueConf{
        Name:       "orders.failed",
        QueueType:  "quorum",
        Durable:    true,
    })
    if err != nil {
        log.Fatal(err)
    }

    // 绑定死信队列
    err = admin.Bind("orders.failed", "failed", "orders.dlx", false, nil)
    if err != nil {
        log.Fatal(err)
    }

    // 声明主交换机
    err = admin.DeclareExchange(rabbitmq.ExchangeConf{
        ExchangeName: "orders",
        Type:         "direct",
        Durable:      true,
    }, nil)
    if err != nil {
        log.Fatal(err)
    }

    // 声明主队列（Quorum Queue，带 Delivery Limit）
    err = admin.DeclareQueueConf(rabbitmq.QueueConf{
        Name:                 "orders.main",
        QueueType:            "quorum",
        Durable:              true,
        DeliveryLimit:         20,  // 限制投递次数
        DeadLetterExchange:     "orders.dlx",  // 死信交换机
        DeadLetterRoutingKey:   "failed",  // 死信路由键
    })
    if err != nil {
        log.Fatal(err)
    }

    // 绑定主队列
    err = admin.Bind("orders.main", "created", "orders", false, nil)
    if err != nil {
        log.Fatal(err)
    }
}
```

### 2. 消费者配置

```go
package main

import (
    "context"
    "fmt"

    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/zeromicro/go-zero/core/logc"
    "github.com/zeromicro/go-zero/core/service"
    "github.com/zeromicro/go-queue/rabbitmq"
)

type OrderHandler struct{}

func (h OrderHandler) Consume(message string) error {
    fmt.Printf("Processing order: %s\n", message)

    // 业务处理逻辑
    err := processOrder(message)
    if err != nil {
        return err  // 返回错误，消息将被重新投递
    }

    return nil
}

func main() {
    listenerConf := rabbitmq.RabbitListenerConf{
        RabbitConf: rabbitmq.RabbitConf{
            Host:     "localhost",
            Port:     5672,
            Username: "guest",
            Password: "guest",
        },
        ListenerQueues: []rabbitmq.ConsumerConf{
            {
                Name:    "orders.main",
                AutoAck: false,  // 必须手动确认
            },
        },
    }

    // 自定义错误处理器
    listener := rabbitmq.MustNewListener(listenerConf, OrderHandler{},
        rabbitmq.WithErrorHandler(func(ctx context.Context, msg amqp.Delivery, err error) {
            // 记录详细错误信息
            deliveryCount := msg.Headers["x-delivery-count"]
            logc.Errorf(ctx, "Failed to process order (delivery count: %v): %s, error: %v",
                deliveryCount, string(msg.Body), err)
        }),
    )

    serviceGroup := service.NewServiceGroup()
    serviceGroup.Add(listener)
    defer serviceGroup.Stop()
    serviceGroup.Start()
}

func processOrder(message string) error {
    // 实际的业务处理逻辑
    return nil
}
```

### 3. 死信消息消费者（可选）

```go
package main

import (
    "fmt"
    "log"

    "github.com/zeromicro/go-zero/core/service"
    "github.com/zeromicro/go-queue/rabbitmq"
)

type FailedOrderHandler struct{}

func (h FailedOrderHandler) Consume(message string) error {
    fmt.Printf("Processing failed order: %s\n", message)

    // 记录失败订单到数据库，发送告警等
    return nil
}

func main() {
    listenerConf := rabbitmq.RabbitListenerConf{
        RabbitConf: rabbitmq.RabbitConf{
            Host:     "localhost",
            Port:     5672,
            Username: "guest",
            Password: "guest",
        },
        ListenerQueues: []rabbitmq.ConsumerConf{
            {
                Name:    "orders.failed",
                AutoAck: true,  // 死信消息可以直接自动确认
            },
        },
    }

    listener := rabbitmq.MustNewListener(listenerConf, FailedOrderHandler{})
    serviceGroup := service.NewServiceGroup()
    serviceGroup.Add(listener)
    defer serviceGroup.Stop()
    serviceGroup.Start()
}
```

## 配置选项说明

### DeliveryLimit 值选择

- `-1`：无限制（不推荐，可能导致死循环）
- `0`：使用 RabbitMQ 默认值（RabbitMQ 4.0+ 默认为 20）
- `1-N`：指定最大投递次数（推荐 10-20）

```go
// 推荐：限制 10 次投递
DeliveryLimit: 10,

// 无限制（谨慎使用）
DeliveryLimit: -1,

// 使用 RabbitMQ 默认值（20）
DeliveryLimit: 0,
```

### 死信配置策略

**策略一：允许消息丢弃（简单）**
```go
// 不配置死信交换机，消息被丢弃
queueConf := rabbitmq.QueueConf{
    Name:         "orders.main",
    QueueType:    "quorum",
    DeliveryLimit: 20,
}
```

**策略二：死信到专用队列（推荐）**
```go
// 失败消息进入死信队列，可以后续分析或重试
queueConf := rabbitmq.QueueConf{
    Name:                 "orders.main",
    QueueType:            "quorum",
    DeliveryLimit:         20,
    DeadLetterExchange:     "orders.dlx",
    DeadLetterRoutingKey:   "failed",
}
```

## 注意事项

1. **AutoAck 必须为 false**
   ```go
   ConsumerConf{
       Name:    "orders.main",
       AutoAck: false,  // 必须手动确认，否则 Nack 不生效
   }
   ```

2. **Delivery Limit 仅对 Quorum Queue 生效**
   - Classic Queue 不会投递次数限制

3. **RabbitMQ 版本要求**
   - RabbitMQ 3.8+ 支持 Quorum Queue
   - RabbitMQ 3.11+ 支持 Delivery Limit
   - RabbitMQ 4.0+ 默认 delivery-limit 为 20

4. **死信队列类型**
   - 建议死信队列也使用 Quorum Queue 以确保数据安全

## 最佳实践

1. **为所有 Quorum Queue 配置 Delivery Limit**
   ```go
   DeliveryLimit: 20,
   ```

2. **为关键队列配置死信交换机**
   ```go
   DeadLetterExchange:     "app.dlx",
   DeadLetterRoutingKey:   "queue-name",
   ```

3. **在错误处理器中记录投递次数**
   ```go
   rabbitmq.WithErrorHandler(func(ctx context.Context, msg amqp.Delivery, err error) {
       deliveryCount := msg.Headers["x-delivery-count"]
       logc.Errorf(ctx, "Error (delivery count: %v): %v", deliveryCount, err)
   })
   ```

4. **监控死信队列**
   - 定期检查死信队列中的消息数量
   - 分析失败原因，修复 bug 或调整业务逻辑

## 参考资料

- [RabbitMQ Quorum Queues 文档](https://www.rabbitmq.com/quorum-queues.html)
- [RabbitMQ Dead Letter Exchange](https://www.rabbitmq.com/dlx.html)
