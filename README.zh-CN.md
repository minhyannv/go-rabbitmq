## go-rabbitmq

[English](README.md) | [简体中文](README.zh-CN.md)

`go-rabbitmq` 是一个基于 `github.com/streadway/amqp` 的 RabbitMQ Go 客户端封装，提供 **自动断线重连** 与 **操作级重试**，并内置 **direct / fanout / topic / headers** 四种交换机的示例与测试（需要可访问的 RabbitMQ）。

### 特性

- **自动重连**：监听 `NotifyClose`，连接断开后触发重连；同时每次操作会确保连接可用
- **操作重试**：声明/绑定/发布/开 channel 等操作失败后按退避策略重试
- **消费自恢复**：`Consume` 在 channel/连接异常时可自动重新建立并继续消费
- **示例**：`examples/` 下提供四种交换机的可执行 demo（运行后自动清理资源）
- **测试**：`rabbitmq/rabbitmq_test.go` 覆盖路由语义与重连行为（RabbitMQ 不可达会自动 skip）

### 目录结构

- **库代码**：`rabbitmq/`
- **示例**：`examples/`（direct / fanout / topic / headers）
- **测试**：`rabbitmq/rabbitmq_test.go`

### 前置要求

- Go：见 `go.mod`（当前为 Go 1.20）
- RabbitMQ：本地或 Docker 均可（建议 `rabbitmq:management` 便于调试）

### 快速开始

#### 1) 启动 RabbitMQ（Docker）

```bash
docker run -d --name go-rabbitmq-rabbit \
  -p 15672:15672 -p 5672:5672 \
  -e RABBITMQ_DEFAULT_USER=test \
  -e RABBITMQ_DEFAULT_PASS=123456 \
  -e RABBITMQ_DEFAULT_VHOST=test \
  rabbitmq:management
```

- 管理后台：`http://localhost:15672`
- 账号密码：`test` / `123456`

#### 2) 配置连接

```bash
export RABBITMQ_URL='amqp://test:123456@localhost:5672/test'
```

#### 3) 运行 examples（四种交换机 demo）

每个 demo 会自动声明 exchange/queue、绑定、启动消费并发布消息验证路由，结束后自动清理资源。

```bash
go run ./examples/direct
go run ./examples/fanout
go run ./examples/topic
go run ./examples/headers
```

#### 4) 运行测试

测试会连接 `RABBITMQ_URL`；如果不可达会自动 skip。

```bash
go test ./...
```

### 四种交换机类型

- **direct**：routing key 精确匹配（`routingKey == bindingKey`）— demo：`examples/direct`
- **fanout**：广播到所有绑定队列（routing key 忽略）— demo：`examples/fanout`
- **topic**：routing key 模式匹配（`*` 匹配一个单词，`#` 匹配零或多个单词）— demo：`examples/topic`
- **headers**：按 headers 匹配（`Args` + `Publishing.Headers`，`x-match=all|any`）— demo：`examples/headers`

### API 最小示例

```go
package main

import (
	"context"
	"os"

	"github.com/minhyannv/go-rabbitmq/rabbitmq"
)

func main() {
	ctx := context.Background()

	c, err := rabbitmq.New(ctx, rabbitmq.Config{
		URL: os.Getenv("RABBITMQ_URL"),
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	_ = c.DeclareExchange(ctx, "ex", "direct", rabbitmq.ExchangeOptions{Durable: true})
	_ = c.DeclareQueue(ctx, "q", rabbitmq.QueueOptions{Durable: true})
	_ = c.BindQueue(ctx, "q", "k", "ex", rabbitmq.BindOptions{})
	_ = c.PublishString(ctx, "ex", "k", "hello")
}
```

### 重连与重试策略

- **重连触发**
  - 被动：连接 `NotifyClose` 后后台重连
  - 主动：每次 API 调用都会先确保连接可用（必要时 `Connect`）
- **ReconnectBackoff（重连退避）**
  - 仅用于连接重建（Dial）
  - `MaxRetries` 约定：`0` 不重试（只尝试 1 次）；`-1` 无限重试（直到 ctx 取消或 `Client.Close()`）
- **OperationBackoff（操作退避）**
  - 用于 open-channel/declare/bind/publish/get 等操作重试
  - `MaxRetries` 约定：同上
- **Consume（消费自恢复）**
  - `Consume` 在 channel/连接异常后会重新建立并继续消费
  - 可用 `ConsumeOptions.RetryBackoff` 覆盖（默认复用 `OperationBackoff`）

### 注意事项

- 未封装 publisher confirm；不保证 exactly-once；请结合幂等消费
- `Immediate` RabbitMQ 不支持；`Mandatory=false` 时不可路由消息会被丢弃
- 断线瞬间 publish/declare 可能失败；重试可能导致重复，请做好幂等
- 依赖 `github.com/streadway/amqp`（如需可适配到 `github.com/rabbitmq/amqp091-go`）


