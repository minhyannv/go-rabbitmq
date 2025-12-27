## go-rabbitmq

[English](README.md) | [简体中文](README.zh-CN.md)

`go-rabbitmq` is a RabbitMQ client wrapper built on `github.com/streadway/amqp`. It provides **auto-reconnect** and **operation-level retries**, plus runnable examples and tests for **direct / fanout / topic / headers** exchanges (requires a reachable RabbitMQ).

### Features

- **Auto-reconnect**: listens on `NotifyClose` and reconnects; every operation also ensures an active connection
- **Operation retries**: declare/bind/publish/open-channel retries with exponential backoff
- **Self-healing consumer loop**: `Consume` can recover after channel/connection failures
- **Examples**: `examples/` contains demos for all 4 exchange types (auto-cleanup)
- **Tests**: `rabbitmq/rabbitmq_test.go` verifies routing semantics + reconnect behavior (skips if RabbitMQ is not reachable)

### Layout

- **Library**: `rabbitmq/`
- **Examples**: `examples/` (direct / fanout / topic / headers)
- **Tests**: `rabbitmq/rabbitmq_test.go`

### Requirements

- Go: see `go.mod` (currently Go 1.20)
- RabbitMQ: local or Docker (recommended: `rabbitmq:management`)

### Quick Start

#### 1) Start RabbitMQ (Docker)

```bash
docker run -d --name go-rabbitmq-rabbit \
  -p 15672:15672 -p 5672:5672 \
  -e RABBITMQ_DEFAULT_USER=test \
  -e RABBITMQ_DEFAULT_PASS=123456 \
  -e RABBITMQ_DEFAULT_VHOST=test \
  rabbitmq:management
```

- Management UI: `http://localhost:15672`
- Credentials: `test` / `123456`

#### 2) Configure connection

```bash
export RABBITMQ_URL='amqp://test:123456@localhost:5672/test'
```

#### 3) Run examples

Each demo declares exchange/queues, binds, consumes and publishes messages to validate routing, then cleans up resources.

```bash
go run ./examples/direct
go run ./examples/fanout
go run ./examples/topic
go run ./examples/headers
```

#### 4) Run tests

Tests connect to `RABBITMQ_URL` and will be skipped if RabbitMQ is not reachable.

```bash
go test ./...
```

### Exchange types

- **direct**: routing key exact match (`routingKey == bindingKey`) — demo: `examples/direct`
- **fanout**: broadcast to all bound queues (routing key ignored) — demo: `examples/fanout`
- **topic**: routing key pattern match (`*` = one word, `#` = zero or more words) — demo: `examples/topic`
- **headers**: headers match (`Args` + `Publishing.Headers`, `x-match=all|any`) — demo: `examples/headers`

### Minimal API example

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

### Reconnect & retry strategy

- **Reconnect triggers**
  - Passive: background reconnect on `NotifyClose`
  - Active: every API call ensures a valid connection (`Connect` when needed)
- **ReconnectBackoff**
  - Used for connection re-dial only
  - `MaxRetries`: `0` = no retries (single attempt); `-1` = retry forever (until ctx cancelled or `Client.Close()`)
- **OperationBackoff**
  - Used for operations (open channel / declare / bind / publish / get)
  - `MaxRetries` semantics: same as above
- **Consume loop**
  - `Consume` re-establishes connection/channel and keeps consuming after failures
  - Override via `ConsumeOptions.RetryBackoff` (defaults to `OperationBackoff`)

### Notes & limitations

- Publisher confirm is not implemented; exactly-once is not guaranteed — use idempotent consumers if needed
- `Immediate` is not supported by RabbitMQ; `Mandatory=false` drops unroutable messages
- In-flight operations can fail during disconnect; retries may lead to duplicates — design for idempotency
- Dependency: `github.com/streadway/amqp` (can be adapted to `github.com/rabbitmq/amqp091-go` if desired)