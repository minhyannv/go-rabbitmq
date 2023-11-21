### 基于 "github.com/streadway/amqp" 库封装的rabbitmq库

### 容器启动RabbitMQ

```bash
docker run -d --name rabbit -p 15672:15672 -p 5672:5672 -e RABBITMQ_DEFAULT_USER=test -e RABBITMQ_DEFAULT_PASS=8PKtAj9TDIuqAuAAL5w7 -e RABBITMQ_DEFAULT_VHOST=test rabbitmq:management
```

### 运行消费者

```go
go run./consumer/consumer.go
```

### 运行生产者

```go
go run./producer/producer.go
```