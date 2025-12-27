package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/minhyannv/go-rabbitmq/rabbitmq"

	"github.com/streadway/amqp"
)

func main() {
	url := strings.TrimSpace(os.Getenv("RABBITMQ_URL"))
	if url == "" {
		url = "amqp://test:123456@localhost:5672/test"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, err := rabbitmq.New(ctx, rabbitmq.Config{URL: url})
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer func() { _ = c.Close() }()

	suffix := time.Now().UnixNano()
	ex := fmt.Sprintf("ex_topic_%d", suffix)
	qAny := fmt.Sprintf("q_topic_user_star_%d", suffix)
	qCreated := fmt.Sprintf("q_topic_user_created_%d", suffix)

	qopt := rabbitmq.QueueOptions{Durable: false, AutoDelete: true}
	eopt := rabbitmq.ExchangeOptions{Durable: false, AutoDelete: true}

	defer func() {
		_ = c.DeleteQueue(context.Background(), qAny, false, false, true)
		_ = c.DeleteQueue(context.Background(), qCreated, false, false, true)
		_ = c.DeleteExchange(context.Background(), ex, false, true)
	}()

	if err := c.DeclareExchange(ctx, ex, "topic", eopt); err != nil {
		log.Fatalf("declare exchange: %v", err)
	}
	if err := c.DeclareQueue(ctx, qAny, qopt); err != nil {
		log.Fatalf("declare queue any: %v", err)
	}
	if err := c.DeclareQueue(ctx, qCreated, qopt); err != nil {
		log.Fatalf("declare queue created: %v", err)
	}
	// user.* matches: user.created, user.deleted, ...
	if err := c.BindQueue(ctx, qAny, "user.*", ex, rabbitmq.BindOptions{}); err != nil {
		log.Fatalf("bind any: %v", err)
	}
	// exact match
	if err := c.BindQueue(ctx, qCreated, "user.created", ex, rabbitmq.BindOptions{}); err != nil {
		log.Fatalf("bind created: %v", err)
	}

	gotAny := make(chan string, 10)
	gotCreated := make(chan string, 10)

	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()

	c.StartConsumer(consCtx, qAny, rabbitmq.ConsumeOptions{AutoAck: true, QosPrefetchCount: 5}, func(ctx context.Context, d amqp.Delivery) error {
		gotAny <- string(d.Body)
		return nil
	})
	c.StartConsumer(consCtx, qCreated, rabbitmq.ConsumeOptions{AutoAck: true, QosPrefetchCount: 5}, func(ctx context.Context, d amqp.Delivery) error {
		gotCreated <- string(d.Body)
		return nil
	})

	log.Printf("publish routingKey=%q", "user.created")
	if err := c.PublishString(ctx, ex, "user.created", "hello-topic-created"); err != nil {
		log.Fatalf("publish: %v", err)
	}

	log.Printf("publish routingKey=%q", "user.deleted")
	if err := c.PublishString(ctx, ex, "user.deleted", "hello-topic-deleted"); err != nil {
		log.Fatalf("publish: %v", err)
	}

	// Expect qAny to get both, qCreated only the created one.
	timeout := time.After(4 * time.Second)
	for got := 0; got < 2; {
		select {
		case m := <-gotAny:
			log.Printf("[qAny] got: %s", m)
			got++
		case <-timeout:
			log.Fatalf("timeout waiting qAny messages")
		}
	}

	select {
	case m := <-gotCreated:
		log.Printf("[qCreated] got: %s", m)
	case <-time.After(2 * time.Second):
		log.Fatalf("timeout waiting qCreated message")
	}
	select {
	case m := <-gotCreated:
		log.Fatalf("unexpected: qCreated got extra: %s", m)
	case <-time.After(800 * time.Millisecond):
		log.Printf("[qCreated] got no extra (expected)")
	}
}
