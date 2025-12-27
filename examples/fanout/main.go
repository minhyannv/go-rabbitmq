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
	ex := fmt.Sprintf("ex_fanout_%d", suffix)
	q1 := fmt.Sprintf("q_fanout_1_%d", suffix)
	q2 := fmt.Sprintf("q_fanout_2_%d", suffix)

	qopt := rabbitmq.QueueOptions{Durable: false, AutoDelete: true}
	eopt := rabbitmq.ExchangeOptions{Durable: false, AutoDelete: true}

	defer func() {
		_ = c.DeleteQueue(context.Background(), q1, false, false, true)
		_ = c.DeleteQueue(context.Background(), q2, false, false, true)
		_ = c.DeleteExchange(context.Background(), ex, false, true)
	}()

	if err := c.DeclareExchange(ctx, ex, "fanout", eopt); err != nil {
		log.Fatalf("declare exchange: %v", err)
	}
	if err := c.DeclareQueue(ctx, q1, qopt); err != nil {
		log.Fatalf("declare queue1: %v", err)
	}
	if err := c.DeclareQueue(ctx, q2, qopt); err != nil {
		log.Fatalf("declare queue2: %v", err)
	}
	// fanout ignores routing key
	if err := c.BindQueue(ctx, q1, "", ex, rabbitmq.BindOptions{}); err != nil {
		log.Fatalf("bind q1: %v", err)
	}
	if err := c.BindQueue(ctx, q2, "", ex, rabbitmq.BindOptions{}); err != nil {
		log.Fatalf("bind q2: %v", err)
	}

	got1 := make(chan string, 1)
	got2 := make(chan string, 1)

	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()

	c.StartConsumer(consCtx, q1, rabbitmq.ConsumeOptions{AutoAck: true, QosPrefetchCount: 1}, func(ctx context.Context, d amqp.Delivery) error {
		select {
		case got1 <- string(d.Body):
		default:
		}
		return nil
	})
	c.StartConsumer(consCtx, q2, rabbitmq.ConsumeOptions{AutoAck: true, QosPrefetchCount: 1}, func(ctx context.Context, d amqp.Delivery) error {
		select {
		case got2 <- string(d.Body):
		default:
		}
		return nil
	})

	log.Printf("publishing to fanout exchange %q (routingKey ignored)", ex)
	if err := c.PublishString(ctx, ex, "", "hello-fanout"); err != nil {
		log.Fatalf("publish: %v", err)
	}

	select {
	case m := <-got1:
		log.Printf("[q1] got: %s", m)
	case <-time.After(3 * time.Second):
		log.Fatalf("timeout waiting q1")
	}
	select {
	case m := <-got2:
		log.Printf("[q2] got: %s", m)
	case <-time.After(3 * time.Second):
		log.Fatalf("timeout waiting q2")
	}
}
