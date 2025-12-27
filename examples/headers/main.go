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
	ex := fmt.Sprintf("ex_headers_%d", suffix)
	q := fmt.Sprintf("q_headers_%d", suffix)

	qopt := rabbitmq.QueueOptions{Durable: false, AutoDelete: true}
	eopt := rabbitmq.ExchangeOptions{Durable: false, AutoDelete: true}

	defer func() {
		_ = c.DeleteQueue(context.Background(), q, false, false, true)
		_ = c.DeleteExchange(context.Background(), ex, false, true)
	}()

	if err := c.DeclareExchange(ctx, ex, "headers", eopt); err != nil {
		log.Fatalf("declare exchange: %v", err)
	}
	if err := c.DeclareQueue(ctx, q, qopt); err != nil {
		log.Fatalf("declare queue: %v", err)
	}

	// headers exchange binds with arguments. x-match=all means all headers must match.
	if err := c.BindQueue(ctx, q, "", ex, rabbitmq.BindOptions{
		Args: amqp.Table{
			"x-match": "all",
			"format":  "pdf",
			"type":    "report",
		},
	}); err != nil {
		log.Fatalf("bind: %v", err)
	}

	got := make(chan string, 2)
	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()

	c.StartConsumer(consCtx, q, rabbitmq.ConsumeOptions{AutoAck: true, QosPrefetchCount: 1}, func(ctx context.Context, d amqp.Delivery) error {
		got <- string(d.Body)
		return nil
	})

	log.Printf("publish headers match (format=pdf,type=report)")
	if err := c.Publish(ctx, ex, "", rabbitmq.PublishOptions{
		Msg: amqp.Publishing{
			ContentType: "text/plain",
			Headers: amqp.Table{
				"format": "pdf",
				"type":   "report",
			},
			Body: []byte("hello-headers-match"),
		},
	}); err != nil {
		log.Fatalf("publish match: %v", err)
	}

	select {
	case m := <-got:
		log.Printf("[q] got: %s", m)
	case <-time.After(3 * time.Second):
		log.Fatalf("timeout waiting matched message")
	}

	log.Printf("publish headers NOT match (type=other) -> should not route")
	if err := c.Publish(ctx, ex, "", rabbitmq.PublishOptions{
		Msg: amqp.Publishing{
			ContentType: "text/plain",
			Headers: amqp.Table{
				"format": "pdf",
				"type":   "other",
			},
			Body: []byte("hello-headers-nomatch"),
		},
	}); err != nil {
		log.Fatalf("publish nomatch: %v", err)
	}

	select {
	case m := <-got:
		log.Fatalf("unexpected got: %s", m)
	case <-time.After(800 * time.Millisecond):
		log.Printf("[q] got nothing (expected)")
	}
}
