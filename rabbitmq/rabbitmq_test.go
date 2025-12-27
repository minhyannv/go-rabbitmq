package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

func testURL() string {
	if v := strings.TrimSpace(os.Getenv("RABBITMQ_URL")); v != "" {
		return v
	}
	// Default matches README's docker example (host assumed localhost).
	return "amqp://test:123456@localhost:5672/test"
}

func requireRabbitMQ(t *testing.T) string {
	t.Helper()
	url := testURL()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Skipf("RabbitMQ not reachable at %q: %v (set RABBITMQ_URL to run)", url, err)
		return ""
	}
	_ = conn.Close()
	// Ensure dial works fast enough.
	select {
	case <-ctx.Done():
	default:
	}
	return url
}

func newClient(t *testing.T, url string) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := New(ctx, Config{
		URL: url,
		ReconnectBackoff: BackoffConfig{
			MaxRetries: 5,
			Initial:    100 * time.Millisecond,
			Max:        500 * time.Millisecond,
			Multiplier: 2,
			Jitter:     0.2,
		},
		OperationBackoff: BackoffConfig{
			MaxRetries: 5,
			Initial:    50 * time.Millisecond,
			Max:        300 * time.Millisecond,
			Multiplier: 2,
			Jitter:     0.2,
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func uniq(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func waitOne(t *testing.T, c *Client, queue string, timeout time.Duration) (string, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	msg, err := c.GetString(ctx, queue, true, 30*time.Millisecond)
	if err != nil {
		return "", false
	}
	return msg, true
}

func TestDirectExchangeRouting(t *testing.T) {
	url := requireRabbitMQ(t)
	c := newClient(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ex := uniq(t, "ex_direct")
	q1 := uniq(t, "q1")
	q2 := uniq(t, "q2")

	t.Cleanup(func() {
		_ = c.DeleteQueue(context.Background(), q1, false, false, true)
		_ = c.DeleteQueue(context.Background(), q2, false, false, true)
		_ = c.DeleteExchange(context.Background(), ex, false, true)
	})

	qopt := QueueOptions{Durable: false, AutoDelete: true}
	eopt := ExchangeOptions{Durable: false, AutoDelete: true}
	if err := c.DeclareExchange(ctx, ex, "direct", eopt); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if err := c.DeclareQueue(ctx, q1, qopt); err != nil {
		t.Fatalf("declare queue1: %v", err)
	}
	if err := c.DeclareQueue(ctx, q2, qopt); err != nil {
		t.Fatalf("declare queue2: %v", err)
	}
	if err := c.BindQueue(ctx, q1, "k1", ex, BindOptions{}); err != nil {
		t.Fatalf("bind q1: %v", err)
	}
	if err := c.BindQueue(ctx, q2, "k2", ex, BindOptions{}); err != nil {
		t.Fatalf("bind q2: %v", err)
	}

	if err := c.PublishString(ctx, ex, "k1", "m1"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if msg, ok := waitOne(t, c, q1, 3*time.Second); !ok || msg != "m1" {
		t.Fatalf("expected q1 receive m1; got ok=%v msg=%q", ok, msg)
	}
	if _, ok := waitOne(t, c, q2, 800*time.Millisecond); ok {
		t.Fatalf("expected q2 receive nothing")
	}
}

func TestFanoutExchangeRouting(t *testing.T) {
	url := requireRabbitMQ(t)
	c := newClient(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ex := uniq(t, "ex_fanout")
	q1 := uniq(t, "q1")
	q2 := uniq(t, "q2")

	t.Cleanup(func() {
		_ = c.DeleteQueue(context.Background(), q1, false, false, true)
		_ = c.DeleteQueue(context.Background(), q2, false, false, true)
		_ = c.DeleteExchange(context.Background(), ex, false, true)
	})

	qopt := QueueOptions{Durable: false, AutoDelete: true}
	eopt := ExchangeOptions{Durable: false, AutoDelete: true}
	if err := c.DeclareExchange(ctx, ex, "fanout", eopt); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if err := c.DeclareQueue(ctx, q1, qopt); err != nil {
		t.Fatalf("declare queue1: %v", err)
	}
	if err := c.DeclareQueue(ctx, q2, qopt); err != nil {
		t.Fatalf("declare queue2: %v", err)
	}
	if err := c.BindQueue(ctx, q1, "", ex, BindOptions{}); err != nil {
		t.Fatalf("bind q1: %v", err)
	}
	if err := c.BindQueue(ctx, q2, "", ex, BindOptions{}); err != nil {
		t.Fatalf("bind q2: %v", err)
	}

	if err := c.PublishString(ctx, ex, "", "m"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if msg, ok := waitOne(t, c, q1, 3*time.Second); !ok || msg != "m" {
		t.Fatalf("expected q1 receive m; got ok=%v msg=%q", ok, msg)
	}
	if msg, ok := waitOne(t, c, q2, 3*time.Second); !ok || msg != "m" {
		t.Fatalf("expected q2 receive m; got ok=%v msg=%q", ok, msg)
	}
}

func TestTopicExchangeRouting(t *testing.T) {
	url := requireRabbitMQ(t)
	c := newClient(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ex := uniq(t, "ex_topic")
	qAny := uniq(t, "q_any")
	qCreated := uniq(t, "q_created")

	t.Cleanup(func() {
		_ = c.DeleteQueue(context.Background(), qAny, false, false, true)
		_ = c.DeleteQueue(context.Background(), qCreated, false, false, true)
		_ = c.DeleteExchange(context.Background(), ex, false, true)
	})

	qopt := QueueOptions{Durable: false, AutoDelete: true}
	eopt := ExchangeOptions{Durable: false, AutoDelete: true}
	if err := c.DeclareExchange(ctx, ex, "topic", eopt); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if err := c.DeclareQueue(ctx, qAny, qopt); err != nil {
		t.Fatalf("declare queue any: %v", err)
	}
	if err := c.DeclareQueue(ctx, qCreated, qopt); err != nil {
		t.Fatalf("declare queue created: %v", err)
	}
	if err := c.BindQueue(ctx, qAny, "user.*", ex, BindOptions{}); err != nil {
		t.Fatalf("bind any: %v", err)
	}
	if err := c.BindQueue(ctx, qCreated, "user.created", ex, BindOptions{}); err != nil {
		t.Fatalf("bind created: %v", err)
	}

	if err := c.PublishString(ctx, ex, "user.created", "c"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if msg, ok := waitOne(t, c, qAny, 3*time.Second); !ok || msg != "c" {
		t.Fatalf("expected qAny receive c; got ok=%v msg=%q", ok, msg)
	}
	if msg, ok := waitOne(t, c, qCreated, 3*time.Second); !ok || msg != "c" {
		t.Fatalf("expected qCreated receive c; got ok=%v msg=%q", ok, msg)
	}

	if err := c.PublishString(ctx, ex, "user.deleted", "d"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if msg, ok := waitOne(t, c, qAny, 3*time.Second); !ok || msg != "d" {
		t.Fatalf("expected qAny receive d; got ok=%v msg=%q", ok, msg)
	}
	if _, ok := waitOne(t, c, qCreated, 800*time.Millisecond); ok {
		t.Fatalf("expected qCreated receive nothing for user.deleted")
	}
}

func TestHeadersExchangeRouting(t *testing.T) {
	url := requireRabbitMQ(t)
	c := newClient(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ex := uniq(t, "ex_headers")
	q := uniq(t, "q")

	t.Cleanup(func() {
		_ = c.DeleteQueue(context.Background(), q, false, false, true)
		_ = c.DeleteExchange(context.Background(), ex, false, true)
	})

	qopt := QueueOptions{Durable: false, AutoDelete: true}
	eopt := ExchangeOptions{Durable: false, AutoDelete: true}
	if err := c.DeclareExchange(ctx, ex, "headers", eopt); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if err := c.DeclareQueue(ctx, q, qopt); err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	bind := BindOptions{
		Args: amqp.Table{
			"x-match": "all",
			"format":  "pdf",
			"type":    "report",
		},
	}
	if err := c.BindQueue(ctx, q, "", ex, bind); err != nil {
		t.Fatalf("bind: %v", err)
	}

	opt := PublishOptions{
		Msg: amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Headers: amqp.Table{
				"format": "pdf",
				"type":   "report",
			},
			Body: []byte("h"),
		},
	}
	if err := c.Publish(ctx, ex, "", opt); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if msg, ok := waitOne(t, c, q, 3*time.Second); !ok || msg != "h" {
		t.Fatalf("expected q receive h; got ok=%v msg=%q", ok, msg)
	}

	// Non-matching headers should not route.
	opt.Msg.Headers = amqp.Table{"format": "pdf", "type": "other"}
	opt.Msg.Body = []byte("no")
	if err := c.Publish(ctx, ex, "", opt); err != nil {
		t.Fatalf("publish nonmatch: %v", err)
	}
	if _, ok := waitOne(t, c, q, 800*time.Millisecond); ok {
		t.Fatalf("expected q receive nothing for non-matching headers")
	}
}

func TestReconnectOnNextOperation(t *testing.T) {
	url := requireRabbitMQ(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, err := New(ctx, Config{
		URL: url,
		ReconnectBackoff: BackoffConfig{
			MaxRetries: 10,
			Initial:    100 * time.Millisecond,
			Max:        800 * time.Millisecond,
			Multiplier: 2,
			Jitter:     0.1,
		},
		OperationBackoff: BackoffConfig{
			MaxRetries: 10,
			Initial:    50 * time.Millisecond,
			Max:        500 * time.Millisecond,
			Multiplier: 2,
			Jitter:     0.1,
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	q := uniq(t, "q_reconnect_op")
	t.Cleanup(func() { _ = c.DeleteQueue(context.Background(), q, false, false, true) })
	if err := c.DeclareQueue(ctx, q, QueueOptions{Durable: false, AutoDelete: true}); err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	// Simulate disconnect.
	old := c.getConn()
	if old == nil {
		t.Fatalf("expected non-nil conn")
	}
	_ = old.Close()

	// Next operation should reconnect automatically.
	msg := uniq(t, "hello")
	if err := c.PublishString(ctx, "", q, msg); err != nil {
		t.Fatalf("publish after close: %v", err)
	}

	// Pull it back to confirm the operation truly succeeded.
	got, err := c.GetString(ctx, q, true, 30*time.Millisecond)
	if err != nil {
		t.Fatalf("get after publish: %v", err)
	}
	if got != msg {
		t.Fatalf("expected %q, got %q", msg, got)
	}
}

func TestConsumeAutoReconnect(t *testing.T) {
	url := requireRabbitMQ(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	c, err := New(ctx, Config{
		URL: url,
		ReconnectBackoff: BackoffConfig{
			MaxRetries: 20,
			Initial:    100 * time.Millisecond,
			Max:        1 * time.Second,
			Multiplier: 2,
			Jitter:     0.1,
		},
		OperationBackoff: BackoffConfig{
			MaxRetries: 20,
			Initial:    50 * time.Millisecond,
			Max:        800 * time.Millisecond,
			Multiplier: 2,
			Jitter:     0.1,
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	q := uniq(t, "q_reconnect_consume")
	t.Cleanup(func() { _ = c.DeleteQueue(context.Background(), q, false, false, true) })
	// AutoDelete must be false here; otherwise the queue may be auto-deleted when the connection drops,
	// making the "reconnect and keep consuming" test invalid.
	if err := c.DeclareQueue(ctx, q, QueueOptions{Durable: false, AutoDelete: false}); err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	received := make(chan string, 10)
	cons := c.StartConsumer(ctx, q, ConsumeOptions{
		AutoAck:          true,
		QosPrefetchCount: 1,
	}, func(ctx context.Context, d amqp.Delivery) error {
		received <- string(d.Body)
		return nil
	})
	t.Cleanup(func() { _ = cons.Close() })

	// Publish first message.
	m1 := "m1"
	if err := c.PublishString(ctx, "", q, m1); err != nil {
		t.Fatalf("publish m1: %v", err)
	}
	select {
	case got := <-received:
		if got != m1 {
			t.Fatalf("expected %q, got %q", m1, got)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting m1")
	}

	// Force disconnect; consumer loop should recover and continue consuming.
	old := c.getConn()
	if old == nil {
		t.Fatalf("expected non-nil conn")
	}
	_ = old.Close()

	// Publish second message; we may need to wait a bit for the consume loop to re-establish.
	m2 := "m2"
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := c.PublishString(ctx, "", q, m2)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("publish m2 after reconnect timed out: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	select {
	case got := <-received:
		if got != m2 {
			t.Fatalf("expected %q, got %q", m2, got)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timeout waiting m2 (consume did not recover?)")
	}
}
