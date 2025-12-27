package rabbitmq

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

// ConsumeOptions controls consumer behavior.
type ConsumeOptions struct {
	// ConsumerTag is an identifier for the consumer; empty means server-generated.
	ConsumerTag string
	// AutoAck enables automatic acknowledgements. If true, the server will consider messages
	// acknowledged immediately after delivery (handler errors cannot trigger requeue).
	AutoAck bool
	// Exclusive makes the consumer exclusive to this connection.
	Exclusive bool
	// NoLocal is not supported by RabbitMQ and should be left false.
	NoLocal bool
	// NoWait tells the server not to respond to the consume method.
	NoWait bool
	// Args are optional arguments (x-*) for consumer creation.
	Args amqp.Table

	// QosPrefetchCount sets QoS prefetch count (0 means keep default).
	QosPrefetchCount int
	// QosPrefetchSize sets QoS prefetch size in bytes (0 means keep default).
	QosPrefetchSize int
	// QosGlobal applies QoS settings to the connection if true.
	QosGlobal bool

	// RequeueOnError controls whether to requeue message when handler returns error.
	// Only applies when AutoAck is false.
	RequeueOnError bool

	// RetryBackoff overrides client's OperationBackoff for the consume loop (0 values mean use client setting).
	RetryBackoff *BackoffConfig
}

func (o ConsumeOptions) normalized(c *Client) ConsumeOptions {
	out := o
	if out.RetryBackoff == nil {
		out.RetryBackoff = &c.cfg.OperationBackoff
	} else {
		n := out.RetryBackoff.normalized()
		out.RetryBackoff = &n
	}
	return out
}

// Handler processes one delivery.
// If AutoAck=false, returning nil will Ack. Returning error will Nack (requeue depends on options).
type Handler func(ctx context.Context, d amqp.Delivery) error

// Consume runs a consuming loop until ctx is cancelled. It automatically retries on connection loss.
func (c *Client) Consume(ctx context.Context, queue string, opt ConsumeOptions, handler Handler) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if handler == nil {
		return fmt.Errorf("rabbitmq: handler is nil")
	}

	opt = opt.normalized(c)

	var lastErr error
	for attempt := 0; ; attempt++ {
		if c.closed.Load() {
			return ErrClosed
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("rabbitmq: consume cancelled: %w (last error: %v)", ctx.Err(), lastErr)
			}
			return ctx.Err()
		default:
		}

		err := c.consumeOnce(ctx, queue, opt, handler)
		if err == nil {
			// consumeOnce only returns nil on graceful shutdown; treat as ctx cancellation.
			return nil
		}
		lastErr = err

		// Retry loop
		if opt.RetryBackoff.MaxRetries >= 0 && attempt >= opt.RetryBackoff.MaxRetries {
			return fmt.Errorf("rabbitmq: consume failed after %d attempts: %w", attempt+1, lastErr)
		}
		sleep := opt.RetryBackoff.durationForAttempt(attempt)
		c.cfg.Logger.Printf("rabbitmq: consume failed (attempt=%d): %v; retry in %s", attempt+1, lastErr, sleep)
		if err := sleepContext(ctx, sleep); err != nil {
			return fmt.Errorf("rabbitmq: consume cancelled: %w (last error: %v)", err, lastErr)
		}
	}
}

func (c *Client) consumeOnce(ctx context.Context, queue string, opt ConsumeOptions, handler Handler) error {
	// Ensure connected first (with retries).
	if err := c.Connect(ctx); err != nil {
		return err
	}
	conn := c.getConn()
	if conn == nil || conn.IsClosed() {
		return fmt.Errorf("rabbitmq: no active connection")
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	if opt.QosPrefetchCount != 0 || opt.QosPrefetchSize != 0 {
		if err := ch.Qos(opt.QosPrefetchCount, opt.QosPrefetchSize, opt.QosGlobal); err != nil {
			return err
		}
	}

	deliveries, err := ch.Consume(queue, opt.ConsumerTag, opt.AutoAck, opt.Exclusive, opt.NoLocal, opt.NoWait, opt.Args)
	if err != nil {
		return err
	}

	// Detect channel close.
	notify := make(chan *amqp.Error, 1)
	ch.NotifyClose(notify)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case cerr := <-notify:
			if cerr == nil {
				return fmt.Errorf("rabbitmq: channel closed")
			}
			return cerr
		case d, ok := <-deliveries:
			if !ok {
				// server canceled consumer or channel closed.
				// Wait a tiny bit to allow NotifyClose to arrive; otherwise return a generic error.
				select {
				case cerr := <-notify:
					if cerr != nil {
						return cerr
					}
				case <-time.After(50 * time.Millisecond):
				}
				return fmt.Errorf("rabbitmq: deliveries channel closed")
			}

			if opt.AutoAck {
				_ = handler(ctx, d)
				continue
			}

			if err := handler(ctx, d); err != nil {
				// Best-effort nack.
				_ = d.Nack(false, opt.RequeueOnError)
				continue
			}
			_ = d.Ack(false)
		}
	}
}

// Consumer is a helper to run Consume in a goroutine.
type Consumer struct {
	done   chan struct{}
	cancel context.CancelFunc
	err    atomic.Value // stores error
}

func (c *Consumer) Done() <-chan struct{} { return c.done }

func (c *Consumer) Err() error {
	v := c.err.Load()
	if v == nil {
		return nil
	}
	return v.(error)
}

func (c *Consumer) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	return nil
}

// StartConsumer starts a consumer loop in background. Cancel the context to stop it.
func (c *Client) StartConsumer(ctx context.Context, queue string, opt ConsumeOptions, handler Handler) *Consumer {
	ctx2, cancel := context.WithCancel(ctx)
	cons := &Consumer{done: make(chan struct{}), cancel: cancel}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(cons.done)
		err := c.Consume(ctx2, queue, opt, handler)
		cons.err.Store(err)
	}()
	return cons
}
