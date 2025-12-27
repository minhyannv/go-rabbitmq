package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

// QueueOptions maps to amqp.Channel.QueueDeclare.
type QueueOptions struct {
	// Durable declares a durable queue that will survive broker restarts.
	Durable bool
	// AutoDelete declares an auto-deleted queue that is deleted when the last consumer unsubscribes.
	AutoDelete bool
	// Exclusive declares an exclusive queue that is only accessible by the declaring connection
	// and is deleted when that connection closes.
	Exclusive bool
	// NoWait tells the server not to respond to the declare method.
	NoWait bool
	// Args are optional arguments (x-*) for queue declaration.
	Args amqp.Table
}

// ExchangeOptions maps to amqp.Channel.ExchangeDeclare.
type ExchangeOptions struct {
	// Durable declares a durable exchange that will survive broker restarts.
	Durable bool
	// AutoDelete declares an auto-deleted exchange that is deleted when the last bound
	// queue or exchange is unbound.
	AutoDelete bool
	// Internal declares an internal exchange that cannot be directly published to by clients.
	Internal bool
	// NoWait tells the server not to respond to the declare method.
	NoWait bool
	// Args are optional arguments (x-*) for exchange declaration.
	Args amqp.Table
}

// BindOptions maps to amqp.Channel.QueueBind.
type BindOptions struct {
	// NoWait tells the server not to respond to the bind method.
	NoWait bool
	// Args are optional arguments. For headers exchanges, binding rules are expressed here,
	// e.g. amqp.Table{"x-match":"all","format":"pdf"}.
	Args amqp.Table
}

func defaultQueueOptions() QueueOptions {
	return QueueOptions{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
}

func defaultExchangeOptions() ExchangeOptions {
	return ExchangeOptions{
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
}

func defaultBindOptions() BindOptions {
	return BindOptions{
		NoWait: false,
		Args:   nil,
	}
}

// DeclareQueue declares a queue.
func (c *Client) DeclareQueue(ctx context.Context, name string, opt QueueOptions) error {
	return c.withChannel(ctx, func(ch *amqp.Channel) error {
		_, err := ch.QueueDeclare(name, opt.Durable, opt.AutoDelete, opt.Exclusive, opt.NoWait, opt.Args)
		return err
	})
}

// DeleteQueue deletes a queue.
func (c *Client) DeleteQueue(ctx context.Context, name string, ifUnused, ifEmpty, noWait bool) error {
	return c.withChannel(ctx, func(ch *amqp.Channel) error {
		_, err := ch.QueueDelete(name, ifUnused, ifEmpty, noWait)
		return err
	})
}

// PurgeQueue purges a queue.
func (c *Client) PurgeQueue(ctx context.Context, name string, noWait bool) error {
	return c.withChannel(ctx, func(ch *amqp.Channel) error {
		_, err := ch.QueuePurge(name, noWait)
		return err
	})
}

// QueueInspect returns queue state.
func (c *Client) QueueInspect(ctx context.Context, name string) (amqp.Queue, error) {
	var q amqp.Queue
	err := c.withChannel(ctx, func(ch *amqp.Channel) error {
		var err error
		q, err = ch.QueueInspect(name)
		return err
	})
	return q, err
}

// DeclareExchange declares an exchange with type "direct", "fanout", "topic", or "headers".
func (c *Client) DeclareExchange(ctx context.Context, name, kind string, opt ExchangeOptions) error {
	return c.withChannel(ctx, func(ch *amqp.Channel) error {
		return ch.ExchangeDeclare(name, kind, opt.Durable, opt.AutoDelete, opt.Internal, opt.NoWait, opt.Args)
	})
}

// DeleteExchange deletes an exchange.
func (c *Client) DeleteExchange(ctx context.Context, name string, ifUnused, noWait bool) error {
	return c.withChannel(ctx, func(ch *amqp.Channel) error {
		return ch.ExchangeDelete(name, ifUnused, noWait)
	})
}

// BindQueue binds a queue to an exchange.
func (c *Client) BindQueue(ctx context.Context, queue, routingKey, exchange string, opt BindOptions) error {
	return c.withChannel(ctx, func(ch *amqp.Channel) error {
		return ch.QueueBind(queue, routingKey, exchange, opt.NoWait, opt.Args)
	})
}

// Get performs a synchronous basic.get on a queue.
// ok=false means the queue was empty at the time of the call.
func (c *Client) Get(ctx context.Context, queue string, autoAck bool) (d amqp.Delivery, ok bool, err error) {
	err = c.withChannel(ctx, func(ch *amqp.Channel) error {
		var e error
		d, ok, e = ch.Get(queue, autoAck)
		return e
	})
	return d, ok, err
}

// GetString polls Get until a message arrives or the context is done.
// This is primarily useful for tests and demos.
func (c *Client) GetString(ctx context.Context, queue string, autoAck bool, pollInterval time.Duration) (string, error) {
	if pollInterval <= 0 {
		pollInterval = 50 * time.Millisecond
	}
	t := time.NewTicker(pollInterval)
	defer t.Stop()

	for {
		d, ok, err := c.Get(ctx, queue, autoAck)
		if err != nil {
			return "", err
		}
		if ok {
			return string(d.Body), nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("rabbitmq: get cancelled: %w", ctx.Err())
		case <-t.C:
		}
	}
}
