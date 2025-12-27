package rabbitmq

import (
	"context"

	"github.com/streadway/amqp"
)

// PublishOptions controls message properties and publishing behavior.
type PublishOptions struct {
	// Mandatory returns unroutable messages to the client (requires Return handler on channel).
	// If false, unroutable messages are dropped by the broker.
	Mandatory bool
	// Immediate requests immediate delivery to a consumer (RabbitMQ does not implement it and will reject).
	Immediate bool
	// Msg is the message payload and properties (Body, Headers, ContentType, DeliveryMode, etc.).
	Msg amqp.Publishing
}

func defaultPublishOptions() PublishOptions {
	return PublishOptions{
		Mandatory: false,
		Immediate: false,
		Msg: amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
		},
	}
}

// Publish publishes a message.
func (c *Client) Publish(ctx context.Context, exchange, routingKey string, opt PublishOptions) error {
	return c.withChannel(ctx, func(ch *amqp.Channel) error {
		return ch.Publish(exchange, routingKey, opt.Mandatory, opt.Immediate, opt.Msg)
	})
}

// PublishString is a convenience method for publishing a string body.
func (c *Client) PublishString(ctx context.Context, exchange, routingKey, body string) error {
	opt := defaultPublishOptions()
	opt.Msg.Body = []byte(body)
	return c.Publish(ctx, exchange, routingKey, opt)
}
