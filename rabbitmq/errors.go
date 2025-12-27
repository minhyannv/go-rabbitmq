package rabbitmq

import "errors"

var (
	// ErrClosed is returned when the client or consumer is closed.
	ErrClosed = errors.New("rabbitmq: closed")
)
