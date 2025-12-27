package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

type dialFunc func(url string) (*amqp.Connection, error)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Client is a resilient RabbitMQ client with auto-reconnect and operation retries.
type Client struct {
	cfg Config

	dial dialFunc

	closed atomic.Bool

	connMu sync.RWMutex
	conn   *amqp.Connection

	connectMu sync.Mutex

	// notifyClose is replaced on each (re)connect.
	notifyCloseMu sync.Mutex
	notifyClose   chan *amqp.Error

	// wg tracks internal goroutines.
	wg sync.WaitGroup
}

// New creates a client and connects immediately.
func New(ctx context.Context, cfg Config) (*Client, error) {
	c := &Client{
		cfg:  cfg.normalized(),
		dial: amqp.Dial,
	}
	if err := c.Connect(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Connect establishes an AMQP connection (or returns nil if already connected).
// It also installs a NotifyClose watcher to trigger auto-reconnect.
func (c *Client) Connect(ctx context.Context) error {
	if c.closed.Load() {
		return ErrClosed
	}
	// Fast path.
	if conn := c.getConn(); conn != nil && !conn.IsClosed() {
		return nil
	}

	c.connectMu.Lock()
	defer c.connectMu.Unlock()

	// Re-check under lock.
	if conn := c.getConn(); conn != nil && !conn.IsClosed() {
		return nil
	}

	var lastErr error
	for attempt := 0; ; attempt++ {
		if c.closed.Load() {
			return ErrClosed
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("rabbitmq: connect cancelled: %w (last error: %v)", ctx.Err(), lastErr)
			}
			return fmt.Errorf("rabbitmq: connect cancelled: %w", ctx.Err())
		default:
		}

		conn, err := c.dial(c.cfg.URL)
		if err == nil {
			c.setConn(conn)
			c.installCloseWatcher(conn)
			c.cfg.Logger.Printf("rabbitmq: connected")
			return nil
		}
		lastErr = err
		if c.cfg.ReconnectBackoff.MaxRetries >= 0 && attempt >= c.cfg.ReconnectBackoff.MaxRetries {
			return fmt.Errorf("rabbitmq: connect failed after %d attempts: %w", attempt+1, err)
		}
		sleep := c.cfg.ReconnectBackoff.durationForAttempt(attempt)
		c.cfg.Logger.Printf("rabbitmq: connect failed (attempt=%d): %v; retry in %s", attempt+1, err, sleep)
		if err := sleepContext(ctx, sleep); err != nil {
			return fmt.Errorf("rabbitmq: connect cancelled: %w (last error: %v)", err, lastErr)
		}
	}
}

func (c *Client) getConn() *amqp.Connection {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

func (c *Client) setConn(conn *amqp.Connection) {
	c.connMu.Lock()
	old := c.conn
	c.conn = conn
	c.connMu.Unlock()
	if old != nil && !old.IsClosed() {
		_ = old.Close()
	}
}

func (c *Client) installCloseWatcher(conn *amqp.Connection) {
	ch := make(chan *amqp.Error, 1)
	conn.NotifyClose(ch)

	c.notifyCloseMu.Lock()
	c.notifyClose = ch
	c.notifyCloseMu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		err := <-ch
		if err == nil {
			return
		}
		if c.closed.Load() {
			return
		}
		c.cfg.Logger.Printf("rabbitmq: connection closed: %v; reconnecting", err)
		// Best-effort reconnect in background; operations will also call Connect when needed.
		_ = c.Connect(context.Background())
	}()
}

// Close closes the client and underlying connection.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	conn := c.getConn()
	if conn != nil && !conn.IsClosed() {
		_ = conn.Close()
	}
	c.wg.Wait()
	return nil
}

func (c *Client) withChannel(ctx context.Context, fn func(ch *amqp.Channel) error) error {
	if c.closed.Load() {
		return ErrClosed
	}

	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := c.Connect(ctx); err != nil {
			lastErr = err
		} else {
			conn := c.getConn()
			if conn == nil || conn.IsClosed() {
				lastErr = errors.New("rabbitmq: no active connection")
			} else {
				ch, err := conn.Channel()
				if err == nil {
					err = fn(ch)
					_ = ch.Close()
					if err == nil {
						return nil
					}
					lastErr = err
				} else {
					lastErr = err
				}
			}
		}

		// Retry on transient/connection issues.
		if c.closed.Load() {
			return ErrClosed
		}
		if c.cfg.OperationBackoff.MaxRetries >= 0 && attempt >= c.cfg.OperationBackoff.MaxRetries {
			return fmt.Errorf("rabbitmq: operation failed after %d attempts: %w", attempt+1, lastErr)
		}
		sleep := c.cfg.OperationBackoff.durationForAttempt(attempt)
		c.cfg.Logger.Printf("rabbitmq: operation failed (attempt=%d): %v; retry in %s", attempt+1, lastErr, sleep)
		if err := sleepContext(ctx, sleep); err != nil {
			return fmt.Errorf("rabbitmq: operation cancelled: %w (last error: %v)", err, lastErr)
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
