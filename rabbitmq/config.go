package rabbitmq

import (
	"math"
	"math/rand"
	"time"
)

// Logger is a minimal logger interface used by this library.
// You can pass log.Default() or any logger that implements these methods.
type Logger interface {
	Printf(format string, v ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// BackoffConfig controls exponential backoff retry behavior.
type BackoffConfig struct {
	// MaxRetries limits the retry attempts.
	// 0 means no retries (only one attempt).
	// -1 means retry forever (until context is cancelled or client is closed).
	MaxRetries int
	// Initial is the first backoff duration.
	Initial time.Duration
	// Max is the maximum backoff duration.
	Max time.Duration
	// Multiplier controls exponential growth (e.g. 2.0).
	Multiplier float64
	// Jitter is a factor in [0,1]. 0.2 means +/-20% randomization.
	Jitter float64
}

func (b BackoffConfig) normalized() BackoffConfig {
	out := b
	if out.Initial <= 0 {
		out.Initial = 200 * time.Millisecond
	}
	if out.Max <= 0 {
		out.Max = 5 * time.Second
	}
	if out.Multiplier <= 1 {
		out.Multiplier = 2
	}
	if out.Jitter < 0 {
		out.Jitter = 0
	}
	if out.Jitter > 1 {
		out.Jitter = 1
	}
	// Allow -1 as "infinite retries".
	if out.MaxRetries < -1 {
		out.MaxRetries = -1
	}
	return out
}

func (b BackoffConfig) durationForAttempt(attempt int) time.Duration {
	// attempt: 0 => Initial, 1 => Initial*Multiplier, ...
	b = b.normalized()
	d := float64(b.Initial) * math.Pow(b.Multiplier, float64(attempt))
	if max := float64(b.Max); d > max {
		d = max
	}
	base := time.Duration(d)
	if b.Jitter == 0 {
		return base
	}
	// jitter in [-jitter, +jitter]
	j := (rand.Float64()*2 - 1) * b.Jitter
	jd := float64(base) * (1 + j)
	if jd < 0 {
		jd = 0
	}
	return time.Duration(jd)
}

// Config configures Client.
type Config struct {
	// URL is the AMQP connection URL, e.g. amqp://user:pass@host:5672/vhost
	URL string

	// ReconnectBackoff controls reconnect retries when connection is lost.
	ReconnectBackoff BackoffConfig

	// OperationBackoff controls retries for operations (declare/bind/publish/open-channel).
	OperationBackoff BackoffConfig

	// Logger receives internal reconnect/retry logs. Default is a no-op logger.
	Logger Logger
}

func (c Config) normalized() Config {
	out := c
	if out.Logger == nil {
		// Prefer silence by default in libraries.
		out.Logger = nopLogger{}
	}
	out.ReconnectBackoff = out.ReconnectBackoff.normalized()
	out.OperationBackoff = out.OperationBackoff.normalized()
	return out
}
