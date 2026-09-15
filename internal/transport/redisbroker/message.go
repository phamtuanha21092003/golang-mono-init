package redisbroker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
)

const (
	fieldBody    = "body"
	fieldAttempt = "attempt"
	fieldError   = "error"
)

var (
	ErrClosed = errors.New("redis broker closed")
	// ErrDrop sends the message to the dead-letter stream without another attempt.
	ErrDrop = errors.New("drop redis message")
)

type Message struct {
	ID      string
	Stream  string
	Body    []byte
	Attempt int
}

type Handler func(ctx context.Context, msg Message) error

type PublishOptions struct {
	MaxLen int64
	Approx bool
}

type ConsumeOptions struct {
	Stream      string
	Group       string
	Consumer    string
	DLQStream   string
	MaxAttempts int
	Count       int64
	Block       time.Duration
	ClaimIdle   time.Duration
	// HandlerTimeout bounds one handler call. 0 uses 30s. Negative disables the timeout.
	// Shutdown does not cancel the handler context, so a deploy does not look like a failure.
	HandlerTimeout time.Duration
}

func normalizeConsumeOptions(opts ConsumeOptions) (ConsumeOptions, error) {
	if opts.Stream == "" {
		return opts, errors.New("stream is required")
	}
	if opts.Group == "" {
		return opts, errors.New("group is required")
	}
	if opts.Consumer == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "local"
		}
		opts.Consumer = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if opts.DLQStream == "" {
		opts.DLQStream = opts.Stream + ".dlq"
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}
	if opts.Count <= 0 {
		opts.Count = 10
	}
	if opts.Block <= 0 {
		opts.Block = 5 * time.Second
	}
	if opts.ClaimIdle <= 0 {
		opts.ClaimIdle = 30 * time.Second
	}
	if opts.HandlerTimeout == 0 {
		opts.HandlerTimeout = 30 * time.Second
	} else if opts.HandlerTimeout < 0 {
		opts.HandlerTimeout = 0
	}
	return opts, nil
}

func marshalPayload(payload any) ([]byte, error) {
	switch value := payload.(type) {
	case nil:
		return nil, errors.New("payload is nil")
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	default:
		body, err := json.Marshal(value)
		if err != nil {
			return nil, errors.Wrap(err, "marshal payload")
		}
		return body, nil
	}
}
