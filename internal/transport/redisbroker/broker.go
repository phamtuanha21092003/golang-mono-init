package redisbroker

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Broker is a Redis Streams consumer group. Delivery is at-least-once:
// a crash between requeue and ack can deliver the same payload twice.
type Broker struct {
	rdb      *redis.Client
	logger   *zap.Logger
	wg       sync.WaitGroup
	mu       sync.Mutex
	closed   chan struct{}
	closeOne sync.Once
	claimOff atomic.Bool
}

func New(client *redis.Client, logger *zap.Logger) *Broker {
	if client == nil {
		panic("redis client is nil")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Broker{
		rdb:    client,
		logger: logger,
		closed: make(chan struct{}),
	}
}

func (b *Broker) EnsureGroup(ctx context.Context, stream, group string) error {
	if stream == "" || group == "" {
		return errors.New("stream and group are required")
	}
	err := b.rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err == nil || isBusyGroup(err) {
		return nil
	}
	return errors.Wrap(err, "create redis consumer group")
}

func (b *Broker) Close(ctx context.Context) error {
	b.closeOne.Do(func() { close(b.closed) })
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

func isUnsupported(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unknown command") || strings.Contains(message, "not supported")
}
