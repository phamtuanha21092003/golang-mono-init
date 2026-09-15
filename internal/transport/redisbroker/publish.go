package redisbroker

import (
	"context"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func (b *Broker) Publish(ctx context.Context, stream string, payload any) (string, error) {
	return b.PublishWith(ctx, stream, payload, PublishOptions{})
}

func (b *Broker) PublishWith(ctx context.Context, stream string, payload any, opts PublishOptions) (string, error) {
	if stream == "" {
		return "", errors.New("stream is required")
	}
	body, err := marshalPayload(payload)
	if err != nil {
		return "", err
	}
	return b.enqueue(ctx, stream, body, 0, "", opts)
}

func (b *Broker) enqueue(ctx context.Context, stream string, body []byte, attempt int, reason string, opts PublishOptions) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	values := map[string]any{
		fieldBody:    string(body),
		fieldAttempt: attempt,
	}
	if reason != "" {
		values[fieldError] = reason
	}
	args := &redis.XAddArgs{
		Stream: stream,
		Values: values,
		MaxLen: opts.MaxLen,
		Approx: opts.Approx,
	}
	id, err := b.rdb.XAdd(ctx, args).Result()
	if err != nil {
		return "", errors.Wrap(err, "redis xadd")
	}
	b.logger.Info("redis published",
		zap.String("stream", stream),
		zap.String("id", id),
	)
	return id, nil
}
