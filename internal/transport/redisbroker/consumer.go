package redisbroker

import (
	"context"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func (b *Broker) StartConsume(ctx context.Context, opts ConsumeOptions, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-b.closed:
		return
	default:
	}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		if err := b.Consume(ctx, opts, handler); err != nil &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, ErrClosed) {
			b.logger.Error("redis consumer exited", zap.Error(err), zap.String("stream", opts.Stream))
		}
	}()
}

func (b *Broker) Consume(ctx context.Context, opts ConsumeOptions, handler Handler) error {
	if handler == nil {
		return errors.New("handler is required")
	}
	opts, err := normalizeConsumeOptions(opts)
	if err != nil {
		return err
	}
	if err := b.EnsureGroup(ctx, opts.Stream, opts.Group); err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-b.closed:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		messages, err := b.next(ctx, opts)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			b.logger.Warn("redis read failed", zap.Error(err), zap.String("stream", opts.Stream))
			if !sleep(ctx, time.Second) {
				return ctx.Err()
			}
			continue
		}
		for _, message := range messages {
			if err := b.handle(ctx, opts, handler, message); err != nil {
				b.logger.Error("redis handle failed", zap.Error(err), zap.String("id", message.ID))
			}
		}
	}
}

func (b *Broker) next(ctx context.Context, opts ConsumeOptions) ([]Message, error) {
	claimed, err := b.claim(ctx, opts)
	if err != nil && ctx.Err() == nil && !errors.Is(err, redis.Nil) {
		if isUnsupported(err) {
			b.claimOff.Store(true)
		} else {
			b.logger.Warn("redis claim skipped", zap.Error(err), zap.String("stream", opts.Stream))
		}
		claimed = nil
	}
	if len(claimed) > 0 {
		return claimed, nil
	}
	return b.readNew(ctx, opts)
}

func (b *Broker) claim(ctx context.Context, opts ConsumeOptions) ([]Message, error) {
	if b.claimOff.Load() {
		return nil, nil
	}
	raw, _, err := b.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   opts.Stream,
		Group:    opts.Group,
		Consumer: opts.Consumer,
		MinIdle:  opts.ClaimIdle,
		Start:    "0-0",
		Count:    opts.Count,
	}).Result()
	if err != nil {
		return nil, err
	}
	return decodeAll(opts.Stream, raw), nil
}

func (b *Broker) readNew(ctx context.Context, opts ConsumeOptions) ([]Message, error) {
	streams, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    opts.Group,
		Consumer: opts.Consumer,
		Streams:  []string{opts.Stream, ">"},
		Count:    opts.Count,
		Block:    opts.Block,
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(streams) == 0 {
		return nil, nil
	}
	return decodeAll(opts.Stream, streams[0].Messages), nil
}

func (b *Broker) handle(ctx context.Context, opts ConsumeOptions, handler Handler, msg Message) error {
	hctx := context.WithoutCancel(ctx)
	if opts.HandlerTimeout > 0 {
		var cancel context.CancelFunc
		hctx, cancel = context.WithTimeout(hctx, opts.HandlerTimeout)
		defer cancel()
	}

	err := handler(hctx, msg)
	// Ack after the handler even if the consumer is shutting down, so a finished
	// message is not delivered again on the next start.
	opCtx, opCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer opCancel()
	if err != nil {
		return b.fail(opCtx, opts, msg, err)
	}
	return b.ack(opCtx, opts, msg.ID)
}

func (b *Broker) fail(ctx context.Context, opts ConsumeOptions, msg Message, cause error) error {
	b.logger.Warn("redis handler failed",
		zap.Error(cause),
		zap.String("id", msg.ID),
		zap.Int("attempt", msg.Attempt),
	)
	next := msg.Attempt + 1
	target := opts.Stream
	if errors.Is(cause, ErrDrop) || next >= opts.MaxAttempts {
		target = opts.DLQStream
	}
	if _, err := b.enqueue(ctx, target, msg.Body, next, cause.Error(), PublishOptions{}); err != nil {
		return err
	}
	return b.ack(ctx, opts, msg.ID)
}

func (b *Broker) ack(ctx context.Context, opts ConsumeOptions, id string) error {
	if err := b.rdb.XAck(ctx, opts.Stream, opts.Group, id).Err(); err != nil {
		return errors.Wrap(err, "redis xack")
	}
	return nil
}

func decodeAll(stream string, raw []redis.XMessage) []Message {
	out := make([]Message, 0, len(raw))
	for _, item := range raw {
		out = append(out, decodeMessage(stream, item))
	}
	return out
}

func decodeMessage(stream string, raw redis.XMessage) Message {
	return Message{
		ID:      raw.ID,
		Stream:  stream,
		Body:    bodyOf(raw.Values),
		Attempt: attemptOf(raw.Values),
	}
}

func bodyOf(values map[string]any) []byte {
	switch value := values[fieldBody].(type) {
	case string:
		return []byte(value)
	case []byte:
		return value
	default:
		return nil
	}
}

func attemptOf(values map[string]any) int {
	switch value := values[fieldAttempt].(type) {
	case string:
		number, _ := strconv.Atoi(value)
		return number
	case int64:
		return int(value)
	case int:
		return value
	default:
		return 0
	}
}

func sleep(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
