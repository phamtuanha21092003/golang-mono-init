package outbox

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type Relay struct {
	store       *Store
	publisher   Publisher
	logs        Logger
	logger      *zap.Logger
	interval    time.Duration
	batch       int
	maxAttempts int
}

func NewRelay(store *Store, publisher Publisher, logs Logger, logger *zap.Logger, interval time.Duration, batch, maxAttempts int) *Relay {
	if interval <= 0 {
		interval = time.Second
	}
	if batch <= 0 {
		batch = 50
	}
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	if logs == nil {
		logs = NopLogger()
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Relay{
		store:       store,
		publisher:   publisher,
		logs:        logs,
		logger:      logger,
		interval:    interval,
		batch:       batch,
		maxAttempts: maxAttempts,
	}
}

func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	if err := r.Once(ctx); err != nil && ctx.Err() == nil {
		r.logger.Warn("outbox tick failed", zap.Error(err))
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.Once(ctx); err != nil && ctx.Err() == nil {
				r.logger.Warn("outbox tick failed", zap.Error(err))
			}
		}
	}
}

func (r *Relay) Once(ctx context.Context) error {
	events, err := r.store.Claim(ctx, r.batch)
	if err != nil {
		return err
	}
	for i := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.dispatch(ctx, events[i])
	}
	return nil
}

func (r *Relay) dispatch(ctx context.Context, ev Event) {
	err := r.publisher.Publish(ctx, ev)
	if err == nil {
		if markErr := r.store.MarkPublished(ctx, ev.ID); markErr != nil {
			r.logger.Error("mark published failed", zap.Error(markErr), zap.String("id", ev.ID))
		}
		_ = r.logs.Write(ctx, LogEntry{
			OutboxID: ev.ID,
			Broker:   ev.Broker,
			Action:   ActionPublish,
			Status:   StatusPublished,
			Detail: map[string]any{
				"eventType":  ev.EventType,
				"routingKey": ev.RoutingKey,
				"exchange":   ev.Exchange,
			},
		})
		r.logger.Info("outbox published",
			zap.String("id", ev.ID),
			zap.String("broker", ev.Broker),
			zap.String("routingKey", ev.RoutingKey),
		)
		return
	}

	attempts := ev.Attempts + 1
	giveUp := attempts >= r.maxAttempts
	if markErr := r.store.MarkFailed(ctx, ev.ID, attempts, err, giveUp); markErr != nil {
		r.logger.Error("mark failed failed", zap.Error(markErr), zap.String("id", ev.ID))
	}
	status := StatusPending
	if giveUp {
		status = StatusFailed
	}
	_ = r.logs.Write(ctx, LogEntry{
		OutboxID: ev.ID,
		Broker:   ev.Broker,
		Action:   ActionFail,
		Status:   status,
		Detail: map[string]any{
			"eventType":  ev.EventType,
			"routingKey": ev.RoutingKey,
			"error":      err.Error(),
			"attempts":   attempts,
		},
	})
	r.logger.Warn("outbox publish failed",
		zap.Error(err),
		zap.String("id", ev.ID),
		zap.Int("attempts", attempts),
		zap.Bool("giveUp", giveUp),
	)
}
