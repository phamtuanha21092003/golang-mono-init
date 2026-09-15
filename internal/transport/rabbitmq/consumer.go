package rabbitmq

import (
	"context"
	"strconv"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"go-init/internal/base"
)

type Message struct {
	Body       []byte
	Headers    amqp.Table
	RoutingKey string
	MessageID  string
	RetryCount int
}

type Handler func(ctx context.Context, msg Message) error

type ConsumeOptions struct {
	Queue        string
	ConsumerTag  string
	Exchange     string
	BindingKey   string
	ExchangeKind string
	Prefetch     int
	MaxRetry     int
	// Args must match an existing queue's arguments (e.g. DLX / x-max-priority) or QueueDeclare fails.
	Args amqp.Table

	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	// HandlerTimeout bounds a single handler call. Shutdown ctx is stripped so
	// canceling the consumer does not look like a handler failure. 0 = no timeout.
	HandlerTimeout time.Duration
}

type Consumer struct {
	Options ConsumeOptions
	Handler Handler
}

// NewConsumeOptions builds a durable classic queue with DLX/DLQ named {queue}.dlx / {queue}.dlq.
// maxPriority > 0 sets x-max-priority (classic queues only; do not combine with NewQuorumConsumeOptions).
func NewConsumeOptions(queue string, maxPriority int32) ConsumeOptions {
	args := amqp.Table{
		"x-dead-letter-exchange":    queue + ".dlx",
		"x-dead-letter-routing-key": queue + ".dlq",
	}
	if maxPriority > 0 {
		args["x-max-priority"] = maxPriority
	}
	return ConsumeOptions{Queue: queue, Args: args}
}

// NewQuorumConsumeOptions builds a quorum queue with the same DLX/DLQ naming as NewConsumeOptions.
// Quorum queues are replicated; they do not use x-max-priority.
func NewQuorumConsumeOptions(queue string) ConsumeOptions {
	return ConsumeOptions{
		Queue: queue,
		Args: amqp.Table{
			amqp.QueueTypeArg:           amqp.QueueTypeQuorum,
			"x-dead-letter-exchange":    queue + ".dlx",
			"x-dead-letter-routing-key": queue + ".dlq",
		},
	}
}

func (r *RabbitMQAdapter) StartConsume(ctx context.Context, opts ConsumeOptions, handler Handler) {
	r.consumeWg.Add(1)
	go func() {
		defer r.consumeWg.Done()
		if err := r.Consume(ctx, opts, handler); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
			r.logger.Error("consumer exited", zap.Error(err), zap.String("queue", opts.Queue))
		}
	}()
}

func (r *RabbitMQAdapter) Consume(ctx context.Context, opts ConsumeOptions, handler Handler) error {
	if opts.Queue == "" {
		return errors.New("queue is required")
	}
	if handler == nil {
		return errors.New("handler is required")
	}
	opts = normalizeConsumeOptions(opts)
	if opts.ConsumerTag == "" {
		opts.ConsumerTag = r.consumerTag(opts.Queue)
	}

	backoff := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-r.closed:
			return ErrClosed
		default:
		}

		err := r.consumeOnce(ctx, opts, handler)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, ErrClosed) {
			return err
		}
		if err == nil {
			err = errors.New("consume stopped unexpectedly")
		}

		r.logger.Warn("consumer restarting", zap.String("queue", opts.Queue), zap.Error(err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.closed:
			return ErrClosed
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (r *RabbitMQAdapter) consumeOnce(ctx context.Context, opts ConsumeOptions, handler Handler) error {
	ch, err := r.openChannel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := setupConsumeTopology(ch, opts); err != nil {
		return err
	}

	deliveries, err := ch.Consume(
		opts.Queue,
		opts.ConsumerTag,
		base.ConsumeAutoAck,
		base.ConsumeExclusive,
		base.ConsumeNoLocal,
		base.ConsumeNoWait,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "consume")
	}

	conn, err := r.connection()
	if err != nil {
		return err
	}
	connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))
	chClosed := ch.NotifyClose(make(chan *amqp.Error, 1))

	r.logger.Info("consumer started", zap.String("queue", opts.Queue), zap.String("tag", opts.ConsumerTag))

	for {
		select {
		case <-ctx.Done():
			_ = ch.Cancel(opts.ConsumerTag, false)
			return ctx.Err()
		case <-r.closed:
			_ = ch.Cancel(opts.ConsumerTag, false)
			return ErrClosed
		case amqpErr := <-connClosed:
			if amqpErr == nil {
				return errors.New("connection closed")
			}
			return errors.Wrap(amqpErr, "connection closed")
		case amqpErr := <-chClosed:
			if amqpErr == nil {
				return errors.New("channel closed")
			}
			return errors.Wrap(amqpErr, "channel closed")
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			if err := r.handleDelivery(ctx, opts, handler, d); err != nil {
				r.logger.Error("handle delivery", zap.Error(err), zap.String("queue", opts.Queue), zap.String("messageId", d.MessageId))
			}
		}
	}
}

func (r *RabbitMQAdapter) handleDelivery(ctx context.Context, opts ConsumeOptions, handler Handler, d amqp.Delivery) error {
	msg := Message{
		Body:       d.Body,
		Headers:    d.Headers,
		RoutingKey: d.RoutingKey,
		MessageID:  d.MessageId,
		RetryCount: retryCountFromHeaders(d.Headers),
	}

	handlerCtx, cancel := handlerContext(ctx, opts.HandlerTimeout)
	defer cancel()

	err := handler(handlerCtx, msg)
	if err == nil {
		return d.Ack(false)
	}
	if msg.RetryCount >= opts.MaxRetry {
		r.logger.Error("dropping message after max retry", zap.Error(err), zap.Int("retry", msg.RetryCount), zap.String("queue", opts.Queue))
		return d.Nack(false, false)
	}
	if pubErr := r.scheduleRetry(opts, d, msg.RetryCount+1); pubErr != nil {
		return d.Nack(false, true)
	}
	return d.Ack(false)
}

func (r *RabbitMQAdapter) scheduleRetry(opts ConsumeOptions, d amqp.Delivery, nextRetry int) error {
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers[base.RetryCountHeader] = strconv.Itoa(nextRetry)

	delay := computeRetryDelay(nextRetry, opts.RetryBaseDelay, opts.RetryMaxDelay)

	ctx, cancel := context.WithTimeout(context.Background(), defaultPublishTimeout)
	defer cancel()

	return r.publishConfirmed(ctx, "", retryQueueName(opts.Queue), false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Timestamp:    time.Now(),
		Body:         d.Body,
		Headers:      headers,
		Priority:     d.Priority,
		ContentType:  d.ContentType,
		Expiration:   strconv.FormatInt(delay.Milliseconds(), 10),
	})
}

func (r *RabbitMQAdapter) consumerTag(queue string) string {
	id := newID()
	if r.appName == "" {
		return queue + "-" + id
	}
	return r.appName + "-" + queue + "-" + id
}

func handlerContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	baseCtx := context.WithoutCancel(parent)
	if timeout > 0 {
		return context.WithTimeout(baseCtx, timeout)
	}
	return context.WithCancel(baseCtx)
}

func computeRetryDelay(retryCount int, baseDelay, maxDelay time.Duration) time.Duration {
	if retryCount <= 0 {
		return baseDelay
	}
	delay := baseDelay << uint(retryCount-1)
	if delay > maxDelay || delay <= 0 {
		delay = maxDelay
	}
	return delay
}

func normalizeConsumeOptions(opts ConsumeOptions) ConsumeOptions {
	if opts.Prefetch <= 0 {
		opts.Prefetch = base.PrefetchCount
	}
	if opts.MaxRetry <= 0 {
		opts.MaxRetry = 5
	}
	if opts.BindingKey == "" {
		opts.BindingKey = opts.Queue
	}
	if opts.ExchangeKind == "" {
		opts.ExchangeKind = base.ExchangeKindDirect
	}
	if opts.RetryBaseDelay <= 0 {
		opts.RetryBaseDelay = 5 * time.Second
	}
	if opts.RetryMaxDelay <= 0 {
		opts.RetryMaxDelay = 2 * time.Minute
	}
	return opts
}

func retryCountFromHeaders(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	raw, ok := headers[base.RetryCountHeader]
	if !ok || raw == nil {
		return 0
	}
	switch v := raw.(type) {
	case string:
		n, _ := strconv.Atoi(v)
		return n
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	default:
		return 0
	}
}
