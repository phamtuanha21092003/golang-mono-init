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

	RetryBaseDelay time.Duration // delay for the 1st retry, doubles each attempt
	RetryMaxDelay  time.Duration // cap for backoff
}

type Consumer struct {
	Options ConsumeOptions
	Handler Handler
}

// durable queue with x-max-priority=10 and DLX/DLQ named {queue}.dlx / {queue}.dlq.
func NewConsumeOptions(queue string, maxPriority int32) ConsumeOptions {
	return ConsumeOptions{
		Queue: queue,
		Args: amqp.Table{
			"x-dead-letter-exchange":    queue + ".dlx",
			"x-dead-letter-routing-key": queue + ".dlq",
			"x-max-priority":            int32(maxPriority),
		},
	}
}

func (r *RabbitMQAdapter) StartConsume(ctx context.Context, opts ConsumeOptions, handler Handler) {
	go func() {
		if err := r.Consume(ctx, opts, handler); err != nil && !errors.Is(err, context.Canceled) {
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

	backoff := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := r.consumeOnce(ctx, opts, handler)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			err = errors.New("consume stopped unexpectedly")
		}

		r.logger.Warn("consumer restarting", zap.String("queue", opts.Queue), zap.Error(err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (r *RabbitMQAdapter) consumeOnce(ctx context.Context, opts ConsumeOptions, handler Handler) error {
	if err := r.ensureConnection(); err != nil {
		return err
	}

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

	connClosed := r.mqConn.NotifyClose(make(chan *amqp.Error, 1))
	chClosed := ch.NotifyClose(make(chan *amqp.Error, 1))

	r.logger.Info("consumer started", zap.String("queue", opts.Queue), zap.String("tag", opts.ConsumerTag))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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

	err := handler(ctx, msg)
	if err == nil {
		return d.Ack(false)
	}
	if msg.RetryCount >= opts.MaxRetry {
		r.logger.Error("dropping message after max retry", zap.Error(err), zap.Int("retry", msg.RetryCount), zap.String("queue", opts.Queue))
		return d.Nack(false, false) // requeue=false → routes to DLX
	}
	if pubErr := r.scheduleRetry(opts, d, msg.RetryCount+1); pubErr != nil {
		return d.Nack(false, true) // requeue on our own publish failure, don't burn a retry
	}
	return d.Ack(false)
}

// scheduleRetry publishes the message into the {queue}.retry queue with a
// per-message TTL that grows with each attempt (exponential backoff, capped
// at opts.RetryMaxDelay). Uses the adapter's dedicated publish channel
// (not the consume channel) so a publish-side error never kills the consumer.
func (r *RabbitMQAdapter) scheduleRetry(opts ConsumeOptions, d amqp.Delivery, nextRetry int) error {
	ch, err := r.getPublishChannel()
	if err != nil {
		return errors.Wrap(err, "get publish channel for retry")
	}

	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers[base.RetryCountHeader] = strconv.Itoa(nextRetry)

	delay := computeRetryDelay(nextRetry, opts.RetryBaseDelay, opts.RetryMaxDelay)

	return ch.PublishWithContext(context.Background(), "", retryQueueName(opts.Queue),
		base.PublishMandatory, base.PublishImmediate,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			MessageId:    d.MessageId,
			Timestamp:    time.Now(),
			Body:         d.Body,
			Headers:      headers,
			Priority:     d.Priority,
			ContentType:  d.ContentType,
			Expiration:   strconv.FormatInt(delay.Milliseconds(), 10),
		},
	)
}

func computeRetryDelay(retryCount int, base, max time.Duration) time.Duration {
	delay := base << uint(retryCount-1) // base * 2^(retryCount-1)
	if delay > max || delay <= 0 {      // guard overflow too
		delay = max
	}
	return delay
}

func republishWithRetry(ch *amqp.Channel, opts ConsumeOptions, d amqp.Delivery, nextRetry int) error {
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers[base.RetryCountHeader] = strconv.Itoa(nextRetry)

	exchange := opts.Exchange
	routingKey := opts.BindingKey
	if exchange == "" {
		routingKey = opts.Queue
	}

	return ch.PublishWithContext(context.Background(), exchange, routingKey, base.PublishMandatory, base.PublishImmediate, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Timestamp:    time.Now(),
		Body:         d.Body,
		Headers:      headers,
		Priority:     d.Priority,
		ContentType:  d.ContentType,
	})
}

func ensureTopology(ch *amqp.Channel, opts ConsumeOptions) error {
	if opts.Queue == "" {
		return errors.New("queue is required")
	}

	if err := ensureDeadLetterFromArgs(ch, opts.Args); err != nil {
		return err
	}

	if opts.Exchange != "" {
		kind := opts.ExchangeKind
		if kind == "" {
			kind = base.ExchangeKindDirect
		}
		if err := ch.ExchangeDeclare(
			opts.Exchange, kind,
			base.ExchangeDurable, base.ExchangeAutoDelete, base.ExchangeInternal, base.ExchangeNoWait,
			nil,
		); err != nil {
			return errors.Wrap(err, "exchange declare")
		}
	}

	queue, err := ch.QueueDeclare(
		opts.Queue,
		base.QueueDurable, base.QueueAutoDelete, base.QueueExclusive, base.QueueNoWait,
		opts.Args,
	)
	if err != nil {
		return errors.Wrap(err, "queue declare")
	}

	if opts.Exchange != "" {
		bindingKey := opts.BindingKey
		if bindingKey == "" {
			bindingKey = opts.Queue
		}
		if err := ch.QueueBind(queue.Name, bindingKey, opts.Exchange, base.QueueNoWait, nil); err != nil {
			return errors.Wrap(err, "queue bind")
		}
	}

	// retry queue: no consumer attaches here; messages sit until their per-message
	// TTL (set at publish time) expires, then get dead-lettered back into opts.Queue
	// via the default exchange (routing key == queue name).
	_, err = ch.QueueDeclare(
		retryQueueName(opts.Queue),
		base.QueueDurable, base.QueueAutoDelete, base.QueueExclusive, base.QueueNoWait,
		amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": opts.Queue,
		},
	)
	if err != nil {
		return errors.Wrap(err, "retry queue declare")
	}

	return nil
}

func ensureDeadLetterFromArgs(ch *amqp.Channel, args amqp.Table) error {
	if args == nil {
		return nil
	}
	dlx, _ := args["x-dead-letter-exchange"].(string)
	dlqKey, _ := args["x-dead-letter-routing-key"].(string)
	if dlx == "" {
		return nil
	}
	if dlqKey == "" {
		dlqKey = dlx
	}

	if err := ch.ExchangeDeclare(
		dlx,
		base.ExchangeKindDirect,
		base.ExchangeDurable,
		base.ExchangeAutoDelete,
		base.ExchangeInternal,
		base.ExchangeNoWait,
		nil,
	); err != nil {
		return errors.Wrap(err, "dlx declare")
	}
	if _, err := ch.QueueDeclare(
		dlqKey,
		base.QueueDurable,
		base.QueueAutoDelete,
		base.QueueExclusive,
		base.QueueNoWait,
		nil,
	); err != nil {
		return errors.Wrap(err, "dlq declare")
	}
	if err := ch.QueueBind(dlqKey, dlqKey, dlx, base.QueueNoWait, nil); err != nil {
		return errors.Wrap(err, "dlq bind")
	}
	return nil
}

func setupConsumeTopology(ch *amqp.Channel, opts ConsumeOptions) error {
	if err := ensureTopology(ch, opts); err != nil {
		return err
	}
	if err := ch.Qos(opts.Prefetch, base.PrefetchSize, base.PrefetchGlobal); err != nil {
		return errors.Wrap(err, "qos")
	}
	return nil
}

func normalizeConsumeOptions(opts ConsumeOptions) ConsumeOptions {
	if opts.Prefetch <= 0 {
		opts.Prefetch = base.PrefetchCount
	}
	if opts.MaxRetry <= 0 {
		opts.MaxRetry = 5
	}
	if opts.ConsumerTag == "" {
		opts.ConsumerTag = opts.Queue
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

func retryQueueName(queue string) string {
	return queue + ".retry"
}
