package rabbitmq

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"go-init/internal/base"
)

type PublishOptions struct {
	Priority  uint8
	Mandatory bool
	Headers   amqp.Table
}

// Publish JSON-encodes payload (unless it is []byte or string) and waits for a
// publisher confirm. Concurrent publishes are serialized on one confirm-mode channel.
//
// Call SetupTopology with the same ConsumeOptions the consumer uses before the
// first publish, otherwise messages to a missing queue are dropped when Mandatory is false.
func (r *RabbitMQAdapter) Publish(ctx context.Context, exchange, routingKey string, payload any, opts PublishOptions) error {
	ctx, cancel := withPublishDeadline(ctx)
	defer cancel()

	body, err := marshalPayload(payload)
	if err != nil {
		return err
	}

	headers := amqp.Table{}
	for k, v := range opts.Headers {
		headers[k] = v
	}
	if _, ok := headers[base.RetryCountHeader]; !ok {
		headers[base.RetryCountHeader] = "0"
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		MessageId:    newID(),
		Timestamp:    time.Now(),
		Body:         body,
		Headers:      headers,
		Priority:     opts.Priority,
	}

	if err := r.publishConfirmed(ctx, exchange, routingKey, opts.Mandatory, msg); err != nil {
		r.logger.Error("publish failed", zap.Error(err), zap.String("routingKey", routingKey))
		return err
	}
	r.logger.Info("published", zap.String("exchange", exchange), zap.String("routingKey", routingKey), zap.String("messageId", msg.MessageId))
	return nil
}

func (r *RabbitMQAdapter) publishConfirmed(ctx context.Context, exchange, routingKey string, mandatory bool, msg amqp.Publishing) error {
	r.pubMu.Lock()
	defer r.pubMu.Unlock()

	var lastErr error
	for attempt := 1; attempt <= openChannelTries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		ch, err := r.openPublishChannelLocked()
		if err != nil {
			lastErr = err
			continue
		}

		conf, err := ch.PublishWithDeferredConfirmWithContext(
			ctx,
			exchange,
			routingKey,
			mandatory,
			base.PublishImmediate,
			msg,
		)
		if err != nil {
			lastErr = errors.Wrap(err, "publish")
			r.dropPublishChannelLocked()
			continue
		}

		waitCtx, cancel := confirmContext(ctx)
		acked, err := conf.WaitContext(waitCtx)
		cancel()
		if err != nil {
			lastErr = errors.Wrap(err, "wait confirm")
			r.dropPublishChannelLocked()
			continue
		}
		if !acked {
			return ErrBrokerNack
		}
		if mandatory {
			select {
			case ret := <-r.pubReturns:
				return errors.Wrapf(ErrUnroutable, "reply=%s exchange=%s key=%s", ret.ReplyText, ret.Exchange, ret.RoutingKey)
			default:
			}
		}
		return nil
	}

	if lastErr == nil {
		lastErr = errors.New("publish failed")
	}
	return lastErr
}

func (r *RabbitMQAdapter) openPublishChannelLocked() (*amqp.Channel, error) {
	if r.pubCh != nil && !r.pubCh.IsClosed() {
		return r.pubCh, nil
	}

	ch, err := r.openChannel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, errors.Wrap(err, "enable confirm mode")
	}
	r.pubCh = ch
	r.pubReturns = ch.NotifyReturn(make(chan amqp.Return, 8))
	return ch, nil
}

func (r *RabbitMQAdapter) dropPublishChannelLocked() {
	if r.pubCh != nil && !r.pubCh.IsClosed() {
		_ = r.pubCh.Close()
	}
	r.pubCh = nil
	r.pubReturns = nil
}

func marshalPayload(payload any) ([]byte, error) {
	switch v := payload.(type) {
	case nil:
		return nil, errors.New("payload is nil")
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		body, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Wrap(err, "marshal payload")
		}
		return body, nil
	}
}

func withPublishDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, defaultPublishTimeout)
}

func confirmContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= confirmWaitTimeout {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, confirmWaitTimeout)
}
