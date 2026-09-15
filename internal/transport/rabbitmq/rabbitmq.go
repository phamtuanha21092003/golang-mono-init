package rabbitmq

import (
	"context"
	"encoding/json"
	"sync"
	"time"
	"uuid"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"go-init/internal/base"
	"go-init/platform/database"
)

type RabbitMQAdapter struct {
	appName string

	uri    string
	mqConn *amqp.Connection
	mqChan *amqp.Channel
	logger *zap.Logger

	connMu sync.Mutex // protect mqConn
	chMu   sync.Mutex // protect mqChan (Publish)
}

func NewRabbitMQAdapter(logger *zap.Logger, uri, appName string) (*RabbitMQAdapter, error) {
	r := &RabbitMQAdapter{uri: uri, logger: logger, appName: appName}
	if err := r.ensureConnection(); err != nil {
		return nil, err
	}
	ch, err := r.openChannel()
	if err != nil {
		return nil, err
	}
	r.mqChan = ch
	return r, nil
}

func (r *RabbitMQAdapter) ensureConnection() error {
	r.connMu.Lock()
	defer r.connMu.Unlock()

	if r.mqConn != nil && !r.mqConn.IsClosed() {
		return nil
	}
	conn, err := database.NewRabbitMQConnection(r.uri, r.appName)
	if err != nil {
		return err
	}
	r.mqConn = conn.Connection
	return nil
}

func (r *RabbitMQAdapter) openChannel() (*amqp.Channel, error) {
	if err := r.ensureConnection(); err != nil {
		return nil, err
	}
	var lastErr error
	for i := 1; i <= 3; i++ {
		ch, err := r.mqConn.Channel()
		if err == nil {
			return ch, nil
		}
		lastErr = err
		time.Sleep(time.Duration(i) * time.Second)
	}
	return nil, errors.Wrap(lastErr, "open channel")
}

// Publish sends a message. It does not declare queues/exchanges/bindings — the consumer owns topology.
// With the default exchange (exchange == ""), routerKey is normally the target queue name.
func (r *RabbitMQAdapter) Publish(exchange, routerKey string, eventPayload interface{}, kind string, priority int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ch, err := r.getPublishChannel()
	if err != nil {
		return err
	}

	eventJson, err := json.Marshal(eventPayload)
	if err != nil {
		return errors.New("error converting struct to json")
	}
	if err := ch.PublishWithContext(ctx,
		exchange,
		routerKey,
		base.PublishMandatory,
		base.PublishImmediate,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			MessageId:    uuid.New().String(),
			Timestamp:    time.Now(),
			Body:         eventJson,
			Headers:      map[string]any{base.RetryCountHeader: "0", "content-type": "application/json"},
			Priority:     uint8(priority),
		},
	); err != nil {
		r.logger.Error(err.Error())
		return errors.Wrap(err, "ch.Publish")
	}
	r.logger.Info(" [x] Sent to routing_key", zap.String("routerKey", routerKey))
	return nil
}

// getPublishChannel returns the channel used for publishing, reopening it if closed.
// Locked to prevent multiple goroutines from opening/overwriting r.mqChan concurrently.
func (r *RabbitMQAdapter) getPublishChannel() (*amqp.Channel, error) {
	r.chMu.Lock()
	defer r.chMu.Unlock()

	if r.mqChan != nil && !r.mqChan.IsClosed() {
		return r.mqChan, nil
	}
	ch, err := r.openChannel()
	if err != nil {
		return nil, err
	}
	r.mqChan = ch
	return ch, nil
}

func (r *RabbitMQAdapter) CreateChannel(exchangeName, queueName, bindingKey, consumerTag, exchangeKind string, opts ConsumeOptions) (*amqp.Channel, error) {
	ch, err := r.openChannel()
	if err != nil {
		r.logger.Error("Open channel", zap.Error(err))
		return nil, err
	}

	if err := setupConsumeTopology(ch, opts); err != nil {
		_ = ch.Close()
		return nil, err
	}

	return ch, nil
}

func (r *RabbitMQAdapter) Disconnect(ctx context.Context) error {
	r.chMu.Lock()
	if r.mqChan != nil && !r.mqChan.IsClosed() {
		_ = r.mqChan.Close()
	}
	r.chMu.Unlock()

	r.connMu.Lock()
	defer r.connMu.Unlock()
	if r.mqConn == nil || r.mqConn.IsClosed() {
		return nil
	}
	return r.mqConn.Close()
}

func (r *RabbitMQAdapter) Shutdown(ctx context.Context) error {
	if r.mqChan != nil && !r.mqChan.IsClosed() {
		return r.mqChan.Close()
	}
	return nil
}
