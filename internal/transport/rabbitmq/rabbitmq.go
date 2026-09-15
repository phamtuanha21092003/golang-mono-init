package rabbitmq

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"go-init/platform/database"
)

const (
	defaultPublishTimeout = 10 * time.Second
	// Must not be lower than the broker heartbeat (10s in database.NewRabbitMQConnection).
	confirmWaitTimeout = 10 * time.Second
	openChannelTries   = 3
)

var (
	ErrClosed     = errors.New("rabbitmq adapter closed")
	ErrBrokerNack = errors.New("broker nacked message")
	ErrUnroutable = errors.New("message was not routed to a queue")
)

type RabbitMQAdapter struct {
	appName string
	uri     string
	logger  *zap.Logger

	connMu sync.Mutex
	conn   *amqp.Connection

	// AMQP channels are not safe for concurrent use. All publishes (including
	// retry republishes) go through pubCh under pubMu, including confirm wait.
	pubMu      sync.Mutex
	pubCh      *amqp.Channel
	pubReturns <-chan amqp.Return

	consumeWg sync.WaitGroup
	closeOnce sync.Once
	closed    chan struct{}
}

func NewRabbitMQAdapter(logger *zap.Logger, uri, appName string) (*RabbitMQAdapter, error) {
	r := &RabbitMQAdapter{
		uri:     uri,
		logger:  logger,
		appName: appName,
		closed:  make(chan struct{}),
	}
	if err := r.ensureConnection(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *RabbitMQAdapter) IsLive() bool {
	select {
	case <-r.closed:
		return false
	default:
	}

	r.connMu.Lock()
	defer r.connMu.Unlock()
	return r.conn != nil && !r.conn.IsClosed()
}

// SetupTopology declares the queue, exchange, bindings, retry queue and DLX/DLQ.
// Safe to call from both producer and consumer; declare is idempotent if args match.
func (r *RabbitMQAdapter) SetupTopology(opts ConsumeOptions) error {
	if opts.Queue == "" {
		return errors.New("queue is required")
	}
	opts = normalizeConsumeOptions(opts)

	ch, err := r.openChannel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ensureTopology(ch, opts)
}

// Close waits for consumers started via StartConsume, then closes the publish
// channel and the connection. Cancel the consume context before calling Close
// so in-flight handlers can finish.
func (r *RabbitMQAdapter) Close(ctx context.Context) error {
	r.closeOnce.Do(func() {
		close(r.closed)
	})

	done := make(chan struct{})
	go func() {
		r.consumeWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		r.logger.Warn("close timed out waiting for consumers")
	}

	r.pubMu.Lock()
	if r.pubCh != nil && !r.pubCh.IsClosed() {
		_ = r.pubCh.Close()
	}
	r.pubCh = nil
	r.pubMu.Unlock()

	r.connMu.Lock()
	defer r.connMu.Unlock()
	if r.conn == nil || r.conn.IsClosed() {
		return nil
	}
	err := r.conn.Close()
	r.conn = nil
	return err
}

// Disconnect is an alias for Close.
func (r *RabbitMQAdapter) Disconnect(ctx context.Context) error {
	return r.Close(ctx)
}

func (r *RabbitMQAdapter) ensureConnection() error {
	select {
	case <-r.closed:
		return ErrClosed
	default:
	}

	r.connMu.Lock()
	defer r.connMu.Unlock()

	if r.conn != nil && !r.conn.IsClosed() {
		return nil
	}

	conn, err := database.NewRabbitMQConnection(r.uri, r.appName)
	if err != nil {
		return err
	}
	r.conn = conn
	r.logger.Info("rabbitmq connected")
	return nil
}

func (r *RabbitMQAdapter) connection() (*amqp.Connection, error) {
	if err := r.ensureConnection(); err != nil {
		return nil, err
	}

	r.connMu.Lock()
	defer r.connMu.Unlock()
	if r.conn == nil || r.conn.IsClosed() {
		return nil, errors.New("connection closed")
	}
	return r.conn, nil
}

func (r *RabbitMQAdapter) openChannel() (*amqp.Channel, error) {
	var lastErr error
	for i := 1; i <= openChannelTries; i++ {
		conn, err := r.connection()
		if err != nil {
			return nil, err
		}
		ch, err := conn.Channel()
		if err == nil {
			return ch, nil
		}
		lastErr = err
		time.Sleep(time.Duration(i) * time.Second)
	}
	return nil, errors.Wrap(lastErr, "open channel")
}
