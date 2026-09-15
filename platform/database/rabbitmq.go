package database

import (
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	_dialAttempts   = 5
	_heartbeat      = 10 * time.Second
	_initialBackoff = time.Second
	_maxBackoff     = 16 * time.Second
)

var ErrCannotConnectRabbitMQ = fmt.Errorf("cannot connect to RabbitMQ")

// NewRabbitMQConnection dials RabbitMQ with exponential backoff.
// Each call returns a new connection; the caller owns Close.
func NewRabbitMQConnection(uri, appName string) (*amqp.Connection, error) {
	var lastErr error
	backoff := _initialBackoff

	for attempt := 1; attempt <= _dialAttempts; attempt++ {
		conn, err := amqp.DialConfig(uri, amqp.Config{
			Heartbeat: _heartbeat,
			Locale:    "en_US",
			Properties: amqp.Table{
				"connection_name": appName,
				"app":             appName,
			},
		})
		if err == nil {
			return conn, nil
		}

		lastErr = err
		slog.Info("RabbitMQ dial failed, backing off",
			"attempt", attempt,
			"backoff", backoff,
			"error", err,
		)
		if attempt == _dialAttempts {
			break
		}
		time.Sleep(backoff)
		if backoff < _maxBackoff {
			backoff *= 2
		}
	}

	slog.Error("RabbitMQ failed to connect", "error", lastErr)
	return nil, fmt.Errorf("%w: %v", ErrCannotConnectRabbitMQ, lastErr)
}
