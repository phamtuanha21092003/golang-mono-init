package database

import (
	"errors"
	"log"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitInstance struct {
	*amqp.Connection
}

var (
	rabbitMu sync.Mutex
	RbConn   *RabbitInstance
)

const (
	_retryTimes     = 5
	_backOffSeconds = 2
)

var ErrCannotConnectRabbitMQ = errors.New("cannot connect to RabbitMQ")

func NewRabbitMQConnection(uri, appName string) (*RabbitInstance, error) {
	rabbitMu.Lock()
	defer rabbitMu.Unlock()
	var (
		counts int64
	)

	if RbConn != nil && !RbConn.IsClosed() {
		return RbConn, nil
	}
	for {
		conn, err := amqp.DialConfig(
			uri,
			amqp.Config{
				Heartbeat: 10 * time.Second,
				Properties: amqp.Table{
					"app": appName,
				},
			},
		)
		if err != nil {
			counts++
		} else {
			RbConn = &RabbitInstance{
				Connection: conn,
			}
			break
		}
		if counts > _retryTimes {
			slog.Error("RabbitMQ failed to retry", "error", err)
			return nil, ErrCannotConnectRabbitMQ
		}

		slog.Info("RabbitMQ backing off for 2 seconds...")
		time.Sleep(_backOffSeconds * time.Second)

		continue

	}

	log.Println("Rabbit mq connected.")

	return RbConn, nil
}
