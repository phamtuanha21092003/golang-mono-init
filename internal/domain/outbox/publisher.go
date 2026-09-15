package outbox

import (
	"context"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"go-init/internal/transport/rabbitmq"
	"go-init/internal/transport/redisbroker"
)

type Publisher interface {
	Publish(ctx context.Context, ev Event) error
}

type Router struct {
	Rabbit Publisher
	Redis  Publisher
}

func (r Router) Publish(ctx context.Context, ev Event) error {
	switch ev.Broker {
	case BrokerRedis:
		if r.Redis == nil {
			return errors.New("redis broker is not configured")
		}
		return r.Redis.Publish(ctx, ev)
	case BrokerRabbitMQ, "":
		if r.Rabbit == nil {
			return errors.New("rabbitmq broker is not configured")
		}
		return r.Rabbit.Publish(ctx, ev)
	default:
		return errors.Errorf("unsupported broker %q", ev.Broker)
	}
}

type RabbitPublisher struct {
	adapter *rabbitmq.RabbitMQAdapter
}

func NewRabbitPublisher(adapter *rabbitmq.RabbitMQAdapter) *RabbitPublisher {
	if adapter == nil {
		panic("outbox rabbit publisher: adapter is nil")
	}
	return &RabbitPublisher{adapter: adapter}
}

func (p *RabbitPublisher) Publish(ctx context.Context, ev Event) error {
	headers := amqp.Table{}
	for k, v := range ev.Headers {
		headers[k] = v
	}
	if ev.EventType != "" {
		headers["x-event-type"] = ev.EventType
	}
	return p.adapter.Publish(ctx, ev.Exchange, ev.RoutingKey, []byte(ev.Payload), rabbitmq.PublishOptions{
		Headers: headers,
	})
}

type RedisPublisher struct {
	broker *redisbroker.Broker
}

func NewRedisPublisher(client *redis.Client, logger *zap.Logger) *RedisPublisher {
	return &RedisPublisher{broker: redisbroker.New(client, logger)}
}

func NewRedisPublisherFromBroker(broker *redisbroker.Broker) *RedisPublisher {
	if broker == nil {
		panic("outbox redis publisher: broker is nil")
	}
	return &RedisPublisher{broker: broker}
}

func (p *RedisPublisher) Publish(ctx context.Context, ev Event) error {
	_, err := p.broker.Publish(ctx, ev.RoutingKey, []byte(ev.Payload))
	return err
}
