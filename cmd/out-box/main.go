package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"go-init/internal/bootstrap"
	"go-init/internal/domain/outbox"
	"go-init/internal/transport/rabbitmq"
	"go-init/internal/transport/redisbroker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "out-box: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	_ = godotenv.Load()
	_ = godotenv.Load("environment/.env.out-box")

	resources, err := bootstrap.Open()
	if err != nil {
		return err
	}
	defer resources.Close()

	if resources.SQL == nil {
		return fmt.Errorf("SQL_URI is required")
	}

	var (
		rabbitPub outbox.Publisher
		redisPub  outbox.Publisher
		mq        *rabbitmq.RabbitMQAdapter
	)
	if resources.Stack.RabbitMQURI != "" {
		mq, err = rabbitmq.NewRabbitMQAdapter(resources.Log, resources.Stack.RabbitMQURI, resources.Stack.Name)
		if err != nil {
			return err
		}
		rabbitPub = outbox.NewRabbitPublisher(mq)
	}
	if resources.Redis != nil {
		redisPub = outbox.NewRedisPublisherFromBroker(redisbroker.New(resources.Redis, resources.Log))
	}
	if rabbitPub == nil && redisPub == nil {
		return fmt.Errorf("RABBITMQ_URI or REDIS_URI is required")
	}

	var mongoDB *mongo.Database
	if resources.Mongo != nil {
		mongoDB = resources.Mongo.DB
	}
	logs := outbox.NewLogSinks(resources.SQL.DB, mongoDB)

	relay := outbox.NewRelay(
		outbox.NewStore(resources.SQL.DB),
		outbox.Router{Rabbit: rabbitPub, Redis: redisPub},
		logs,
		resources.Log,
		resources.Stack.OutboxPoll,
		resources.Stack.OutboxBatch,
		resources.Stack.OutboxMaxAttempts,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	resources.Log.Info("outbox relay started")
	if err := relay.Run(ctx); err != nil && ctx.Err() == nil {
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if mq != nil {
		_ = mq.Close(shutdownCtx)
	}
	return nil
}
