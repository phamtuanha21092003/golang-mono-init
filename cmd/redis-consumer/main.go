package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"go.uber.org/zap"

	"go-init/internal/bootstrap"
	"go-init/internal/transport/redisbroker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "redis-consumer: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	_ = godotenv.Load()

	resources, err := bootstrap.Open()
	if err != nil {
		return err
	}
	defer resources.Close()

	if resources.Redis == nil {
		return fmt.Errorf("REDIS_URI is required")
	}

	broker := redisbroker.New(resources.Redis, resources.Log)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker.StartConsume(ctx, redisbroker.ConsumeOptions{
		Stream:   resources.Stack.Stream,
		Group:    resources.Stack.StreamGroup,
		Consumer: resources.Stack.Name,
	}, func(ctx context.Context, msg redisbroker.Message) error {
		resources.Log.Info("redis message",
			zap.String("id", msg.ID),
			zap.String("stream", msg.Stream),
			zap.Int("attempt", msg.Attempt),
			zap.ByteString("body", msg.Body),
		)
		return nil
	})

	<-ctx.Done()
	resources.Log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return broker.Close(shutdownCtx)
}
