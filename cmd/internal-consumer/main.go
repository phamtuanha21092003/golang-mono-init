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

	"go-init/internal/config"
	"go-init/internal/domain/consumer"
	"go-init/internal/service"
	"go-init/internal/transport/rabbitmq"
	"go-init/platform/database"
	"go-init/platform/logger"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "internal-consumer: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	_ = godotenv.Load("environment/.env.internal-consumer")

	cfg, err := config.NewInternalConsumerCfg()
	if err != nil {
		return err
	}

	if err := logger.SetUpLogger(cfg.App.Debug); err != nil {
		return err
	}
	defer logger.Sync()
	log := logger.GetLogger()

	sqlDB := database.NewDatabaseConn(cfg.SqlUri)

	mq, err := rabbitmq.NewRabbitMQAdapter(log, cfg.RabbitMQUri, cfg.App.Name)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	deps := consumer.Deps{
		Logger: log,
		SQL:    sqlDB,
		Common: service.NewCommonService(),
	}

	for _, q := range cfg.Queues {
		h, err := consumer.HandlerFor(q.Handler, deps)
		if err != nil {
			return err
		}
		mq.StartConsume(ctx, consumer.ConsumeOptionsFrom(q), h)
		log.Info("started consumer",
			zap.String("queue", q.Name),
			zap.String("handler", q.Handler),
			zap.String("exchange", q.Exchange),
			zap.String("bindingKey", q.BindingKey),
		)
	}

	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return mq.Close(shutdownCtx)
}
