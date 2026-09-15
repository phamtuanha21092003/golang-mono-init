package consumer

import (
	"go.uber.org/zap"

	"go-init/internal/config"
	"go-init/internal/service"
	"go-init/internal/transport/rabbitmq"
	"go-init/platform/database"
)

type Deps struct {
	Logger *zap.Logger
	SQL    *database.SqlxDatabase
	Common service.CommonService
}

func ConsumeOptionsFrom(q config.QueueConfig) rabbitmq.ConsumeOptions {
	opts := rabbitmq.NewConsumeOptions(q.Name, q.MaxPriority)
	opts.Exchange = q.Exchange
	opts.BindingKey = q.BindingKey
	opts.ExchangeKind = q.ExchangeKind
	return opts
}
