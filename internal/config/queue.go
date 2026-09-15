package config

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type QueueConfig struct {
	Name         string `mapstructure:"name"`
	Exchange     string `mapstructure:"exchange"`
	BindingKey   string `mapstructure:"binding_key"`
	ExchangeKind string `mapstructure:"exchange_kind"`
	Handler      string `mapstructure:"handler"`
	MaxPriority  int32  `mapstructure:"max_priority"`
}

func ParseQueues(v *viper.Viper) ([]QueueConfig, error) {
	var queues []QueueConfig
	if err := v.UnmarshalKey("queues", &queues); err != nil {
		return nil, errors.Wrap(err, "unmarshal queues")
	}
	if len(queues) == 0 {
		return nil, errors.New("at least one [[queues]] entry is required")
	}
	for i := range queues {
		if err := normalizeQueue(&queues[i]); err != nil {
			return nil, errors.Wrapf(err, "queues[%d]", i)
		}
	}
	return queues, nil
}

func normalizeQueue(q *QueueConfig) error {
	if q.Name == "" {
		return errors.New("name is required")
	}
	if q.Handler == "" {
		return errors.New("handler is required")
	}
	if q.ExchangeKind == "" {
		q.ExchangeKind = "direct"
	}
	if q.BindingKey == "" {
		q.BindingKey = q.Name
	}
	return nil
}
