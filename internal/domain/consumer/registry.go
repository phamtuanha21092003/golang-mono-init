package consumer

import (
	"github.com/pkg/errors"

	"go-init/internal/transport/rabbitmq"
)

type factory func(Deps) rabbitmq.Handler

var handlerFactories = map[string]factory{
	"common": newCommonHandler,
}

func HandlerFor(name string, deps Deps) (rabbitmq.Handler, error) {
	f, ok := handlerFactories[name]
	if !ok {
		return nil, errors.Errorf("unknown queue handler %q", name)
	}
	return f(deps), nil
}
