package consumer

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"

	"go-init/internal/dto"
	"go-init/internal/transport/rabbitmq"
)

func newCommonHandler(deps Deps) rabbitmq.Handler {
	return func(ctx context.Context, msg rabbitmq.Message) error {
		env := decodeEnvelope(msg.Body)
		if deps.Logger != nil {
			deps.Logger.Info("common message",
				zap.String("messageId", msg.MessageID),
				zap.Int("retry", msg.RetryCount),
				zap.String("kind", env.Kind),
				zap.ByteString("body", msg.Body),
			)
		}
		if deps.Common == nil {
			return nil
		}
		return deps.Common.Handle(ctx, env, msg)
	}
}

func decodeEnvelope(body []byte) dto.Envelope {
	var env dto.Envelope
	if len(body) == 0 {
		return env
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return dto.Envelope{}
	}
	return env
}
