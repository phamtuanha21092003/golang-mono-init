package service

import (
	"context"

	"go-init/internal/dto"
	"go-init/internal/transport/rabbitmq"
)

type CommonService interface {
	Handle(ctx context.Context, env dto.Envelope, msg rabbitmq.Message) error
}

type commonService struct{}

func NewCommonService() CommonService {
	return &commonService{}
}

func (s *commonService) Handle(ctx context.Context, env dto.Envelope, msg rabbitmq.Message) error {
	return nil
}
