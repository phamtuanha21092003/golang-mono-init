package base

import "context"

type ICronJob interface {
	Name() string
	Schedule() string
	Run(ctx context.Context)
}
