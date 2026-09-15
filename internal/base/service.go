package base

import (
	"context"

	"gorm.io/gorm"
)

type IBaseService[T any, ID any] interface {
	Paging(ctx context.Context, condition any, paging PagingInput, option QueryOption) ([]T, int64, error)
	Page(ctx context.Context, condition any, paging PagingInput, option QueryOption) (*PagingResponseDto, error)
	GetByID(ctx context.Context, id ID, option QueryOption) (*T, error)
	First(ctx context.Context, condition any, option QueryOption) (*T, error)
	GetAll(ctx context.Context, condition any, option QueryOption) ([]T, error)
	Create(ctx context.Context, entity *T) error
	BulkCreate(ctx context.Context, entities []T) error
	CreateOption(ctx context.Context, entity *T, option InsertOption) error
	Update(ctx context.Context, entity *T) error
	PatchUpdate(ctx context.Context, entity *T, infoUpdate any) error
	Delete(ctx context.Context, id ID) error
	DeleteUnscoped(ctx context.Context, id ID) error
	DeleteWithConds(ctx context.Context, condition any) error
	FirstOrCreate(ctx context.Context, entity *T, condition any) error
	Count(ctx context.Context, condition any) (int64, error)
	InTx(ctx context.Context, fn func(repo IBaseGormRepository[T, ID]) error) error
}

type BaseService[T any, ID any] struct {
	Repo IBaseGormRepository[T, ID]
	Tx   ITransactionManager
}

func NewBaseService[T any, ID any](db *gorm.DB) *BaseService[T, ID] {
	service := &BaseService[T, ID]{
		Repo: NewBaseGormRepository[T, ID](db),
		Tx:   NewTransactionManager(db),
	}
	var _ IBaseService[T, ID] = service
	return service
}

func (service *BaseService[T, ID]) Paging(ctx context.Context, condition any, paging PagingInput, option QueryOption) ([]T, int64, error) {
	return service.Repo.Paging(ctx, condition, paging, option)
}

func (service *BaseService[T, ID]) Page(ctx context.Context, condition any, paging PagingInput, option QueryOption) (*PagingResponseDto, error) {
	paging.Normalize()
	rows, total, err := service.Repo.Paging(ctx, condition, paging, option)
	if err != nil {
		return nil, err
	}
	return &PagingResponseDto{
		Data:        rows,
		Count:       total,
		PageNum:     paging.PageNum,
		PageSize:    paging.PageSize,
		HasNextPage: paging.PageNum*paging.PageSize < total,
		HasPrevPage: paging.PageNum > 1,
	}, nil
}

func (service *BaseService[T, ID]) GetByID(ctx context.Context, id ID, option QueryOption) (*T, error) {
	return service.Repo.GetByID(ctx, id, option)
}

func (service *BaseService[T, ID]) First(ctx context.Context, condition any, option QueryOption) (*T, error) {
	return service.Repo.First(ctx, condition, option)
}

func (service *BaseService[T, ID]) GetAll(ctx context.Context, condition any, option QueryOption) ([]T, error) {
	return service.Repo.GetAll(ctx, condition, option)
}

func (service *BaseService[T, ID]) Create(ctx context.Context, entity *T) error {
	return service.Repo.Create(ctx, entity)
}

func (service *BaseService[T, ID]) BulkCreate(ctx context.Context, entities []T) error {
	return service.Repo.BulkCreate(ctx, entities)
}

func (service *BaseService[T, ID]) CreateOption(ctx context.Context, entity *T, option InsertOption) error {
	return service.Repo.CreateOption(ctx, entity, option)
}

func (service *BaseService[T, ID]) Update(ctx context.Context, entity *T) error {
	return service.Repo.Update(ctx, entity)
}

func (service *BaseService[T, ID]) PatchUpdate(ctx context.Context, entity *T, infoUpdate any) error {
	return service.Repo.PatchUpdate(ctx, entity, infoUpdate)
}

func (service *BaseService[T, ID]) Delete(ctx context.Context, id ID) error {
	return service.Repo.Delete(ctx, id)
}

func (service *BaseService[T, ID]) DeleteUnscoped(ctx context.Context, id ID) error {
	return service.Repo.DeleteUnscoped(ctx, id)
}

func (service *BaseService[T, ID]) DeleteWithConds(ctx context.Context, condition any) error {
	return service.Repo.DeleteWithConds(ctx, condition)
}

func (service *BaseService[T, ID]) FirstOrCreate(ctx context.Context, entity *T, condition any) error {
	return service.Repo.FirstOrCreate(ctx, entity, condition)
}

func (service *BaseService[T, ID]) Count(ctx context.Context, condition any) (int64, error) {
	return service.Repo.Count(ctx, condition)
}

// InTx runs fn with a repository bound to one transaction. The callback error rolls the transaction back.
func (service *BaseService[T, ID]) InTx(ctx context.Context, fn func(repo IBaseGormRepository[T, ID]) error) error {
	_, err := service.Tx.AtomicTx(ctx, func(tx *gorm.DB) (any, error) {
		return nil, fn(service.Repo.WithDB(tx))
	})
	return err
}
