package base

import (
	"context"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type ITransactionManager interface {
	AtomicTx(ctx context.Context, fn func(tx *gorm.DB) (any, error)) (any, error)
}

type transactionManager struct {
	db *gorm.DB
}

func NewTransactionManager(db *gorm.DB) ITransactionManager {
	if db == nil {
		panic("gorm db is nil")
	}
	return &transactionManager{db: db}
}

func (tm *transactionManager) AtomicTx(ctx context.Context, fn func(tx *gorm.DB) (any, error)) (any, error) {
	tx := tm.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, errors.Wrap(tx.Error, "begin transaction")
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			_ = tx.Rollback()
			panic(recovered)
		}
	}()

	data, err := fn(tx)
	if err != nil {
		if rbErr := tx.Rollback().Error; rbErr != nil {
			return nil, errors.Wrapf(err, "rollback: %v", rbErr)
		}
		return nil, err
	}

	if err := tx.Commit().Error; err != nil {
		return nil, errors.Wrap(err, "commit transaction")
	}
	return data, nil
}
