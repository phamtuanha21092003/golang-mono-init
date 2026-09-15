package repository

import (
	"go-init/internal/base"

	"gorm.io/gorm"
)

// NewGorm builds the shared GORM repository. Concrete repos embed or wrap the result.
func NewGorm[T any, ID any](db *gorm.DB) *base.BaseGormRepository[T, ID] {
	return base.NewBaseGormRepository[T, ID](db)
}
