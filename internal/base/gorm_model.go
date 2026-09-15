package base

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// SQLModel is the shared row shape for GORM entities.
// JSON stays camelCase so Fiber, Gin, and chi responses match.
type SQLModel struct {
	ID        uint           `gorm:"primaryKey" json:"id"`
	CreatedAt time.Time      `gorm:"autoCreateTime" json:"createdAt"`
	UpdatedAt time.Time      `gorm:"autoUpdateTime" json:"updatedAt"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`
}

// SQLModelUUID is the same row with a string primary key generated on insert.
type SQLModelUUID struct {
	ID        string         `gorm:"primaryKey;type:varchar(36)" json:"id"`
	CreatedAt time.Time      `gorm:"autoCreateTime" json:"createdAt"`
	UpdatedAt time.Time      `gorm:"autoUpdateTime" json:"updatedAt"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`
}

func (m *SQLModelUUID) BeforeCreate(*gorm.DB) error {
	if m.ID == "" {
		m.ID = uuid.NewString()
	}
	return nil
}
