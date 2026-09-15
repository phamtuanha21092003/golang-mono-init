package base

import (
	"time"
)

const (
	defaultPageNum  int64 = 1
	defaultPageSize int64 = 10
	maxPageSize     int64 = 100
)

type (
	PagingResponseDto struct {
		PageSize    int64       `json:"pageSize" example:"10"`
		PageNum     int64       `json:"pageNum" example:"1"`
		Count       int64       `json:"count" example:"100"`
		HasNextPage bool        `json:"hasNext" example:"true"`
		HasPrevPage bool        `json:"hasPrevious" example:"false"`
		Data        interface{} `json:"data"`
	}

	PagingInput struct {
		PageSize  int64       `query:"pageSize" example:"10"`
		PageNum   int64       `query:"pageNum" example:"1"`
		Sort      string      `query:"sort" example:"createdAt"`
		Keyword   string      `query:"keyword" example:"abc"`
		SortDesc  bool        `query:"sortDesc" example:"true"`
		Condition []Condition `query:"-"`
	}

	BaseSchema[T any] struct {
		ID        T         `json:"id"`
		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
	}

	BaseSchemaUUid struct {
		BaseSchema[string]
	}

	Condition struct {
		Field    string
		Operator string // e.g., "$eq", "$gt", "$lt", "$in", etc.
		Value    interface{}
	}
)

// Normalize applies valid default values for pagination, avoiding negative skip or oversized pages.
func (p *PagingInput) Normalize() {
	if p.PageNum < 1 {
		p.PageNum = defaultPageNum
	}
	if p.PageSize < 1 {
		p.PageSize = defaultPageSize
	}
	if p.PageSize > maxPageSize {
		p.PageSize = maxPageSize
	}
}

// ToBaseDto copies the shared fields from a model into the outward-facing schema.
func ToBaseDto[T any](m *BaseModel[T]) *BaseSchema[T] {
	if m == nil {
		return nil
	}
	return &BaseSchema[T]{
		ID:        m.ID,
		CreatedAt: m.CreatedAt,
		UpdatedAt: m.UpdatedAt,
	}
}
