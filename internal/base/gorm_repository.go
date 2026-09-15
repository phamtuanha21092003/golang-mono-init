package base

import (
	"context"
	"regexp"
	"strings"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const defaultBatchSize = 100

var (
	identPattern   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	preloadPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*$`)
	orderPattern   = regexp.MustCompile(`(?i)^[A-Za-z_][A-Za-z0-9_.]*(\s+(asc|desc))?$`)
)

type IBaseGormRepository[T any, ID any] interface {
	GetAll(ctx context.Context, condition any, option QueryOption) ([]T, error)
	Paging(ctx context.Context, condition any, paging PagingInput, option QueryOption) ([]T, int64, error)
	GetByID(ctx context.Context, id ID, option QueryOption) (*T, error)
	First(ctx context.Context, condition any, option QueryOption) (*T, error)
	Create(ctx context.Context, entity *T) error
	CreateOption(ctx context.Context, entity *T, option InsertOption) error
	Update(ctx context.Context, entity *T) error
	PatchUpdate(ctx context.Context, entity *T, infoUpdate any) error
	Delete(ctx context.Context, id ID) error
	DeleteUnscoped(ctx context.Context, id ID) error
	DeleteWithConds(ctx context.Context, condition any) error
	FirstOrCreate(ctx context.Context, entity *T, condition any) error
	BulkCreate(ctx context.Context, entities []T) error
	Count(ctx context.Context, condition any) (int64, error)
	RawQuery(ctx context.Context, query string, values ...any) (*gorm.DB, error)
	RawQueryPaging(ctx context.Context, paging PagingInput, query string, values ...any) (*gorm.DB, int64, error)
	ExecQuery(ctx context.Context, query string, values ...any) (int64, error)
	BulkRawUpdate(ctx context.Context, query string, values []any) error
	AssociationAppend(ctx context.Context, option AssociationOption[T]) error
	AssociationClear(ctx context.Context, option AssociationOption[T]) error
	WithDB(db *gorm.DB) IBaseGormRepository[T, ID]
}

type BaseGormRepository[T any, ID any] struct {
	DB *gorm.DB
}

func NewBaseGormRepository[T any, ID any](db *gorm.DB) *BaseGormRepository[T, ID] {
	if db == nil {
		panic("gorm db is nil")
	}
	repo := &BaseGormRepository[T, ID]{DB: db}
	var _ IBaseGormRepository[T, ID] = repo
	return repo
}

func (repo *BaseGormRepository[T, ID]) WithDB(db *gorm.DB) IBaseGormRepository[T, ID] {
	clone := *repo
	clone.DB = db
	return &clone
}

func (repo *BaseGormRepository[T, ID]) GetAll(ctx context.Context, condition any, option QueryOption) ([]T, error) {
	query, err := repo.prepare(ctx, condition, option)
	if err != nil {
		return nil, err
	}
	var results []T
	if err := query.Find(&results).Error; err != nil {
		return nil, mapErr(err)
	}
	return results, nil
}

func (repo *BaseGormRepository[T, ID]) Paging(ctx context.Context, condition any, paging PagingInput, option QueryOption) ([]T, int64, error) {
	query, err := repo.prepare(ctx, condition, option)
	if err != nil {
		return nil, 0, err
	}

	var total int64
	if err := query.Session(&gorm.Session{}).Count(&total).Error; err != nil {
		return nil, 0, mapErr(err)
	}

	pageQuery, err := applyPage(query.Session(&gorm.Session{}), paging, option)
	if err != nil {
		return nil, 0, err
	}

	var entities []T
	if err := pageQuery.Find(&entities).Error; err != nil {
		return nil, 0, mapErr(err)
	}
	return entities, total, nil
}

func (repo *BaseGormRepository[T, ID]) GetByID(ctx context.Context, id ID, option QueryOption) (*T, error) {
	query, err := repo.prepare(ctx, nil, option)
	if err != nil {
		return nil, err
	}
	var result T
	if err := query.First(&result, id).Error; err != nil {
		return nil, mapErr(err)
	}
	return &result, nil
}

func (repo *BaseGormRepository[T, ID]) First(ctx context.Context, condition any, option QueryOption) (*T, error) {
	if condition == nil {
		return nil, errors.Wrap(ErrInvalidInput, "condition is required")
	}
	query, err := repo.prepare(ctx, condition, option)
	if err != nil {
		return nil, err
	}
	var result T
	if err := query.First(&result).Error; err != nil {
		return nil, mapErr(err)
	}
	return &result, nil
}

func (repo *BaseGormRepository[T, ID]) Create(ctx context.Context, entity *T) error {
	if entity == nil {
		return errors.Wrap(ErrInvalidInput, "entity is nil")
	}
	return mapErr(repo.withReturning(repo.DB.WithContext(ctx)).Create(entity).Error)
}

func (repo *BaseGormRepository[T, ID]) CreateOption(ctx context.Context, entity *T, option InsertOption) error {
	if entity == nil {
		return errors.Wrap(ErrInvalidInput, "entity is nil")
	}
	query := repo.DB.WithContext(ctx)
	if option.FullSaveAssociations {
		query = query.Session(&gorm.Session{FullSaveAssociations: true})
	}
	if len(option.Omit) > 0 {
		if err := validateIdents(option.Omit, "omit"); err != nil {
			return err
		}
		query = query.Omit(option.Omit...)
	}
	return mapErr(repo.withReturning(query).Create(entity).Error)
}

func (repo *BaseGormRepository[T, ID]) BulkCreate(ctx context.Context, entities []T) error {
	if len(entities) == 0 {
		return errors.Wrap(ErrInvalidInput, "entities list cannot be empty")
	}
	return mapErr(repo.DB.WithContext(ctx).CreateInBatches(entities, defaultBatchSize).Error)
}

func (repo *BaseGormRepository[T, ID]) Update(ctx context.Context, entity *T) error {
	if entity == nil {
		return errors.Wrap(ErrInvalidInput, "entity is nil")
	}
	return mapErr(repo.withReturning(repo.DB.WithContext(ctx)).Save(entity).Error)
}

func (repo *BaseGormRepository[T, ID]) PatchUpdate(ctx context.Context, entity *T, infoUpdate any) error {
	if entity == nil {
		return errors.Wrap(ErrInvalidInput, "entity is nil")
	}
	return mapErr(repo.withReturning(repo.DB.WithContext(ctx).Model(entity)).Updates(infoUpdate).Error)
}

func (repo *BaseGormRepository[T, ID]) Delete(ctx context.Context, id ID) error {
	res := repo.DB.WithContext(ctx).Delete(new(T), id)
	if res.Error != nil {
		return mapErr(res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (repo *BaseGormRepository[T, ID]) DeleteUnscoped(ctx context.Context, id ID) error {
	res := repo.DB.WithContext(ctx).Unscoped().Delete(new(T), id)
	if res.Error != nil {
		return mapErr(res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (repo *BaseGormRepository[T, ID]) DeleteWithConds(ctx context.Context, condition any) error {
	if condition == nil {
		return errors.Wrap(ErrInvalidInput, "condition is required")
	}
	query, err := repo.prepare(ctx, condition, QueryOption{})
	if err != nil {
		return err
	}
	return mapErr(query.Delete(new(T)).Error)
}

func (repo *BaseGormRepository[T, ID]) FirstOrCreate(ctx context.Context, entity *T, condition any) error {
	if entity == nil {
		return errors.Wrap(ErrInvalidInput, "entity is nil")
	}
	query := repo.DB.WithContext(ctx)
	if condition != nil {
		query = query.Where(condition)
	}
	return mapErr(repo.withReturning(query).FirstOrCreate(entity).Error)
}

func (repo *BaseGormRepository[T, ID]) Count(ctx context.Context, condition any) (int64, error) {
	query, err := repo.prepare(ctx, condition, QueryOption{})
	if err != nil {
		return 0, err
	}
	var count int64
	if err := query.Count(&count).Error; err != nil {
		return 0, mapErr(err)
	}
	return count, nil
}

func (repo *BaseGormRepository[T, ID]) RawQuery(ctx context.Context, query string, values ...any) (*gorm.DB, error) {
	raw := repo.DB.WithContext(ctx).Raw(query, values...)
	return raw, raw.Error
}

func (repo *BaseGormRepository[T, ID]) RawQueryPaging(ctx context.Context, paging PagingInput, query string, values ...any) (*gorm.DB, int64, error) {
	paging.Normalize()
	var total int64
	countQuery := "SELECT COUNT(*) FROM (" + query + ") AS result"
	if err := repo.DB.WithContext(ctx).Raw(countQuery, values...).Scan(&total).Error; err != nil {
		return nil, 0, mapErr(err)
	}

	offset := (paging.PageNum - 1) * paging.PageSize
	paged := query + " LIMIT ? OFFSET ?"
	args := append(append([]any{}, values...), paging.PageSize, offset)
	raw := repo.DB.WithContext(ctx).Raw(paged, args...)
	return raw, total, raw.Error
}

func (repo *BaseGormRepository[T, ID]) ExecQuery(ctx context.Context, query string, values ...any) (int64, error) {
	res := repo.DB.WithContext(ctx).Exec(query, values...)
	if res.Error != nil {
		return 0, mapErr(res.Error)
	}
	return res.RowsAffected, nil
}

// BulkRawUpdate binds values as a Postgres array. The SQL must use a single array placeholder.
func (repo *BaseGormRepository[T, ID]) BulkRawUpdate(ctx context.Context, rawQuery string, values []any) error {
	if len(values) == 0 {
		return nil
	}
	res := repo.DB.WithContext(ctx).Exec(rawQuery, pq.Array(values))
	return mapErr(res.Error)
}

func (repo *BaseGormRepository[T, ID]) AssociationAppend(ctx context.Context, option AssociationOption[T]) error {
	if err := validateAssociation(option.Entity, option.AssociationName); err != nil {
		return err
	}
	if len(option.Values) == 0 {
		return errors.Wrap(ErrInvalidInput, "association values are required")
	}
	err := repo.DB.WithContext(ctx).Model(option.Entity).Association(option.AssociationName).Append(option.Values...)
	return mapErr(err)
}

func (repo *BaseGormRepository[T, ID]) AssociationClear(ctx context.Context, option AssociationOption[T]) error {
	if err := validateAssociation(option.Entity, option.AssociationName); err != nil {
		return err
	}
	return mapErr(repo.DB.WithContext(ctx).Model(option.Entity).Association(option.AssociationName).Clear())
}

func (repo *BaseGormRepository[T, ID]) query(ctx context.Context) *gorm.DB {
	var model T
	return repo.DB.WithContext(ctx).Model(&model)
}

func (repo *BaseGormRepository[T, ID]) prepare(ctx context.Context, condition any, option QueryOption) (*gorm.DB, error) {
	query := repo.query(ctx)
	if condition != nil {
		query = query.Where(condition)
	}
	return repo.buildOption(query, option)
}

func (repo *BaseGormRepository[T, ID]) buildOption(query *gorm.DB, option QueryOption) (*gorm.DB, error) {
	if option.Unscoped {
		query = query.Unscoped()
	}
	for _, preload := range option.Preloads {
		if !preloadPattern.MatchString(preload) {
			return nil, errors.Wrapf(ErrInvalidInput, "invalid preload %q", preload)
		}
		query = query.Preload(preload)
	}
	if len(option.Selects) > 0 {
		if err := validateIdents(option.Selects, "select"); err != nil {
			return nil, err
		}
		query = query.Select(option.Selects)
	}
	if len(option.Omits) > 0 {
		if err := validateIdents(option.Omits, "omit"); err != nil {
			return nil, err
		}
		query = query.Omit(option.Omits...)
	}
	if option.ForUpdate {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	return query, nil
}

func (repo *BaseGormRepository[T, ID]) withReturning(query *gorm.DB) *gorm.DB {
	if repo.DB != nil && repo.DB.Dialector.Name() == "postgres" {
		return query.Clauses(clause.Returning{})
	}
	return query
}

func applyPage(query *gorm.DB, paging PagingInput, option QueryOption) (*gorm.DB, error) {
	paging.Normalize()
	offset := (paging.PageNum - 1) * paging.PageSize
	query = query.Offset(int(offset)).Limit(int(paging.PageSize))

	if paging.Sort == "" && option.Order != "" {
		if !orderPattern.MatchString(option.Order) {
			return nil, errors.Wrapf(ErrInvalidInput, "invalid order %q", option.Order)
		}
		return query.Order(option.Order), nil
	}
	if paging.Sort == "" {
		return query.Order("id ASC"), nil
	}
	if !identPattern.MatchString(paging.Sort) {
		return nil, errors.Wrapf(ErrInvalidInput, "invalid sort field %q", paging.Sort)
	}
	direction := "ASC"
	if paging.SortDesc {
		direction = "DESC"
	}
	return query.Order(paging.Sort + " " + direction), nil
}

func validateIdents(columns []string, kind string) error {
	for _, column := range columns {
		if !identPattern.MatchString(column) {
			return errors.Wrapf(ErrInvalidInput, "invalid %s %q", kind, column)
		}
	}
	return nil
}

func validateAssociation[T any](entity *T, name string) error {
	if entity == nil {
		return errors.Wrap(ErrInvalidInput, "association entity is nil")
	}
	if !preloadPattern.MatchString(name) {
		return errors.Wrapf(ErrInvalidInput, "invalid association %q", name)
	}
	return nil
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrNotFound
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "duplicate key") || strings.Contains(message, "duplicate entry") {
		return Conflict(err)
	}
	return err
}
