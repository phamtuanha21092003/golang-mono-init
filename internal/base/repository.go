package base

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type IBaseRepository[T IMongoModel[ID], ID any] interface {
	MogoCollection() *mongo.Collection
	BatchCreate(ctx context.Context, entities []T) error
	Create(ctx context.Context, entity T) (*mongo.InsertOneResult, error)
	FindByID(ctx context.Context, id ID) (T, error)
	First(ctx context.Context, conditions []Condition) (T, error)
	FindAll(ctx context.Context, conditions []Condition) ([]T, error)
	FindPaged(ctx context.Context, conditions []Condition, paging PagingInput) (*PagingResponseDto, error)
	Update(ctx context.Context, id ID, entity T) error
	UpdateOneByID(ctx context.Context, id ID, entity map[string]any) error
	Delete(ctx context.Context, id ID) error
}

type BaseRepository[T IMongoModel[ID], ID any] struct {
	Collection string
	DB         *mongo.Database
}

func NewBaseRepository[T IMongoModel[ID], ID any](db *mongo.Database, collection string) *BaseRepository[T, ID] {
	repo := &BaseRepository[T, ID]{DB: db, Collection: collection}
	// check compile-time with type assertion
	var _ IBaseRepository[T, ID] = repo
	return repo
}

func (r *BaseRepository[T, ID]) MogoCollection() *mongo.Collection {
	return r.DB.Collection(r.Collection)
}

func (r *BaseRepository[T, ID]) collection() *mongo.Collection {
	return r.DB.Collection(r.Collection)
}

// BatchCreate inserts multiple entities into the collection.
func (r *BaseRepository[T, ID]) BatchCreate(ctx context.Context, entities []T) error {
	if len(entities) == 0 {
		return errors.Wrap(ErrInvalidInput, "entities list cannot be empty")
	}

	documents := make([]interface{}, 0, len(entities))
	for _, entity := range entities {
		r.applyTimestamps(entity)
		r.applyGeneratedID(entity)
		documents = append(documents, entity)
	}

	_, err := r.collection().InsertMany(ctx, documents)
	return err
}

// Create inserts a single new entity.
func (r *BaseRepository[T, ID]) Create(ctx context.Context, entity T) (*mongo.InsertOneResult, error) {
	r.applyTimestamps(entity)
	r.applyGeneratedID(entity)

	return r.collection().InsertOne(ctx, entity)
}

// FindByID returns the entity matching the given _id.
func (r *BaseRepository[T, ID]) FindByID(ctx context.Context, id ID) (T, error) {
	var result T
	err := r.collection().FindOne(ctx, bson.M{"_id": id}).Decode(&result)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return result, ErrNotFound
	}
	if err != nil {
		return result, err
	}
	return result, nil
}

// First returns the first entity matching the given conditions.
func (r *BaseRepository[T, ID]) First(ctx context.Context, conditions []Condition) (T, error) {
	var result T
	if len(conditions) == 0 {
		return result, errors.Wrap(ErrInvalidInput, "conditions cannot be empty")
	}

	filter := buildFilter(conditions)
	err := r.collection().FindOne(ctx, filter).Decode(&result)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return result, ErrNotFound
	}

	if err != nil {
		return result, err
	}

	return result, nil
}

// FindAll returns every entity matching the given conditions.
func (r *BaseRepository[T, ID]) FindAll(ctx context.Context, conditions []Condition) ([]T, error) {
	filter := buildFilter(conditions)

	cursor, err := r.collection().Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []T
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

// FindPaged returns entities for a page, with sorting and the total record count.
func (r *BaseRepository[T, ID]) FindPaged(ctx context.Context, conditions []Condition, paging PagingInput) (*PagingResponseDto, error) {
	paging.Normalize()

	filter := buildFilter(conditions)

	totalCount, err := r.collection().CountDocuments(ctx, filter)
	if err != nil {
		return nil, err
	}

	findOptions := options.Find().
		SetLimit(paging.PageSize).
		SetSkip((paging.PageNum - 1) * paging.PageSize)

	if paging.Sort != "" {
		sortOrder := 1
		if paging.SortDesc {
			sortOrder = -1
		}
		findOptions.SetSort(bson.M{paging.Sort: sortOrder})
	}

	cursor, err := r.collection().Find(ctx, filter, findOptions)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []T
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return &PagingResponseDto{
		Data:        results,
		Count:       totalCount,
		PageNum:     paging.PageNum,
		PageSize:    paging.PageSize,
		HasNextPage: paging.PageNum*paging.PageSize < totalCount,
		HasPrevPage: paging.PageNum > 1,
	}, nil
}

// Update one document by id
func (r *BaseRepository[T, ID]) UpdateOneByID(ctx context.Context, id ID, entity map[string]any) error {
	entity["updatedAt"] = time.Now()

	res, err := r.collection().UpdateOne(
		ctx,
		bson.M{"_id": id},
		bson.M{
			"$set": entity,
		},
	)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return ErrNotFound
	}

	return nil
}

// Update replaces the whole document matching the given _id.
func (r *BaseRepository[T, ID]) Update(ctx context.Context, id ID, entity T) error {
	if updater, ok := any(entity).(interface{ SetUpdatedAt() }); ok {
		updater.SetUpdatedAt()
	}

	res, err := r.collection().ReplaceOne(ctx, bson.M{"_id": id}, entity)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

// Delete removes the document matching the given _id.
func (r *BaseRepository[T, ID]) Delete(ctx context.Context, id ID) error {
	res, err := r.collection().DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return err
	}
	if res.DeletedCount == 0 {
		return ErrNotFound
	}
	return nil
}

// applyTimestamps sets timestamps when the entity implements Timestamper.
func (r *BaseRepository[T, ID]) applyTimestamps(entity T) {
	entity.SetTimestamps()
}

// applyGeneratedID generates a new ObjectID when the ID is still the zero value and its type is primitive.ObjectID.
func (r *BaseRepository[T, ID]) applyGeneratedID(entity T) {
	entity.SetID()
}

// buildFilter builds a bson filter from the condition list
func buildFilter(conditions []Condition) bson.M {
	filter := bson.M{}

	for _, cond := range conditions {
		if cond.Operator == "" {
			filter[cond.Field] = cond.Value
			continue
		}

		if _, ok := filter[cond.Field]; !ok {
			filter[cond.Field] = bson.M{}
		}

		filter[cond.Field].(bson.M)[cond.Operator] = cond.Value
	}

	return filter
}
