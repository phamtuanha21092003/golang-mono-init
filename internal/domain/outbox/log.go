package outbox

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"gorm.io/gorm"
)

type Logger interface {
	Write(ctx context.Context, entry LogEntry) error
}

type nopLogger struct{}

func NopLogger() Logger { return nopLogger{} }

func (nopLogger) Write(context.Context, LogEntry) error { return nil }

// Fanout writes the same log to every sink. One sink failing does not skip the rest.
type Fanout []Logger

func (f Fanout) Write(ctx context.Context, entry LogEntry) error {
	entry = prepareLog(entry)
	var first error
	for _, sink := range f {
		if sink == nil {
			continue
		}
		if err := sink.Write(ctx, entry); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func prepareLog(entry LogEntry) LogEntry {
	if entry.ID == "" {
		entry.ID = uuid.NewString()
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	if entry.Detail == nil {
		entry.Detail = map[string]any{}
	}
	return entry
}

type PostgresLogger struct {
	db *gorm.DB
}

func NewPostgresLogger(db *gorm.DB) *PostgresLogger {
	if db == nil {
		panic("outbox postgres logger: db is nil")
	}
	return &PostgresLogger{db: db}
}

func (l *PostgresLogger) Write(ctx context.Context, entry LogEntry) error {
	entry = prepareLog(entry)
	if err := l.db.WithContext(ctx).Create(&entry).Error; err != nil {
		return errors.Wrap(err, "insert outbox log")
	}
	return nil
}

type MongoLogger struct {
	coll *mongo.Collection
}

func NewMongoLogger(db *mongo.Database, collection string) *MongoLogger {
	if db == nil {
		panic("outbox mongo logger: db is nil")
	}
	if collection == "" {
		collection = "outbox_logs"
	}
	return &MongoLogger{coll: db.Collection(collection)}
}

func (l *MongoLogger) Write(ctx context.Context, entry LogEntry) error {
	entry = prepareLog(entry)
	if _, err := l.coll.InsertOne(ctx, entry); err != nil {
		return errors.Wrap(err, "insert outbox mongo log")
	}
	return nil
}

func NewLogSinks(sqlDB *gorm.DB, mongoDB *mongo.Database) Logger {
	var sinks Fanout
	if sqlDB != nil {
		sinks = append(sinks, NewPostgresLogger(sqlDB))
	}
	if mongoDB != nil {
		sinks = append(sinks, NewMongoLogger(mongoDB, "outbox_logs"))
	}
	if len(sinks) == 0 {
		return NopLogger()
	}
	return sinks
}
