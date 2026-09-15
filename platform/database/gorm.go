package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	mysqldriver "gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// SQL access in this module:
//   - GORM (this file + internal/base BaseGormRepository) for entity CRUD
//   - sqlx (sqlx.go) for hand-written SQL on Postgres or MySQL
//   - Mongo (mogo.go + BaseMongoRepository) for documents
//   - Elasticsearch (elasticsearch.go) for search indexes
//   - ClickHouse (clickhouse.go) for analytics
//   - Redis (redis.go) for cache; durable queues live in internal/transport/redisbroker
//   - EMQX (emqx.go) for MQTT publish and subscribe

type SQLDriver string

const (
	Postgres SQLDriver = "postgres"
	MySQL    SQLDriver = "mysql"
)

var (
	GormConn *GormDatabase
	onceGorm sync.Once
)

type GormDatabase struct {
	*gorm.DB
}

func ParseSQLDriver(raw string) (SQLDriver, error) {
	switch SQLDriver(strings.ToLower(strings.TrimSpace(raw))) {
	case "", Postgres:
		return Postgres, nil
	case MySQL:
		return MySQL, nil
	default:
		return "", fmt.Errorf("unsupported sql driver %q", raw)
	}
}

// OpenGorm opens a pooled GORM connection. debug enables info-level SQL logs.
func OpenGorm(driver SQLDriver, uri string, debug bool) (*GormDatabase, error) {
	if uri == "" {
		return nil, errors.New("sql uri is required")
	}

	var dialector gorm.Dialector
	switch driver {
	case "", Postgres:
		dialector = postgres.Open(uri)
	case MySQL:
		dialector = mysqldriver.Open(uri)
	default:
		return nil, fmt.Errorf("unsupported sql driver %q", driver)
	}

	level := logger.Warn
	if debug {
		level = logger.Info
	}

	db, err := gorm.Open(dialector, &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		Logger: logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags),
			logger.Config{
				SlowThreshold:             time.Second,
				LogLevel:                  level,
				IgnoreRecordNotFoundError: true,
				Colorful:                  true,
			},
		),
	})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("sql pool: %w", err)
	}
	sqlDB.SetMaxOpenConns(50)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)

	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &GormDatabase{DB: db}, nil
}

// InitDatabaseConn opens the singleton Postgres connection. A failed open panics.
func InitDatabaseConn(uri string) *GormDatabase {
	onceGorm.Do(func() {
		db, err := OpenGorm(Postgres, uri, true)
		if err != nil {
			panic(fmt.Sprintf("Unable to connection to database: %v\n", err))
		}
		GormConn = db
	})
	return GormConn
}

func (db *GormDatabase) Close() error {
	if db == nil || db.DB == nil {
		return nil
	}
	sqlDB, err := db.DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func (db *GormDatabase) PingContext(ctx context.Context) error {
	if db == nil || db.DB == nil {
		return errors.New("database is not connected")
	}
	sqlDB, err := db.DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

func (db GormDatabase) PrePing() error {
	var err error
	for range 3 {
		err = db.DB.Raw("SELECT 1").Error
		if err == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return err
}

func CreateTestDatabase(databaseURL string) {
	u, err := url.Parse(databaseURL)
	if err != nil {
		panic(err)
	}
	user := u.User.Username()
	password, _ := u.User.Password()
	databaseName := strings.ReplaceAll(u.Path, "/", "")
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		user, password, u.Host, "postgres")

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec("DROP DATABASE IF EXISTS " + databaseName + " WITH (FORCE);")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("CREATE DATABASE " + databaseName)
	if err != nil {
		panic(err)
	}
}

func (s *GormDatabase) RecordNotFound(err error) bool {
	return err != nil && errors.Is(err, gorm.ErrRecordNotFound)
}

func (s *GormDatabase) ErrorQuery(err error) bool {
	return err != nil && !errors.Is(err, gorm.ErrRecordNotFound)
}
