package bootstrap

import (
	"context"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"go-init/internal/config"
	"go-init/platform/database"
	"go-init/platform/logger"
)

// Resources opens the optional SQL and Redis dependencies described by the environment.
type Resources struct {
	Log           *zap.Logger
	Stack         *config.Stack
	SQL           *database.GormDatabase
	SQLX          *database.SqlxDatabase
	Redis         *redis.Client
	RedisCache    *redis.Client
	Mongo         *database.MongoInstance
	Elasticsearch *database.Elasticsearch
	ClickHouse    *database.ClickHouse
	MQTT          *database.MQTTPool
}

func Open() (*Resources, error) {
	stack, err := config.LoadStack()
	if err != nil {
		return nil, err
	}
	if err := logger.SetUpLogger(stack.Debug); err != nil {
		return nil, err
	}
	log := logger.GetLogger()

	resources := &Resources{Log: log, Stack: stack}
	if stack.SQLURI != "" {
		driver, err := database.ParseSQLDriver(stack.SQLDriver)
		if err != nil {
			return nil, err
		}
		sqlDB, err := database.OpenGorm(driver, stack.SQLURI, stack.Debug)
		if err != nil {
			return nil, err
		}
		resources.SQL = sqlDB
		sqlxDB, err := database.OpenSQLX(driver, stack.SQLURI)
		if err != nil {
			_ = resources.Close()
			return nil, err
		}
		resources.SQLX = sqlxDB
		log.Info("sql connected", zap.String("driver", string(driver)))
	}
	if stack.RedisURI != "" {
		client, err := database.OpenRedis(stack.RedisURI)
		if err != nil {
			_ = resources.Close()
			return nil, err
		}
		resources.Redis = client
		log.Info("redis connected")
	}
	if stack.RedisCacheURI != "" {
		client, err := database.OpenRedis(stack.RedisCacheURI)
		if err != nil {
			_ = resources.Close()
			return nil, err
		}
		resources.RedisCache = client
		log.Info("redis cache connected")
	}
	if stack.MongoURI != "" {
		mongoDB, err := database.NewMongoConnection(stack.MongoURI)
		if err != nil {
			_ = resources.Close()
			return nil, err
		}
		resources.Mongo = mongoDB
		log.Info("mongo connected")
	}
	if stack.ElasticsearchURI != "" {
		es, err := database.OpenElasticsearch(stack.ElasticsearchURI)
		if err != nil {
			_ = resources.Close()
			return nil, err
		}
		resources.Elasticsearch = es
		log.Info("elasticsearch connected")
	}
	if stack.ClickHouseURI != "" {
		ch, err := database.OpenClickHouse(stack.ClickHouseURI)
		if err != nil {
			_ = resources.Close()
			return nil, err
		}
		resources.ClickHouse = ch
		log.Info("clickhouse connected")
	}
	if stack.MQTTURI != "" {
		pool, err := database.OpenMQTT(stack.MQTTURI, stack.MQTTPoolSize)
		if err != nil {
			_ = resources.Close()
			return nil, err
		}
		resources.MQTT = pool
		log.Info("mqtt connected")
	}
	return resources, nil
}

func (r *Resources) Close() error {
	if r == nil {
		return nil
	}
	if r.SQL != nil {
		_ = r.SQL.Close()
	}
	if r.SQLX != nil {
		_ = r.SQLX.Close()
	}
	if r.Redis != nil {
		_ = r.Redis.Close()
	}
	if r.RedisCache != nil {
		_ = r.RedisCache.Close()
	}
	if r.Mongo != nil && r.Mongo.Client != nil {
		_ = r.Mongo.Client.Disconnect(context.Background())
	}
	if r.ClickHouse != nil {
		_ = r.ClickHouse.Close()
	}
	if r.MQTT != nil {
		r.MQTT.Close()
	}
	return logger.Sync()
}

func (r *Resources) Checks() []func(context.Context) error {
	if r == nil {
		return nil
	}
	var checks []func(context.Context) error
	if r.SQL != nil {
		sqlDB := r.SQL
		checks = append(checks, sqlDB.PingContext)
	}
	if r.SQLX != nil {
		sqlxDB := r.SQLX
		checks = append(checks, sqlxDB.PingContext)
	}
	if r.Redis != nil {
		client := r.Redis
		checks = append(checks, func(ctx context.Context) error {
			return client.Ping(ctx).Err()
		})
	}
	if r.RedisCache != nil {
		client := r.RedisCache
		checks = append(checks, func(ctx context.Context) error {
			return client.Ping(ctx).Err()
		})
	}
	if r.Mongo != nil && r.Mongo.Client != nil {
		client := r.Mongo.Client
		checks = append(checks, func(ctx context.Context) error {
			return client.Ping(ctx, nil)
		})
	}
	if r.Elasticsearch != nil {
		es := r.Elasticsearch
		checks = append(checks, es.Ping)
	}
	if r.ClickHouse != nil {
		ch := r.ClickHouse
		checks = append(checks, ch.Ping)
	}
	if r.MQTT != nil {
		pool := r.MQTT
		checks = append(checks, pool.Ping)
	}
	return checks
}
