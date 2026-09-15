package database

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

const redisPingTimeout = 5 * time.Second

var (
	rdb       *redis.Client
	redisOnce sync.Once
)

// NewRedisConnection initializes the singleton Redis connection (with timeout + ping).
// Only a single Redis client is supported.
func NewRedisConnection(redisURL string) (*redis.Client, error) {
	var initErr error

	redisOnce.Do(func() {
		opt, err := redis.ParseURL(redisURL)
		if err != nil {
			initErr = errors.Wrap(err, "couldn't parse redis url")
			return
		}

		opt.ReadTimeout = 3 * time.Second
		opt.WriteTimeout = 3 * time.Second
		opt.ConnMaxIdleTime = 5 * time.Minute
		opt.MinIdleConns = 10
		opt.PoolSize = 100
		opt.PoolTimeout = 15 * time.Second
		opt.ConnMaxLifetime = 30 * time.Minute

		client := redis.NewClient(opt)

		ctx, cancel := context.WithTimeout(context.Background(), redisPingTimeout)
		defer cancel()

		if err := client.Ping(ctx).Err(); err != nil {
			initErr = errors.Wrap(err, "ping redis failed")
			return
		}

		rdb = client
		log.Println("Redis database connected.")
	})

	if initErr != nil {
		return nil, initErr
	}
	return rdb, nil
}
