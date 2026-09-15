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
func NewRedisConnection(redisURL string) (*redis.Client, error) {
	var initErr error

	redisOnce.Do(func() {
		client, err := OpenRedis(redisURL)
		if err != nil {
			initErr = err
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

// OpenRedis opens one Redis client. Services that keep a token store and a cache
// call this once per URL.
func OpenRedis(redisURL string) (*redis.Client, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse redis url")
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
		_ = client.Close()
		return nil, errors.Wrap(err, "ping redis failed")
	}
	return client, nil
}
