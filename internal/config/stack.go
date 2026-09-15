package config

import (
	"os"
	"strconv"
	"time"
)

const (
	SQLDriverPostgres = "postgres"
	SQLDriverMySQL    = "mysql"
)

// Stack is the process configuration shared by the HTTP servers and the Redis consumer.
// Empty URIs mean that dependency is not used.
type Stack struct {
	*App

	SQLDriver         string
	SQLURI            string
	RedisURI          string
	RedisCacheURI     string
	RabbitMQURI       string
	MongoURI          string
	ElasticsearchURI  string
	ClickHouseURI     string
	MQTTURI           string
	MQTTPoolSize      int
	Stream            string
	StreamGroup       string
	OutboxPoll        time.Duration
	OutboxBatch       int
	OutboxMaxAttempts int
}

func LoadStack() (*Stack, error) {
	if err := LoadAppCfg(); err != nil {
		return nil, err
	}

	driver := os.Getenv("SQL_DRIVER")
	if driver == "" {
		driver = SQLDriverPostgres
	}

	return &Stack{
		App:               GetAppCfg(),
		SQLDriver:         driver,
		SQLURI:            os.Getenv("SQL_URI"),
		RedisURI:          os.Getenv("REDIS_URI"),
		RedisCacheURI:     os.Getenv("REDIS_CACHE_URI"),
		RabbitMQURI:       os.Getenv("RABBITMQ_URI"),
		MongoURI:          os.Getenv("MONGO_URI"),
		ElasticsearchURI:  os.Getenv("ELASTICSEARCH_URI"),
		ClickHouseURI:     os.Getenv("CLICKHOUSE_URI"),
		MQTTURI:           os.Getenv("MQTT_URI"),
		MQTTPoolSize:      intOr("MQTT_POOL_SIZE", 4),
		Stream:            envOr("REDIS_STREAM", "events"),
		StreamGroup:       envOr("REDIS_GROUP", "workers"),
		OutboxPoll:        durationOr("OUTBOX_POLL", time.Second),
		OutboxBatch:       intOr("OUTBOX_BATCH", 50),
		OutboxMaxAttempts: intOr("OUTBOX_MAX_ATTEMPTS", 5),
	}, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func durationOr(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func intOr(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
