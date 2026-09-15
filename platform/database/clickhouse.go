package database

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pkg/errors"
)

// ClickHouse is the native client used by nethub-posthog's analytics store.
type ClickHouse struct {
	Conn driver.Conn
}

// OpenClickHouse accepts a DSN such as clickhouse://user:pass@host:9000/analytics
// or an HTTP URL http://host:8123/analytics.
func OpenClickHouse(raw string) (*ClickHouse, error) {
	opts, err := parseClickHouse(raw)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "open clickhouse")
	}
	ctx, cancel := context.WithTimeout(context.Background(), opts.DialTimeout)
	defer cancel()
	ch := &ClickHouse{Conn: conn}
	if err := ch.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return ch, nil
}

func (c *ClickHouse) Ping(ctx context.Context) error {
	if c == nil || c.Conn == nil {
		return errors.New("clickhouse is not connected")
	}
	if err := c.Conn.Ping(ctx); err != nil {
		return errors.Wrap(err, "clickhouse ping")
	}
	return nil
}

func (c *ClickHouse) Exec(ctx context.Context, query string, args ...any) error {
	if err := c.Conn.Exec(ctx, query, args...); err != nil {
		return errors.Wrap(err, "clickhouse exec")
	}
	return nil
}

func (c *ClickHouse) Select(ctx context.Context, dest any, query string, args ...any) error {
	if err := c.Conn.Select(ctx, dest, query, args...); err != nil {
		return errors.Wrap(err, "clickhouse select")
	}
	return nil
}

func (c *ClickHouse) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	rows, err := c.Conn.Query(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "clickhouse query")
	}
	return rows, nil
}

func (c *ClickHouse) PrepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	batch, err := c.Conn.PrepareBatch(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "clickhouse prepare batch")
	}
	return batch, nil
}

func (c *ClickHouse) Close() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

func parseClickHouse(raw string) (*clickhouse.Options, error) {
	if raw == "" {
		return nil, errors.New("clickhouse url is required")
	}
	opts, err := clickhouse.ParseDSN(raw)
	if err != nil {
		return nil, errors.Wrap(err, "parse clickhouse url")
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 5 * time.Second
	}
	return opts, nil
}
