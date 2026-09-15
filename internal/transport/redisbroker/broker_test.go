package redisbroker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func TestPublishAndConsumeAck(t *testing.T) {
	broker, client := testBroker(t)
	ctx := context.Background()
	const stream = "events"
	const group = "workers"

	if err := broker.EnsureGroup(ctx, stream, group); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.Publish(ctx, stream, map[string]string{"kind": "ping"}); err != nil {
		t.Fatal(err)
	}

	got := make(chan Message, 1)
	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- broker.Consume(consumeCtx, ConsumeOptions{
			Stream:         stream,
			Group:          group,
			Consumer:       "test",
			Block:          50 * time.Millisecond,
			HandlerTimeout: -1,
		}, func(ctx context.Context, msg Message) error {
			got <- msg
			cancel()
			return nil
		})
	}()

	select {
	case msg := <-got:
		if string(msg.Body) == "" || msg.Attempt != 0 {
			t.Fatalf("message = %+v body=%s", msg, msg.Body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not stop")
	}

	pending, err := client.XPending(ctx, stream, group).Result()
	if err != nil {
		t.Fatal(err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending = %d", pending.Count)
	}
}

func TestFailedHandlerGoesToDLQ(t *testing.T) {
	broker, client := testBroker(t)
	ctx := context.Background()
	const stream = "jobs"
	const group = "workers"

	if err := broker.EnsureGroup(ctx, stream, group); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.Publish(ctx, stream, "payload"); err != nil {
		t.Fatal(err)
	}

	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- broker.Consume(consumeCtx, ConsumeOptions{
			Stream:         stream,
			Group:          group,
			Consumer:       "test",
			DLQStream:      stream + ".dlq",
			MaxAttempts:    1,
			Block:          50 * time.Millisecond,
			HandlerTimeout: -1,
		}, func(ctx context.Context, msg Message) error {
			return errors.New("boom")
		})
	}()

	deadline := time.Now().Add(3 * time.Second)
	for {
		rows, err := client.XRange(ctx, stream+".dlq", "-", "+").Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) > 0 {
			cancel()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for dlq")
		}
		time.Sleep(20 * time.Millisecond)
	}

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not stop")
	}
}

func testBroker(t *testing.T) (*Broker, *redis.Client) {
	t.Helper()
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return New(client, zap.NewNop()), client
}
