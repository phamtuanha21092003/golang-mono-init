package rabbitmq

import (
	"testing"
	"time"

	"go-init/internal/base"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestComputeRetryDelay(t *testing.T) {
	baseDelay := 5 * time.Second
	maxDelay := 2 * time.Minute

	if got := computeRetryDelay(1, baseDelay, maxDelay); got != 5*time.Second {
		t.Fatalf("retry 1: got %s", got)
	}
	if got := computeRetryDelay(2, baseDelay, maxDelay); got != 10*time.Second {
		t.Fatalf("retry 2: got %s", got)
	}
	if got := computeRetryDelay(3, baseDelay, maxDelay); got != 20*time.Second {
		t.Fatalf("retry 3: got %s", got)
	}
	if got := computeRetryDelay(10, baseDelay, maxDelay); got != maxDelay {
		t.Fatalf("retry 10 should cap: got %s", got)
	}
}

func TestRetryCountFromHeaders(t *testing.T) {
	if n := retryCountFromHeaders(nil); n != 0 {
		t.Fatalf("nil headers: %d", n)
	}
	if n := retryCountFromHeaders(amqp.Table{base.RetryCountHeader: "3"}); n != 3 {
		t.Fatalf("string: %d", n)
	}
	if n := retryCountFromHeaders(amqp.Table{base.RetryCountHeader: int32(2)}); n != 2 {
		t.Fatalf("int32: %d", n)
	}
}

func TestNormalizeConsumeOptionsDefaults(t *testing.T) {
	opts := normalizeConsumeOptions(ConsumeOptions{Queue: "jobs"})
	if opts.Prefetch != base.PrefetchCount {
		t.Fatalf("prefetch: %d", opts.Prefetch)
	}
	if opts.MaxRetry != 5 {
		t.Fatalf("max retry: %d", opts.MaxRetry)
	}
	if opts.BindingKey != "jobs" {
		t.Fatalf("binding key: %s", opts.BindingKey)
	}
	if opts.ConsumerTag != "" {
		t.Fatalf("tag should stay empty so Consume can assign a unique one, got %q", opts.ConsumerTag)
	}
	if opts.RetryBaseDelay != 5*time.Second || opts.RetryMaxDelay != 2*time.Minute {
		t.Fatalf("retry delays: %s %s", opts.RetryBaseDelay, opts.RetryMaxDelay)
	}
}

func TestNewID(t *testing.T) {
	a, b := newID(), newID()
	if len(a) != 32 || len(b) != 32 {
		t.Fatalf("expected 32 hex chars, got %q %q", a, b)
	}
	if a == b {
		t.Fatal("ids should be unique")
	}
}

func TestRetryQueueName(t *testing.T) {
	if got := retryQueueName("jobs"); got != "jobs.retry" {
		t.Fatalf("got %s", got)
	}
}

func TestMarshalPayload(t *testing.T) {
	b, err := marshalPayload([]byte("raw"))
	if err != nil || string(b) != "raw" {
		t.Fatalf("bytes: %s %v", b, err)
	}
	b, err = marshalPayload("hi")
	if err != nil || string(b) != "hi" {
		t.Fatalf("string: %s %v", b, err)
	}
	b, err = marshalPayload(map[string]int{"n": 1})
	if err != nil || string(b) != `{"n":1}` {
		t.Fatalf("json: %s %v", b, err)
	}
}
