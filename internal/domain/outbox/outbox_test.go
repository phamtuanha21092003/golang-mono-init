package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestWriterEnqueueAndClaim(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	writer := NewWriter(store, NopLogger())

	ev, err := writer.Enqueue(ctx, nil, Message{
		EventType:  "order.created",
		Payload:    map[string]int{"id": 1},
		Broker:     BrokerRedis,
		RoutingKey: "orders",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != StatusPending || ev.Broker != BrokerRedis {
		t.Fatalf("event = %+v", ev)
	}

	claimed, err := store.Claim(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != ev.ID || claimed[0].Status != StatusPublishing {
		t.Fatalf("claimed = %+v", claimed)
	}

	second, err := store.Claim(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("expected empty claim, got %d", len(second))
	}

	if err := store.MarkPublished(ctx, ev.ID); err != nil {
		t.Fatal(err)
	}
	var row Event
	if err := store.DB().First(&row, "id = ?", ev.ID).Error; err != nil {
		t.Fatal(err)
	}
	if row.Status != StatusPublished || row.PublishedAt == nil {
		t.Fatalf("published row = %+v", row)
	}
}

func TestRelayPublishesAndLogs(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	writer := NewWriter(store, NopLogger())
	if _, err := writer.Enqueue(ctx, nil, Message{
		EventType:  "ping",
		Payload:    map[string]string{"k": "v"},
		Broker:     BrokerRabbitMQ,
		Exchange:   "common.exchange",
		RoutingKey: "common.routing-key",
	}); err != nil {
		t.Fatal(err)
	}

	pub := &fakePublisher{}
	logs := &memoryLogger{}
	relay := NewRelay(store, pub, logs, nil, time.Second, 10, 5)
	if err := relay.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if pub.calls != 1 {
		t.Fatalf("publish calls = %d", pub.calls)
	}
	if logs.len() != 1 || logs.last().Action != ActionPublish {
		t.Fatalf("logs = %+v", logs.entries)
	}

	var row Event
	if err := store.DB().First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Status != StatusPublished {
		t.Fatalf("status = %s", row.Status)
	}
}

func TestRelayRetriesThenGivesUp(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	writer := NewWriter(store, NopLogger())
	if _, err := writer.Enqueue(ctx, nil, Message{
		EventType:  "boom",
		Payload:    json.RawMessage(`{"ok":false}`),
		Broker:     BrokerRedis,
		RoutingKey: "jobs",
	}); err != nil {
		t.Fatal(err)
	}

	pub := &fakePublisher{err: errors.New("broker down")}
	relay := NewRelay(store, pub, NopLogger(), nil, time.Second, 10, 2)

	if err := relay.Once(ctx); err != nil {
		t.Fatal(err)
	}
	var row Event
	if err := store.DB().First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Status != StatusPending || row.Attempts != 1 {
		t.Fatalf("after first fail: %+v", row)
	}

	if err := relay.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Status != StatusFailed || row.Attempts != 2 {
		t.Fatalf("after give up: %+v", row)
	}
}

func TestRouterRequiresConfiguredBroker(t *testing.T) {
	router := Router{}
	err := router.Publish(context.Background(), Event{Broker: BrokerRedis})
	if err == nil || !strings.Contains(err.Error(), "redis") {
		t.Fatalf("err = %v", err)
	}
	err = router.Publish(context.Background(), Event{Broker: BrokerRabbitMQ})
	if err == nil || !strings.Contains(err.Error(), "rabbitmq") {
		t.Fatalf("err = %v", err)
	}

	rabbit := &fakePublisher{}
	redis := &fakePublisher{}
	router = Router{Rabbit: rabbit, Redis: redis}
	if err := router.Publish(context.Background(), Event{Broker: BrokerRedis}); err != nil {
		t.Fatal(err)
	}
	if redis.calls != 1 || rabbit.calls != 0 {
		t.Fatalf("rabbit=%d redis=%d", rabbit.calls, redis.calls)
	}
}

func TestFanoutWritesEverySink(t *testing.T) {
	a := &memoryLogger{}
	b := &memoryLogger{}
	if err := (Fanout{a, b}).Write(context.Background(), LogEntry{Action: ActionEnqueue, Status: StatusPending}); err != nil {
		t.Fatal(err)
	}
	if a.len() != 1 || b.len() != 1 {
		t.Fatalf("a=%d b=%d", a.len(), b.len())
	}
	if a.last().ID == "" || a.last().ID != b.last().ID {
		t.Fatalf("ids %q %q", a.last().ID, b.last().ID)
	}
}

func TestEnqueueValidation(t *testing.T) {
	store := newTestStore(t)
	writer := NewWriter(store, NopLogger())
	if _, err := writer.Enqueue(context.Background(), nil, Message{Payload: map[string]int{"n": 1}}); err == nil {
		t.Fatal("expected error")
	}
}

type fakePublisher struct {
	err   error
	calls int
}

func (f *fakePublisher) Publish(context.Context, Event) error {
	f.calls++
	return f.err
}

type memoryLogger struct {
	mu      sync.Mutex
	entries []LogEntry
}

func (m *memoryLogger) Write(_ context.Context, entry LogEntry) error {
	entry = prepareLog(entry)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entry)
	return nil
}

func (m *memoryLogger) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

func (m *memoryLogger) last() LogEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries[len(m.entries)-1]
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	db, err := gorm.Open(sqlite.Open("file:"+name+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&Event{}, &LogEntry{}); err != nil {
		t.Fatal(err)
	}
	return NewStore(db)
}
