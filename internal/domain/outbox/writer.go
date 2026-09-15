package outbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type Writer struct {
	store *Store
	logs  Logger
}

func NewWriter(store *Store, logs Logger) *Writer {
	if store == nil {
		panic("outbox writer: store is nil")
	}
	return &Writer{store: store, logs: logs}
}

// Enqueue inserts a pending event. Pass tx so it commits with the business write.
func (w *Writer) Enqueue(ctx context.Context, tx *gorm.DB, msg Message) (*Event, error) {
	ev, err := newEvent(msg)
	if err != nil {
		return nil, err
	}
	if err := w.store.Insert(ctx, tx, ev); err != nil {
		return nil, err
	}
	return ev, nil
}

// LogEnqueue writes an enqueue record to Postgres and/or Mongo. Call after the
// business transaction commits so the log does not see an uncommitted row.
func (w *Writer) LogEnqueue(ctx context.Context, ev *Event) {
	if w.logs == nil || ev == nil {
		return
	}
	_ = w.logs.Write(ctx, LogEntry{
		OutboxID: ev.ID,
		Broker:   ev.Broker,
		Action:   ActionEnqueue,
		Status:   ev.Status,
		Detail: map[string]any{
			"eventType":  ev.EventType,
			"routingKey": ev.RoutingKey,
			"exchange":   ev.Exchange,
		},
	})
}

func newEvent(msg Message) (*Event, error) {
	if msg.EventType == "" {
		return nil, errors.New("event type is required")
	}
	if msg.RoutingKey == "" {
		return nil, errors.New("routing key is required")
	}
	broker := msg.Broker
	if broker == "" {
		broker = BrokerRabbitMQ
	}
	if broker != BrokerRabbitMQ && broker != BrokerRedis {
		return nil, errors.Errorf("unsupported broker %q", broker)
	}
	body, err := marshalPayload(msg.Payload)
	if err != nil {
		return nil, err
	}
	headers := msg.Headers
	if headers == nil {
		headers = map[string]string{}
	}
	now := time.Now()
	return &Event{
		ID:         uuid.NewString(),
		EventType:  msg.EventType,
		Payload:    body,
		Headers:    headers,
		Status:     StatusPending,
		Broker:     broker,
		Exchange:   msg.Exchange,
		RoutingKey: msg.RoutingKey,
		CreatedAt:  now,
		UpdatedAt:  now,
	}, nil
}

func marshalPayload(payload any) (json.RawMessage, error) {
	switch v := payload.(type) {
	case nil:
		return nil, errors.New("payload is nil")
	case json.RawMessage:
		if len(v) == 0 {
			return nil, errors.New("payload is empty")
		}
		return v, nil
	case []byte:
		if len(v) == 0 {
			return nil, errors.New("payload is empty")
		}
		if !json.Valid(v) {
			encoded, err := json.Marshal(string(v))
			return encoded, err
		}
		return v, nil
	default:
		body, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Wrap(err, "marshal outbox payload")
		}
		return body, nil
	}
}
