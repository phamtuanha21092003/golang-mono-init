package outbox

import (
	"encoding/json"
	"time"
)

const (
	BrokerRabbitMQ = "rabbitmq"
	BrokerRedis    = "redis"

	StatusPending    = "pending"
	StatusPublishing = "publishing"
	StatusPublished  = "published"
	StatusFailed     = "failed"

	ActionEnqueue = "enqueue"
	ActionPublish = "publish"
	ActionFail    = "fail"
)

// Event is the transactional outbox row. Write it in the same SQL transaction
// as the business change; the relay publishes later.
type Event struct {
	ID          string            `gorm:"primaryKey;type:varchar(36)" json:"id"`
	EventType   string            `gorm:"not null" json:"eventType"`
	Payload     json.RawMessage   `gorm:"type:text;not null" json:"payload"`
	Headers     map[string]string `gorm:"serializer:json;type:text" json:"headers"`
	Status      string            `gorm:"not null;index:idx_outbox_events_status_created" json:"status"`
	Broker      string            `gorm:"not null" json:"broker"`
	Exchange    string            `json:"exchange"`
	RoutingKey  string            `gorm:"not null" json:"routingKey"`
	Error       json.RawMessage   `gorm:"type:text" json:"error,omitempty"`
	Attempts    int               `gorm:"not null" json:"attempts"`
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	PublishedAt *time.Time        `json:"publishedAt,omitempty"`
}

func (Event) TableName() string {
	return "outbox_events"
}

// Message is what a service passes to Writer.Enqueue.
type Message struct {
	EventType  string
	Payload    any
	Headers    map[string]string
	Broker     string
	Exchange   string
	RoutingKey string
}

type LogEntry struct {
	ID        string         `gorm:"primaryKey;type:varchar(36)" json:"id" bson:"_id"`
	OutboxID  string         `gorm:"index" json:"outboxId" bson:"outboxId"`
	Broker    string         `json:"broker" bson:"broker"`
	Action    string         `json:"action" bson:"action"`
	Status    string         `json:"status" bson:"status"`
	Detail    map[string]any `gorm:"serializer:json;type:text" json:"detail" bson:"detail"`
	CreatedAt time.Time      `json:"createdAt" bson:"createdAt"`
}

func (LogEntry) TableName() string {
	return "outbox_logs"
}
