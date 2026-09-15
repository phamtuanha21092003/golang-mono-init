package dto

import "encoding/json"

// Envelope is a minimal message wrapper so a queue can carry multiple kinds
// without putting business fields on the RabbitMQ handler.
type Envelope struct {
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload,omitempty"`
}
