package models

import (
	"time"

	"github.com/google/uuid"
)

// Routing keys are <aggregate>.<past-tense-event>, lowercase, dot-separated.
// The broker's topic exchange matches on these; consumers own the bindings, so
// adding a subscriber never touches publishing code.
const (
	EventOrderCreated   = "order.created"
	EventOrderPaid      = "order.paid"
	EventOrderFailed    = "order.failed"
	EventOrderCancelled = "order.cancelled"
	EventOrderExpired   = "order.expired"
	EventOrderFulfilled = "order.fulfilled"
	EventRefundRequested = "payment.refund_requested"
	EventRefundCompleted = "payment.refunded"
)

// Envelope is what goes in the outbox payload and on the wire, unchanged. The
// relay is a dumb pipe: it reads the row, publishes the payload under the row's
// routing key, and knows nothing about what any of it means.
//
// The design specified a ULID for EventID; the schema column is uuid, so this is
// a UUIDv4. Both are collision-free unique ids and only the ULID's sortability is
// lost — which nothing here relies on, because outbox.id already orders events.
type Envelope struct {
	EventID    uuid.UUID      `json:"eventId"`
	EventType  string         `json:"eventType"`
	OccurredAt time.Time      `json:"occurredAt"`
	Aggregate  AggregateRef   `json:"aggregate"`
	Data       map[string]any `json:"data,omitempty"`
}

type AggregateRef struct {
	Type string `json:"type"`
	ID   uint64 `json:"id"`
}

// NewEnvelope builds an event for an order aggregate.
//
// The body carries IDENTIFIERS, not state. By the time a consumer opens this the
// order may have moved on, so the event means "something happened to order 1001,
// go look" — anything authoritative is re-read from Postgres. Putting the order
// status in here and trusting it is how a consumer acts on a world that no longer
// exists.
func NewOrderEvent(eventType string, orderID uint64, data map[string]any) Envelope {
	return Envelope{
		EventID:    uuid.New(),
		EventType:  eventType,
		OccurredAt: time.Now().UTC(),
		Aggregate:  AggregateRef{Type: "order", ID: orderID},
		Data:       data,
	}
}
