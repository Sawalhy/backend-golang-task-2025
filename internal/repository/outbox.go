package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/tracing"
)

type OutboxRepo struct{ *Store }

func (s *Store) Outbox() *OutboxRepo { return &OutboxRepo{s} }

// Enqueue writes an event in the caller's transaction.
//
// This is the whole outbox pattern in one method. The alternative — commit the
// order, then publish to RabbitMQ — is a dual write across two systems with no
// shared transaction: crash in between and the order exists but nothing will
// ever process it (failure mode B). Publishing first is worse, since the event
// may describe an order that never committed.
//
// Writing the event to the same Postgres transaction as the state change makes
// "committed but never queued" unrepresentable. The relay publishes afterwards,
// at-least-once, which is why every consumer dedupes on event_id.
//
// Passing tx here is not optional in spirit: an Enqueue outside the state
// change's transaction is the exact bug this prevents.
// The envelope's own EventID becomes the row's event_id and travels with the
// payload, so the consumer's dedupe key and the stored row agree. Two ids for
// one event would let a consumer dedupe on a value the outbox never recorded.
func (r *OutboxRepo) Enqueue(ctx context.Context, tx *gorm.DB, ev models.Envelope) (uuid.UUID, error) {
	body, err := json.Marshal(ev)
	if err != nil {
		return uuid.Nil, fmt.Errorf("marshalling %s payload: %w", ev.EventType, err)
	}

	// Captured here, inside the caller's transaction, because this is the last
	// point at which the originating request is still on the stack. By the time
	// the relay reads this row the request is long gone, so a trace context not
	// written down now is unrecoverable.
	row := models.Outbox{
		EventID:    ev.EventID,
		RoutingKey: ev.EventType,
		Payload:    body,
		TraceID:    tracing.Traceparent(ctx),
	}
	if err := r.txOrDB(tx).WithContext(ctx).Create(&row).Error; err != nil {
		return uuid.Nil, fmt.Errorf("enqueuing %s: %w", ev.EventType, err)
	}
	return row.EventID, nil
}

// ClaimUnsent takes a batch of unpublished events for this relay instance.
//
// FOR UPDATE SKIP LOCKED is what allows two relay instances to run without
// double-publishing: the rows one instance holds are invisible to the other for
// the life of the transaction.
//
// ORDER BY id keeps publication roughly in causal order. Roughly is honest —
// with concurrent relays it is not a total order, which is why consumers must
// not assume event ordering.
//
// The predicate matches the partial index outbox_unsent exactly. Writing
// `WHERE sent_at = $1` with a NULL argument would look equivalent and would
// never use the index — Postgres has to prove the implication at plan time.
func (r *OutboxRepo) ClaimUnsent(ctx context.Context, tx *gorm.DB, limit int) ([]models.Outbox, error) {
	var out []models.Outbox
	err := r.txOrDB(tx).WithContext(ctx).Raw(`
		SELECT * FROM outbox
		 WHERE sent_at IS NULL
		 ORDER BY id
		 FOR UPDATE SKIP LOCKED
		 LIMIT ?`, limit).Scan(&out).Error
	if err != nil {
		return nil, fmt.Errorf("claiming outbox rows: %w", err)
	}
	return out, nil
}

// MarkSent is called only after the broker has acknowledged the publish.
//
// The ordering is load-bearing. A plain publish() is a socket write, not a
// delivery guarantee; marking sent_at on the back of it means a broker crash
// between the write and the ack silently drops the event and the outbox believes
// it was delivered. Confirm first, then mark — which makes duplicates possible
// and losses impossible, the correct direction to fail.
func (r *OutboxRepo) MarkSent(ctx context.Context, tx *gorm.DB, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	err := r.txOrDB(tx).WithContext(ctx).
		Exec(`UPDATE outbox SET sent_at = now() WHERE id IN ?`, ids).Error
	if err != nil {
		return fmt.Errorf("marking %d outbox rows sent: %w", len(ids), err)
	}
	return nil
}

// RecordFailure bumps the attempt counter for events the broker refused, so a
// poison event is visible in metrics rather than retried silently forever.
func (r *OutboxRepo) RecordFailure(ctx context.Context, tx *gorm.DB, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	err := r.txOrDB(tx).WithContext(ctx).
		Exec(`UPDATE outbox SET attempts = attempts + 1 WHERE id IN ?`, ids).Error
	if err != nil {
		return fmt.Errorf("recording outbox failure: %w", err)
	}
	return nil
}

// PendingCount backs the relay's lag metric: a backlog that grows without bound
// means the relay is down or the broker is rejecting, and it is the first signal
// that async processing has stalled.
func (r *OutboxRepo) PendingCount(ctx context.Context) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).
		Raw(`SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&n).Error
	if err != nil {
		return 0, fmt.Errorf("counting unsent outbox rows: %w", err)
	}
	return n, nil
}

// OldestUnsentAge reports how long the oldest unpublished event has waited.
// Count alone is ambiguous — 500 rows moving fast is healthy, 3 rows stuck for
// ten minutes is not.
func (r *OutboxRepo) OldestUnsentAge(ctx context.Context) (time.Duration, error) {
	var secs *float64
	err := r.db.WithContext(ctx).Raw(`
		SELECT EXTRACT(EPOCH FROM (now() - min(created_at)))
		  FROM outbox WHERE sent_at IS NULL`).Scan(&secs).Error
	if err != nil {
		return 0, fmt.Errorf("measuring outbox lag: %w", err)
	}
	if secs == nil {
		return 0, nil // nothing pending
	}
	return time.Duration(*secs * float64(time.Second)), nil
}
