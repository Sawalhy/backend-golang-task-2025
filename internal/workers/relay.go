package workers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/metrics"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/tracing"
)

// Relay drains the outbox into RabbitMQ.
//
// It is the second half of the outbox pattern. The order transaction wrote both
// the order and its event atomically; the relay's only job is to get the event
// onto the broker and record that it did. It understands nothing about orders —
// it reads a routing key and a payload and publishes them.
//
// Delivery is AT-LEAST-ONCE, and that is a choice, not a limitation. The relay
// can crash after the broker acks but before the commit marking sent_at, in
// which case the event publishes twice. Every consumer therefore dedupes on
// event_id. The alternative ordering loses events instead, which is far worse:
// a duplicate order.paid sends a second email, a lost one means the customer is
// never told and the order never fulfils.
type Relay struct {
	store     *repository.Store
	pub       *Publisher
	interval  time.Duration
	batchSize int
	log       *slog.Logger
}

func NewRelay(store *repository.Store, pub *Publisher, interval time.Duration, batchSize int, log *slog.Logger) *Relay {
	if batchSize <= 0 {
		batchSize = 100
	}
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return &Relay{store: store, pub: pub, interval: interval, batchSize: batchSize, log: log}
}

// Run polls until ctx is cancelled.
//
// Polling, not LISTEN/NOTIFY. NOTIFY is not durable — a notification fired while
// the relay is restarting is simply gone, so a poll loop would be needed as a
// backstop anyway, and then it is the only thing actually providing the
// guarantee. One mechanism beats two, one of which is decorative.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.log.Info("relay started", "interval", r.interval, "batch_size", r.batchSize)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("relay stopping")
			return nil

		case <-ticker.C:
			// Drain fully rather than one batch per tick, so a backlog clears at
			// broker speed instead of batchSize per interval.
			for {
				n, err := r.DrainOnce(ctx)
				if err != nil {
					// Log and keep going. A broker blip must not kill the relay;
					// unsent rows stay unsent and the next tick retries them.
					r.log.Error("relay batch failed", "error", err)
					break
				}
				if n < r.batchSize {
					break
				}
			}
		}
	}
}

// DrainOnce claims a batch, publishes it, and marks it sent. Exported so a test
// or an operator can drain deterministically instead of waiting on the ticker.
//
// DELIBERATE EXCEPTION to "never do network I/O inside a transaction": the
// publish happens while the claiming transaction is still open. That rule exists
// because a lock held across a slow call blocks every other writer of those rows
// — but these rows are claimed with FOR UPDATE SKIP LOCKED, so a second relay
// instance steps over them instantly rather than waiting. Nobody blocks, so the
// cost the rule protects against does not arise here.
//
// The alternative is worse. Committing the claim, publishing, then updating in a
// second transaction re-creates the dual write the outbox exists to remove: a
// crash between the two leaves rows claimed-but-unpublished with nothing to
// recover them.
func (r *Relay) DrainOnce(ctx context.Context) (int, error) {
	var published int

	err := r.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		rows, err := r.store.Outbox().ClaimUnsent(ctx, tx, r.batchSize)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}

		sent := make([]uint64, 0, len(rows))
		for _, row := range rows {
			// Resume the trace the ORDER REQUEST started, not the relay's own. The
			// publish then hangs under the POST that caused it, minutes later and
			// in a different process, which is the entire point of storing the
			// traceparent on the row.
			pubCtx, span := tracing.Tracer().Start(
				tracing.WithTraceparent(ctx, row.TraceID),
				"outbox publish "+row.RoutingKey,
				trace.WithSpanKind(trace.SpanKindProducer),
				trace.WithAttributes(
					attribute.String("messaging.system", "rabbitmq"),
					attribute.String("messaging.destination.name", row.RoutingKey),
					attribute.String("messaging.message.id", row.EventID.String()),
				))

			err := r.pub.PublishConfirmed(pubCtx, row.RoutingKey, row.EventID.String(), row.Payload)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()

			if err != nil {
				// Stop at the first failure. Marking the ones already confirmed
				// is still correct, and the rest stay unsent for the next pass.
				r.log.Error("publish failed, deferring remainder",
					"event_id", row.EventID, "routing_key", row.RoutingKey, "error", err)
				break
			}
			sent = append(sent, row.ID)
		}

		// sent_at is only set for events the broker actually ACKED.
		if err := r.store.Outbox().MarkSent(ctx, tx, sent); err != nil {
			return err
		}
		if len(sent) < len(rows) {
			failed := make([]uint64, 0, len(rows)-len(sent))
			for _, row := range rows[len(sent):] {
				failed = append(failed, row.ID)
			}
			if err := r.store.Outbox().RecordFailure(ctx, tx, failed); err != nil {
				return err
			}
		}

		published = len(sent)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("draining outbox: %w", err)
	}

	if published > 0 {
		r.log.Debug("relay published batch", "count", published)
	}
	return published, nil
}

// ReportOutboxLag logs outbox depth and the age of the oldest unsent event.
//
// Count alone is ambiguous: 500 rows moving fast is healthy, three rows stuck
// for ten minutes is an outage. Age is the signal that distinguishes them, and
// it is the first thing to alert on — a rising oldest_age_seconds is exactly
// what a wedged relay looks like from the outside.
//
// Deliberately not a method on Relay: it depends only on the database, so it
// must keep reporting while the broker is down and the relay is between
// sessions. That is precisely when you most want to see it.
func ReportOutboxLag(ctx context.Context, store *repository.Store, log *slog.Logger) {
	pending, err := store.Outbox().PendingCount(ctx)
	if err != nil {
		log.Error("reading outbox depth", "error", err)
		return
	}
	age, err := store.Outbox().OldestUnsentAge(ctx)
	if err != nil {
		log.Error("reading outbox lag", "error", err)
		return
	}

	// The same two numbers the log line carries, published for Prometheus. Read
	// as a PAIR: both climbing is a dead relay or an unreachable broker, count
	// flat while age climbs is a single poison row blocking the queue behind it.
	metrics.OutboxPendingRows.Set(float64(pending))
	metrics.OutboxOldestUnsentSeconds.Set(age.Seconds())

	log.Info("outbox lag", "pending", pending, "oldest_age_seconds", age.Seconds())
}
