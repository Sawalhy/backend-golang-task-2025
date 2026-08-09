package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
)

// Handler processes one event. Returning nil acks the delivery.
//
// Handlers MUST be idempotent. Delivery is at-least-once from the relay and
// again from the broker on redelivery, so every handler will eventually see the
// same event twice. In this codebase that property comes from CAS: the second
// run finds the transition already made and does nothing.
type Handler func(ctx context.Context, ev models.Envelope) error

// RunConsumer consumes a queue with a bounded worker pool until ctx is cancelled.
//
// This is the whole "worker pool" pattern in Go, and it is deliberately small.
// errgroup.SetLimit(n) caps how many handlers run at once, so N deliveries
// become at most N concurrent goroutines rather than an unbounded fan-out that
// would exhaust the database pool the moment a backlog arrives.
//
// Every goroutine started here has a guaranteed exit path: g.Go returns when its
// handler returns, the range ends when the broker closes the delivery channel,
// and g.Wait blocks until all of them are done. A goroutine with no exit path is
// a leak, and a blocked goroutine is never collected.
func RunConsumer(ctx context.Context, b *Broker, queue string, prefetch, concurrency int, h Handler, log *slog.Logger) error {
	ch, deliveries, err := b.Consume(queue, prefetch)
	if err != nil {
		return err
	}
	// Closing the channel is what stops the broker sending more work. Deferred
	// immediately after acquiring it, in the same discipline as rows.Close().
	defer ch.Close()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	log.Info("consumer started", "queue", queue, "prefetch", prefetch, "concurrency", concurrency)

	for {
		select {
		case <-ctx.Done():
			log.Info("consumer draining", "queue", queue)
			// Wait for in-flight handlers rather than abandoning them mid-charge.
			return g.Wait()

		case d, ok := <-deliveries:
			if !ok {
				// The broker closed the stream — connection lost, or the queue
				// was deleted. Drain what is running and report, so the
				// supervisor can reconnect.
				log.Warn("delivery channel closed", "queue", queue)
				return g.Wait()
			}

			g.Go(func() error {
				handleDelivery(gctx, d, queue, h, log)
				// Handler errors are dealt with by ack/nack, not by returning an
				// error here: a single poisoned message must not tear down the
				// whole consumer via errgroup's first-error cancellation.
				return nil
			})
		}
	}
}

func handleDelivery(ctx context.Context, d amqp.Delivery, queue string, h Handler, log *slog.Logger) {
	var ev models.Envelope
	if err := json.Unmarshal(d.Body, &ev); err != nil {
		// Unparseable will never become parseable. Straight to the DLQ rather
		// than round-tripping forever.
		log.Error("discarding unparseable message", "queue", queue, "error", err)
		_ = d.Nack(false, false)
		return
	}

	l := log.With("queue", queue, "event_id", ev.EventID, "event_type", ev.EventType,
		"order_id", ev.Aggregate.ID)

	if err := h(ctx, ev); err != nil {
		// One retry, then dead-letter. Requeueing indefinitely on a message that
		// always fails is an infinite loop that looks like a busy worker; the
		// Redelivered flag is what bounds it without needing a counter.
		requeue := !d.Redelivered
		l.Error("handler failed", "error", err, "requeue", requeue)
		_ = d.Nack(false, requeue)
		return
	}

	// Ack only after the work is committed. Acking first would mean a crash
	// mid-handler loses the job (failure mode E).
	if err := d.Ack(false); err != nil {
		l.Error("acking delivery", "error", err)
	}
}

// PaymentHandler adapts the payment service onto the Handler signature.
func PaymentHandler(process func(ctx context.Context, orderID uint64) error) Handler {
	return func(ctx context.Context, ev models.Envelope) error {
		if ev.Aggregate.ID == 0 {
			return fmt.Errorf("event %s carries no order id", ev.EventID)
		}
		return process(ctx, ev.Aggregate.ID)
	}
}
