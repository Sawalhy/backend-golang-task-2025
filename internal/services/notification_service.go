package services

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
)

// Notifier is the port for an actual delivery channel. Real implementations
// would wrap SendGrid, Twilio and so on; the interface is what keeps the retry,
// lease and dedupe logic testable without a network.
type Notifier interface {
	Send(ctx context.Context, channel, kind string, orderID uint64) error
	Channel() string
}

const maxNotificationAttempts = 5

// NotificationService delivers one notification per (order, channel, kind).
//
// Failure mode I is "sent twice, or never", and the two halves need different
// mechanisms:
//
//	never  -> the row is written first and only marked SENT after delivery, so a
//	          crash leaves a claimable row rather than a forgotten intent.
//	twice  -> the unique index on (order_id, channel, kind) means a duplicate
//	          event cannot create a second row, and the lease means two workers
//	          cannot hold the same row at once.
//
// Exactly-once delivery to an external system is not actually achievable — we
// cannot send an email and record that we sent it in one atomic step. What this
// buys is at-least-once with a very small duplicate window, which for
// notifications is the correct side to err on.
type NotificationService struct {
	store    *repository.Store
	notifier Notifier
	lease    time.Duration
	log      *slog.Logger
}

func NewNotificationService(store *repository.Store, notifier Notifier, lease time.Duration, log *slog.Logger) *NotificationService {
	if lease <= 0 {
		lease = 60 * time.Second
	}
	return &NotificationService{store: store, notifier: notifier, lease: lease, log: log}
}

// Handle processes one event for this service's channel.
func (s *NotificationService) Handle(ctx context.Context, ev models.Envelope) error {
	kind, ok := notificationKind(ev.EventType)
	if !ok {
		return nil // not a notifiable event for this channel
	}
	orderID := ev.Aggregate.ID
	channel := s.notifier.Channel()

	// Step 1: record the intent. Committed before any sending, so the intent
	// survives a crash.
	var (
		notification *models.Notification
		claimed      bool
	)
	err := s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		n, _, err := s.store.Notifications().Ensure(ctx, tx, orderID, channel, kind)
		if err != nil {
			return err
		}
		if n.Status == models.NotificationSent {
			return nil // already delivered; a duplicate event
		}

		ok, err := s.store.Notifications().Claim(ctx, tx, n.ID, s.lease)
		if err != nil {
			return err
		}
		notification, claimed = n, ok
		return nil
	})
	if err != nil {
		return fmt.Errorf("preparing %s notification for order %d: %w", channel, orderID, err)
	}
	if notification == nil || !claimed {
		// Either already sent, or another worker holds a live lease. Both mean
		// there is nothing for this delivery to do.
		return nil
	}
	// Ensure read the row before Claim incremented the counter, so the in-memory
	// copy is one behind the database. Corrected here rather than re-read, so the
	// log line reports the attempt that is actually about to happen.
	notification.Attempts++

	// Steps 2 and 3: send, then record the outcome.
	return s.deliver(ctx, notification)
}

// RetryFailed re-drives notifications whose send failed and whose attempt budget
// is not yet spent, returning how many it attempted.
//
// This exists because the event-driven path cannot retry itself: by the time a
// send fails, the broker delivery has been acked and the message is gone. The
// row is left UNCLAIMED, and only a scan can find it again — the same reason the
// reaper is a timer rather than a consumer. Nothing publishes an event to
// announce that an email did not arrive.
//
// The caller's tick interval IS the backoff. Retrying in a tight loop would burn
// all five attempts inside a second, which for a transport that is briefly down
// means five failures and a permanently FAILED row.
func (s *NotificationService) RetryFailed(ctx context.Context, limit int) (int, error) {
	channel := s.notifier.Channel()

	rows, err := s.store.Notifications().ClaimRetryable(ctx, channel, s.lease, maxNotificationAttempts, limit)
	if err != nil {
		return 0, err
	}

	for i := range rows {
		// Sequential on purpose. This is a backlog of things that already failed;
		// draining it fast enough to compete with live traffic for pool
		// connections would make a transport blip into a database problem.
		if err := s.deliver(ctx, &rows[i]); err != nil {
			// One row failing to record its outcome must not abandon the rest —
			// they hold live leases and would otherwise wait out the full TTL.
			s.log.ErrorContext(ctx, "recording notification retry outcome",
				"notification_id", rows[i].ID, "channel", channel, "error", err)
		}
	}

	if len(rows) > 0 {
		s.log.InfoContext(ctx, "retried failed notifications", "channel", channel, "count", len(rows))
	}
	return len(rows), nil
}

// deliver sends a claimed notification and records what happened. Shared by the
// event path and the retry sweep, so a failure is recorded identically however
// the row was claimed — the divergence that let retries go missing in the first
// place.
//
// The send is outside any transaction: it is a network call, and holding a row
// lock across it is the mistake rule 4 exists to prevent.
func (s *NotificationService) deliver(ctx context.Context, n *models.Notification) error {
	sendErr := s.notifier.Send(ctx, n.Channel, n.Kind, n.OrderID)

	return s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		if sendErr != nil {
			s.log.WarnContext(ctx, "notification send failed",
				"order_id", n.OrderID, "channel", n.Channel,
				"attempt", n.Attempts, "max_attempts", maxNotificationAttempts,
				"error", sendErr)
			return s.store.Notifications().MarkFailed(ctx, tx, n.ID, sendErr.Error(), maxNotificationAttempts)
		}
		return s.store.Notifications().MarkSent(ctx, tx, n.ID)
	})
}

// SweepExpiredLeases reclaims rows abandoned by workers that died mid-send.
func (s *NotificationService) SweepExpiredLeases(ctx context.Context) error {
	n, err := s.store.Notifications().ReleaseExpiredLeases(ctx, 200)
	if err != nil {
		return err
	}
	if n > 0 {
		s.log.InfoContext(ctx, "reclaimed abandoned notification leases", "count", n)
	}
	return nil
}

// notificationKind maps an event onto the kind of message it produces. Events
// with no mapping are simply not notifiable.
func notificationKind(eventType string) (string, bool) {
	switch eventType {
	case models.EventOrderPaid:
		return "confirmation", true
	case models.EventOrderCancelled, models.EventOrderExpired:
		return "cancellation", true
	case models.EventRefundCompleted:
		return "refund", true
	default:
		return "", false
	}
}

// --- stand-in transports ---------------------------------------------------

// LoggingNotifier is the demo transport: it logs instead of sending, and fails
// occasionally so the retry, lease and dead-letter paths are actually exercised
// rather than merely written.
type LoggingNotifier struct {
	channel     string
	failureRate float64
	latency     time.Duration
	log         *slog.Logger
}

func NewLoggingNotifier(channel string, failureRate float64, latency time.Duration, log *slog.Logger) *LoggingNotifier {
	return &LoggingNotifier{channel: channel, failureRate: failureRate, latency: latency, log: log}
}

func (n *LoggingNotifier) Channel() string { return n.channel }

func (n *LoggingNotifier) Send(ctx context.Context, channel, kind string, orderID uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(n.latency):
	}

	if rand.Float64() < n.failureRate {
		return fmt.Errorf("simulated %s transport failure", channel)
	}

	n.log.InfoContext(ctx, "notification sent",
		"channel", channel, "kind", kind, "order_id", orderID)
	return nil
}
