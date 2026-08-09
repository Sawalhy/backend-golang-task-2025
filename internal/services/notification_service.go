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

	// Step 2: send. Outside any transaction — this is a network call, and
	// holding a row lock across it is the mistake rule 4 exists to prevent.
	sendErr := s.notifier.Send(ctx, channel, kind, orderID)

	// Step 3: record the outcome.
	return s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		if sendErr != nil {
			s.log.WarnContext(ctx, "notification send failed",
				"order_id", orderID, "channel", channel, "attempt", notification.Attempts, "error", sendErr)
			return s.store.Notifications().MarkFailed(ctx, tx, notification.ID, sendErr.Error(), maxNotificationAttempts)
		}
		return s.store.Notifications().MarkSent(ctx, tx, notification.ID)
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
