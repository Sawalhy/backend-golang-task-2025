package repository

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/database"
)

type NotificationRepo struct{ *Store }

func (s *Store) Notifications() *NotificationRepo { return &NotificationRepo{s} }

// Ensure creates the notification row if it does not exist, and reports whether
// this caller created it.
//
// The unique index on (order_id, channel, kind) is what makes "send exactly one
// confirmation email per order" an invariant rather than a hope. Two duplicate
// order.paid deliveries both reach here; the index rejects the second, so only
// one row — and therefore only one email — can exist.
//
// ON CONFLICT DO NOTHING rather than catching the error, so a duplicate is an
// ordinary zero-row result instead of an exception on a normal path.
func (r *NotificationRepo) Ensure(ctx context.Context, tx *gorm.DB, orderID uint64, channel, kind string) (*models.Notification, bool, error) {
	res := r.txOrDB(tx).WithContext(ctx).Exec(`
		INSERT INTO notifications (order_id, channel, kind, status)
		VALUES (?, ?, ?, 'UNCLAIMED')
		ON CONFLICT (order_id, channel, kind) DO NOTHING`,
		orderID, channel, kind)
	if res.Error != nil {
		return nil, false, fmt.Errorf("ensuring notification for order %d: %w", orderID, res.Error)
	}
	created := res.RowsAffected == 1

	var n models.Notification
	err := r.txOrDB(tx).WithContext(ctx).
		Where("order_id = ? AND channel = ? AND kind = ?", orderID, channel, kind).
		Take(&n).Error
	if err != nil {
		return nil, false, fmt.Errorf("reading notification for order %d: %w", orderID, err)
	}
	return &n, created, nil
}

// Claim takes a lease on a notification so exactly one worker sends it.
//
// The lease, rather than a plain SENDING flag, is what survives a worker dying
// mid-send. A flag with no expiry would leave the row SENDING forever and the
// customer would never be told anything. With a lease, the row becomes claimable
// again once it expires — at the cost of a possible duplicate send, which for a
// notification is the right way round: a second email is an annoyance, a missing
// order confirmation is a support ticket.
//
// The CAS covers both cases in one statement: claimable means UNCLAIMED, or
// SENDING with an expired lease.
func (r *NotificationRepo) Claim(ctx context.Context, tx *gorm.DB, id uint64, lease time.Duration) (bool, error) {
	res := r.txOrDB(tx).WithContext(ctx).Exec(`
		UPDATE notifications
		   SET status      = 'SENDING',
		       lease_until = now() + (? * interval '1 second'),
		       attempts    = attempts + 1
		 WHERE id = ?
		   AND (status = 'UNCLAIMED'
		        OR (status = 'SENDING' AND lease_until < now()))`,
		lease.Seconds(), id)
	if res.Error != nil {
		return false, fmt.Errorf("claiming notification %d: %w", id, res.Error)
	}
	return res.RowsAffected == 1, nil
}

func (r *NotificationRepo) MarkSent(ctx context.Context, tx *gorm.DB, id uint64) error {
	res := r.txOrDB(tx).WithContext(ctx).Exec(`
		UPDATE notifications
		   SET status = 'SENT', sent_at = now(), lease_until = NULL
		 WHERE id = ? AND status = 'SENDING'`, id)
	if res.Error != nil {
		return fmt.Errorf("marking notification %d sent: %w", id, res.Error)
	}
	return nil
}

// MarkFailed records a permanent failure after the attempt budget is spent.
func (r *NotificationRepo) MarkFailed(ctx context.Context, tx *gorm.DB, id uint64, reason string, maxAttempts int) error {
	res := r.txOrDB(tx).WithContext(ctx).Exec(`
		UPDATE notifications
		   SET status      = CASE WHEN attempts >= ? THEN 'FAILED'::notification_status
		                          ELSE 'UNCLAIMED'::notification_status END,
		       lease_until = NULL,
		       last_error  = ?
		 WHERE id = ? AND status = 'SENDING'`,
		maxAttempts, reason, id)
	if res.Error != nil {
		return fmt.Errorf("marking notification %d failed: %w", id, res.Error)
	}
	return nil
}

// ReleaseExpiredLeases returns abandoned rows to the claimable pool. Backstop for
// a worker that died between claiming and sending.
func (r *NotificationRepo) ReleaseExpiredLeases(ctx context.Context, limit int) (int64, error) {
	res := r.db.WithContext(ctx).Exec(`
		UPDATE notifications
		   SET status = 'UNCLAIMED', lease_until = NULL
		 WHERE id IN (
		   SELECT id FROM notifications
		    WHERE status = 'SENDING' AND lease_until < now()
		    ORDER BY id
		    FOR UPDATE SKIP LOCKED
		    LIMIT ?)`, limit)
	if res.Error != nil {
		return 0, fmt.Errorf("releasing expired notification leases: %w", res.Error)
	}
	return res.RowsAffected, nil
}

func (r *NotificationRepo) ListForOrder(ctx context.Context, orderID uint64) ([]models.Notification, error) {
	var out []models.Notification
	err := r.db.WithContext(ctx).Where("order_id = ?", orderID).Order("id").Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("listing notifications for order %d: %w", orderID, err)
	}
	return out, nil
}

// IsDuplicate reports whether err is the unique index rejecting a second
// notification for the same order/channel/kind.
func IsDuplicate(err error) bool { return database.IsUniqueViolation(err) }
