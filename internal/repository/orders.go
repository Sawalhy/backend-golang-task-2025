package repository

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
)

type OrderRepo struct{ *Store }

func (s *Store) Orders() *OrderRepo { return &OrderRepo{s} }

// Transition is the only sanctioned way to change orders.status. Nothing else in
// the codebase writes that column.
//
// It combines the two guards that make at-least-once delivery survivable:
//
//   - The state machine check rejects edges that make no sense (PAID → PENDING),
//     turning a class of bug into a loud error instead of corrupt data.
//   - The CAS — `AND status = from` — makes the write conditional on the world
//     still being as the caller believed. Two workers both handling a duplicate
//     `order.created` both try PENDING → CHARGING; exactly one succeeds.
//
// The bool is the entire point. `true` means THIS caller performed the
// transition and owns whatever follows. `false` means someone else got there
// first — not an error, and usually the correct response is to do nothing,
// because the other worker is already doing it.
//
// Callers must check it. Ignoring the bool re-introduces the double-charge this
// exists to prevent.
func (r *OrderRepo) Transition(ctx context.Context, tx *gorm.DB, orderID uint64, from, to models.OrderStatus) (bool, error) {
	if !models.CanTransition(from, to) {
		return false, fmt.Errorf("%w: %s -> %s", models.ErrIllegalTransition, from, to)
	}

	// paid_at and cancelled_at are stamped by the same statement that moves the
	// status, so the timestamp can never disagree with the state it describes.
	res := r.txOrDB(tx).WithContext(ctx).Exec(`
		UPDATE orders
		   SET status     = ?::order_status,
		       updated_at = now(),
		       paid_at    = CASE WHEN ?::order_status = 'PAID'
		                         THEN now() ELSE paid_at END,
		       cancelled_at = CASE WHEN ?::order_status
		                                IN ('CANCELLED','CANCELLED_REFUNDED','EXPIRED')
		                           THEN now() ELSE cancelled_at END
		 WHERE id     = ?
		   AND status = ?::order_status`,
		to, to, to, orderID, from)

	if res.Error != nil {
		return false, fmt.Errorf("transitioning order %d %s -> %s: %w", orderID, from, to, res.Error)
	}
	return res.RowsAffected == 1, nil
}

// Create inserts the order row. Items, reservations and the outbox event are
// written by the caller inside the same transaction.
func (r *OrderRepo) Create(ctx context.Context, tx *gorm.DB, o *models.Order) error {
	if err := r.txOrDB(tx).WithContext(ctx).Create(o).Error; err != nil {
		return fmt.Errorf("creating order: %w", err)
	}
	return nil
}

func (r *OrderRepo) CreateItems(ctx context.Context, tx *gorm.DB, items []models.OrderItem) error {
	if len(items) == 0 {
		return nil
	}
	if err := r.txOrDB(tx).WithContext(ctx).Create(&items).Error; err != nil {
		return fmt.Errorf("creating order items: %w", err)
	}
	return nil
}

func (r *OrderRepo) GetByID(ctx context.Context, id uint64) (*models.Order, error) {
	var o models.Order
	err := r.db.WithContext(ctx).Preload("Items").Where("id = ?", id).Take(&o).Error
	if err != nil {
		if notFound(err) {
			return nil, fmt.Errorf("order %d: %w", id, models.ErrNotFound)
		}
		return nil, fmt.Errorf("reading order %d: %w", id, err)
	}
	return &o, nil
}

// GetForUpdate re-reads an order inside a transaction holding a row lock. Used by
// the cancel path, which must decide based on a status that cannot shift under it.
func (r *OrderRepo) GetForUpdate(ctx context.Context, tx *gorm.DB, id uint64) (*models.Order, error) {
	var o models.Order
	err := r.txOrDB(tx).WithContext(ctx).
		Raw(`SELECT * FROM orders WHERE id = ? FOR UPDATE`, id).
		Take(&o).Error
	if err != nil {
		if notFound(err) {
			return nil, fmt.Errorf("order %d: %w", id, models.ErrNotFound)
		}
		return nil, fmt.Errorf("locking order %d: %w", id, err)
	}
	return &o, nil
}

// FindByIdempotencyKey supports client retries of POST /orders. A dropped
// response must not become a second order.
func (r *OrderRepo) FindByIdempotencyKey(ctx context.Context, userID uint64, key string) (*models.Order, error) {
	var o models.Order
	err := r.db.WithContext(ctx).Preload("Items").
		Where("idempotency_key = ? AND user_id = ?", key, userID).Take(&o).Error
	if err != nil {
		if notFound(err) {
			return nil, models.ErrNotFound
		}
		return nil, fmt.Errorf("looking up idempotency key: %w", err)
	}
	return &o, nil
}

type ListFilter struct {
	UserID *uint64
	Status *models.OrderStatus
	Limit  int
	Offset int
}

// List serves both GET /orders and GET /admin/orders. The composite indexes
// (user_id, created_at DESC) and (status, created_at DESC) mean each variant
// seeks once and walks the page, with no sort node.
func (r *OrderRepo) List(ctx context.Context, f ListFilter) ([]models.Order, int64, error) {
	q := r.db.WithContext(ctx).Model(&models.Order{})
	if f.UserID != nil {
		q = q.Where("user_id = ?", *f.UserID)
	}
	if f.Status != nil {
		q = q.Where("status = ?::order_status", *f.Status)
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("counting orders: %w", err)
	}

	var out []models.Order
	err := q.Order("created_at DESC").Limit(f.Limit).Offset(f.Offset).Find(&out).Error
	if err != nil {
		return nil, 0, fmt.Errorf("listing orders: %w", err)
	}
	return out, total, nil
}

// --- reservations ----------------------------------------------------------

func (r *OrderRepo) CreateReservations(ctx context.Context, tx *gorm.DB, res []models.Reservation) error {
	if len(res) == 0 {
		return nil
	}
	if err := r.txOrDB(tx).WithContext(ctx).Create(&res).Error; err != nil {
		return fmt.Errorf("creating reservations: %w", err)
	}
	return nil
}

func (r *OrderRepo) ReservationsForOrder(ctx context.Context, tx *gorm.DB, orderID uint64, status models.ReservationStatus) ([]models.Reservation, error) {
	var out []models.Reservation
	err := r.txOrDB(tx).WithContext(ctx).
		Where("order_id = ? AND status = ?::reservation_status", orderID, status).
		Order("product_id"). // same total order as intake, so releases cannot deadlock either
		Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("reading reservations for order %d: %w", orderID, err)
	}
	return out, nil
}

// SetReservationStatus is a CAS like every other state change here.
func (r *OrderRepo) SetReservationStatus(ctx context.Context, tx *gorm.DB, id uint64, from, to models.ReservationStatus) (bool, error) {
	res := r.txOrDB(tx).WithContext(ctx).Exec(`
		UPDATE reservations
		   SET status = ?::reservation_status, updated_at = now()
		 WHERE id = ? AND status = ?::reservation_status`,
		to, id, from)
	if res.Error != nil {
		return false, fmt.Errorf("transitioning reservation %d: %w", id, res.Error)
	}
	return res.RowsAffected == 1, nil
}

// ClaimExpiredReservations is the reaper's query (failure mode F).
//
// SKIP LOCKED is what makes a table usable as a work queue. Without it, ten
// reaper instances running this all queue behind the same row, wake when the
// winner commits, re-check, find it taken, and get nothing — ten workers with
// the throughput of one. With it, the second instance steps over the locked row
// and takes the next.
//
// The predicate is written to match the partial index res_expiring exactly;
// Postgres only uses a partial index when it can prove the query predicate
// implies the index predicate.
func (r *OrderRepo) ClaimExpiredReservations(ctx context.Context, tx *gorm.DB, limit int) ([]models.Reservation, error) {
	var out []models.Reservation
	err := r.txOrDB(tx).WithContext(ctx).Raw(`
		SELECT * FROM reservations
		 WHERE status = 'HELD' AND expires_at < now()
		 ORDER BY expires_at
		 FOR UPDATE SKIP LOCKED
		 LIMIT ?`, limit).Scan(&out).Error
	if err != nil {
		return nil, fmt.Errorf("claiming expired reservations: %w", err)
	}
	return out, nil
}

// ExpireBefore is used by tests to age reservations without sleeping.
func (r *OrderRepo) ExpireBefore(ctx context.Context, orderID uint64, t time.Time) error {
	return r.db.WithContext(ctx).
		Exec(`UPDATE reservations SET expires_at = ? WHERE order_id = ?`, t, orderID).Error
}
