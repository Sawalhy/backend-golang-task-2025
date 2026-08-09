package repository

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/database"
)

type PaymentRepo struct{ *Store }

func (s *Store) Payments() *PaymentRepo { return &PaymentRepo{s} }

// CreateIntent opens a payment intent and commits it BEFORE the provider is
// called. That ordering is the double-charge defence (failure mode C).
//
// The row's id is the idempotency key sent to the provider. Because it is
// committed first, a crash between here and the provider call leaves a durable
// INITIATED row, and the retry finds it and reuses the same key — so the
// provider recognises the second request as the first one and charges once.
// Generating the key at call time instead would produce a fresh key per attempt,
// which is a charge per attempt.
//
// A unique violation here is the partial index doing its job: another intent for
// this order is already INITIATED, UNKNOWN or SUCCEEDED. That is a race we lost,
// not a failure — the caller loads the existing intent and continues.
func (r *PaymentRepo) CreateIntent(ctx context.Context, tx *gorm.DB, orderID uint64, amountCents int64, provider string) (*models.Payment, error) {
	p := models.Payment{
		ID:          uuid.New(),
		OrderID:     orderID,
		Status:      models.PaymentInitiated,
		AmountCents: amountCents,
		Provider:    provider,
	}

	err := r.txOrDB(tx).WithContext(ctx).Create(&p).Error
	if err != nil {
		if database.IsUniqueViolation(err) {
			return nil, fmt.Errorf("opening intent for order %d: %w", orderID, models.ErrPaymentPending)
		}
		return nil, fmt.Errorf("opening intent for order %d: %w", orderID, err)
	}
	return &p, nil
}

// FindLive returns the order's live intent, if any. "Live" is exactly the set in
// the partial unique index: INITIATED, UNKNOWN, SUCCEEDED.
//
// UNKNOWN is in that set deliberately. It means the provider call timed out and
// the customer may or may not have been charged — opening a second intent in
// that state is how a real double charge happens. DECLINED is excluded because a
// declined card followed by a different card is a genuinely new intent, so
// DECLINED, DECLINED, SUCCEEDED is a legal history.
func (r *PaymentRepo) FindLive(ctx context.Context, tx *gorm.DB, orderID uint64) (*models.Payment, error) {
	var p models.Payment
	err := r.txOrDB(tx).WithContext(ctx).Raw(`
		SELECT * FROM payments
		 WHERE order_id = ?
		   AND status IN ('INITIATED','UNKNOWN','SUCCEEDED')`, orderID).Take(&p).Error
	if err != nil {
		if notFound(err) {
			return nil, models.ErrNotFound
		}
		return nil, fmt.Errorf("finding live intent for order %d: %w", orderID, err)
	}
	return &p, nil
}

func (r *PaymentRepo) GetByID(ctx context.Context, id uuid.UUID) (*models.Payment, error) {
	var p models.Payment
	err := r.db.WithContext(ctx).Where("id = ?", id).Take(&p).Error
	if err != nil {
		if notFound(err) {
			return nil, fmt.Errorf("payment %s: %w", id, models.ErrNotFound)
		}
		return nil, fmt.Errorf("reading payment %s: %w", id, err)
	}
	return &p, nil
}

// SetStatus is a CAS on the payment intent. Returns whether this caller made the
// change, so a duplicate delivery that finds the intent already SUCCEEDED does
// not fire the fulfilment side effects a second time.
func (r *PaymentRepo) SetStatus(ctx context.Context, tx *gorm.DB, id uuid.UUID, from, to models.PaymentStatus, providerRef *string) (bool, error) {
	res := r.txOrDB(tx).WithContext(ctx).Exec(`
		UPDATE payments
		   SET status       = ?::payment_status,
		       provider_ref = COALESCE(?, provider_ref),
		       updated_at   = now()
		 WHERE id     = ?
		   AND status = ?::payment_status`,
		to, providerRef, id, from)

	if res.Error != nil {
		return false, fmt.Errorf("transitioning payment %s %s -> %s: %w", id, from, to, res.Error)
	}
	return res.RowsAffected == 1, nil
}

// IncrementAttempts records that we are about to call the provider again. Kept
// separate from SetStatus because the attempt count rises while the status
// deliberately does not — the intent stays INITIATED across retries so the
// idempotency key stays stable.
func (r *PaymentRepo) IncrementAttempts(ctx context.Context, tx *gorm.DB, id uuid.UUID) error {
	err := r.txOrDB(tx).WithContext(ctx).
		Exec(`UPDATE payments SET attempts = attempts + 1, updated_at = now() WHERE id = ?`, id).Error
	if err != nil {
		return fmt.Errorf("incrementing attempts for payment %s: %w", id, err)
	}
	return nil
}

// ListForOrder backs GET /orders/{id}/status, which needs attempt history:
// RabbitMQ is transport, not storage, so once a message is consumed the broker
// cannot answer "what happened to this order's charge?". Only a table can.
func (r *PaymentRepo) ListForOrder(ctx context.Context, orderID uint64) ([]models.Payment, error) {
	var out []models.Payment
	err := r.db.WithContext(ctx).
		Where("order_id = ?", orderID).Order("created_at").Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("listing payments for order %d: %w", orderID, err)
	}
	return out, nil
}
