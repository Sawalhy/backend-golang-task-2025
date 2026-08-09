package services

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
)

// RecoverStuckCharges resumes payments abandoned by a worker that died.
//
// This closes the "gets stuck" half of failure mode E. A worker SIGKILLed
// between the PENDING -> CHARGING transition and settlement leaves an order in
// CHARGING with an INITIATED intent, and nothing else in the system can recover
// it: redelivery skips non-PENDING orders, the reaper only expires PENDING ones,
// and reconciliation only looks at UNKNOWN. Without this sweep the order holds
// its stock forever while the customer may already have been charged.
//
// The recovery is to CALL THE PROVIDER AGAIN with the same idempotency key. That
// is not a retry in the dangerous sense — the key was committed before the first
// attempt and has not changed, so the provider either replays the original
// result (the charge landed and we simply never heard) or performs it for the
// first time (it never landed). Either way the customer is charged exactly once,
// which is the entire reason the key lives on the payments row rather than being
// generated per call.
//
// olderThan is a grace period, and it has to exceed the provider timeout.
// Sweeping too eagerly would "recover" a payment that is merely slow, doubling
// the work while the first call is still in flight.
func (s *PaymentService) RecoverStuckCharges(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	if limit <= 0 {
		limit = 50
	}

	// Claim in a short transaction, then release it. The provider calls below
	// must not happen with a transaction open.
	var claimed []models.Payment
	err := s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		var err error
		claimed, err = s.store.Payments().ClaimStuckIntents(ctx, tx, olderThan, limit)
		return err
	})
	if err != nil {
		return 0, err
	}
	if len(claimed) == 0 {
		return 0, nil
	}

	recovered := 0
	for _, payment := range claimed {
		if err := s.resumeCharge(ctx, payment); err != nil {
			// One poisoned payment must not stop the sweep; the rest of the
			// batch is independent and the next pass will retry this one.
			s.log.ErrorContext(ctx, "recovering stuck charge failed",
				"payment_id", payment.ID, "order_id", payment.OrderID, "error", err)
			continue
		}
		recovered++
	}

	s.log.InfoContext(ctx, "recovered charges abandoned by a dead worker",
		"claimed", len(claimed), "recovered", recovered)
	return recovered, nil
}

// resumeCharge re-drives one abandoned intent to a terminal state.
func (s *PaymentService) resumeCharge(ctx context.Context, payment models.Payment) error {
	order, err := s.store.Orders().GetByID(ctx, payment.OrderID)
	if err != nil {
		return err
	}

	// An UNKNOWN intent already has a dedicated path: ask the provider what it
	// believes happened rather than charging again.
	if payment.Status == models.PaymentUnknown {
		return s.ReconcileUnknown(ctx, &payment)
	}

	// Same key as the original attempt. This is the safe re-drive.
	result, chargeErr := s.provider.Charge(ctx, ChargeRequest{
		IdempotencyKey: payment.IdempotencyKey(),
		AmountCents:    payment.AmountCents,
		Currency:       order.Currency,
		OrderID:        payment.OrderID,
	})

	switch {
	case result.Outcome == OutcomeUnknown:
		// Still no verdict. Park it in UNKNOWN so the partial unique index keeps
		// blocking a second intent, and let reconciliation take it from here.
		s.log.WarnContext(ctx, "resumed charge still has no verdict",
			"payment_id", payment.ID, "error", chargeErr)
		return s.markUnknown(ctx, &payment)

	case chargeErr != nil:
		return fmt.Errorf("resuming charge for order %d: %w", payment.OrderID, chargeErr)

	case result.Outcome == OutcomeDeclined:
		return s.settleDecline(ctx, payment.OrderID, &payment, result.Reason)

	default:
		return s.settleSuccess(ctx, payment.OrderID, &payment, result.ProviderRef)
	}
}

// SweepUnknownPayments reconciles every payment left in UNKNOWN.
//
// UNKNOWN means the provider call timed out and we genuinely cannot say whether
// the customer was charged. The order sits in CHARGING and the partial unique
// index deliberately blocks a second intent, so nothing moves until somebody
// asks the provider what actually happened. That is this sweep's job, and it is
// why UNKNOWN is a state rather than an error.
func (s *PaymentService) SweepUnknownPayments(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 50
	}

	var pending []models.Payment
	err := s.store.DB().WithContext(ctx).Raw(`
		SELECT * FROM payments
		 WHERE status = 'UNKNOWN'
		 ORDER BY updated_at
		 LIMIT ?`, limit).Scan(&pending).Error
	if err != nil {
		return 0, fmt.Errorf("listing unknown payments: %w", err)
	}

	resolved := 0
	for i := range pending {
		if err := s.ReconcileUnknown(ctx, &pending[i]); err != nil {
			s.log.ErrorContext(ctx, "reconciling unknown payment failed",
				"payment_id", pending[i].ID, "error", err)
			continue
		}
		resolved++
	}
	return resolved, nil
}
