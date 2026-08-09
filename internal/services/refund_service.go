package services

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
)

// HandleRefund processes a payment.refund_requested event.
//
// This is the compensating action of the saga. A saga cannot roll back a
// committed step in another system — the money has left the customer's account
// and no transaction spans us and the card network — so "undo" is a second
// forward action that negates the first.
//
// It is emitted when a cancel and a charge collide and the charge wins
// (failure mode D). By the time this runs the order is already
// CANCELLED_REFUNDED and the stock is already back; only the money is
// outstanding.
func (s *PaymentService) HandleRefund(ctx context.Context, ev models.Envelope) error {
	raw, ok := ev.Data["paymentId"].(string)
	if !ok || raw == "" {
		return fmt.Errorf("refund event %s carries no paymentId", ev.EventID)
	}
	paymentID, err := uuid.Parse(raw)
	if err != nil {
		return fmt.Errorf("refund event %s has malformed paymentId %q: %w", ev.EventID, raw, err)
	}

	payment, err := s.store.Payments().GetByID(ctx, paymentID)
	if err != nil {
		return err
	}

	// Only a succeeded charge can be refunded. A duplicate delivery finds the
	// payment already REFUNDED and stops here — refunding twice would pay the
	// customer their money back a second time.
	if payment.Status != models.PaymentSucceeded {
		s.log.InfoContext(ctx, "skipping refund, payment is not SUCCEEDED",
			"payment_id", paymentID, "status", payment.Status)
		return nil
	}

	providerRef := ""
	if payment.ProviderRef != nil {
		providerRef = *payment.ProviderRef
	}
	if providerRef == "" {
		// No provider reference means we never confirmed a charge landed. Doing
		// nothing and escalating beats guessing at the card network.
		return fmt.Errorf("payment %s is SUCCEEDED but has no provider reference", paymentID)
	}

	// Network call, no transaction open.
	if err := s.provider.Refund(ctx, providerRef, payment.AmountCents); err != nil {
		// Returning an error nacks the delivery, so it is retried and then
		// dead-lettered. A refund that will not go through needs a human.
		return fmt.Errorf("refunding payment %s: %w", paymentID, err)
	}

	return s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		ok, err := s.store.Payments().SetStatus(ctx, tx, paymentID,
			models.PaymentSucceeded, models.PaymentRefunded, nil)
		if err != nil {
			return err
		}
		if !ok {
			return nil // another delivery got there first
		}

		s.log.InfoContext(ctx, "refund completed",
			"payment_id", paymentID, "order_id", payment.OrderID, "amount_cents", payment.AmountCents)

		ev := models.NewOrderEvent(models.EventRefundCompleted, payment.OrderID, map[string]any{
			"paymentId":   paymentID.String(),
			"amountCents": payment.AmountCents,
		})
		_, err = s.store.Outbox().Enqueue(ctx, tx, ev)
		return err
	})
}

// ReconcileUnknown re-checks payments left in UNKNOWN.
//
// UNKNOWN means the provider call timed out and we genuinely do not know whether
// the customer was charged. The order is stuck in CHARGING and the partial
// unique index deliberately blocks a second intent, so nothing moves until
// somebody asks the provider what actually happened. That is this loop's job,
// and it is why UNKNOWN is a state rather than an error.
//
// A real integration would call the provider's lookup API with the idempotency
// key. The simulated provider exposes the same shape.
func (s *PaymentService) ReconcileUnknown(ctx context.Context, payment *models.Payment) error {
	lookup, ok := s.provider.(interface {
		Lookup(key string) (ChargeResult, bool)
	})
	if !ok {
		return errors.New("payment provider does not support reconciliation lookups")
	}

	res, found := lookup.Lookup(payment.IdempotencyKey())
	if !found {
		// The provider has no record of the key, so the request never landed.
		// Safe to treat as declined and give the customer their stock back.
		s.log.InfoContext(ctx, "reconciled UNKNOWN payment as never charged", "payment_id", payment.ID)
		return s.resolveUnknown(ctx, payment, models.PaymentDeclined)
	}

	switch res.Outcome {
	case OutcomeSucceeded:
		s.log.InfoContext(ctx, "reconciled UNKNOWN payment as charged", "payment_id", payment.ID)
		return s.resolveUnknown(ctx, payment, models.PaymentSucceeded)
	case OutcomeDeclined:
		return s.resolveUnknown(ctx, payment, models.PaymentDeclined)
	default:
		return nil // still unknown; try again next sweep
	}
}

// resolveUnknown moves a payment out of UNKNOWN and settles its order.
func (s *PaymentService) resolveUnknown(ctx context.Context, payment *models.Payment, to models.PaymentStatus) error {
	err := s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		ok, err := s.store.Payments().SetStatus(ctx, tx, payment.ID, models.PaymentUnknown, to, nil)
		if err != nil || !ok {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Reuse the ordinary settle paths so the order transitions, reservations and
	// events are handled in exactly one place.
	settled := *payment
	switch to {
	case models.PaymentSucceeded:
		settled.Status = models.PaymentSucceeded
		return s.settleSuccess(ctx, payment.OrderID, &settled, "")
	default:
		settled.Status = models.PaymentInitiated
		return s.settleDecline(ctx, payment.OrderID, &settled, "reconciled: provider never charged")
	}
}
