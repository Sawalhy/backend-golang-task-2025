package services

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
)

// CancelResult tells the handler whether the cancellation is final or pending, so
// the API can answer 200 vs 202 honestly rather than claiming a cancellation it
// cannot yet guarantee.
type CancelResult struct {
	Order   *models.Order
	Pending bool // true when a charge is in flight and the outcome is not yet known
}

// Cancel handles PUT /orders/{id}/cancel.
//
// (Noted in passing: a cancel is a state change, so POST /orders/{id}/cancellation
// would be more RESTful than PUT on a verb. The spec says PUT at README.md:86, so
// PUT is what this implements.)
//
// The interesting case is the second one. There are three:
//
//	PENDING   -> nothing has been charged. Cancel outright, give the stock back.
//	CHARGING  -> a payment is IN FLIGHT and we cannot see its outcome from here.
//	             We must not release the stock and we must not report success.
//	anything else -> terminal or already cancelling; 409.
//
// For CHARGING we move to CANCELLING, which is not "cancelled" — it is a recorded
// intent to cancel. The payment worker finishes its provider call, finds the
// order in CANCELLING instead of CHARGING, and settles accordingly: refund and
// CANCELLED_REFUNDED if the charge landed, plain CANCELLED if it was declined.
//
// That is failure mode D. The naive version cancels the order and releases the
// stock while the charge is in flight, and the customer ends up charged for an
// order the system says was cancelled.
//
// The row lock is what makes the read-then-branch safe: without FOR UPDATE the
// status could change between reading it and acting on it.
func (s *OrderService) Cancel(ctx context.Context, orderID, userID uint64, isAdmin bool) (*CancelResult, error) {
	var out *CancelResult

	err := s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		order, err := s.store.Orders().GetForUpdate(ctx, tx, orderID)
		if err != nil {
			return err
		}
		if !isAdmin && order.UserID != userID {
			return fmt.Errorf("order %d: %w", orderID, models.ErrNotFound)
		}

		switch order.Status {
		case models.OrderPending:
			ok, err := s.store.Orders().Transition(ctx, tx, orderID,
				models.OrderPending, models.OrderCancelled)
			if err != nil {
				return err
			}
			if !ok {
				// Lost the race against the payment worker, which just moved this
				// to CHARGING. Re-read and fall through to the CHARGING branch.
				return models.ErrLostRace
			}
			if err := settleReservations(ctx, s.store, tx, orderID, false); err != nil {
				return err
			}
			ev := models.NewOrderEvent(models.EventOrderCancelled, orderID, map[string]any{
				"customerId": order.UserID,
				"refunded":   false,
			})
			if _, err := s.store.Outbox().Enqueue(ctx, tx, ev); err != nil {
				return err
			}
			order.Status = models.OrderCancelled
			out = &CancelResult{Order: order, Pending: false}
			return nil

		case models.OrderCharging:
			ok, err := s.store.Orders().Transition(ctx, tx, orderID,
				models.OrderCharging, models.OrderCancelling)
			if err != nil {
				return err
			}
			if !ok {
				return models.ErrLostRace
			}
			// Deliberately NO reservation release here. The stock stays held
			// until the payment worker learns whether the card was charged; if we
			// released now and the charge succeeded, we would have sold stock we
			// just gave away.
			order.Status = models.OrderCancelling
			out = &CancelResult{Order: order, Pending: true}
			return nil

		default:
			return fmt.Errorf("order %d is %s: %w", orderID, order.Status, models.ErrOrderNotOpen)
		}
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Fulfil moves a paid order to FULFILLED. Admin-driven: this system has no
// warehouse to hear from, so the transition is exposed rather than inferred.
func (s *OrderService) Fulfil(ctx context.Context, orderID uint64) error {
	return s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		ok, err := s.store.Orders().Transition(ctx, tx, orderID, models.OrderPaid, models.OrderFulfilled)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("order %d is not PAID: %w", orderID, models.ErrOrderNotOpen)
		}
		ev := models.NewOrderEvent(models.EventOrderFulfilled, orderID, nil)
		_, err = s.store.Outbox().Enqueue(ctx, tx, ev)
		return err
	})
}
