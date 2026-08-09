package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
)

type PaymentService struct {
	store    *repository.Store
	provider PaymentProvider
	log      *slog.Logger
}

func NewPaymentService(store *repository.Store, provider PaymentProvider, log *slog.Logger) *PaymentService {
	return &PaymentService{store: store, provider: provider, log: log}
}

// ProcessOrder handles one order.created delivery.
//
// The shape of this function is the whole design. Read the transaction
// boundaries, not the happy path:
//
//	tx 1  CAS PENDING -> CHARGING          (claims the work)
//	tx 2  open/find the payment intent     (commits the idempotency key)
//	--    call the provider                (NO transaction, NO lock held)
//	tx 3  record the outcome               (CAS again, because the world moved)
//
// The gap in the middle is deliberate. A payment call takes seconds; holding a
// transaction open across it would pin a pool connection and, worse, hold row
// locks on inventory that every other buyer of those products queues behind.
//
// Because nothing is locked during the call, the order CAN change underneath us
// — that is failure mode D, a cancel arriving mid-charge — so tx 3 never assumes
// the order is still CHARGING. It re-reads and branches.
//
// Delivery is at-least-once, so this may be called twice for the same order.
// Every step is a CAS, so the second call finds each transition already made and
// does nothing.
func (s *PaymentService) ProcessOrder(ctx context.Context, orderID uint64) error {
	order, err := s.store.Orders().GetByID(ctx, orderID)
	if err != nil {
		return err
	}

	// A duplicate delivery for an order that is already past PENDING is the
	// expected case, not an error. Ack it and move on.
	if order.Status != models.OrderPending {
		s.log.DebugContext(ctx, "skipping order not in PENDING",
			"order_id", orderID, "status", order.Status)
		return nil
	}

	// tx 1 — claim it. If this CAS fails, another worker took the same duplicate
	// delivery microseconds earlier. Exactly one of us proceeds.
	var claimed bool
	err = s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		var err error
		claimed, err = s.store.Orders().Transition(ctx, tx, orderID, models.OrderPending, models.OrderCharging)
		return err
	})
	if err != nil {
		return fmt.Errorf("claiming order %d for payment: %w", orderID, err)
	}
	if !claimed {
		s.log.DebugContext(ctx, "another worker claimed this order", "order_id", orderID)
		return nil
	}

	// tx 2 — the idempotency key must be durable BEFORE the provider is called.
	// If the process dies between this commit and the call, the retry finds this
	// row and reuses the same key, so the provider charges once.
	var payment *models.Payment
	err = s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		p, err := s.store.Payments().CreateIntent(ctx, tx, orderID, order.TotalCents, s.provider.Name())
		if errors.Is(err, models.ErrPaymentPending) {
			// An intent already exists — this is a retry of the same intent.
			// Reuse it, key and all.
			p, err = s.store.Payments().FindLive(ctx, tx, orderID)
		}
		if err != nil {
			return err
		}
		if err := s.store.Payments().IncrementAttempts(ctx, tx, p.ID); err != nil {
			return err
		}
		payment = p
		return nil
	})
	if err != nil {
		return fmt.Errorf("opening payment intent for order %d: %w", orderID, err)
	}

	// A SUCCEEDED intent means a previous attempt charged the card and died
	// before recording the order transition. Do not charge again — go straight
	// to settling the order.
	if payment.Status == models.PaymentSucceeded {
		return s.settleSuccess(ctx, orderID, payment, "")
	}

	// --- no transaction open, no lock held ---------------------------------
	result, chargeErr := s.provider.Charge(ctx, ChargeRequest{
		IdempotencyKey: payment.IdempotencyKey(),
		AmountCents:    payment.AmountCents,
		Currency:       order.Currency,
		OrderID:        orderID,
	})

	switch {
	case chargeErr != nil && result.Outcome != OutcomeUnknown:
		return fmt.Errorf("charging order %d: %w", orderID, chargeErr)

	case result.Outcome == OutcomeUnknown:
		// The customer may or may not have been charged. Record UNKNOWN, which
		// blocks a second intent via the partial unique index, and leave the
		// order CHARGING for the reconciler. Guessing either way is a real
		// financial error.
		s.log.WarnContext(ctx, "payment outcome unknown, leaving for reconciliation",
			"order_id", orderID, "payment_id", payment.ID)
		return s.markUnknown(ctx, payment)

	case result.Outcome == OutcomeDeclined:
		return s.settleDecline(ctx, orderID, payment, result.Reason)

	default:
		return s.settleSuccess(ctx, orderID, payment, result.ProviderRef)
	}
}

// settleSuccess records a successful charge and moves the order — or compensates
// if a cancel got in first.
func (s *PaymentService) settleSuccess(ctx context.Context, orderID uint64, payment *models.Payment, providerRef string) error {
	return s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		if payment.Status == models.PaymentInitiated {
			var ref *string
			if providerRef != "" {
				ref = &providerRef
			}
			ok, err := s.store.Payments().SetStatus(ctx, tx, payment.ID,
				models.PaymentInitiated, models.PaymentSucceeded, ref)
			if err != nil {
				return err
			}
			if !ok {
				// Another delivery already settled this intent.
				return nil
			}
		}

		// Re-read under a row lock. Between the provider call and here, a cancel
		// may have moved the order to CANCELLING.
		order, err := s.store.Orders().GetForUpdate(ctx, tx, orderID)
		if err != nil {
			return err
		}

		switch order.Status {
		case models.OrderCharging:
			ok, err := s.store.Orders().Transition(ctx, tx, orderID, models.OrderCharging, models.OrderPaid)
			if err != nil || !ok {
				return err
			}
			// The goods are sold: reserved stock leaves for good.
			if err := s.consumeReservations(ctx, tx, orderID, true); err != nil {
				return err
			}
			ev := models.NewOrderEvent(models.EventOrderPaid, orderID, map[string]any{
				"customerId": order.UserID,
				"totalCents": order.TotalCents,
				"currency":   order.Currency,
			})
			_, err = s.store.Outbox().Enqueue(ctx, tx, ev)
			return err

		case models.OrderCancelling:
			// Failure mode D, resolved: the customer cancelled while the charge
			// was in flight, and the charge won. We cannot un-take the money, so
			// we compensate — the saga's rollback is a refund, not an undo.
			ok, err := s.store.Orders().Transition(ctx, tx, orderID,
				models.OrderCancelling, models.OrderCancelledRefunded)
			if err != nil || !ok {
				return err
			}
			if err := s.consumeReservations(ctx, tx, orderID, false); err != nil {
				return err
			}
			refund := models.NewOrderEvent(models.EventRefundRequested, orderID, map[string]any{
				"paymentId":   payment.ID.String(),
				"amountCents": payment.AmountCents,
				"providerRef": providerRef,
			})
			if _, err := s.store.Outbox().Enqueue(ctx, tx, refund); err != nil {
				return err
			}
			cancelled := models.NewOrderEvent(models.EventOrderCancelled, orderID, map[string]any{
				"customerId": order.UserID,
				"refunded":   true,
			})
			_, err = s.store.Outbox().Enqueue(ctx, tx, cancelled)
			return err

		default:
			// PAID already, or something terminal. A duplicate delivery landing
			// here is normal; anything else is worth a loud line in the log.
			s.log.InfoContext(ctx, "charge settled against non-charging order",
				"order_id", orderID, "status", order.Status)
			return nil
		}
	})
}

func (s *PaymentService) settleDecline(ctx context.Context, orderID uint64, payment *models.Payment, reason string) error {
	return s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		// Only CAS the intent when it is still INITIATED. Reconciliation arrives
		// here with a payment already moved out of UNKNOWN, and an unconditional
		// CAS would find the row in the wrong state, return false, and abandon
		// the ORDER half of the settlement — leaving it stuck in CHARGING with
		// its stock held forever. Mirrors the same guard in settleSuccess.
		if payment.Status == models.PaymentInitiated {
			ok, err := s.store.Payments().SetStatus(ctx, tx, payment.ID,
				models.PaymentInitiated, models.PaymentDeclined, nil)
			if err != nil {
				return err
			}
			if !ok {
				return nil // already settled by a duplicate delivery
			}
		}

		order, err := s.store.Orders().GetForUpdate(ctx, tx, orderID)
		if err != nil {
			return err
		}

		// The card was refused, so no money moved and there is nothing to
		// compensate. Both the charging and cancelling paths end the same way:
		// give the stock back.
		var to models.OrderStatus
		var eventType string
		switch order.Status {
		case models.OrderCharging:
			to, eventType = models.OrderFailed, models.EventOrderFailed
		case models.OrderCancelling:
			to, eventType = models.OrderCancelled, models.EventOrderCancelled
		default:
			return nil
		}

		moved, err := s.store.Orders().Transition(ctx, tx, orderID, order.Status, to)
		if err != nil || !moved {
			return err
		}
		if err := s.releaseReservations(ctx, tx, orderID); err != nil {
			return err
		}

		ev := models.NewOrderEvent(eventType, orderID, map[string]any{
			"customerId": order.UserID,
			"reason":     reason,
		})
		_, err = s.store.Outbox().Enqueue(ctx, tx, ev)
		return err
	})
}

func (s *PaymentService) markUnknown(ctx context.Context, payment *models.Payment) error {
	return s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		_, err := s.store.Payments().SetStatus(ctx, tx, payment.ID,
			models.PaymentInitiated, models.PaymentUnknown, nil)
		return err
	})
}

func (s *PaymentService) consumeReservations(ctx context.Context, tx *gorm.DB, orderID uint64, sold bool) error {
	return settleReservations(ctx, s.store, tx, orderID, sold)
}

func (s *PaymentService) releaseReservations(ctx context.Context, tx *gorm.DB, orderID uint64) error {
	return settleReservations(ctx, s.store, tx, orderID, false)
}
