package services

import (
	"context"
	"log/slog"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/metrics"
)

// ReaperService reclaims stock held by orders that were never paid for
// (failure mode F: the customer opens checkout, holds the last unit, and walks
// away — without this, that unit is unsellable forever).
//
// It is a PERIODIC LOOP, not a queue consumer, and that is a design statement
// worth defending. A queue delivers "something happened". The reaper's trigger is
// "Sarah still hasn't paid" — and the absence of an event is not an event. No
// message will ever arrive to announce that nothing happened, so a timer is the
// only thing that can notice.
type ReaperService struct {
	store     *repository.Store
	batchSize int
	log       *slog.Logger
}

func NewReaperService(store *repository.Store, batchSize int, log *slog.Logger) *ReaperService {
	if batchSize <= 0 {
		batchSize = 100
	}
	return &ReaperService{store: store, batchSize: batchSize, log: log}
}

// ReapOnce expires one batch. Returns how many orders it expired, so the caller
// can drain a backlog by looping until it returns 0 rather than waiting a full
// interval per batch.
//
// Batching is deliberate: one transaction expiring 40,000 abandoned reservations
// would hold locks across the whole set and block live orders on those products.
func (s *ReaperService) ReapOnce(ctx context.Context) (int, error) {
	var expired int

	err := s.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		// SKIP LOCKED lets several worker instances reap concurrently without
		// two of them fighting over the same rows.
		claimed, err := s.store.Orders().ClaimExpiredReservations(ctx, tx, s.batchSize)
		if err != nil {
			return err
		}
		if len(claimed) == 0 {
			return nil
		}

		// Distinct orders, in id order. Ordering here keeps the reaper's lock
		// sequence consistent with everything else that touches these rows.
		seen := make(map[uint64]struct{}, len(claimed))
		orderIDs := make([]uint64, 0, len(claimed))
		for _, r := range claimed {
			if _, ok := seen[r.OrderID]; ok {
				continue
			}
			seen[r.OrderID] = struct{}{}
			orderIDs = append(orderIDs, r.OrderID)
		}

		for _, orderID := range orderIDs {
			// The CAS is the guard, and it is doing real work here. A reservation
			// can be past its expiry while its order is legitimately mid-charge:
			// the payment worker claimed it at 14:59 and the provider is still
			// thinking at 15:01. Expiring that order would release stock for a
			// purchase that is about to succeed.
			//
			// PENDING -> EXPIRED only succeeds if nobody has claimed the order,
			// so a charging order is skipped and its reservations stay HELD until
			// the payment settles them.
			ok, err := s.store.Orders().Transition(ctx, tx, orderID,
				models.OrderPending, models.OrderExpired)
			if err != nil {
				return err
			}
			if !ok {
				s.log.DebugContext(ctx, "skipping expiry, order no longer pending",
					"order_id", orderID)
				continue
			}

			if err := settleReservations(ctx, s.store, tx, orderID, false); err != nil {
				return err
			}

			ev := models.NewOrderEvent(models.EventOrderExpired, orderID, map[string]any{
				"reason": "reservation_expired",
			})
			if _, err := s.store.Outbox().Enqueue(ctx, tx, ev); err != nil {
				return err
			}
			expired++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	if expired > 0 {
		// A spike here means payments are failing, not that customers got bored
		// (failure mode F) — the two look identical in the order table and only
		// the rate of this counter separates them.
		metrics.ReservationsExpired.Add(float64(expired))
		s.log.InfoContext(ctx, "reclaimed stock from expired orders", "orders", expired)
	}
	return expired, nil
}
