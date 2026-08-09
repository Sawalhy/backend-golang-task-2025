package services

import (
	"context"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
)

// settleReservations resolves every HELD reservation for an order.
//
//	sold=true  -> the units shipped: reserved goes down, available does NOT go up.
//	sold=false -> the order died: the units return to available.
//
// Shared by the payment settle paths, the cancel path and the reaper, because all
// three need exactly this and three copies would drift.
//
// Two properties make it safe to call twice, which at-least-once delivery
// guarantees will happen:
//
//   - The reservation status is CASed first. If another path already settled it,
//     RowsAffected is 0 and we skip the inventory move — without that check, a
//     duplicate release would return the same units twice and invent stock.
//   - Rows come back ordered by product_id, the same total order intake uses, so
//     settling and a concurrent order cannot deadlock against each other.
func settleReservations(ctx context.Context, store *repository.Store, tx *gorm.DB, orderID uint64, sold bool) error {
	held, err := store.Orders().ReservationsForOrder(ctx, tx, orderID, models.ReservationHeld)
	if err != nil {
		return err
	}

	target := models.ReservationReleased
	if sold {
		target = models.ReservationCommitted
	}

	for _, r := range held {
		ok, err := store.Orders().SetReservationStatus(ctx, tx, r.ID, models.ReservationHeld, target)
		if err != nil {
			return err
		}
		if !ok {
			continue // someone else settled this one; the inventory move is theirs
		}

		if sold {
			err = store.Inventory().Commit(ctx, tx, r.ProductID, r.Qty)
		} else {
			err = store.Inventory().Release(ctx, tx, r.ProductID, r.Qty)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
