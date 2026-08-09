package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

func newReaper(store *repository.Store) *services.ReaperService {
	return services.NewReaperService(store, 100, logger.New("error", false))
}

// age pushes an order's reservations into the past.
//
// Rewriting expires_at rather than sleeping: the real TTL is 15 minutes, and a
// test that waits for it is not a test anyone runs.
func age(t *testing.T, store *repository.Store, orderID uint64) {
	t.Helper()
	require.NoError(t, store.Orders().ExpireBefore(
		context.Background(), orderID, time.Now().UTC().Add(-time.Minute)))
}

func orderStatusOf(t *testing.T, store *repository.Store, orderID uint64) models.OrderStatus {
	t.Helper()
	o, err := store.Orders().GetByID(context.Background(), orderID)
	require.NoError(t, err)
	return o.Status
}

// Failure mode F: a customer opens checkout, holds the last unit, and walks
// away. Without the reaper that unit is unsellable forever.
func TestReaperReclaimsStockFromAbandonedOrders(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "abandoner@example.com")
	product := mustProduct(t, store, "ABANDONED", 5000, 3)

	order, err := newOrderSvc(store).Create(ctx, services.CreateOrderInput{
		UserID: user,
		Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 2}},
	})
	require.NoError(t, err)

	// Held, so unavailable to anyone else.
	inv, err := store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	require.Equal(t, 1, inv.Available)
	require.Equal(t, 2, inv.Reserved)

	age(t, store, order.ID)

	reaped, err := newReaper(store).ReapOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, reaped)

	assert.Equal(t, models.OrderExpired, orderStatusOf(t, store, order.ID))

	// The units must return to available, not merely leave reserved.
	inv, err = store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	assert.Equal(t, 3, inv.Available, "stock must be back on sale")
	assert.Equal(t, 0, inv.Reserved)

	held, err := store.Orders().ReservationsForOrder(ctx, nil, order.ID, models.ReservationHeld)
	require.NoError(t, err)
	assert.Empty(t, held, "no reservation may still be HELD")
}

// The reclaimed stock has to be genuinely sellable again, not just a number that
// went up.
func TestReclaimedStockCanBeBoughtAgain(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	buyer := seedUser(t, db, "first@example.com")
	latecomer := seedUser(t, db, "second@example.com")
	product := mustProduct(t, store, "SECOND-CHANCE", 7500, 1)
	orders := newOrderSvc(store)

	abandoned, err := orders.Create(ctx, services.CreateOrderInput{
		UserID: buyer,
		Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 1}},
	})
	require.NoError(t, err)

	// While it is held, nobody else can have it.
	_, err = orders.Create(ctx, services.CreateOrderInput{
		UserID: latecomer,
		Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 1}},
	})
	require.ErrorIs(t, err, models.ErrInsufficientStock)

	age(t, store, abandoned.ID)
	_, err = newReaper(store).ReapOnce(ctx)
	require.NoError(t, err)

	// Now they can.
	recovered, err := orders.Create(ctx, services.CreateOrderInput{
		UserID: latecomer,
		Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 1}},
	})
	require.NoError(t, err, "reclaimed stock must be sellable again")
	assert.NotEqual(t, abandoned.ID, recovered.ID)
}

// The subtle one, and the reason the reaper CASes instead of expiring whatever
// it claimed.
//
// A reservation can be past its expiry while its order is legitimately
// mid-charge: the payment worker claimed it at 14:59 and the provider is still
// thinking at 15:01. Expiring that order would release stock for a purchase
// about to succeed, and the customer would be charged for goods already given
// away.
//
// PENDING -> EXPIRED only succeeds if nobody else has moved the order, so a
// charging order is skipped and its reservations stay HELD.
func TestReaperWillNotExpireAnOrderBeingCharged(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "charging@example.com")
	product := mustProduct(t, store, "IN-FLIGHT", 2500, 4)

	order, err := newOrderSvc(store).Create(ctx, services.CreateOrderInput{
		UserID: user,
		Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 2}},
	})
	require.NoError(t, err)

	// A payment worker claims it.
	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		ok, err := store.Orders().Transition(ctx, tx, order.ID, models.OrderPending, models.OrderCharging)
		require.True(t, ok)
		return err
	}))

	age(t, store, order.ID)

	reaped, err := newReaper(store).ReapOnce(ctx)
	require.NoError(t, err)
	assert.Zero(t, reaped, "a charging order must not be expired")

	assert.Equal(t, models.OrderCharging, orderStatusOf(t, store, order.ID))

	// And its stock must stay held for the charge that is still in flight.
	inv, err := store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, inv.Available)
	assert.Equal(t, 2, inv.Reserved, "stock must not be released under a live charge")

	held, err := store.Orders().ReservationsForOrder(ctx, nil, order.ID, models.ReservationHeld)
	require.NoError(t, err)
	assert.Len(t, held, 1, "the reservation must remain HELD")
}

// The reaper runs on a timer and several worker instances may run it at once.
// Releasing the same reservation twice would invent stock that does not exist —
// the CAS on the reservation is what prevents it.
func TestReaperIsIdempotent(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "repeat@example.com")
	product := mustProduct(t, store, "REPEATED", 1000, 5)

	order, err := newOrderSvc(store).Create(ctx, services.CreateOrderInput{
		UserID: user,
		Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 3}},
	})
	require.NoError(t, err)

	age(t, store, order.ID)
	reaper := newReaper(store)

	first, err := reaper.ReapOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, first)

	for i := 0; i < 3; i++ {
		again, err := reaper.ReapOnce(ctx)
		require.NoError(t, err)
		assert.Zero(t, again, "there is nothing left to reap")
	}

	inv, err := store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	assert.Equal(t, 5, inv.Available, "stock must be restored exactly once, never inflated")
	assert.Equal(t, 0, inv.Reserved)
}

// Expiry is a state change like any other, so it publishes through the outbox in
// the same transaction. Without the event, nothing downstream can tell the
// customer their order lapsed.
func TestReaperPublishesExpiryThroughTheOutbox(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "notified@example.com")
	product := mustProduct(t, store, "EXPIRING", 800, 2)

	order, err := newOrderSvc(store).Create(ctx, services.CreateOrderInput{
		UserID: user,
		Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 1}},
	})
	require.NoError(t, err)

	age(t, store, order.ID)
	_, err = newReaper(store).ReapOnce(ctx)
	require.NoError(t, err)

	var events int64
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM outbox WHERE routing_key = ? AND sent_at IS NULL`,
		models.EventOrderExpired).Scan(&events).Error)
	assert.EqualValues(t, 1, events, "expiry must be announced exactly once")
}

// Only expired reservations are touched. A reaper that reclaimed live ones would
// cancel orders customers are still paying for.
func TestReaperLeavesUnexpiredReservationsAlone(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "fresh@example.com")
	product := mustProduct(t, store, "STILL-VALID", 1200, 6)

	order, err := newOrderSvc(store).Create(ctx, services.CreateOrderInput{
		UserID: user,
		Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 4}},
	})
	require.NoError(t, err)

	// No ageing: the reservation is well within its TTL.
	reaped, err := newReaper(store).ReapOnce(ctx)
	require.NoError(t, err)
	assert.Zero(t, reaped)

	assert.Equal(t, models.OrderPending, orderStatusOf(t, store, order.ID))

	inv, err := store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, inv.Available)
	assert.Equal(t, 4, inv.Reserved)
}

// A batch must handle many abandoned orders in one pass, and each order's stock
// has to return to the right product.
func TestReaperReclaimsAcrossManyOrders(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "bulk@example.com")
	first := mustProduct(t, store, "BULK-A", 100, 50)
	second := mustProduct(t, store, "BULK-B", 200, 50)
	orders := newOrderSvc(store)

	const abandoned = 12
	for i := 0; i < abandoned; i++ {
		order, err := orders.Create(ctx, services.CreateOrderInput{
			UserID: user,
			Lines: []services.OrderLine{
				{ProductID: first.ID, Qty: 1},
				{ProductID: second.ID, Qty: 2},
			},
		})
		require.NoError(t, err)
		age(t, store, order.ID)
	}

	reaped, err := newReaper(store).ReapOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, abandoned, reaped)

	invA, err := store.Inventory().Get(ctx, first.ID)
	require.NoError(t, err)
	assert.Equal(t, 50, invA.Available)
	assert.Equal(t, 0, invA.Reserved)

	invB, err := store.Inventory().Get(ctx, second.ID)
	require.NoError(t, err)
	assert.Equal(t, 50, invB.Available, "each product gets back exactly what it lent")
	assert.Equal(t, 0, invB.Reserved)
}
