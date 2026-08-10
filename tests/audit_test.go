package tests

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
)

// anOrder creates a PENDING order to hang audit rows off. Nothing here cares
// about the order's contents, only that its id resolves — but each test needs
// its own SKU, which is why this takes one rather than deriving it.
func anOrder(t *testing.T, store *repository.Store, sku, email string) uint64 {
	t.Helper()

	user := seedUser(t, store.DB(), email)
	product := mustProduct(t, store, sku, 2500, 10)

	order, err := newOrderSvc(store).Create(context.Background(), services.CreateOrderInput{
		UserID: user,
		Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 1}},
	})
	require.NoError(t, err)
	return order.ID
}

func auditRows(t *testing.T, store *repository.Store) []models.AuditLog {
	t.Helper()
	var out []models.AuditLog
	require.NoError(t, store.DB().Order("id").Find(&out).Error)
	return out
}

// transition runs one state change in its own transaction and reports whether
// this caller won the CAS.
func transition(t *testing.T, ctx context.Context, store *repository.Store, orderID uint64, from, to models.OrderStatus) bool {
	t.Helper()
	var won bool
	err := store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		ok, err := store.Orders().Transition(ctx, tx, orderID, from, to)
		won = ok
		return err
	})
	require.NoError(t, err)
	return won
}

// README.md:63 requires audit logs "for tracking changes". Every order state
// change funnels through Transition, which is why the audit write lives there —
// it makes "no status change goes unrecorded" an invariant rather than something
// each of the eight call sites has to remember.
func TestOrderTransitionWritesAnAuditRow(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	orderID := anOrder(t, store, "AUDIT-BASIC", "audit@example.com")
	require.Empty(t, auditRows(t, store), "creating an order writes no status change")

	require.True(t, transition(t, ctx, store, orderID,models.OrderPending, models.OrderCharging))

	rows := auditRows(t, store)
	require.Len(t, rows, 1)

	entry := rows[0]
	assert.Equal(t, "order", entry.EntityType)
	assert.Equal(t, strconv.FormatUint(orderID, 10), entry.EntityID)
	assert.Equal(t, "status_change", entry.Action)
	assert.JSONEq(t, `{"status":"PENDING"}`, string(entry.Before))
	assert.JSONEq(t, `{"status":"CHARGING"}`, string(entry.After))
	assert.Nil(t, entry.ActorUserID, "a system-driven transition is attributed to nobody")
}

// RowsAffected == 0 means someone else moved the order first. That is a lost
// race, not a change — auditing it would fill the log with transitions that
// never happened, which is exactly the noise that makes an audit trail useless.
func TestLostTransitionRaceWritesNoAuditRow(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	orderID := anOrder(t, store, "AUDIT-RACE", "race@example.com")

	require.True(t, transition(t, ctx, store, orderID,models.OrderPending, models.OrderCharging))
	require.False(t, transition(t, ctx, store, orderID,models.OrderPending, models.OrderCharging),
		"the second caller must lose the CAS")

	assert.Len(t, auditRows(t, store), 1, "only the winner is recorded")
}

// The actor rides on the context because the audit row is written in the
// repository and the only layer that knows who is calling is the HTTP
// middleware. An admin fulfilling an order must be attributable.
func TestAuditRowCarriesTheActingUser(t *testing.T) {
	store, db := newStore(t)

	admin := seedUser(t, db, "admin-actor@example.com")
	orderID := anOrder(t, store, "AUDIT-ACTOR", "customer-actor@example.com")

	ctx := models.WithActor(context.Background(), admin)
	require.True(t, transition(t, ctx, store, orderID,models.OrderPending, models.OrderCancelled))

	rows := auditRows(t, store)
	require.Len(t, rows, 1)
	require.NotNil(t, rows[0].ActorUserID)
	assert.Equal(t, admin, *rows[0].ActorUserID)
}

// A full lifecycle leaves a readable trail rather than one row per run of the
// worker: every edge appears exactly once, in order.
func TestAuditTrailFollowsTheWholeLifecycle(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	orderID := anOrder(t, store, "AUDIT-TRAIL", "trail@example.com")

	require.True(t, transition(t, ctx, store, orderID,models.OrderPending, models.OrderCharging))
	require.True(t, transition(t, ctx, store, orderID,models.OrderCharging, models.OrderPaid))
	require.True(t, transition(t, ctx, store, orderID,models.OrderPaid, models.OrderFulfilled))

	rows := auditRows(t, store)
	require.Len(t, rows, 3)

	want := [][2]string{
		{"PENDING", "CHARGING"},
		{"CHARGING", "PAID"},
		{"PAID", "FULFILLED"},
	}
	for i, w := range want {
		assert.JSONEq(t, `{"status":"`+w[0]+`"}`, string(rows[i].Before))
		assert.JSONEq(t, `{"status":"`+w[1]+`"}`, string(rows[i].After))
	}
}
