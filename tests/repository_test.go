package tests

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
)

// Repository methods the higher-level suites reach only incidentally.

// "Live" is exactly the set in the partial unique index: INITIATED, UNKNOWN and
// SUCCEEDED. DECLINED is excluded because a declined card followed by a
// different card is a genuinely new intent, so DECLINED, DECLINED, SUCCEEDED is
// a legal history.
func TestFindLiveIntentMatchesTheIndex(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "live@example.com")
	product := mustProduct(t, store, "LIVE-1", 1000, 5)
	order, err := newOrderSvc(store).Create(ctx, oneLineOrder(user, product.ID))
	require.NoError(t, err)

	_, err = store.Payments().FindLive(ctx, nil, order.ID)
	require.ErrorIs(t, err, models.ErrNotFound, "no intent exists yet")

	var paymentID uuid.UUID
	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		p, err := store.Payments().CreateIntent(ctx, tx, order.ID, 1000, "test")
		if err != nil {
			return err
		}
		paymentID = p.ID
		return nil
	}))

	live, err := store.Payments().FindLive(ctx, nil, order.ID)
	require.NoError(t, err)
	assert.Equal(t, paymentID, live.ID)
	assert.Equal(t, models.PaymentInitiated, live.Status)

	// UNKNOWN still blocks: the customer may have been charged.
	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		_, err := store.Payments().SetStatus(ctx, tx, paymentID,
			models.PaymentInitiated, models.PaymentUnknown, nil)
		return err
	}))
	live, err = store.Payments().FindLive(ctx, nil, order.ID)
	require.NoError(t, err)
	assert.Equal(t, models.PaymentUnknown, live.Status)

	// DECLINED does not, so a new card can be tried.
	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		_, err := store.Payments().SetStatus(ctx, tx, paymentID,
			models.PaymentUnknown, models.PaymentDeclined, nil)
		return err
	}))
	_, err = store.Payments().FindLive(ctx, nil, order.ID)
	assert.ErrorIs(t, err, models.ErrNotFound, "a declined intent is not live")

	// ...and a fresh intent is now permitted.
	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		_, err := store.Payments().CreateIntent(ctx, tx, order.ID, 1000, "test")
		return err
	}))
}

func TestGetPaymentByID(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "getpay@example.com")
	product := mustProduct(t, store, "GETPAY-1", 750, 5)
	order, err := newOrderSvc(store).Create(ctx, oneLineOrder(user, product.ID))
	require.NoError(t, err)

	var paymentID uuid.UUID
	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		p, err := store.Payments().CreateIntent(ctx, tx, order.ID, 750, "test")
		if err != nil {
			return err
		}
		paymentID = p.ID
		return nil
	}))

	got, err := store.Payments().GetByID(ctx, paymentID)
	require.NoError(t, err)
	assert.Equal(t, order.ID, got.OrderID)
	assert.EqualValues(t, 750, got.AmountCents)
	assert.Equal(t, paymentID.String(), got.IdempotencyKey(),
		"the payment id IS the provider idempotency key")

	_, err = store.Payments().GetByID(ctx, uuid.New())
	assert.ErrorIs(t, err, models.ErrNotFound)
}

// Audit entries take a tx so they commit atomically with the change they
// describe. An audit log that can disagree with the data is worse than none.
func TestAuditEntriesCommitWithTheirChange(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "audited@example.com")
	before, _ := json.Marshal(map[string]any{"available": 10})
	after, _ := json.Marshal(map[string]any{"available": 3})

	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		return store.Audit().Record(ctx, tx, &models.AuditLog{
			ActorUserID: &user,
			EntityType:  "inventory",
			EntityID:    "1",
			Action:      "restock",
			Before:      before,
			After:       after,
		})
	}))

	var count int64
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM audit_logs WHERE entity_type = 'inventory' AND action = 'restock'`).
		Scan(&count).Error)
	assert.EqualValues(t, 1, count)

	// A rolled-back change must take its audit entry with it.
	err := store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		if err := store.Audit().Record(ctx, tx, &models.AuditLog{
			EntityType: "inventory", EntityID: "2", Action: "rolled-back",
		}); err != nil {
			return err
		}
		return assertRollback
	})
	require.ErrorIs(t, err, assertRollback)

	require.NoError(t, db.Raw(
		`SELECT count(*) FROM audit_logs WHERE action = 'rolled-back'`).Scan(&count).Error)
	assert.Zero(t, count, "an audit entry must not survive the transaction it described")
}

// IsDuplicate is what lets callers treat the notification unique index as an
// ordinary outcome rather than an exception.
func TestIsDuplicateRecognisesTheUniqueIndex(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "dupe-detect@example.com")

	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		_, _, err := store.Notifications().Ensure(ctx, tx, orderID, "email", "confirmation")
		return err
	}))

	// A raw insert bypassing ON CONFLICT must be rejected by the index.
	err := db.Exec(`
		INSERT INTO notifications (order_id, channel, kind, status)
		VALUES (?, 'email', 'confirmation', 'UNCLAIMED')`, orderID).Error

	require.Error(t, err)
	assert.True(t, repository.IsDuplicate(err),
		"the send-once invariant must be recognisable as a duplicate, not a mystery")
}

// Ensure is idempotent: the second call returns the existing row rather than
// creating a second one.
func TestEnsureReportsWhetherItCreated(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "ensure@example.com")

	var firstID, secondID uint64
	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		n, created, err := store.Notifications().Ensure(ctx, tx, orderID, "email", "confirmation")
		require.True(t, created, "the first call creates the row")
		firstID = n.ID
		return err
	}))

	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		n, created, err := store.Notifications().Ensure(ctx, tx, orderID, "email", "confirmation")
		assert.False(t, created, "the second call finds the existing row")
		secondID = n.ID
		return err
	}))

	assert.Equal(t, firstID, secondID)
}

// assertRollback forces a transaction to roll back so the audit assertion can
// check that entries do not outlive the change they describe.
var assertRollback = errors.New("deliberate rollback")

// oneLineOrder is the smallest valid order: one unit of one product.
func oneLineOrder(userID, productID uint64) services.CreateOrderInput {
	return services.CreateOrderInput{
		UserID: userID,
		Lines:  []services.OrderLine{{ProductID: productID, Qty: 1}},
	}
}
