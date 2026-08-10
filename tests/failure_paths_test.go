package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/database"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

// What happens when the database fails part-way through an operation.
//
// The assertion in every case is NOT "an error came back" — that only tests that
// Go propagates errors. It is "nothing was left behind": no half-written order,
// no reserved stock with no order to claim it, no state change without the event
// that announces it.

func countRows(t *testing.T, db *gorm.DB, table string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Raw("SELECT count(*) FROM "+table).Scan(&n).Error)
	return n
}

// Intake writes an order, its items, its reservations and its outbox event in
// ONE transaction. If the inventory update fails, none of it may survive —
// otherwise a customer has an order for stock that was never reserved.
func TestIntakeRollsBackEntirelyWhenInventoryFails(t *testing.T) {
	store, db, faults := newStoreWithFaults(t)
	ctx := context.Background()

	user := seedUser(t, db, "fault-intake@example.com")
	product := mustProduct(t, store, "FAULT-1", 1000, 10)

	rule := faults.FailNext("UPDATE inventory", 4, errors.New("connection reset by peer"))

	_, err := newOrderSvc(store).Create(ctx, oneLineOrder(user, product.ID))
	require.Error(t, err)
	assert.Positive(t, rule.timesFired())

	assert.Zero(t, countRows(t, db, "orders"), "no order may survive a failed reservation")
	assert.Zero(t, countRows(t, db, "order_items"))
	assert.Zero(t, countRows(t, db, "reservations"))
	assert.Zero(t, countRows(t, db, "outbox"), "and no event may announce it")

	inv, err := store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	assert.Equal(t, 10, inv.Available, "stock must be untouched")
	assert.Equal(t, 0, inv.Reserved)
}

// The outbox write is the LAST thing intake does. If it fails, the order must
// not exist either — an order with no event is stranded forever, which is
// exactly the failure mode B the outbox exists to prevent.
func TestIntakeRollsBackWhenTheOutboxWriteFails(t *testing.T) {
	store, db, faults := newStoreWithFaults(t)
	ctx := context.Background()

	user := seedUser(t, db, "fault-outbox@example.com")
	product := mustProduct(t, store, "FAULT-2", 1000, 10)

	faults.FailNext("outbox", 4, errors.New("disk full"))

	_, err := newOrderSvc(store).Create(ctx, oneLineOrder(user, product.ID))
	require.Error(t, err)

	assert.Zero(t, countRows(t, db, "orders"),
		"an order whose event could not be written must not exist")

	inv, err := store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	assert.Equal(t, 10, inv.Available, "and its stock must be released")
	assert.Equal(t, 0, inv.Reserved)
}

// The retry path, end to end, with an error Postgres would really produce.
//
// A deadlock is transient: the same transaction replayed usually succeeds. This
// injects one, and asserts the operation SUCCEEDS anyway — proving InTx retried
// rather than surfacing the error.
func TestDeadlockIsRetriedAndSucceeds(t *testing.T) {
	store, db, faults := newStoreWithFaults(t)
	ctx := context.Background()

	user := seedUser(t, db, "fault-deadlock@example.com")
	product := mustProduct(t, store, "FAULT-3", 1000, 10)

	rule := faults.FailNextRetryable("UPDATE inventory", 1) // fails once, then clears

	order, err := newOrderSvc(store).Create(ctx, oneLineOrder(user, product.ID))
	require.NoError(t, err, "a deadlock must be retried, not surfaced")
	require.NotNil(t, order)

	assert.Equal(t, 1, rule.timesFired())

	// The retry replays the WHOLE transaction, so the result must be one order
	// and one reservation — not two of anything.
	assert.EqualValues(t, 1, countRows(t, db, "orders"))
	assert.EqualValues(t, 1, countRows(t, db, "reservations"))

	inv, err := store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	assert.Equal(t, 9, inv.Available, "stock reserved exactly once despite the retry")
	assert.Equal(t, 1, inv.Reserved)
}

// The other half of the classification: a constraint violation means an
// invariant HELD and we lost a race. Replaying produces the identical result, so
// retrying it is a hang. It must surface immediately.
func TestConstraintViolationIsNotRetried(t *testing.T) {
	store, db, faults := newStoreWithFaults(t)
	ctx := context.Background()

	user := seedUser(t, db, "fault-unique@example.com")
	product := mustProduct(t, store, "FAULT-4", 1000, 10)

	rule := faults.FailNextTerminal("UPDATE inventory", 10)

	_, err := newOrderSvc(store).Create(ctx, oneLineOrder(user, product.ID))
	require.Error(t, err)

	assert.Equal(t, 1, rule.timesFired(),
		"a unique violation must be returned on the first attempt, not retried")
	assert.True(t, database.IsUniqueViolation(err) || errors.Is(err, models.ErrInsufficientStock),
		"the classification must survive the wrapping")

	assert.Zero(t, countRows(t, db, "orders"))
}

// Retries are bounded. A fault that never clears must give up rather than spin,
// and the error must say so.
func TestRetriesAreBounded(t *testing.T) {
	store, db, faults := newStoreWithFaults(t)
	ctx := context.Background()

	user := seedUser(t, db, "fault-bounded@example.com")
	product := mustProduct(t, store, "FAULT-5", 1000, 10)

	rule := faults.FailNextRetryable("UPDATE inventory", 1000) // never clears

	start := time.Now()
	_, err := newOrderSvc(store).Create(ctx, oneLineOrder(user, product.ID))
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "still conflicting",
		"exhausting the budget should say so rather than look like a fresh failure")

	// DeadlockRetries is 3, so 1 initial attempt plus 3 retries.
	assert.Equal(t, 4, rule.timesFired(), "the retry budget must be finite")
	assert.Less(t, elapsed, 30*time.Second, "backoff must be bounded, not unbounded")

	assert.Zero(t, countRows(t, db, "orders"))
}

// The reaper releases stock and marks reservations in one transaction. A failure
// part-way must leave the order claimable rather than half-reaped: stock back
// but the reservation still HELD would be inventing inventory.
func TestReaperRollsBackOnFailure(t *testing.T) {
	store, db, faults := newStoreWithFaults(t)
	ctx := context.Background()

	user := seedUser(t, db, "fault-reaper@example.com")
	product := mustProduct(t, store, "FAULT-6", 1000, 10)

	order, err := newOrderSvc(store).Create(ctx, oneLineOrder(user, product.ID))
	require.NoError(t, err)
	age(t, store, order.ID)

	faults.FailNext("UPDATE inventory", 10, errors.New("connection reset"))

	_, err = newReaper(store).ReapOnce(ctx)
	require.Error(t, err)

	// Nothing moved: the order is still PENDING and the stock is still held.
	assert.Equal(t, models.OrderPending, orderStatusOf(t, store, order.ID))

	inv, err := store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	assert.Equal(t, 9, inv.Available, "a half-reaped order must not release stock")
	assert.Equal(t, 1, inv.Reserved)

	held, err := store.Orders().ReservationsForOrder(ctx, nil, order.ID, models.ReservationHeld)
	require.NoError(t, err)
	assert.Len(t, held, 1, "the reservation stays HELD so the next sweep retries it")

	// And once the fault clears, the reap succeeds.
	faults.disarm()
	reaped, err := newReaper(store).ReapOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, reaped)
	assert.Equal(t, models.OrderExpired, orderStatusOf(t, store, order.ID))
}

// A state change and the event announcing it are written together. If the event
// fails, the transition must roll back too — a PAID order with no order.paid
// event never gets fulfilled or notified.
func TestPaymentSettlementRollsBackWhenTheEventFails(t *testing.T) {
	store, db, faults := newStoreWithFaults(t)
	ctx := context.Background()

	user := seedUser(t, db, "fault-settle@example.com")
	product := mustProduct(t, store, "FAULT-7", 4200, 10)

	order, err := newOrderSvc(store).Create(ctx, oneLineOrder(user, product.ID))
	require.NoError(t, err)

	provider := newScriptedProvider(services.OutcomeSucceeded)
	payments := services.NewPaymentService(store, provider, logger.New("error", false))

	// Let intake's own outbox row through, then fail the settlement event.
	faults.FailNext("outbox", 10, errors.New("disk full"))

	err = payments.ProcessOrder(ctx, order.ID)
	require.Error(t, err)

	// Disarm BEFORE asserting. The matcher is a substring, so a fault armed on
	// "outbox" also matches `SELECT ... FROM outbox` — the verification query
	// would fail instead of the code under test.
	faults.disarm()

	// The order must not be left PAID without its event.
	status := orderStatusOf(t, store, order.ID)
	assert.NotEqual(t, models.OrderPaid, status,
		"a transition whose event could not be written must roll back")

	var paidEvents int64
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM outbox WHERE routing_key = ?`, models.EventOrderPaid).
		Scan(&paidEvents).Error)
	assert.Zero(t, paidEvents)
}

// --- a failure that is genuinely real ---------------------------------------

// Everything above injects a synthetic error: the database never actually
// failed. This kills the backend process running the transaction, from another
// connection, so Postgres itself aborts the work while it holds locks.
//
// That is the difference between testing OUR handling and testing the
// database's behaviour, and it is the only way to see a real rollback.
func TestConnectionKilledMidTransactionRollsBack(t *testing.T) {
	store, _ := newIsolatedStore(t)
	ctx := context.Background()

	product := mustProduct(t, store, "TERMINATED-1", 1000, 10)

	// A second connection to do the killing. It must NOT truncate — that would
	// wipe the product this test just created.
	killer := newAuxStore(t)

	err := store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		// Reserve stock, so the transaction is holding a real row lock.
		if err := store.Inventory().Reserve(ctx, tx, product.ID, 3); err != nil {
			return err
		}

		var pid int
		if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
			return err
		}
		require.Positive(t, pid)

		// Terminate our own backend from outside. Postgres aborts the
		// transaction and drops the locks; no COMMIT is possible.
		var killed bool
		require.NoError(t, killer.DB().
			Raw("SELECT pg_terminate_backend(?)", pid).Scan(&killed).Error)
		require.True(t, killed, "the backend should have been terminated")

		// Anything else on this connection now fails.
		return tx.Exec("SELECT 1").Error
	})

	require.Error(t, err, "a terminated backend cannot commit")

	// The reservation is gone, because it was never committed. Read through the
	// other connection, since ours was destroyed.
	inv, invErr := killer.Inventory().Get(ctx, product.ID)
	require.NoError(t, invErr)
	assert.Equal(t, 10, inv.Available,
		"a transaction killed mid-flight must leave no trace")
	assert.Equal(t, 0, inv.Reserved)
}
